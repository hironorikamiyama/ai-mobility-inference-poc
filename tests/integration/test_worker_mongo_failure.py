import os
import subprocess
import sys
import tempfile
import time
import uuid

from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError


STREAM = "inference_jobs"
GROUP = "inference_workers"


def test_mongodb_failure_keeps_message_pending():
    suffix = uuid.uuid4().hex
    job_id = f"ci-mongo-failure-{suffix}"
    consumer = f"ci-mongo-failure-worker-{suffix}"

    redis_client = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True,
        socket_connect_timeout=3,
    )

    mongo_client = MongoClient(
        os.getenv(
            "MONGO_URI",
            "mongodb://localhost:27017/",
        ),
        serverSelectionTimeoutMS=3000,
    )

    collection = mongo_client["ai_mobility"]["inference_results"]

    worker_process = None
    message_id = None
    worker_log = tempfile.TemporaryFile(mode="w+t")

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        # Consumer Groupを準備
        try:
            redis_client.xgroup_create(
                STREAM,
                GROUP,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        # Workerに渡すMongoDB接続先だけを無効化
        env = os.environ.copy()
        env["REDIS_HOST"] = os.getenv(
            "REDIS_HOST", "localhost"
        )
        env["MONGO_URI"] = (
            "mongodb://127.0.0.1:1/"
            "?directConnection=true"
        )
        env["CONSUMER_NAME"] = consumer
        env["READ_BATCH_SIZE"] = "1"
        env["OMP_NUM_THREADS"] = "1"
        env["MKL_NUM_THREADS"] = "1"
        env["PYTHONUNBUFFERED"] = "1"

        worker_process = subprocess.Popen(
            [sys.executable, "-m", "src.worker"],
            env=env,
            stdout=worker_log,
            stderr=subprocess.STDOUT,
        )

        # Consumer Groupが存在することを確認
        deadline = time.monotonic() + 30

        while time.monotonic() < deadline:
            if worker_process.poll() is not None:
                raise RuntimeError(
                    "Worker exited during startup"
                )

            groups = redis_client.xinfo_groups(STREAM)
            if any(group["name"] == GROUP for group in groups):
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "Consumer Group startup timed out"
            )

        # MongoDB保存に失敗させるジョブを投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        print(
            f"Job submitted: {job_id}",
            flush=True,
        )

        # 保存失敗のログが出るまで待機
        deadline = time.monotonic() + 35
        error_observed = False

        while time.monotonic() < deadline:
            if worker_process.poll() is not None:
                raise RuntimeError(
                    "Worker exited unexpectedly"
                )

            worker_log.flush()
            worker_log.seek(0)
            logs = worker_log.read()

            if (
                "Processing failed:" in logs
                and f"stream_id={message_id}" in logs
            ):
                error_observed = True
                break

            time.sleep(0.2)

        assert error_observed, (
            "MongoDB save failure was not observed"
        )

        # 未ACKの対象ジョブがPendingに残っている
        pending = redis_client.xpending_range(
            STREAM,
            GROUP,
            min=message_id,
            max=message_id,
            count=1,
        )

        assert len(pending) == 1
        assert pending[0]["message_id"] == message_id
        assert pending[0]["consumer"] == consumer

        # 正常なMongoDBにも対象データが存在しない
        assert collection.find_one(
            {"_id": job_id}
        ) is None

        print(
            "MongoDB failure confirmed",
            flush=True,
        )
        print(
            "Redis Pending retained; ACK not executed",
            flush=True,
        )

    finally:
        # 最初にWorkerを停止して再処理を防ぐ
        if worker_process is not None:
            if worker_process.poll() is None:
                worker_process.terminate()
                try:
                    worker_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    worker_process.kill()
                    worker_process.wait(timeout=5)

        worker_log.flush()
        worker_log.seek(0)
        logs = worker_log.read()

        if sys.exc_info()[0] is not None:
            print(
                "\n=== Worker log ===\n" + logs,
                file=sys.stderr,
            )

        # CIテストで作成したPendingのみ後片付け
        if message_id is not None:
            redis_client.xack(
                STREAM, GROUP, message_id
            )
            redis_client.xdel(
                STREAM, message_id
            )

        collection.delete_one({"_id": job_id})

        mongo_client.close()
        redis_client.close()
        worker_log.close()
