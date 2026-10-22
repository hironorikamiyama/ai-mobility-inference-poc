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


def test_worker_reclaims_pending_message():
    suffix = uuid.uuid4().hex
    job_id = f"ci-recovery-{suffix}"
    consumer_a = f"ci-worker-a-{suffix}"
    consumer_b = f"ci-worker-b-{suffix}"

    redis_client = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True,
        socket_connect_timeout=3,
    )

    mongo_client = MongoClient(
        os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
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
                STREAM, GROUP, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        # ジョブを投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # Worker A相当のConsumerが受信する
        messages = redis_client.xreadgroup(
            GROUP,
            consumer_a,
            {STREAM: ">"},
            count=1,
            block=2000,
        )

        assert messages
        received_id, fields = messages[0][1][0]

        assert received_id == message_id
        assert fields["job_id"] == job_id

        # ACKしないことでPendingを再現
        pending = redis_client.xpending_range(
            STREAM, GROUP,
            min=message_id,
            max=message_id,
            count=1,
        )

        assert len(pending) == 1
        assert pending[0]["consumer"] == consumer_a

        assert collection.find_one({"_id": job_id}) is None

        print(
            f"Pending created: message_id={message_id}",
            flush=True,
        )

        # Worker Bを実際に起動
        env = os.environ.copy()
        env["REDIS_HOST"] = os.getenv(
            "REDIS_HOST", "localhost"
        )
        env["MONGO_URI"] = os.getenv(
            "MONGO_URI", "mongodb://localhost:27017/"
        )
        env["CONSUMER_NAME"] = consumer_b
        env["READ_BATCH_SIZE"] = "1"
        env["OMP_NUM_THREADS"] = "1"
        env["MKL_NUM_THREADS"] = "1"
        env["PYTHONUNBUFFERED"] = "1"
        env["RECLAIM_IDLE_MS"] = "1000"

        worker_process = subprocess.Popen(
            [sys.executable, "-m", "src.worker"],
            env=env,
            stdout=worker_log,
            stderr=subprocess.STDOUT,
        )

        # 30秒のアイドル期間を含めて再回収を待機
        deadline = time.monotonic() + 20

        while time.monotonic() < deadline:
            if worker_process.poll() is not None:
                raise RuntimeError(
                    "Worker B exited unexpectedly"
                )

            document = collection.find_one({"_id": job_id})

            pending = redis_client.xpending_range(
                STREAM, GROUP,
                min=message_id,
                max=message_id,
                count=1,
            )

            if document is not None and not pending:
                assert abs(document["score"] - 0.5) < 1e-5
                assert document["device"] == "cpu"
                assert collection.count_documents(
                    {"_id": job_id}
                ) == 1

                print(
                    f"Recovery succeeded: job_id={job_id}",
                    flush=True,
                )
                break

            time.sleep(0.5)
        else:
            raise TimeoutError(
                "Pending recovery or ACK timed out"
            )

    except Exception:
        # Workerログを停止後に出力できるようにする
        raise

    finally:
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
                "\n=== Worker B log ===\n" + logs,
                file=sys.stderr,
            )

        if message_id is not None:
            redis_client.xdel(STREAM, message_id)

        collection.delete_one({"_id": job_id})
        mongo_client.close()
        redis_client.close()
        worker_log.close()
