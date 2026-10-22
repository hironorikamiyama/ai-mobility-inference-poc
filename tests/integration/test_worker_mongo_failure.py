import os
import sys
import time
import uuid

from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError


STREAM = "inference_jobs"
GROUP = "inference_workers"


def test_mongodb_failure_keeps_message_pending(worker_processes):
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
        os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        serverSelectionTimeoutMS=3000,
    )
    collection = mongo_client["ai_mobility"]["inference_results"]

    worker = None
    message_id = None

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

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

        # MongoDB保存だけを失敗させるWorkerを起動
        worker = worker_processes(
            consumer=consumer,
            mongo_uri=(
                "mongodb://127.0.0.1:1/"
                "?directConnection=true"
            ),
            # 保存エラー発生後の即時再回収を防ぐ
            reclaim_idle_ms=30000,
        )

        worker.wait_until_started()

        # Workerへジョブを投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # MongoDB接続失敗ログを待つ
        deadline = time.monotonic() + 35

        while time.monotonic() < deadline:
            if worker.process.poll() is not None:
                raise RuntimeError(
                    "Worker exited unexpectedly:\n"
                    + worker.read_log()
                )

            logs = worker.read_log()

            if (
                "Processing failed:" in logs
                and f"stream_id={message_id}" in logs
            ):
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "MongoDB save failure was not observed"
            )

        # WorkerがACKしなかったことを確認
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

        # 正常なMongoDBには結果が存在しない
        assert collection.find_one({"_id": job_id}) is None

        print("MongoDB failure confirmed", flush=True)
        print(
            "Redis Pending retained; ACK not executed",
            flush=True,
        )

    finally:
        # データ削除前にWorkerを停止
        if worker is not None:
            worker.stop()

        if sys.exc_info()[0] is not None and worker is not None:
            print(
                "\n=== Worker log ===\n"
                + worker.read_log(),
                file=sys.stderr,
            )

        # テスト用メッセージのみ削除
        if message_id is not None:
            redis_client.xack(STREAM, GROUP, message_id)
            redis_client.xdel(STREAM, message_id)

        collection.delete_one({"_id": job_id})
        mongo_client.close()
        redis_client.close()

        # ログファイルのcloseはfixtureが担当
