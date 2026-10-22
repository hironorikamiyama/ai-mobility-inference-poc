import os
import sys
import time
import uuid

from pymongo import MongoClient
from redis import Redis


STREAM = "inference_jobs"
GROUP = "inference_workers"


def test_worker_end_to_end(worker_processes):
    suffix = uuid.uuid4().hex
    job_id = f"ci-e2e-{suffix}"
    consumer = f"ci-e2e-worker-{suffix}"

    redis_client = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True,
        socket_connect_timeout=3,
    )

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017/",
    )

    mongo_client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=3000,
    )
    collection = mongo_client["ai_mobility"]["inference_results"]

    worker = None
    message_id = None

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        # 共通fixtureを使用して実際のWorkerを起動
        worker = worker_processes(
            consumer=consumer,
            mongo_uri=mongo_uri,
            reclaim_idle_ms=30000,
        )

        worker.wait_until_started()

        # Redis Streamsへジョブを投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # MongoDB保存・Redis ACKの完了を待機
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            if worker.process.poll() is not None:
                raise RuntimeError(
                    "Worker exited during processing:\n"
                    + worker.read_log()
                )

            document = collection.find_one({"_id": job_id})

            pending = redis_client.xpending_range(
                STREAM,
                GROUP,
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

                logs = worker.read_log()
                assert f"Processed: job_id={job_id}" in logs

                print(
                    f"Worker E2E succeeded: job_id={job_id}",
                    flush=True,
                )
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "MongoDB save or Redis ACK timed out"
            )

    finally:
        # Redis/MongoDBのテストデータ削除前にWorkerを停止
        if worker is not None:
            worker.stop()

        if sys.exc_info()[0] is not None and worker is not None:
            print(
                "\n=== Worker log ===\n"
                + worker.read_log(),
                file=sys.stderr,
            )

        if message_id is not None:
            redis_client.xdel(STREAM, message_id)

        collection.delete_one({"_id": job_id})
        mongo_client.close()
        redis_client.close()

        # ログファイルはfixtureが閉じる
