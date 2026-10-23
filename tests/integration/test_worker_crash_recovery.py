import os
import sys
import time
import uuid

from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError


STREAM = "inference_jobs"
GROUP = "inference_workers"


def get_pending(client, message_id):
    return client.xpending_range(
        STREAM,
        GROUP,
        min=message_id,
        max=message_id,
        count=1,
    )


def test_worker_crash_recovery(worker_processes):
    suffix = uuid.uuid4().hex

    job_id = f"ci-crash-{suffix}"
    consumer_a = f"ci-crash-a-{suffix}"
    consumer_b = f"ci-crash-b-{suffix}"

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

    worker_a = None
    worker_b = None
    message_id = None

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        # Consumer Group準備
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

        # 1. Worker Aを起動
        # Redis取得後、MongoDB保存前の遅延を想定
        worker_a = worker_processes(
            consumer=consumer_a,
            mongo_uri=mongo_uri,
            reclaim_idle_ms=30000,
            processing_delay_seconds=0,
            pause_before_save_seconds=10,
        )

        worker_a.wait_until_started()

        # 2. ジョブ投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        print(
            f"Submitted: job_id={job_id}, "
            f"message_id={message_id}",
            flush=True,
        )

         # 3. 推論完了・MongoDB保存直前への到達を確認
        marker = (
            f"Before MongoDB save: job_id={job_id}, "
            f"stream_id={message_id}"
        )

        deadline = time.monotonic() + 15

        while time.monotonic() < deadline:
            if worker_a.process.poll() is not None:
                raise RuntimeError(
                    "Worker A exited unexpectedly:\n"
                    + worker_a.read_log()
                )

            if marker in worker_a.read_log():
                break

            time.sleep(0.1)
        else:
            raise TimeoutError(
                "Worker A did not reach pre-save checkpoint"
            )

        pending = get_pending(redis_client, message_id)

        assert len(pending) == 1
        assert pending[0]["consumer"] == consumer_a
        assert pending[0]["times_delivered"] == 1

        assert collection.find_one({"_id": job_id}) is None

        # 4. MongoDB保存直前でWorker Aを強制終了
        assert worker_a.process.poll() is None

        worker_a.process.kill()
        worker_a.process.wait(timeout=5)

        assert worker_a.process.poll() is not None

        print(
            "Worker A killed at pre-save checkpoint",
            flush=True,
        )

        # 強制終了後もMongoDB未保存・Redis Pending保持
        assert collection.find_one({"_id": job_id}) is None

        pending = get_pending(redis_client, message_id)
        assert len(pending) == 1
        assert pending[0]["consumer"] == consumer_a

        # 5. 終了後もPendingが残っている
        pending = get_pending(
            redis_client,
            message_id,
        )

        assert len(pending) == 1
        assert pending[0]["consumer"] == consumer_a
        assert pending[0]["times_delivered"] == 1

        assert collection.find_one({"_id": job_id}) is None

        # 6. Worker BでPendingを再回収
        worker_b = worker_processes(
            consumer=consumer_b,
            mongo_uri=mongo_uri,
            reclaim_idle_ms=1000,
            processing_delay_seconds=0,
        )

        worker_b.wait_until_started()

        deadline = time.monotonic() + 25

        while time.monotonic() < deadline:
            if worker_b.process.poll() is not None:
                raise RuntimeError(
                    "Worker B exited unexpectedly:\n"
                    + worker_b.read_log()
                )

            document = collection.find_one(
                {"_id": job_id}
            )

            pending = get_pending(
                redis_client,
                message_id,
            )

            processed = (
                f"Processed: job_id={job_id}"
                in worker_b.read_log()
            )

            if document is not None and not pending and processed:
                assert abs(document["score"] - 0.5) < 1e-5
                assert document["device"] == "cpu"

                assert collection.count_documents(
                    {"_id": job_id}
                ) == 1

                print(
                    "Worker B recovery succeeded",
                    flush=True,
                )
                print(
                    "MongoDB save and Redis ACK confirmed",
                    flush=True,
                )
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "Worker B recovery or ACK timed out"
            )

    finally:
        # 先にすべてのWorkerを停止
        if worker_a is not None:
            worker_a.stop()

        if worker_b is not None:
            worker_b.stop()

        if sys.exc_info()[0] is not None:
            if worker_a is not None:
                print(
                    "\n=== Worker A log ===\n"
                    + worker_a.read_log(),
                    file=sys.stderr,
                )

            if worker_b is not None:
                print(
                    "\n=== Worker B log ===\n"
                    + worker_b.read_log(),
                    file=sys.stderr,
                )

        # テスト用データのみ削除
        if message_id is not None:
            redis_client.xack(
                STREAM,
                GROUP,
                message_id,
            )
            redis_client.xdel(
                STREAM,
                message_id,
            )

        collection.delete_one({"_id": job_id})

        mongo_client.close()
        redis_client.close()
