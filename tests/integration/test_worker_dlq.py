import os
import subprocess
import sys
import tempfile
import time
import uuid

from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError

from src.retry_policy import should_dead_letter


STREAM = "inference_jobs"
GROUP = "inference_workers"
DLQ = "inference_jobs_dead"


def get_pending(redis_client, message_id):
    return redis_client.xpending_range(
        STREAM,
        GROUP,
        min=message_id,
        max=message_id,
        count=1,
    )


def stop_worker(process):
    if process is None or process.poll() is not None:
        return

    process.terminate()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def test_retry_exhaustion_moves_job_to_dlq():
    suffix = uuid.uuid4().hex
    job_id = f"ci-dlq-{suffix}"
    initial_consumer = f"ci-dlq-initial-{suffix}"
    worker_consumer = f"ci-dlq-worker-{suffix}"

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
    dlq_message_id = None
    worker_log = tempfile.TemporaryFile(mode="w+t")

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        # 再試行ポリシーからDLQ移行回数を取得
        thresholds = [
            count
            for count in range(2, 21)
            if should_dead_letter(count)
            and not should_dead_letter(count - 1)
        ]

        assert len(thresholds) == 1, (
            "Unexpected retry policy threshold"
        )

        threshold = thresholds[0]

        print(
            f"DLQ threshold: delivery_count={threshold}",
            flush=True,
        )

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

        # 1. ジョブ投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # 2. Consumerが受信してPendingにする
        messages = redis_client.xreadgroup(
            GROUP,
            initial_consumer,
            {STREAM: ">"},
            count=1,
            block=2000,
        )

        assert messages
        assert messages[0][1][0][0] == message_id

        # 3. 再試行上限の直前をRedisで再現
        # XCLAIMのRETRYCOUNTで配信回数を設定し、
        # IDLEで30秒以上経過した状態にする。
        claimed = redis_client.execute_command(
            "XCLAIM",
            STREAM,
            GROUP,
            initial_consumer,
            0,
            message_id,
            "IDLE",
            31_000,
            "RETRYCOUNT",
            threshold - 1,
        )

        assert claimed

        pending = get_pending(redis_client, message_id)

        assert len(pending) == 1
        assert pending[0]["times_delivered"] == threshold - 1

        # 4. 実際のWorkerを起動
        env = os.environ.copy()
        env.update({
            "REDIS_HOST": os.getenv(
                "REDIS_HOST", "localhost"
            ),
            "MONGO_URI": os.getenv(
                "MONGO_URI",
                "mongodb://localhost:27017/",
            ),
            "CONSUMER_NAME": worker_consumer,
            "READ_BATCH_SIZE": "1",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "PYTHONUNBUFFERED": "1",
            "WORKER_PROCESSING_DELAY_SECONDS": "0",
        })

        worker_process = subprocess.Popen(
            [sys.executable, "-m", "src.worker"],
            env=env,
            stdout=worker_log,
            stderr=subprocess.STDOUT,
        )

        # 5. Workerが再回収してDLQに隔離するまで待つ
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            if worker_process.poll() is not None:
                raise RuntimeError(
                    "Worker exited unexpectedly"
                )

            worker_log.flush()
            worker_log.seek(0)
            logs = worker_log.read()

            dlq_entries = redis_client.xrange(DLQ)

            matches = [
                (entry_id, fields)
                for entry_id, fields in dlq_entries
                if message_id in fields.values()
            ]

            pending = get_pending(redis_client, message_id)

            if (
                matches
                and not pending
                and f"Retry exhausted: stream_id={message_id}"
                in logs
            ):
                assert len(matches) == 1

                dlq_message_id, dlq_record = matches[0]

                assert dlq_record
                assert collection.find_one(
                    {"_id": job_id}
                ) is None

                print(
                    "DLQ isolation succeeded",
                    flush=True,
                )
                print(
                    "Original message ACK confirmed",
                    flush=True,
                )
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "DLQ isolation or ACK timed out"
            )

        # 6. 元ジョブはPendingから消えている
        assert not get_pending(redis_client, message_id)

        # 7. MongoDBには推論結果が存在しない
        assert collection.count_documents(
            {"_id": job_id}
        ) == 0

    finally:
        stop_worker(worker_process)

        worker_log.flush()
        worker_log.seek(0)

        if sys.exc_info()[0] is not None:
            print(
                "\n=== Worker log ===\n"
                + worker_log.read(),
                file=sys.stderr,
            )

        # テストで作成したデータのみ削除
        if message_id is not None:
            redis_client.xack(
                STREAM, GROUP, message_id
            )
            redis_client.xdel(
                STREAM, message_id
            )

        if dlq_message_id is not None:
            redis_client.xdel(
                DLQ, dlq_message_id
            )

        collection.delete_one({"_id": job_id})

        mongo_client.close()
        redis_client.close()
        worker_log.close()
