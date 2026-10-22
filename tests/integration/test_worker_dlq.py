import os
import sys
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


def test_retry_exhaustion_moves_job_to_dlq(worker_processes):
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
    dlq_message_id = None

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

        # 1. Redis Streamsへジョブを投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # 2. 初期Consumerが取得してPendingにする
        messages = redis_client.xreadgroup(
            GROUP,
            initial_consumer,
            {STREAM: ">"},
            count=1,
            block=2000,
        )

        assert messages
        assert messages[0][1][0][0] == message_id

        # 3. XCLAIMで再試行上限直前の状態を再現
        # WorkerのXAUTOCLAIMで配信回数を上限に到達させる
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

        # 4. 共通fixtureで実際のWorkerを起動
        worker = worker_processes(
            consumer=worker_consumer,
            mongo_uri=mongo_uri,
            # DLQ検証では事前にIDLEを設定済み
            reclaim_idle_ms=30000,
        )

        worker.wait_until_started()

        # 5. DLQ記録と元メッセージACKを確認
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            if worker.process.poll() is not None:
                raise RuntimeError(
                    "Worker exited unexpectedly:\n"
                    + worker.read_log()
                )

            logs = worker.read_log()

            dlq_entries = redis_client.xrange(DLQ)

            matches = [
                (entry_id, fields)
                for entry_id, fields in dlq_entries
                if fields.get("original_stream_id") == message_id
            ]

            pending = get_pending(redis_client, message_id)

            if (
                matches
                and not pending
                and f"Retry exhausted: stream_id={message_id}" in logs
            ):
                assert len(matches) == 1

                dlq_message_id, dlq_record = matches[0]

                assert dlq_record
                assert collection.find_one({"_id": job_id}) is None

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

        # 7. MongoDBに推論結果が存在しない
        assert collection.count_documents({"_id": job_id}) == 0

    finally:
        # Redisの後片付けより先にWorkerを停止
        if worker is not None:
            worker.stop()

        if sys.exc_info()[0] is not None and worker is not None:
            print(
                "\n=== Worker log ===\n"
                + worker.read_log(),
                file=sys.stderr,
            )

        # テストが作成したメッセージのみ削除
        if message_id is not None:
            redis_client.xack(STREAM, GROUP, message_id)
            redis_client.xdel(STREAM, message_id)

        if dlq_message_id is not None:
            redis_client.xdel(DLQ, dlq_message_id)

        collection.delete_one({"_id": job_id})

        mongo_client.close()
        redis_client.close()

        # ログファイルのcloseはfixtureが担当
