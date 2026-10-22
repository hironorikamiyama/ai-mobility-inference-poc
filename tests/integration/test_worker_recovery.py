import os
import sys
import time
import uuid

from pymongo import MongoClient
from redis import Redis
from redis.exceptions import ResponseError


STREAM = "inference_jobs"
GROUP = "inference_workers"


def test_worker_reclaims_pending_message(worker_processes):
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

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017/",
    )

    mongo_client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=3000,
    )

    collection = mongo_client["ai_mobility"]["inference_results"]

    worker_b = None
    message_id = None

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

        # 1. Redis Streamsへジョブ投入
        message_id = redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # 2. Consumer Aが取得し、ACKせずPendingに残す
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

        pending = redis_client.xpending_range(
            STREAM,
            GROUP,
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

        # 3. 共通ヘルパーでWorker Bを起動
        # CI専用の短縮設定（通常Workerの既定値は30秒）
        worker_b = worker_processes(
            consumer=consumer_b,
            mongo_uri=mongo_uri,
            reclaim_idle_ms=1000,
        )

        worker_b.wait_until_started()

        # 4. Pending再回収・MongoDB保存・ACKを確認
        deadline = time.monotonic() + 20

        while time.monotonic() < deadline:
            if worker_b.process.poll() is not None:
                raise RuntimeError(
                    "Worker B exited unexpectedly"
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

                logs = worker_b.read_log()
                assert f"Processed: job_id={job_id}" in logs

                print(
                    f"Recovery succeeded: job_id={job_id}",
                    flush=True,
                )
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "Pending recovery or ACK timed out"
            )

    finally:
        # データ削除より先にWorkerを停止
        if worker_b is not None:
            worker_b.stop()

        if sys.exc_info()[0] is not None and worker_b is not None:
            print(
                "\n=== Worker B log ===\n"
                + worker_b.read_log(),
                file=sys.stderr,
            )

        # テストが作成したメッセージのみ後片付け
        if message_id is not None:
            redis_client.xack(STREAM, GROUP, message_id)
            redis_client.xdel(STREAM, message_id)

        collection.delete_one({"_id": job_id})

        mongo_client.close()
        redis_client.close()

        # Workerのログファイルはfixture側で閉じる
