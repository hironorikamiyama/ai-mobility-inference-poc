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


def read_log(log_file):
    log_file.flush()
    log_file.seek(0)
    return log_file.read()


def stop_worker(process):
    if process is None or process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def start_worker(consumer, mongo_uri, log_file):
    env = os.environ.copy()
    env.update({
        "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
        "MONGO_URI": mongo_uri,
        "CONSUMER_NAME": consumer,
        "READ_BATCH_SIZE": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "PYTHONUNBUFFERED": "1",
        "WORKER_PROCESSING_DELAY_SECONDS": "0",
    })

    return subprocess.Popen(
        [sys.executable, "-m", "src.worker"],
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )


def wait_until_started(process, log_file, timeout=30):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Worker exited during startup")

        if "Worker started:" in read_log(log_file):
            return

        time.sleep(0.2)

    raise TimeoutError("Worker startup timed out")


def test_mongodb_recovery_reprocesses_pending():
    suffix = uuid.uuid4().hex
    job_id = f"ci-mongo-recovery-{suffix}"
    consumer_a = f"ci-failure-{suffix}"
    consumer_b = f"ci-recovery-{suffix}"

    redis_client = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True,
        socket_connect_timeout=3,
    )

    healthy_mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017/",
    )
    mongo_client = MongoClient(
        healthy_mongo_uri,
        serverSelectionTimeoutMS=3000,
    )
    collection = mongo_client["ai_mobility"]["inference_results"]

    worker_a = None
    worker_b = None
    message_id = None

    log_a = tempfile.TemporaryFile(mode="w+t")
    log_b = tempfile.TemporaryFile(mode="w+t")

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        try:
            redis_client.xgroup_create(
                STREAM, GROUP, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        # 1. Worker Aを無効なMongoDB接続先で起動
        worker_a = start_worker(
            consumer=consumer_a,
            mongo_uri=(
                "mongodb://127.0.0.1:1/"
                "?directConnection=true"
            ),
            log_file=log_a,
        )
        wait_until_started(worker_a, log_a)

        # 2. Worker Aに処理させるジョブを投入
        message_id = redis_client.xadd(
            STREAM,
            {"job_id": job_id, "input_value": "0.5"},
        )

        # 3. MongoDB保存失敗を確認
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            if worker_a.poll() is not None:
                raise RuntimeError(
                    "Worker A exited unexpectedly"
                )

            logs = read_log(log_a)

            if (
                "Processing failed:" in logs
                and f"stream_id={message_id}" in logs
            ):
                break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "MongoDB failure was not observed"
            )

        pending = redis_client.xpending_range(
            STREAM, GROUP,
            min=message_id,
            max=message_id,
            count=1,
        )
        assert len(pending) == 1
        assert pending[0]["consumer"] == consumer_a
        assert pending[0]["times_delivered"] == 1

        assert collection.find_one({"_id": job_id}) is None

        print("MongoDB failure confirmed", flush=True)
        print("Message remains Pending", flush=True)

        # 4. 障害Workerを停止
        stop_worker(worker_a)
        assert worker_a.poll() is not None

        # 5. Worker Bを正常なMongoDB接続先で起動
        worker_b = start_worker(
            consumer=consumer_b,
            mongo_uri=healthy_mongo_uri,
            log_file=log_b,
        )
        wait_until_started(worker_b, log_b)

        # 6. 30秒の再回収条件を含めて処理完了を待つ
        deadline = time.monotonic() + 80

        while time.monotonic() < deadline:
            if worker_b.poll() is not None:
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

            recovered_by_b = (
                f"Processed: job_id={job_id}"
                in read_log(log_b)
            )

            if (
                document is not None
                and not pending
                and recovered_by_b
            ):
                assert abs(document["score"] - 0.5) < 1e-5
                assert document["device"] == "cpu"

                assert collection.count_documents(
                    {"_id": job_id}
                ) == 1

                print(
                    "MongoDB recovery succeeded",
                    flush=True,
                )
                print(
                    "Worker B processed and ACKed the job",
                    flush=True,
                )
                break

            time.sleep(0.5)
        else:
            raise TimeoutError(
                "Pending reprocessing or ACK timed out"
            )

    finally:
        # 後片付け前に両Workerを停止
        stop_worker(worker_a)
        stop_worker(worker_b)

        if sys.exc_info()[0] is not None:
            print(
                "\n=== Worker A log ===\n"
                + read_log(log_a),
                file=sys.stderr,
            )
            print(
                "\n=== Worker B log ===\n"
                + read_log(log_b),
                file=sys.stderr,
            )

        if message_id is not None:
            redis_client.xack(STREAM, GROUP, message_id)
            redis_client.xdel(STREAM, message_id)

        collection.delete_one({"_id": job_id})

        mongo_client.close()
        redis_client.close()
        log_a.close()
        log_b.close()
