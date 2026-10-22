import os
import subprocess
import sys
import tempfile
import time
import uuid

from pathlib import Path

from pymongo import MongoClient
from redis import Redis


STREAM = "inference_jobs"
GROUP = "inference_workers"


def test_worker_end_to_end():
    job_id = f"ci-e2e-{uuid.uuid4().hex}"

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
    worker_log = tempfile.TemporaryFile(mode="w+t")

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        env = os.environ.copy()
        env["REDIS_HOST"] = "localhost"
        env["MONGO_URI"] = os.getenv(
            "MONGO_URI", "mongodb://localhost:27017/"
        )
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

        # Workerの初期化とConsumer Group作成を待機
        deadline = time.monotonic() + 60

        while time.monotonic() < deadline:
            if worker_process.poll() is not None:
                raise RuntimeError(
                    "Worker exited during startup"
                )

            try:
                groups = redis_client.xinfo_groups(STREAM)
                if any(g["name"] == GROUP for g in groups):
                    break
            except Exception:
                pass

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "Worker Consumer Group startup timed out"
            )

        # 実際のWorkerが処理するジョブを投入
        redis_client.xadd(
            STREAM,
            {
                "job_id": job_id,
                "input_value": "0.5",
            },
        )

        # MongoDB保存とACK完了まで待機
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            if worker_process.poll() is not None:
                raise RuntimeError(
                    "Worker exited during processing"
                )

            document = collection.find_one({"_id": job_id})

            if document is not None:
                assert abs(document["score"] - 0.5) < 1e-5
                assert document["device"] == "cpu"

                pending = redis_client.xpending(STREAM, GROUP)
                if pending["pending"] == 0:
                    break

            time.sleep(0.2)
        else:
            raise TimeoutError(
                "MongoDB save or Redis ACK timed out"
            )

        assert collection.count_documents(
            {"_id": job_id}
        ) == 1

    except Exception:
        worker_log.flush()
        worker_log.seek(0)
        print(
            "\n=== Worker log ===\n"
            + worker_log.read(),
            file=sys.stderr,
        )
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

        collection.delete_one({"_id": job_id})
        mongo_client.close()
        redis_client.close()
        worker_log.close()
