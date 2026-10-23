import os
import subprocess
import sys
import tempfile
import time

import pytest


class WorkerProcess:
    def __init__(
        self,
        consumer,
        mongo_uri,
        reclaim_idle_ms=30000,
        processing_delay_seconds=0,
    ):
        self.consumer = consumer
        self.log = tempfile.TemporaryFile(mode="w+t")

        env = os.environ.copy()
        env.update({
            "REDIS_HOST": os.getenv(
                "REDIS_HOST", "localhost"
            ),
            "MONGO_URI": mongo_uri,
            "CONSUMER_NAME": consumer,
            "READ_BATCH_SIZE": "1",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "PYTHONUNBUFFERED": "1",
            "WORKER_PROCESSING_DELAY_SECONDS": str(processing_delay_seconds),
            "RECLAIM_IDLE_MS": str(reclaim_idle_ms),
        })

        try:
            self.process = subprocess.Popen(
                [sys.executable, "-m", "src.worker"],
                env=env,
                stdout=self.log,
                stderr=subprocess.STDOUT,
            )
        except Exception:
            self.log.close()
            raise

    def read_log(self):
        self.log.flush()
        self.log.seek(0)
        return self.log.read()

    def wait_until_started(self, timeout=30):
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    "Worker exited during startup:\n"
                    + self.read_log()
                )

            if "Worker started:" in self.read_log():
                return

            time.sleep(0.2)

        raise TimeoutError(
            "Worker startup timed out:\n"
            + self.read_log()
        )

    def stop(self):
        if self.process.poll() is not None:
            return

        self.process.terminate()

        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    def close(self):
        try:
            self.stop()
        finally:
            self.log.close()


@pytest.fixture
def worker_processes():
    workers = []

    def start(
        consumer,
        mongo_uri,
        reclaim_idle_ms=30000,
        processing_delay_seconds=0,
    ):
        worker = WorkerProcess(
            consumer=consumer,
            mongo_uri=mongo_uri,
            reclaim_idle_ms=reclaim_idle_ms,
            processing_delay_seconds=processing_delay_seconds,
        )
        workers.append(worker)
        return worker

    yield start

    # テスト成功・失敗に関係なく終了する
    for worker in reversed(workers):
        worker.close()
