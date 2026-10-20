import os
import time
from datetime import datetime, timezone

import redis
import torch
from redis.exceptions import ResponseError

from src.database import get_collection, save_result
from src.inference import InferenceWorker


STREAM_NAME = "inference_jobs"
GROUP_NAME = "inference_workers"
DEAD_LETTER_STREAM = "inference_jobs_dead"
RECLAIM_IDLE_MS = 30_000


def create_group(client):
    try:
        client.xgroup_create(
            STREAM_NAME,
            GROUP_NAME,
            id="0",
            mkstream=True,
        )
        print("Consumer Group created", flush=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def validate_job(fields):
    job_id = fields["job_id"]

    if not job_id:
        raise ValueError("job_id is empty")

    value = float(fields["input_value"])

    if not 0.0 <= value <= 1.0:
        raise ValueError(
            "input_value must be between 0 and 1"
        )

    return job_id, value


def process_message(
    client,
    collection,
    worker,
    message_id,
    fields,
):
    started = time.perf_counter()

    # 不正な入力は再実行しても回復しないため隔離する
    try:
        job_id, input_value = validate_job(fields)
    except (ValueError, KeyError, TypeError) as exc:
        client.xadd(
            DEAD_LETTER_STREAM,
            {
                "original_stream_id": message_id,
                "reason": str(exc),
                "payload": str(fields),
                "failed_at": datetime.now(
                    timezone.utc
                ).isoformat(),
            },
        )
        client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id,
        )
        print(
            f"Rejected: stream_id={message_id}, "
            f"reason={exc}",
            flush=True,
        )
        return

    # 障害再現用。通常は0秒
    delay = float(
        os.getenv("WORKER_PROCESSING_DELAY_SECONDS", "0")
    )
    if delay > 0:
        time.sleep(delay)

    image = torch.full(
        (1, 1, 16, 16),
        input_value,
    )

    score = worker.predict(image)[0]

    # 保存が成功した後でのみACKする
    save_result(
        collection=collection,
        job_id=job_id,
        input_value=input_value,
        score=score,
        device=worker.device.type,
    )

    acknowledged = client.xack(
        STREAM_NAME,
        GROUP_NAME,
        message_id,
    )

    elapsed_ms = (
        time.perf_counter() - started
    ) * 1000

    print(
        f"Processed: job_id={job_id}, "
        f"stream_id={message_id}, "
        f"score={score:.6f}, "
        f"elapsed_ms={elapsed_ms:.2f}, "
        f"ack={acknowledged}",
        flush=True,
    )


def main():
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=6379,
        decode_responses=True,
    )

    collection = get_collection()
    worker = InferenceWorker()

    consumer = os.getenv(
        "CONSUMER_NAME",
        os.uname().nodename,
    )

    create_group(client)

    print(
        f"Worker started: "
        f"consumer={consumer}, "
        f"device={worker.device.type}",
        flush=True,
    )

    while True:
        try:
            # 停止したWorkerなどの未ACKジョブを回収
            # 30秒以上アイドル状態のものが対象
            claimed = client.xautoclaim(
                STREAM_NAME,
                GROUP_NAME,
                consumer,
                min_idle_time=RECLAIM_IDLE_MS,
                start_id="0-0",
                count=10,
            )

            pending_messages = claimed[1]

            if pending_messages:
                messages = pending_messages
            else:
                # 未処理の新規メッセージを取得
                result = client.xreadgroup(
                    GROUP_NAME,
                    consumer,
                    {STREAM_NAME: ">"},
                    count=10,
                    block=2000,
                )

                messages = (
                    result[0][1] if result else []
                )

            for message_id, fields in messages:
                try:
                    process_message(
                        client,
                        collection,
                        worker,
                        message_id,
                        fields,
                    )
                except Exception as exc:
                    # ACKしない。後で再取得可能にする
                    print(
                        f"Processing failed: "
                        f"stream_id={message_id}, "
                        f"error={exc}",
                        flush=True,
                    )

        except (redis.RedisError, OSError) as exc:
            print(
                f"Worker connection error: {exc}",
                flush=True,
            )
            time.sleep(5)


if __name__ == "__main__":
    main()
