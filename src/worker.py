"""Redis Streamsから推論ジョブを処理するWorker。"""

import os
import time
from datetime import datetime, timezone

import redis
import torch
from redis.exceptions import ResponseError

from src.database import get_collection, save_result
from src.inference import InferenceWorker
from src.performance_metrics import calculate_latencies
from src.retry_policy import (
    make_retry_exhausted_record,
    should_dead_letter,
)

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

    input_value = float(fields["input_value"])

    if not 0 <= input_value <= 1:
        raise ValueError(
            "input_value must be between 0 and 1"
        )

    return job_id, input_value


def dead_letter_and_ack(client, message_id, record):
    """DLQへの記録とACKをRedis内で原子的に実行する。"""
    record = {
        **record,
        "failed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    with client.pipeline(transaction=True) as pipe:
        pipe.xadd(DEAD_LETTER_STREAM, record)
        pipe.xack(STREAM_NAME, GROUP_NAME, message_id)
        results = pipe.execute()

    if results[1] != 1:
        print(
            f"Warning: ACK count={results[1]}, "
            f"stream_id={message_id}",
            flush=True,
        )


def get_delivery_count(client, message_id):
    """Pending情報から配信回数を取得する。"""
    entries = client.xpending_range(
        STREAM_NAME,
        GROUP_NAME,
        min=message_id,
        max=message_id,
        count=1,
    )

    if not entries:
        raise RuntimeError(
            f"Pending entry not found: {message_id}"
        )

    return int(entries[0]["times_delivered"])


def process_message(
    client,
    collection,
    worker,
    message_id,
    fields,
    received_at=None,
):
    started_at = datetime.now(timezone.utc)

    if received_at is None:
        received_at = started_at

    started = time.perf_counter()

    try:
        job_id, input_value = validate_job(fields)
    except (ValueError, KeyError, TypeError) as exc:
        dead_letter_and_ack(
            client,
            message_id,
            {
                "original_stream_id": message_id,
                "reason": str(exc),
                "payload": str(fields),
            },
        )
        print(
            f"Rejected: stream_id={message_id}, "
            f"reason={exc}",
            flush=True,
        )
        return

    delay = float(
        os.getenv("WORKER_PROCESSING_DELAY_SECONDS", "0")
    )

    if delay > 0:
        time.sleep(delay)

    # Tensor生成時間
    tensor_started = time.perf_counter()

    image = torch.full(
        (1, 1, 16, 16),
        input_value,
    )

    tensor_ms = (
        time.perf_counter() - tensor_started
    ) * 1000

    # 推論実行時間
    inference_started = time.perf_counter()

    score = worker.predict(image)[0]

    inference_ms = (
        time.perf_counter() - inference_started
    ) * 1000

    # MongoDB保存時間
    mongo_started = time.perf_counter()

    # MongoDB保存が成功した場合のみACKする
    save_result(
        collection=collection,
        job_id=job_id,
        input_value=input_value,
        score=score,
        device=worker.device.type,
    )

    mongo_save_ms = (
        time.perf_counter() - mongo_started
    ) * 1000

    saved_at = datetime.now(timezone.utc)

    latencies = calculate_latencies(
        message_id=message_id,
        started_at=started_at,
        saved_at=saved_at,
    )

    created_at = datetime.fromtimestamp(
        int(message_id.split("-", 1)[0]) / 1000,
        tz=timezone.utc,
    )

    redis_wait_ms = (
        received_at - created_at
    ).total_seconds() * 1000

    dispatch_wait_ms = (
        started_at - received_at
    ).total_seconds() * 1000

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
        f"queue_ms={latencies['queue_latency_ms']:.3f}, "
        f"worker_ms={latencies['worker_latency_ms']:.3f}, "
        f"e2e_ms={latencies['end_to_end_latency_ms']:.3f}, "
        f"redis_wait_ms={redis_wait_ms:.3f}, "
        f"dispatch_wait_ms={dispatch_wait_ms:.3f}, "
        f"tensor_ms={tensor_ms:.3f}, "
        f"inference_ms={inference_ms:.3f}, "
        f"mongo_save_ms={mongo_save_ms:.3f}, "
        f"ack={acknowledged}",
        flush=True,
    )

def handle_message(
    client,
    collection,
    worker,
    message_id,
    fields,
    received_at=None,
):
    """配信回数を確認して処理または隔離を行う。"""
    delivery_count = get_delivery_count(
        client, message_id
    )

    if should_dead_letter(delivery_count):
        record = make_retry_exhausted_record(
            message_id=message_id,
            fields=fields,
            delivery_count=delivery_count,
        )

        dead_letter_and_ack(
            client, message_id, record
        )

        print(
            f"Retry exhausted: "
            f"stream_id={message_id}, "
            f"delivery_count={delivery_count}",
            flush=True,
        )
        return

    process_message(
        client,
        collection,
        worker,
        message_id,
        fields,
        received_at=received_at,
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
                batch_size = int(
                    os.getenv("READ_BATCH_SIZE", "10")
                )
                if batch_size < 1:
                    raise ValueError(
                        "READ_BATCH_SIZE must be >= 1"
                    )

                result = client.xreadgroup(
                    GROUP_NAME,
                    consumer,
                    {STREAM_NAME: ">"},
                    count=batch_size,
                    block=2000,
                )
                messages = (
                    result[0][1] if result else []
                )

            received_at = datetime.now(timezone.utc)

            for message_id, fields in messages:
                try:
                    handle_message(
                        client,
                        collection,
                        worker,
                        message_id,
                        fields,
                        received_at=received_at,
                    )
                except Exception as exc:
                    # ACKせずPendingに残す
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
