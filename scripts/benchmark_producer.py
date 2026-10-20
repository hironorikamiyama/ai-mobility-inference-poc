"""Redis Pipelineを使った性能試験用Producer。"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import redis


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--host", default=os.getenv("REDIS_HOST", "redis"))
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be >= 1")

    client = redis.Redis(
        host=args.host,
        port=args.port,
        decode_responses=True,
    )

    client.ping()

    pipeline = client.pipeline(transaction=False)

    for i in range(1, args.count + 1):
        pipeline.xadd(
            "inference_jobs",
            {
                "job_id": f"{args.run_id}-{i}",
                "input_value": "0.5",
            },
        )

    started_at = datetime.now(timezone.utc)
    start = time.perf_counter()

    results = pipeline.execute()

    duration = time.perf_counter() - start
    finished_at = datetime.now(timezone.utc)

    print(json.dumps({
        "run_id": args.run_id,
        "count": len(results),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "producer_duration_ms": round(duration * 1000, 3),
        "first_stream_id": results[0],
        "last_stream_id": results[-1],
    }, indent=2))


if __name__ == "__main__":
    main()
