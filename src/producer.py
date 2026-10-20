import os
import uuid

import redis

STREAM_NAME = "inference_jobs"


def main():
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True,
    )

    for value in [0.2, 0.5, 0.8]:
        job = {
            "job_id": str(uuid.uuid4()),
            "input_value": str(value),
        }

        message_id = client.xadd(STREAM_NAME, job)

        print(
            f"Queued: stream_id={message_id}, "
            f"job_id={job['job_id']}, "
            f"input_value={value}",
            flush=True,
        )


if __name__ == "__main__":
    main()
