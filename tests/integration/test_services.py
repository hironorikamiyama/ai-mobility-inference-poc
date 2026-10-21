import os
import uuid

from pymongo import MongoClient
from redis import Redis

from src.database import save_result


def test_redis_stream_and_mongodb():
    suffix = uuid.uuid4().hex
    stream = f"ci_inference_jobs_{suffix}"
    group = "ci_workers"
    job_id = f"ci-{suffix}"

    redis_client = Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
        socket_connect_timeout=3,
    )
    mongo_client = MongoClient(
        os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        serverSelectionTimeoutMS=3000,
    )
    collection = mongo_client["ai_mobility_ci"]["inference_results"]

    try:
        assert redis_client.ping()
        mongo_client.admin.command("ping")

        redis_client.xgroup_create(
            stream, group, id="0", mkstream=True
        )
        redis_client.xadd(
            stream,
            {"job_id": job_id, "input_value": "0.5"},
        )

        messages = redis_client.xreadgroup(
            group, "ci-worker", {stream: ">"}, count=1
        )
        assert len(messages) == 1
        message_id, fields = messages[0][1][0]
        assert fields["job_id"] == job_id

        save_result(
            collection=collection,
            job_id=job_id,
            input_value=0.5,
            score=0.5,
            device="cpu",
        )
        assert collection.count_documents({"_id": job_id}) == 1

        # 同一ジョブを再保存してもドキュメントを重複させない
        save_result(
            collection=collection,
            job_id=job_id,
            input_value=0.5,
            score=0.5,
            device="cpu",
        )
        assert collection.count_documents({"_id": job_id}) == 1

        assert redis_client.xack(stream, group, message_id) == 1
        assert redis_client.xpending(stream, group)["pending"] == 0

    finally:
        redis_client.delete(stream)
        collection.delete_one({"_id": job_id})
        mongo_client.close()
        redis_client.close()
