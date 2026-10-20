import os
from datetime import datetime, timezone

from pymongo import MongoClient


def get_collection():
    client = MongoClient(
        os.getenv("MONGO_URI", "mongodb://mongo:27017/"),
        serverSelectionTimeoutMS=5000,
    )

    database = client["ai_mobility"]
    collection = database["inference_results"]

    return collection


def save_result(
    collection,
    job_id: str,
    input_value: float,
    score: float,
    device: str,
):
    result = {
        "job_id": job_id,
        "input_value": input_value,
        "score": score,
        "device": device,
        "processed_at": datetime.now(timezone.utc),
    }

    collection.replace_one(
        {"_id": job_id},
        {"_id": job_id, **result},
        upsert=True,
    )

    return result
