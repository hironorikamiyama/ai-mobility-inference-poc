"""Redis Streams推論基盤の処理時間計測。"""

from datetime import datetime, timezone


def stream_created_at(message_id: str) -> datetime:
    """Redis Stream IDから投入時刻を取得する。"""
    milliseconds = int(message_id.split("-", 1)[0])
    return datetime.fromtimestamp(
        milliseconds / 1000,
        tz=timezone.utc,
    )


def calculate_latencies(
    message_id: str,
    started_at: datetime,
    saved_at: datetime,
) -> dict[str, float]:
    """Redis投入からMongoDB保存までの各区間を算出する。"""
    created_at = stream_created_at(message_id)

    return {
        "queue_latency_ms": round(
            (started_at - created_at).total_seconds() * 1000,
            3,
        ),
        "worker_latency_ms": round(
            (saved_at - started_at).total_seconds() * 1000,
            3,
        ),
        "end_to_end_latency_ms": round(
            (saved_at - created_at).total_seconds() * 1000,
            3,
        ),
    }
