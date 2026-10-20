"""Redis Streamsの再試行ポリシー。"""

MAX_ATTEMPTS = 3


def should_dead_letter(delivery_count: int) -> bool:
    """許容した試行回数を超えたか判定する。

    delivery_count:
        Redis Consumer Groupで記録された配信回数。
    """
    if delivery_count < 1:
        raise ValueError(
            "delivery_count must be >= 1"
        )

    return delivery_count > MAX_ATTEMPTS


def make_retry_exhausted_record(
    message_id: str,
    fields: dict,
    delivery_count: int,
) -> dict[str, str]:
    """試行回数超過時の隔離レコードを作成する。"""
    return {
        "original_stream_id": message_id,
        "reason": "retry_limit_exceeded",
        "delivery_count": str(delivery_count),
        "payload": str(fields),
    }
