from unittest.mock import MagicMock, patch

from src.worker import (
    GROUP_NAME,
    STREAM_NAME,
    DEAD_LETTER_STREAM,
    handle_message,
    dead_letter_and_ack,
)


def make_client(delivery_count):
    client = MagicMock()
    client.xpending_range.return_value = [
        {"times_delivered": delivery_count}
    ]
    return client


def test_third_delivery_is_processed():
    client = make_client(3)
    collection = MagicMock()
    worker = MagicMock()

    with patch("src.worker.process_message") as process:
        handle_message(
            client,
            collection,
            worker,
            "123-0",
            {"job_id": "test-001", "input_value": "0.5"},
        )

    process.assert_called_once()


def test_fourth_delivery_is_dead_lettered():
    client = make_client(4)

    handle_message(
        client,
        MagicMock(),
        MagicMock(),
        "123-0",
        {"job_id": "test-001", "input_value": "0.5"},
    )

    pipe = client.pipeline.return_value.__enter__.return_value

    pipe.xadd.assert_called_once()
    args, _ = pipe.xadd.call_args

    assert args[0] == DEAD_LETTER_STREAM
    assert args[1]["reason"] == "retry_limit_exceeded"
    assert args[1]["delivery_count"] == "4"

    pipe.xack.assert_called_once_with(
        STREAM_NAME, GROUP_NAME, "123-0"
    )
    pipe.execute.assert_called_once()


def test_dead_letter_does_not_ack_if_redis_fails():
    client = MagicMock()
    pipe = client.pipeline.return_value.__enter__.return_value
    pipe.execute.side_effect = ConnectionError("Redis unavailable")

    try:
        dead_letter_and_ack(
            client,
            "123-0",
            {
                "original_stream_id": "123-0",
                "reason": "test failure",
                "payload": "{}",
            },
        )
    except ConnectionError:
        pass
    else:
        raise AssertionError("Expected Redis error")

    # トランザクションが成功したとは扱わない
    pipe.execute.assert_called_once()
