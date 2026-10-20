import pytest

from src.retry_policy import (
    MAX_ATTEMPTS,
    make_retry_exhausted_record,
    should_dead_letter,
)


@pytest.mark.parametrize(
    "count",
    [1, 2, 3],
)
def test_retry_allowed(count):
    assert should_dead_letter(count) is False


def test_retry_limit_exceeded():
    assert should_dead_letter(4) is True


def test_retry_limit_constant():
    assert MAX_ATTEMPTS == 3


def test_invalid_delivery_count():
    with pytest.raises(ValueError):
        should_dead_letter(0)


def test_retry_exhausted_record():
    record = make_retry_exhausted_record(
        message_id="123-0",
        fields={
            "job_id": "test-001",
            "input_value": "0.7",
        },
        delivery_count=4,
    )

    assert record["original_stream_id"] == "123-0"
    assert record["reason"] == "retry_limit_exceeded"
    assert record["delivery_count"] == "4"
    assert "test-001" in record["payload"]
