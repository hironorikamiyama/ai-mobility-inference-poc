from datetime import datetime, timezone

import pytest

from src.performance_metrics import (
    calculate_latencies,
    stream_created_at,
)


def test_stream_created_at():
    actual = stream_created_at("1000000-0")
    assert actual == datetime.fromtimestamp(
        1000, tz=timezone.utc
    )


def test_calculate_latencies():
    started = datetime.fromtimestamp(
        1000.050, tz=timezone.utc
    )
    saved = datetime.fromtimestamp(
        1000.075, tz=timezone.utc
    )

    result = calculate_latencies(
        "1000000-0",
        started,
        saved,
    )

    assert result["queue_latency_ms"] == pytest.approx(50)
    assert result["worker_latency_ms"] == pytest.approx(25)
    assert result["end_to_end_latency_ms"] == pytest.approx(75)


def test_e2e_equals_queue_plus_worker():
    started = datetime.fromtimestamp(
        1000.020, tz=timezone.utc
    )
    saved = datetime.fromtimestamp(
        1000.035, tz=timezone.utc
    )

    result = calculate_latencies(
        "1000000-0",
        started,
        saved,
    )

    assert result["end_to_end_latency_ms"] == pytest.approx(
        result["queue_latency_ms"]
        + result["worker_latency_ms"],
        abs=0.001,
    )
