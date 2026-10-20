import pytest

from scripts.analyze_e2e import (
    build_report,
    parse_records,
    summarize,
)


def make_log(job_id, queue, worker, e2e, ack=1):
    return (
        f"2026-10-20T06:00:00Z "
        f"Processed: job_id={job_id}, "
        f"stream_id=123-0, score=0.500000, "
        f"elapsed_ms=2.00, "
        f"queue_ms={queue:.3f}, "
        f"worker_ms={worker:.3f}, "
        f"e2e_ms={e2e:.3f}, ack={ack}"
    )


def test_summarize():
    result = summarize([10, 20, 30, 40, 50])
    assert result["samples"] == 5
    assert result["p50_ms"] == 30
    assert result["p95_ms"] == pytest.approx(48)
    assert result["p99_ms"] == pytest.approx(49.6)


def test_summarize_empty():
    assert summarize([]) is None


def test_parse_records_filters_run_id():
    log = "\n".join([
        make_log("run-a-1", 10, 2, 12),
        make_log("run-b-1", 20, 2, 22),
    ])

    records = parse_records(log, "run-a")
    assert len(records) == 1
    assert "run-a-1" in records


def test_parse_records_ignores_failed_ack():
    log = make_log("run-a-1", 10, 2, 12, ack=0)
    assert parse_records(log, "run-a") == {}


def test_duplicate_job_id_counted_once():
    line = make_log("run-a-1", 10, 2, 12)
    report = build_report(
        line + "\n" + line,
        "run-a",
        expected_jobs=1,
    )
    assert report["completed_jobs"] == 1


def test_build_report():
    log = "\n".join([
        make_log("run-a-1", 10, 2, 12),
        make_log("run-a-2", 20, 3, 23),
    ])
    result = build_report(log, "run-a", expected_jobs=2)

    assert result["completed_jobs"] == 2
    assert result["completion_rate_percent"] == 100
    assert result["queue_latency"]["p50_ms"] == 15
    assert result["end_to_end_latency"]["p50_ms"] == 17.5


def test_invalid_expected_jobs():
    with pytest.raises(ValueError):
        build_report("", "run-a", expected_jobs=0)


def test_parse_records_with_dispatch_metrics():
    log = (
        "2026-10-20T07:46:57Z "
        "Processed: job_id=run-a-1, "
        "stream_id=123-0, score=0.500000, "
        "elapsed_ms=2.00, "
        "queue_ms=10.000, "
        "worker_ms=2.000, "
        "e2e_ms=12.000, "
        "redis_wait_ms=9.000, "
        "dispatch_wait_ms=1.000, "
        "ack=1"
    )

    report = build_report(
        log,
        "run-a",
        expected_jobs=1,
    )

    assert report["completed_jobs"] == 1
    assert report["queue_latency"]["mean_ms"] == 10
    assert report["end_to_end_latency"]["mean_ms"] == 12
