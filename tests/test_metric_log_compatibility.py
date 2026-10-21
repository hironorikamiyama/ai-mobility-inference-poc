from scripts.analyze_e2e import parse_records
from scripts.analyze_dispatch import analyze


RUN_ID = "perf-compat-test"

OLD_LOG = (
    "Processed: job_id=perf-compat-test-1, "
    "stream_id=1234567890000-0, "
    "score=0.500000, "
    "elapsed_ms=3.00, "
    "queue_ms=10.000, "
    "worker_ms=2.000, "
    "e2e_ms=12.000, "
    "redis_wait_ms=9.500, "
    "dispatch_wait_ms=0.500, "
    "ack=1"
)

NEW_LOG = (
    "Processed: job_id=perf-compat-test-2, "
    "stream_id=1234567890000-1, "
    "score=0.500000, "
    "elapsed_ms=3.00, "
    "queue_ms=10.000, "
    "worker_ms=2.000, "
    "e2e_ms=12.000, "
    "redis_wait_ms=9.500, "
    "dispatch_wait_ms=0.500, "
    "tensor_ms=0.100, "
    "inference_ms=0.200, "
    "mongo_save_ms=1.500, "
    "ack=1"
)


def test_e2e_parser_accepts_old_and_new_logs():
    records = parse_records(
        OLD_LOG + "\n" + NEW_LOG, RUN_ID
    )

    assert len(records) == 2
    assert records[f"{RUN_ID}-1"]["e2e"] == 12.0
    assert records[f"{RUN_ID}-2"]["e2e"] == 12.0


def test_dispatch_parser_accepts_old_and_new_logs():
    report = analyze(
        OLD_LOG + "\n" + NEW_LOG, RUN_ID
    )

    assert report["completed_jobs"] == 2
    assert report["max_decomposition_error_ms"] == 0.0
