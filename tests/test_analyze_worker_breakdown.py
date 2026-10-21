from scripts.analyze_worker_breakdown import analyze


def test_worker_breakdown_metrics():
    run_id = "perf-test"

    log_text = (
        "Processed: job_id=perf-test-1, "
        "worker_ms=10.000, "
        "tensor_ms=1.000, "
        "inference_ms=2.000, "
        "mongo_save_ms=6.000, "
        "ack=1\n"
        "Processed: job_id=perf-test-2, "
        "worker_ms=20.000, "
        "tensor_ms=2.000, "
        "inference_ms=3.000, "
        "mongo_save_ms=14.000, "
        "ack=1\n"
    )

    report = analyze(log_text, run_id)

    assert report["completed_jobs_with_breakdown"] == 2
    assert report["worker_ms"]["max_ms"] == 20.0
    assert report["tensor_ms"]["mean_ms"] == 1.5
    assert report["inference_ms"]["max_ms"] == 3.0
    assert report["mongo_save_ms"]["max_ms"] == 14.0
    assert report["slowest_jobs"][0]["job_id"] == "perf-test-2"


def test_worker_breakdown_excludes_old_and_failed_logs():
    log_text = (
        "Processed: job_id=perf-test-1, "
        "worker_ms=10.000, ack=1\n"
        "Processed: job_id=perf-test-2, "
        "worker_ms=11.000, tensor_ms=1.000, "
        "inference_ms=2.000, mongo_save_ms=7.000, "
        "ack=0\n"
    )

    report = analyze(log_text, "perf-test")

    assert report["completed_jobs_with_breakdown"] == 0
    assert "slowest_jobs" not in report
