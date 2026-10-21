"""WorkerログからQueue/Worker/E2Eレイテンシを集計する。"""

import json
import re
import sys
from pathlib import Path
from statistics import mean, median

import numpy as np

PATTERN = re.compile(
    r"Processed: job_id=(\S+), .*?"
    r"queue_ms=([-\d.]+), "
    r"worker_ms=([-\d.]+), "
    r"e2e_ms=([-\d.]+), "
    r"(?:redis_wait_ms=[-\d.]+, "
    r"dispatch_wait_ms=[-\d.]+, )?"
    r"(?:tensor_ms=[-\d.]+, "
    r"inference_ms=[-\d.]+, "
    r"mongo_save_ms=[-\d.]+, )?"
    r"ack=(\d+)"
)

EXPECTED_JOBS = 100


def summarize(values):
    if not values:
        return None

    return {
        "samples": len(values),
        "mean_ms": round(mean(values), 3),
        "p50_ms": round(median(values), 3),
        "p95_ms": round(float(np.percentile(values, 95)), 3),
        "p99_ms": round(float(np.percentile(values, 99)), 3),
        "max_ms": round(max(values), 3),
    }


def parse_records(log_text, run_id):
    """対象run_idのACK成功ログをjob_id単位で抽出する。"""
    records = {}

    for line in log_text.splitlines():
        match = PATTERN.search(line)
        if not match:
            continue

        job_id, queue, worker, e2e, ack = match.groups()

        if not re.fullmatch(
            re.escape(run_id) + r"-[1-9]\d*",
            job_id,
        ):
            continue

        if ack != "1":
            continue

        records[job_id] = {
            "queue": float(queue),
            "worker": float(worker),
            "e2e": float(e2e),
        }

    return records


def build_report(log_text, run_id, expected_jobs=EXPECTED_JOBS):
    if expected_jobs < 1:
        raise ValueError("expected_jobs must be >= 1")

    records = parse_records(log_text, run_id)

    def metric(name):
        return summarize(
            [r[name] for r in records.values()]
        )

    completed = len(records)

    return {
        "run_id": run_id,
        "expected_jobs": expected_jobs,
        "completed_jobs": completed,
        "completion_rate_percent": round(
            completed / expected_jobs * 100, 2
        ),
        "queue_latency": metric("queue"),
        "worker_latency": metric("worker"),
        "end_to_end_latency": metric("e2e"),
    }


def main():
    if len(sys.argv) not in (3, 4):
        raise SystemExit(
            "Usage: python scripts/analyze_e2e.py "
            "<logfile> <run_id> [expected_jobs]"
        )

    log_path = Path(sys.argv[1])
    run_id = sys.argv[2]
    expected_jobs = (
        int(sys.argv[3]) if len(sys.argv) == 4
        else EXPECTED_JOBS
    )

    report = build_report(
        log_path.read_text(encoding="utf-8"),
        run_id,
        expected_jobs,
    )

    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
