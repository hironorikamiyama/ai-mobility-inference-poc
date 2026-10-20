"""WorkerログからRedis待機と取得後待機を集計する。"""

import json
import re
import sys
from pathlib import Path

import numpy as np

PATTERN = re.compile(
    r"Processed: job_id=(\S+), .*?"
    r"queue_ms=([-\d.]+), .*?"
    r"redis_wait_ms=([-\d.]+), "
    r"dispatch_wait_ms=([-\d.]+), "
    r"ack=1"
)


def summarize(values):
    if not values:
        return None
    return {
        "samples": len(values),
        "mean_ms": round(float(np.mean(values)), 3),
        "p50_ms": round(float(np.percentile(values, 50)), 3),
        "p95_ms": round(float(np.percentile(values, 95)), 3),
        "p99_ms": round(float(np.percentile(values, 99)), 3),
        "max_ms": round(float(np.max(values)), 3),
    }


def analyze(log_text, run_id):
    rows = {}

    for line in log_text.splitlines():
        match = PATTERN.search(line)
        if not match:
            continue

        job_id, queue, redis_wait, dispatch = match.groups()
        if not re.fullmatch(re.escape(run_id) + r"-[1-9]\d*", job_id):
            continue

        rows[job_id] = {
            "queue": float(queue),
            "redis_wait": float(redis_wait),
            "dispatch": float(dispatch),
        }

    errors = [
        abs(r["queue"] - r["redis_wait"] - r["dispatch"])
        for r in rows.values()
    ]

    return {
        "run_id": run_id,
        "completed_jobs": len(rows),
        "redis_wait_latency": summarize(
            [r["redis_wait"] for r in rows.values()]
        ),
        "dispatch_wait_latency": summarize(
            [r["dispatch"] for r in rows.values()]
        ),
        "queue_latency": summarize(
            [r["queue"] for r in rows.values()]
        ),
        "max_decomposition_error_ms": (
            round(max(errors), 6) if errors else None
        ),
    }


def main():
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python scripts/analyze_dispatch.py <log> <run_id>"
        )

    log_text = Path(sys.argv[1]).read_text(encoding="utf-8")
    print(json.dumps(
        analyze(log_text, sys.argv[2]),
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
