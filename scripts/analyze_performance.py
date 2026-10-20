"""WorkerログからCPU推論PoCの性能指標を集計する。"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from statistics import mean, median

PATTERN = re.compile(
    r"Processed: job_id=(\S+), "
    r"stream_id=\S+, "
    r"score=[\d.]+, "
    r"elapsed_ms=([\d.]+), "
    r"ack=(\d+)"
)


def percentile(values, p):
    values = sorted(values)
    if not values:
        raise ValueError("No measurements")

    pos = (len(values) - 1) * p / 100
    low = int(pos)
    high = min(low + 1, len(values) - 1)
    fraction = pos - low

    return (
        values[low] * (1 - fraction)
        + values[high] * fraction
    )


def main():
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python scripts/analyze_performance.py "
            "<logfile> <run_id>"
        )

    log_path = Path(sys.argv[1])
    run_id = sys.argv[2]
    records = {}

    for line in log_path.read_text(
        encoding="utf-8"
    ).splitlines():
        match = PATTERN.search(line)
        if not match:
            continue

        job_id, elapsed, ack = match.groups()

        if not job_id.startswith(run_id + "-"):
            continue

        if ack != "1":
            continue

        try:
            timestamp = datetime.fromisoformat(
                line.split(" ", 1)[0].replace(
                    "Z", "+00:00"
                )
            )
        except (ValueError, IndexError):
            continue

        records[job_id] = {
            "timestamp": timestamp,
            "elapsed_ms": float(elapsed),
        }

    ordered = sorted(
        records.values(),
        key=lambda x: x["timestamp"],
    )

    elapsed = [r["elapsed_ms"] for r in ordered]
    if not elapsed:
        raise SystemExit("No successful records found")

    # 初回5件を除外した定常時の処理時間も計測
    steady = elapsed[5:] if len(elapsed) > 5 else []

    duration = (
        ordered[-1]["timestamp"]
        - ordered[0]["timestamp"]
    ).total_seconds()

    def stats(values):
        if not values:
            return None
        return {
            "samples": len(values),
            "mean_ms": round(mean(values), 3),
            "p50_ms": round(median(values), 3),
            "p95_ms": round(percentile(values, 95), 3),
            "p99_ms": round(percentile(values, 99), 3),
            "max_ms": round(max(values), 3),
        }

    report = {
        "run_id": run_id,
        "expected_jobs": 100,
        "completed_jobs": len(records),
        "completion_rate_percent": round(
            len(records) / 100 * 100, 2
        ),
        "worker_latency_all": stats(elapsed),
        "worker_latency_excluding_first_5": stats(steady),
        "completion_observation_seconds": round(
            duration, 3
        ),
        "observed_completion_rate_per_second": (
            round((len(records) - 1) / duration, 3)
            if duration > 0 else None
        ),
    }

    print(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
