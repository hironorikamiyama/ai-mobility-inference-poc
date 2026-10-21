"""WorkerのTensor生成・推論・MongoDB保存時間を集計する。"""

import argparse
import json
import re
from pathlib import Path

import numpy as np


METRICS = (
    "worker_ms",
    "tensor_ms",
    "inference_ms",
    "mongo_save_ms",
)


def summarize(values):
    return {
        "samples": len(values),
        "mean_ms": round(float(np.mean(values)), 3),
        "p50_ms": round(float(np.percentile(values, 50)), 3),
        "p95_ms": round(float(np.percentile(values, 95)), 3),
        "p99_ms": round(float(np.percentile(values, 99)), 3),
        "max_ms": round(float(np.max(values)), 3),
    }


def analyze(log_text, run_id):
    records = {}

    for line in log_text.splitlines():
        if "Processed: job_id=" not in line:
            continue

        fields = dict(
            re.findall(
                r"([a-z_]+)=([^,\s]+)",
                line.split("Processed: ", 1)[1],
            )
        )

        job_id = fields.get("job_id", "")

        if not re.fullmatch(
            re.escape(run_id) + r"-[1-9]\d*",
            job_id,
        ):
            continue

        if fields.get("ack") != "1":
            continue

        if not all(name in fields for name in METRICS):
            continue

        records[job_id] = {
            name: float(fields[name])
            for name in METRICS
        }

    report = {
        "run_id": run_id,
        "completed_jobs_with_breakdown": len(records),
    }

    if not records:
        return report

    for name in METRICS:
        report[name] = summarize([
            record[name] for record in records.values()
        ])

    report["slowest_jobs"] = sorted(
        [
            {"job_id": job_id, **record}
            for job_id, record in records.items()
        ],
        key=lambda record: record["worker_ms"],
        reverse=True,
    )[:10]

    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file")
    parser.add_argument("run_id")
    args = parser.parse_args()

    log_text = Path(args.log_file).read_text(
        encoding="utf-8"
    )

    report = analyze(log_text, args.run_id)

    print(json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
