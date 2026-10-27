#!/usr/bin/env python3
"""Phase 19: Aggregate Kubernetes recovery measurements."""

import argparse
import csv
import json
import math
from pathlib import Path
from statistics import mean, median, stdev

METRICS = {
    "pod_recreated_observed": "Pod recreation observed",
    "pod_ready_observed": "Pod Ready observed",
    "job_recovered_observed": "Job recovery observed",
    "ready_to_recovery": "Ready to recovery",
}


def load_records(input_dir: Path):
    records = []

    for path in sorted(input_dir.glob("phase18-*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))

        if not isinstance(data, dict):
            raise ValueError(f"Invalid JSON object: {path}")

        records.append(data)

    return records



def validate_seconds(value, job_id, metric):
    if isinstance(value, bool):
        raise ValueError(
            f"Invalid measurement: {job_id}/{metric}={value}"
        )

    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid measurement: {job_id}/{metric}={value}"
        ) from exc

    if not math.isfinite(number) or number < 0:
        raise ValueError(
            f"Invalid measurement: {job_id}/{metric}={value}"
        )

    return number

def summarize(records):
    if not records:
        raise ValueError("No Phase 18 measurement records found")

    passed = [
        record for record in records
        if record.get("status") == "passed"
    ]

    summary = {
        "total_runs": len(records),
        "passed_runs": len(passed),
        "failed_runs": len(records) - len(passed),
        "metrics": {},
    }

    for key in METRICS:
        values = [
            validate_seconds(
                record["measurements_seconds"][key],
                record.get("job_id", "unknown"),
                key,
            )
            for record in passed
        ]

        if not values:
            summary["metrics"][key] = None
            continue

        summary["metrics"][key] = {
            "mean": round(mean(values), 3),
            "median": round(median(values), 3),
            "min": round(min(values), 3),
            "max": round(max(values), 3),
            "stdev": round(stdev(values), 3)
            if len(values) >= 2 else None,
        }

    return summary


def write_csv(records, output_path: Path):
    fieldnames = [
        "job_id",
        "status",
        *METRICS.keys(),
    ]

    with output_path.open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            row = {
                "job_id": record.get("job_id", ""),
                "status": record.get("status", ""),
            }

            measurements = record.get(
                "measurements_seconds", {}
            )

            for key in METRICS:
                row[key] = measurements.get(key, "")

            writer.writerow(row)


def write_markdown(summary, output_path: Path):
    lines = [
        "# Phase 19 Kubernetes Recovery Report",
        "",
        f"- Total runs: {summary['total_runs']}",
        f"- Passed: {summary['passed_runs']}",
        f"- Failed: {summary['failed_runs']}",
        "",
        "## Recovery time statistics",
        "",
        "| Metric | Mean (s) | Median (s) | "
        "Min (s) | Max (s) | StdDev (s) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for key, label in METRICS.items():
        metric = summary["metrics"][key]

        if metric is None:
            values = ["N/A"] * 5
        else:
            values = [
                f"{metric[name]:.3f}"
                if metric[name] is not None else "N/A"
                for name in (
                    "mean", "median", "min", "max", "stdev"
                )
            ]

        lines.append(
            f"| {label} | " + " | ".join(values) + " |"
        )

    lines.extend([
        "",
        "## Interpretation and limitations",
        "",
        "- Measurements are based on polling observation times.",
        "- Job recovery includes a 30-second test-only pause.",
        "- The recorded sample size is insufficient for "
        "reliable p95 assessment.",
        "- These results do not establish a production recovery SLO.",
        "",
    ])

    output_path.write_text(
        "\n".join(lines), encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/phase18"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/phase19"),
    )
    args = parser.parse_args()

    records = load_records(args.input)
    summary = summarize(records)

    args.output.mkdir(parents=True, exist_ok=True)

    summary_path = args.output / "summary.json"
    csv_path = args.output / "recovery-results.csv"
    md_path = args.output / "recovery-report.md"

    summary_path.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    write_csv(records, csv_path)
    write_markdown(summary, md_path)

    print(json.dumps(summary, indent=2))
    print(f"Summary: {summary_path}")
    print(f"CSV: {csv_path}")
    print(f"Markdown: {md_path}")


if __name__ == "__main__":
    main()
