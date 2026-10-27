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
CONDITION_KEYS = (
    "failure_scenario",
    "measurement_method",
    "test_pause_seconds",
    "job_recovery_includes_test_pause",
    "worker_readiness_probe",
)


def summarize_measurement_conditions(records):
    unavailable = []
    known_conditions = []

    for record in records:
        job_id = record.get("job_id", "unknown")
        conditions = record.get("measurement_conditions")
        comparable = record.get("measurement_conditions_comparable")
        reason = record.get("measurement_conditions_reason")
        if comparable is not True:
            unavailable.append(
                f"{job_id}: measurement conditions are not verified comparable"
                + (f" ({reason})" if isinstance(reason, str) and reason else "")
            )
            continue

        if not isinstance(conditions, dict):
            unavailable.append(f"{job_id}: measurement_conditions is missing")
            continue

        observations = record.get("measurement_conditions_observations")
        if not isinstance(observations, dict):
            unavailable.append(
                f"{job_id}: per-Pod measurement condition observations are missing"
            )
            continue

        record_known = True
        for pod_key in ("old_pod", "replacement_pod"):
            observation = observations.get(pod_key)
            pod_conditions = (
                observation.get("conditions")
                if isinstance(observation, dict)
                else None
            )
            if not isinstance(pod_conditions, dict):
                unavailable.append(
                    f"{job_id}/{pod_key}: measurement conditions are missing"
                )
                record_known = False
            elif any(
                type(pod_conditions.get(key)) is not type(conditions.get(key))
                or pod_conditions.get(key) != conditions.get(key)
                for key in CONDITION_KEYS
            ):
                unavailable.append(
                    f"{job_id}/{pod_key}: conditions do not match the "
                    "verified common measurement_conditions"
                )
                record_known = False

        for key in CONDITION_KEYS:
            value = conditions.get(key)
            if key in ("failure_scenario", "measurement_method"):
                valid = isinstance(value, str) and bool(value.strip())
            elif key == "test_pause_seconds":
                try:
                    valid = (
                        not isinstance(value, bool)
                        and isinstance(value, (int, float))
                        and math.isfinite(float(value))
                        and value >= 0
                    )
                except OverflowError:
                    valid = False
            else:
                valid = isinstance(value, bool)
            if not valid:
                unavailable.append(
                    f"{job_id}/{key}: condition is missing or invalid"
                )
                record_known = False

        extra_keys = set(conditions) - set(CONDITION_KEYS)
        missing_keys = set(CONDITION_KEYS) - set(conditions)
        if extra_keys or missing_keys:
            unavailable.append(
                f"{job_id}: condition keys differ "
                f"(missing={sorted(missing_keys)}, extra={sorted(extra_keys)})"
            )
            record_known = False

        if record_known:
            known_conditions.append((job_id, conditions))

    if unavailable:
        reason = "Measurement conditions are unknown or incomplete: " + "; ".join(
            unavailable
        )
        return {
            "measurement_conditions_comparable": False,
            "measurement_conditions_reason": reason,
        }

    if not known_conditions:
        return {
            "measurement_conditions_comparable": False,
            "measurement_conditions_reason": (
                "No measurement conditions are available."
            ),
        }

    reference_job, reference = known_conditions[0]
    mismatches = []
    for job_id, conditions in known_conditions[1:]:
        for key in CONDITION_KEYS:
            if conditions[key] != reference[key]:
                mismatches.append(
                    f"{key} differs between {reference_job} "
                    f"({reference[key]!r}) and {job_id} ({conditions[key]!r})"
                )

    if mismatches:
        return {
            "measurement_conditions_comparable": False,
            "measurement_conditions_reason": (
                "Measurement conditions differ across runs: "
                + "; ".join(mismatches)
            ),
        }

    return {
        "measurement_conditions_comparable": True,
        "measurement_conditions_reason": (
            f"All {len(known_conditions)} runs have identical, known "
            "measurement conditions."
        ),
        "measurement_conditions": {
            key: reference[key]
            for key in CONDITION_KEYS
        },
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
        **summarize_measurement_conditions(records),
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
        f"- Measurement conditions comparable: "
        f"{summary['measurement_conditions_comparable']}",
        f"- Measurement conditions: "
        f"{summary['measurement_conditions_reason']}",
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
