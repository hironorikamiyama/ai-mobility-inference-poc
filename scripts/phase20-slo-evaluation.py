#!/usr/bin/env python3
"""Phase 20: Evaluate Phase 19 recovery statistics against example SLOs."""

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

METRICS = {
    "pod_recreated_observed": "Pod recreation observed",
    "pod_ready_observed": "Pod Ready observed",
    "job_recovered_observed": "Job recovery observed",
    "ready_to_recovery": "Ready to recovery",
}

CONDITION_KEYS = {
    "failure_scenario",
    "measurement_method",
    "test_pause_seconds",
    "job_recovery_includes_test_pause",
    "worker_readiness_probe",
}

DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "phase20-slo.json"
)


def reject_json_constant(value):
    raise ValueError(f"Invalid JSON numeric constant: {value}")


def load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file, parse_constant=reject_json_constant)


def is_finite_nonnegative_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value)) and value >= 0
    except OverflowError:
        return False


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("SLO configuration must be a JSON object")

    description = config.get("description")
    if not isinstance(description, str) or not description.strip():
        raise ValueError("Configuration description must be a non-empty string")

    minimum_samples = config.get("minimum_samples")
    if (
        isinstance(minimum_samples, bool)
        or not isinstance(minimum_samples, int)
        or minimum_samples < 1
    ):
        raise ValueError("minimum_samples must be a positive integer")

    conditions = config.get("measurement_conditions")
    if not isinstance(conditions, dict) or set(conditions) != CONDITION_KEYS:
        raise ValueError(
            "measurement_conditions must define exactly: "
            + ", ".join(sorted(CONDITION_KEYS))
        )
    for key in ("failure_scenario", "measurement_method"):
        if not isinstance(conditions[key], str) or not conditions[key].strip():
            raise ValueError(f"measurement_conditions.{key} must be a non-empty string")
    if not is_finite_nonnegative_number(conditions["test_pause_seconds"]):
        raise ValueError(
            "measurement_conditions.test_pause_seconds must be finite and non-negative"
        )
    for key in ("job_recovery_includes_test_pause", "worker_readiness_probe"):
        if not isinstance(conditions[key], bool):
            raise ValueError(f"measurement_conditions.{key} must be a boolean")

    metric_config = config.get("metrics")
    if not isinstance(metric_config, dict) or set(metric_config) != set(METRICS):
        raise ValueError(
            "metrics must define exactly: " + ", ".join(METRICS)
        )

    required_count = 0
    for name, settings in metric_config.items():
        if not isinstance(settings, dict):
            raise ValueError(f"metrics.{name} must be an object")
        if set(settings) != {"threshold_seconds", "required"}:
            raise ValueError(
                f"metrics.{name} must define threshold_seconds and required"
            )
        if not is_finite_nonnegative_number(settings["threshold_seconds"]):
            raise ValueError(
                f"metrics.{name}.threshold_seconds must be finite and non-negative"
            )
        if not isinstance(settings["required"], bool):
            raise ValueError(f"metrics.{name}.required must be a boolean")
        required_count += settings["required"]

    if required_count == 0:
        raise ValueError("At least one metric must be required")

    return config


def load_config(path):
    return validate_config(load_json(path))


def conditions_match(expected, observed):
    if not isinstance(observed, dict) or set(observed) != set(expected):
        return False
    for key, expected_value in expected.items():
        observed_value = observed[key]
        if (
            isinstance(expected_value, bool)
            or isinstance(observed_value, bool)
            or not isinstance(expected_value, (int, float))
            or not isinstance(observed_value, (int, float))
        ):
            if (
                type(observed_value) is not type(expected_value)
                or observed_value != expected_value
            ):
                return False
        elif not is_finite_nonnegative_number(observed_value) or (
            float(observed_value) != float(expected_value)
        ):
            return False
    return True


def evaluate(summary, config):
    validate_config(config)
    if not isinstance(summary, dict):
        raise ValueError("Phase 19 summary must be a JSON object")

    expected_conditions = config["measurement_conditions"]
    observed_conditions = summary.get("measurement_conditions")
    conditions_comparable = summary.get("measurement_conditions_comparable")
    conditions_reason = summary.get("measurement_conditions_reason")
    conditions_agree = conditions_match(
        expected_conditions,
        observed_conditions,
    )
    if conditions_comparable is False:
        conditions_agree = False
    elif conditions_comparable is not None and conditions_comparable is not True:
        conditions_agree = False

    if (
        conditions_comparable is not False
        or not isinstance(conditions_reason, str)
        or not conditions_reason.strip()
    ):
        conditions_reason = (
            "Measurement conditions are missing or do not exactly match "
            "the configured conditions."
        )

    sample_count = summary.get("passed_runs")
    if (
        isinstance(sample_count, bool)
        or not isinstance(sample_count, int)
        or sample_count < 0
    ):
        sample_count = None

    summary_metrics = summary.get("metrics")
    if not isinstance(summary_metrics, dict):
        summary_metrics = {}

    results = {}
    for name, label in METRICS.items():
        settings = config["metrics"][name]
        result = {
            "label": label,
            "status": "INCONCLUSIVE",
            "required": settings["required"],
            "actual_mean_seconds": None,
            "threshold_seconds": settings["threshold_seconds"],
            "sample_count": sample_count,
            "reason": "",
        }

        if not conditions_agree:
            result["reason"] = conditions_reason
        elif sample_count is None:
            result["reason"] = "The passed-run sample count is missing or invalid."
        elif sample_count < config["minimum_samples"]:
            result["reason"] = (
                f"Only {sample_count} passed samples are available; "
                f"{config['minimum_samples']} are required."
            )
        else:
            metric_summary = summary_metrics.get(name)
            actual = (
                metric_summary.get("mean")
                if isinstance(metric_summary, dict)
                else None
            )
            if not is_finite_nonnegative_number(actual):
                result["reason"] = (
                    "A finite, non-negative mean measurement is unavailable."
                )
            else:
                result["actual_mean_seconds"] = actual
                if actual <= settings["threshold_seconds"]:
                    result["status"] = "PASS"
                    result["reason"] = (
                        f"Mean {actual:g}s is at or below the "
                        f"{settings['threshold_seconds']:g}s threshold."
                    )
                else:
                    result["status"] = "FAIL"
                    result["reason"] = (
                        f"Mean {actual:g}s exceeds the "
                        f"{settings['threshold_seconds']:g}s threshold."
                    )

        results[name] = result

    required_results = [
        result for result in results.values()
        if result["required"]
    ]
    if any(result["status"] == "FAIL" for result in required_results):
        overall_status = "FAIL"
    elif any(
        result["status"] == "INCONCLUSIVE"
        for result in required_results
    ):
        overall_status = "INCONCLUSIVE"
    else:
        overall_status = "PASS"

    return {
        "report": "Phase 20 SLO evaluation",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "assessment": config["description"],
        "evaluation_statistic": "mean",
        "minimum_samples": config["minimum_samples"],
        "overall_status": overall_status,
        "measurement_conditions": {
            "expected": expected_conditions,
            "observed": observed_conditions,
            "match": conditions_agree,
        },
        "sample_count": sample_count,
        "metrics": results,
        "interpretation": (
            "Thresholds are illustrative PoC values and do not establish "
            "a production SLO."
        ),
    }


def write_markdown(report, output_path):
    conditions = report["measurement_conditions"]
    lines = [
        "# Phase 20 Recovery SLO Evaluation",
        "",
        f"- Overall result: **{report['overall_status']}**",
        f"- Assessment: {report['assessment']}",
        f"- Statistic: {report['evaluation_statistic']}",
        f"- Minimum samples: {report['minimum_samples']}",
        f"- Passed samples: "
        f"{report['sample_count'] if report['sample_count'] is not None else 'N/A'}",
        f"- Measurement conditions match: {conditions['match']}",
        "",
        "## Results",
        "",
        "| Metric | Required | Result | Mean (s) | Threshold (s) | "
        "Samples | Reason |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]

    for result in report["metrics"].values():
        actual = result["actual_mean_seconds"]
        actual_text = f"{actual:g}" if actual is not None else "N/A"
        reason = result["reason"].replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| {result['label']} | {result['required']} | "
            f"{result['status']} | {actual_text} | "
            f"{result['threshold_seconds']:g} | "
            f"{result['sample_count'] if result['sample_count'] is not None else 'N/A'} "
            f"| {reason} |"
        )

    lines.extend([
        "",
        "## Measurement conditions",
        "",
        f"- Expected: `{json.dumps(conditions['expected'], sort_keys=True)}`",
        f"- Observed: `{json.dumps(conditions['observed'], sort_keys=True)}`",
        "",
        report["interpretation"],
        "",
    ])
    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_reports(report, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "slo-evaluation.json"
    markdown_path = output_dir / "slo-evaluation.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(report, markdown_path)
    return json_path, markdown_path


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate Phase 19 recovery statistics against example SLOs."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/phase19/summary.json"),
        help="Phase 19 summary.json path",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="SLO evaluation JSON configuration",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/phase20"),
        help="Directory for Phase 20 JSON and Markdown reports",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    summary = load_json(args.input)
    report = evaluate(summary, config)
    json_path, markdown_path = write_reports(report, args.output)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"JSON: {json_path}")
    print(f"Markdown: {markdown_path}")


if __name__ == "__main__":
    main()
