"""Tests for Phase 19 recovery report aggregation."""

import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "phase19-recovery-report.py"
)

spec = importlib.util.spec_from_file_location(
    "phase19_recovery_report", SCRIPT
)
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def make_record(job_id, status="passed", value=5.0):
    record = {
        "job_id": job_id,
        "status": status,
    }
    if status == "passed":
        record["measurements_seconds"] = {
            key: value for key in report.METRICS
        }
    return record


def make_conditions(**overrides):
    conditions = {
        "failure_scenario": "worker_pod_deletion",
        "measurement_method": "kubectl_polling_observation",
        "test_pause_seconds": 30.0,
        "job_recovery_includes_test_pause": True,
        "worker_readiness_probe": False,
    }
    conditions.update(overrides)
    return conditions


def add_verified_conditions(record, conditions):
    record["measurement_conditions"] = conditions
    record["measurement_conditions_comparable"] = True
    record["measurement_conditions_reason"] = (
        "All required measurement conditions are known and identical."
    )
    record["measurement_conditions_observations"] = {
        key: {
            "pod_name": pod_name,
            "collected_at": "2026-10-27T00:00:00+00:00",
            "conditions": dict(conditions),
            "reasons": {},
        }
        for key, pod_name in (
            ("old_pod", "worker-old"),
            ("replacement_pod", "worker-new"),
        )
    }


def test_summarize_three_passed_records():
    records = [
        make_record("job1", value=4.0),
        make_record("job2", value=6.0),
        make_record("job3", value=8.0),
    ]

    summary = report.summarize(records)

    assert summary["total_runs"] == 3
    assert summary["passed_runs"] == 3
    assert summary["failed_runs"] == 0

    result = summary["metrics"]["pod_ready_observed"]
    assert result["mean"] == 6.0
    assert result["median"] == 6.0
    assert result["min"] == 4.0
    assert result["max"] == 8.0
    assert result["stdev"] == 2.0


def test_summarize_identical_known_conditions():
    records = [
        make_record("job1"),
        make_record("job2"),
    ]
    for record in records:
        add_verified_conditions(record, make_conditions())

    summary = report.summarize(records)

    assert summary["measurement_conditions_comparable"] is True
    assert summary["measurement_conditions"] == make_conditions()
    assert "identical, known" in summary["measurement_conditions_reason"]


def test_summarize_condition_mismatch_as_incomparable():
    first = make_record("job1")
    second = make_record("job2")
    add_verified_conditions(first, make_conditions())
    add_verified_conditions(
        second,
        make_conditions(test_pause_seconds=0),
    )

    summary = report.summarize([first, second])

    assert summary["measurement_conditions_comparable"] is False
    assert "measurement_conditions" not in summary
    assert "test_pause_seconds differs" in summary["measurement_conditions_reason"]


def test_summarize_missing_conditions_as_incomparable():
    record = make_record("legacy")

    summary = report.summarize([record])

    assert summary["measurement_conditions_comparable"] is False
    assert "measurement_conditions" not in summary
    assert "not verified comparable" in summary["measurement_conditions_reason"]


def test_summarize_unknown_condition_includes_collection_reason():
    record = make_record("unknown")
    record["measurement_conditions_comparable"] = False
    record["measurement_conditions_reason"] = (
        "Measurement conditions are unknown: replacement_pod/test_pause_seconds: "
        "Environment variable is not set."
    )

    summary = report.summarize([record])

    assert summary["measurement_conditions_comparable"] is False
    assert "measurement_conditions" not in summary
    assert "Environment variable is not set." in (
        summary["measurement_conditions_reason"]
    )


def test_summarize_respects_phase18_incomparable_flag():
    record = make_record("mismatch")
    add_verified_conditions(record, make_conditions())
    record["measurement_conditions_comparable"] = False
    record["measurement_conditions_reason"] = (
        "Measurement conditions differ between old and replacement Pods: "
        "worker_readiness_probe"
    )

    summary = report.summarize([record])

    assert summary["measurement_conditions_comparable"] is False
    assert "measurement_conditions" not in summary
    assert "worker_readiness_probe" in summary["measurement_conditions_reason"]


def test_legacy_record_with_only_one_pod_conditions_is_not_comparable():
    record = make_record("phase21-legacy")
    record["measurement_conditions"] = make_conditions()

    summary = report.summarize([record])

    assert summary["measurement_conditions_comparable"] is False
    assert "measurement_conditions" not in summary


def test_failed_record_excluded_from_statistics():
    records = [
        make_record("success", value=5.0),
        make_record("failed", status="failed"),
    ]

    summary = report.summarize(records)

    assert summary["total_runs"] == 2
    assert summary["passed_runs"] == 1
    assert summary["failed_runs"] == 1
    assert summary["metrics"]["pod_ready_observed"]["mean"] == 5.0
    assert summary["metrics"]["pod_ready_observed"]["stdev"] is None


def test_empty_records_raise_error():
    with pytest.raises(ValueError, match="No Phase 18"):
        report.summarize([])


def test_missing_measurement_is_rejected():
    record = make_record("missing")
    del record["measurements_seconds"]["pod_ready_observed"]

    with pytest.raises((KeyError, ValueError)):
        report.summarize([record])


def test_non_numeric_measurement_is_rejected():
    record = make_record("invalid")
    record["measurements_seconds"]["pod_ready_observed"] = "invalid"

    with pytest.raises((TypeError, ValueError)):
        report.summarize([record])


def test_all_failed_records():
    summary = report.summarize([
        make_record("failed1", status="failed")
    ])

    assert summary["passed_runs"] == 0
    assert summary["failed_runs"] == 1
    assert all(
        value is None
        for value in summary["metrics"].values()
    )


def test_load_records_ignores_summary_json(tmp_path):
    measurement = make_record("job1")
    (tmp_path / "phase18-job1.json").write_text(
        json.dumps(measurement),
        encoding="utf-8",
    )
    (tmp_path / "summary.json").write_text(
        '{"total_runs": 1}',
        encoding="utf-8",
    )

    records = report.load_records(tmp_path)

    assert len(records) == 1
    assert records[0]["job_id"] == "job1"


def test_csv_and_markdown_outputs(tmp_path):
    records = [make_record("job1")]
    summary = report.summarize(records)

    csv_path = tmp_path / "results.csv"
    md_path = tmp_path / "report.md"

    report.write_csv(records, csv_path)
    report.write_markdown(summary, md_path)

    csv_text = csv_path.read_text(encoding="utf-8")
    md_text = md_path.read_text(encoding="utf-8")

    assert "job1,passed" in csv_text
    assert "Phase 19 Kubernetes Recovery Report" in md_text
    assert "Pod Ready observed" in md_text
    assert "30-second test-only pause" in md_text


@pytest.mark.parametrize(
    "invalid_value",
    [
        -1.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        True,
        None,
    ],
)
def test_invalid_measurement_values_are_rejected(invalid_value):
    record = make_record("invalid-value")
    record["measurements_seconds"][
        "pod_ready_observed"
    ] = invalid_value

    with pytest.raises(ValueError, match="Invalid measurement"):
        report.summarize([record])


def test_zero_seconds_is_valid():
    record = make_record("zero", value=0.0)
    summary = report.summarize([record])

    assert summary["metrics"]["pod_ready_observed"]["mean"] == 0.0
