"""Tests for Phase 20 recovery SLO evaluation."""

import importlib.util
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "phase20-slo-evaluation.py"
CONFIG_PATH = ROOT / "config" / "phase20-slo.json"

spec = importlib.util.spec_from_file_location("phase20_slo_evaluation", SCRIPT)
slo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(slo)


def load_default_config():
    return slo.load_config(CONFIG_PATH)


def make_summary(config=None, value=5.0, sample_count=3, conditions=None):
    config = config or load_default_config()
    return {
        "total_runs": sample_count,
        "passed_runs": sample_count,
        "failed_runs": 0,
        "measurement_conditions": (
            config["measurement_conditions"] if conditions is None else conditions
        ),
        "metrics": {
            name: {"mean": value}
            for name in config["metrics"]
        },
    }


def test_matching_conditions_and_threshold_values_pass():
    config = load_default_config()
    summary = make_summary(config, value=0)
    summary["metrics"]["pod_ready_observed"]["mean"] = (
        config["metrics"]["pod_ready_observed"]["threshold_seconds"]
    )

    report = slo.evaluate(summary, config)

    assert report["overall_status"] == "PASS"
    assert report["metrics"]["pod_ready_observed"]["status"] == "PASS"
    assert report["metrics"]["pod_ready_observed"]["actual_mean_seconds"] == 15


def test_required_metric_over_threshold_fails():
    config = load_default_config()
    summary = make_summary(config, value=16)

    report = slo.evaluate(summary, config)

    assert report["metrics"]["pod_ready_observed"]["status"] == "FAIL"
    assert report["overall_status"] == "FAIL"


def test_phase19_summary_without_conditions_is_inconclusive():
    config = load_default_config()
    summary = make_summary(config)
    del summary["measurement_conditions"]

    report = slo.evaluate(summary, config)

    assert report["overall_status"] == "INCONCLUSIVE"
    assert all(
        metric["status"] == "INCONCLUSIVE"
        for metric in report["metrics"].values()
    )
    assert report["metrics"]["pod_ready_observed"]["actual_mean_seconds"] is None


def test_conditions_mismatch_is_inconclusive():
    config = load_default_config()
    conditions = dict(config["measurement_conditions"])
    conditions["test_pause_seconds"] = 0

    report = slo.evaluate(
        make_summary(config, conditions=conditions),
        config,
    )

    assert report["overall_status"] == "INCONCLUSIVE"
    assert report["measurement_conditions"]["match"] is False


def test_equivalent_numeric_condition_encodings_match():
    config = load_default_config()
    conditions = dict(config["measurement_conditions"])
    conditions["test_pause_seconds"] = 30.0

    report = slo.evaluate(
        make_summary(config, conditions=conditions),
        config,
    )

    assert report["measurement_conditions"]["match"] is True
    assert report["overall_status"] == "PASS"


def test_missing_metric_is_inconclusive():
    config = load_default_config()
    summary = make_summary(config)
    del summary["metrics"]["pod_ready_observed"]

    report = slo.evaluate(summary, config)

    assert report["metrics"]["pod_ready_observed"]["status"] == "INCONCLUSIVE"
    assert report["overall_status"] == "INCONCLUSIVE"


def test_insufficient_sample_count_is_inconclusive():
    config = load_default_config()
    summary = make_summary(config, sample_count=2)

    report = slo.evaluate(summary, config)

    assert report["metrics"]["pod_ready_observed"]["status"] == "INCONCLUSIVE"
    assert report["metrics"]["pod_ready_observed"]["sample_count"] == 2


def test_required_fail_takes_precedence_over_other_inconclusive_metrics():
    config = load_default_config()
    summary = make_summary(config, value=16)
    del summary["metrics"]["job_recovered_observed"]

    report = slo.evaluate(summary, config)

    assert report["metrics"]["job_recovered_observed"]["status"] == "INCONCLUSIVE"
    assert report["metrics"]["pod_ready_observed"]["status"] == "FAIL"
    assert report["overall_status"] == "FAIL"


@pytest.mark.parametrize(
    "invalid_threshold",
    [-1, float("nan"), float("inf"), float("-inf"), True],
)
def test_invalid_thresholds_are_rejected(invalid_threshold):
    config = load_default_config()
    config["metrics"]["pod_ready_observed"][
        "threshold_seconds"
    ] = invalid_threshold

    with pytest.raises(ValueError, match="threshold_seconds"):
        slo.validate_config(config)


@pytest.mark.parametrize(
    "invalid_pause",
    [-1, float("nan"), float("inf"), float("-inf"), True],
)
def test_invalid_test_pause_is_rejected(invalid_pause):
    config = load_default_config()
    config["measurement_conditions"]["test_pause_seconds"] = invalid_pause

    with pytest.raises(ValueError, match="test_pause_seconds"):
        slo.validate_config(config)


@pytest.mark.parametrize("invalid_minimum", [0, -1, 1.5, True])
def test_invalid_minimum_samples_is_rejected(invalid_minimum):
    config = load_default_config()
    config["minimum_samples"] = invalid_minimum

    with pytest.raises(ValueError, match="minimum_samples"):
        slo.validate_config(config)


@pytest.mark.parametrize(
    "invalid_measurement",
    [-1, float("nan"), float("inf"), float("-inf"), True, None, "invalid"],
)
def test_invalid_measurement_is_inconclusive(invalid_measurement):
    config = load_default_config()
    summary = make_summary(config)
    summary["metrics"]["pod_ready_observed"]["mean"] = invalid_measurement

    report = slo.evaluate(summary, config)

    assert report["metrics"]["pod_ready_observed"]["status"] == "INCONCLUSIVE"


def test_zero_threshold_and_measurement_are_valid_boundary_values():
    config = load_default_config()
    config["metrics"]["pod_ready_observed"]["threshold_seconds"] = 0
    summary = make_summary(config, value=0)

    report = slo.evaluate(summary, config)

    assert report["metrics"]["pod_ready_observed"]["status"] == "PASS"


def test_reports_include_json_and_markdown_without_overwriting_input(tmp_path):
    config = load_default_config()
    summary = make_summary(config, value=5)
    input_path = tmp_path / "summary.json"
    input_path.write_text(json.dumps(summary), encoding="utf-8")
    before = input_path.read_bytes()
    report = slo.evaluate(summary, config)

    json_path, markdown_path = slo.write_reports(report, tmp_path / "phase20")

    assert json.loads(json_path.read_text(encoding="utf-8"))["overall_status"] == "PASS"
    assert "Phase 20 Recovery SLO Evaluation" in markdown_path.read_text(
        encoding="utf-8"
    )
    assert input_path.read_bytes() == before


def test_markdown_preserves_space_in_condition_mismatch_reason(tmp_path):
    config = load_default_config()
    conditions = dict(config["measurement_conditions"])
    conditions["test_pause_seconds"] = 0
    report = slo.evaluate(
        make_summary(config, conditions=conditions),
        config,
    )
    markdown_path = tmp_path / "slo-evaluation.md"

    slo.write_markdown(report, markdown_path)

    markdown = markdown_path.read_text(encoding="utf-8")
    assert "do not exactly match the configured conditions." in markdown
    assert "do not exactlymatch" not in markdown


def test_cli_writes_both_report_formats(tmp_path, monkeypatch, capsys):
    config = load_default_config()
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(make_summary(config, value=5)),
        encoding="utf-8",
    )
    output_dir = tmp_path / "reports"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--input",
            str(summary_path),
            "--config",
            str(CONFIG_PATH),
            "--output",
            str(output_dir),
        ],
    )

    slo.main()

    assert (output_dir / "slo-evaluation.json").is_file()
    assert (output_dir / "slo-evaluation.md").is_file()
    assert "JSON:" in capsys.readouterr().out


def test_non_finite_json_constant_is_rejected(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text('{"value": NaN}', encoding="utf-8")

    with pytest.raises(ValueError, match="NaN"):
        slo.load_json(path)


def test_phase19_summary_schema_remains_compatible():
    phase19_script = ROOT / "scripts" / "phase19-recovery-report.py"
    phase19_spec = importlib.util.spec_from_file_location(
        "phase19_recovery_report_for_phase20_test",
        phase19_script,
    )
    phase19 = importlib.util.module_from_spec(phase19_spec)
    phase19_spec.loader.exec_module(phase19)
    phase19_summary = phase19.summarize([
        {
            "job_id": f"job-{index}",
            "status": "passed",
            "measurements_seconds": {
                name: 5 for name in phase19.METRICS
            },
        }
        for index in range(3)
    ])

    report = slo.evaluate(phase19_summary, load_default_config())

    assert report["overall_status"] == "INCONCLUSIVE"
    assert "measurement_conditions" not in phase19_summary


def summarize_phase19_records(config, *, value, include_conditions=True):
    phase19_script = ROOT / "scripts" / "phase19-recovery-report.py"
    phase19_spec = importlib.util.spec_from_file_location(
        "phase19_recovery_report_for_phase20_chain_test",
        phase19_script,
    )
    phase19 = importlib.util.module_from_spec(phase19_spec)
    phase19_spec.loader.exec_module(phase19)

    records = []
    for index in range(3):
        record = {
            "job_id": f"job-{index}",
            "status": "passed",
            "measurements_seconds": {
                name: value for name in phase19.METRICS
            },
            "measurement_conditions_comparable": True,
            "measurement_conditions_reason": (
                "All required measurement conditions are known and identical."
            ),
            "measurement_conditions_observations": {
                key: {
                    "pod_name": pod_name,
                    "collected_at": "2026-10-27T00:00:00+00:00",
                    "conditions": dict(config["measurement_conditions"]),
                    "reasons": {},
                }
                for key, pod_name in (
                    ("old_pod", "worker-old"),
                    ("replacement_pod", "worker-new"),
                )
            },
        }
        if include_conditions:
            record["measurement_conditions"] = dict(
                config["measurement_conditions"]
            )
        else:
            record.pop("measurement_conditions_observations")
            record["measurement_conditions_comparable"] = False
            record["measurement_conditions_reason"] = (
                "measurement_conditions is missing"
            )
        records.append(record)
    return phase19.summarize(records)


def test_phase19_matching_conditions_flow_to_phase20_pass():
    config = load_default_config()
    summary = summarize_phase19_records(config, value=5)

    report = slo.evaluate(summary, config)

    assert summary["measurement_conditions_comparable"] is True
    assert report["overall_status"] == "PASS"


def test_phase19_matching_conditions_flow_to_phase20_fail():
    config = load_default_config()
    summary = summarize_phase19_records(config, value=100)

    report = slo.evaluate(summary, config)

    assert summary["measurement_conditions_comparable"] is True
    assert report["overall_status"] == "FAIL"


def test_phase19_unknown_conditions_flow_to_phase20_inconclusive():
    config = load_default_config()
    summary = summarize_phase19_records(
        config,
        value=1,
        include_conditions=False,
    )

    report = slo.evaluate(summary, config)

    assert summary["measurement_conditions_comparable"] is False
    assert report["overall_status"] == "INCONCLUSIVE"
    assert "measurement_conditions is missing" in (
        report["metrics"]["pod_ready_observed"]["reason"]
    )


def test_non_required_fail_does_not_override_required_passes():
    config = load_default_config()
    summary = make_summary(config, value=5)
    summary["metrics"]["ready_to_recovery"]["mean"] = 100

    report = slo.evaluate(summary, config)

    assert report["metrics"]["ready_to_recovery"]["status"] == "FAIL"
    assert report["overall_status"] == "PASS"
