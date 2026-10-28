"""Tests for Phase 23 recovery visualization."""

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "phase23-recovery-visualization.py"
spec = importlib.util.spec_from_file_location("phase23_visualization", SCRIPT)
visualization = importlib.util.module_from_spec(spec)
spec.loader.exec_module(visualization)


def conditions(**overrides):
    result = {
        "failure_scenario": "worker_pod_deletion",
        "measurement_method": "kubectl_polling_observation",
        "test_pause_seconds": 30,
        "job_recovery_includes_test_pause": True,
        "worker_readiness_probe": False,
    }
    result.update(overrides)
    return result


def measurement(
    job_id,
    value,
    observed_at="2026-10-28T00:00:00+00:00",
    *,
    status="passed",
    conditions_comparable=True,
    measurement_conditions=None,
):
    record = {
        "job_id": job_id,
        "status": status,
        "observed_at": observed_at,
        "measurements_seconds": {
            metric: value for metric in visualization.METRICS
        },
    }
    if conditions_comparable is not None:
        record.update({
            "measurement_conditions": (
                measurement_conditions or conditions()
            ),
            "measurement_conditions_comparable": conditions_comparable,
            "measurement_conditions_reason": (
                "Conditions match."
                if conditions_comparable
                else "Conditions could not be verified."
            ),
        })
    return record


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def write_phase22_artifacts(input_dir, records, *, phase19=True, phase20=True):
    for index, record in enumerate(records):
        write_json(
            input_dir / "phase18" / f"phase18-record-{index}.json",
            record,
        )
    if phase19:
        write_json(input_dir / "phase19" / "summary.json", {
            "total_runs": len(records),
            "passed_runs": sum(item["status"] == "passed" for item in records),
            "failed_runs": sum(item["status"] == "failed" for item in records),
            "measurement_conditions_comparable": all(
                item.get("measurement_conditions_comparable") is True
                for item in records
                if item["status"] == "passed"
            ),
            "measurement_conditions_reason": "All known conditions match.",
            "measurement_conditions": conditions(),
            "metrics": {
                metric: {
                    "mean": 12.5,
                    "median": 12,
                    "min": 10,
                    "max": 15,
                    "stdev": 2.5,
                }
                for metric in visualization.METRICS
            },
        })
    if phase20:
        write_json(input_dir / "phase20" / "slo-evaluation.json", {
            "overall_status": "PASS",
            "minimum_samples": 3,
            "metrics": {
                metric: {
                    "status": "PASS",
                    "reason": "Mean is within threshold.",
                    "threshold_seconds": 20,
                    "actual_mean_seconds": 12.5,
                }
                for metric in visualization.METRICS
            },
        })
    write_json(input_dir / "phase22-result.json", {
        "status": "PASS",
        "measurement_count": len(records),
    })


def test_generates_static_html_and_json_from_phase22_directory(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [measurement("job-1", 10)])

    analysis = visualization.generate(input_dir, output_dir)

    assert (output_dir / visualization.HTML_NAME).is_file()
    assert (output_dir / visualization.ANALYSIS_NAME).is_file()
    assert analysis["measurement_count"] == 1
    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )
    assert "<svg" in html_text
    assert "PASS" in html_text
    assert "phase19" == analysis["statistics_source"]


def test_cli_generates_reports_from_phase22_directory(tmp_path, capsys):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [measurement("job-1", 10)])

    exit_code = visualization.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
    ])

    assert exit_code == 0
    assert (output_dir / visualization.HTML_NAME).is_file()
    assert "measurement_count" in capsys.readouterr().out


def test_multiple_measurements_are_sorted_by_timestamp(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [
        measurement("later", 20, "2026-10-28T02:00:00+00:00"),
        measurement("earlier", 10, "2026-10-28T01:00:00+00:00"),
    ])

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["ordering"] == "timestamp"
    assert [item["job_id"] for item in analysis["measurements"]] == [
        "earlier",
        "later",
    ]


def test_missing_timestamp_is_reported_as_unknown_order(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [
        measurement("dated", 10),
        measurement("undated", 12, observed_at=None),
    ])

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["ordering"] == "unknown"
    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )
    assert "Measurement order is unknown" in html_text
    assert "file names are not treated as chronology" in html_text


def test_identical_measurement_conditions_are_comparable(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [
        measurement("one", 10),
        measurement("two", 11),
    ])

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["measurement_conditions"]["comparable"] is True
    assert analysis["measurement_conditions"]["conditions"] == conditions()


def test_condition_mismatch_is_not_comparable(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [
        measurement("one", 10),
        measurement(
            "two",
            11,
            measurement_conditions=conditions(test_pause_seconds=0),
        ),
    ])

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["measurement_conditions"]["comparable"] is False
    assert analysis["measurement_conditions"]["conditions"] is None
    assert "differ" in analysis["measurement_conditions"]["reason"]


def test_old_format_data_without_timestamp_or_conditions_is_supported(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_json(input_dir / "phase18" / "phase18-old.json", {
        "job_id": "old",
        "status": "passed",
        "measurements_seconds": {
            metric: 7 for metric in visualization.METRICS
        },
    })
    write_json(input_dir / "phase22-result.json", {"status": "PASS"})

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["measurement_count"] == 1
    assert analysis["ordering"] == "unknown"
    assert analysis["measurement_conditions"]["comparable"] is False
    assert analysis["statistics_source"] == "phase18"
    assert analysis["statistics"]["pod_ready_observed"]["mean"] == 7


def test_zero_measurements_generate_empty_analysis(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    input_dir.mkdir()
    (input_dir / "phase18").mkdir()

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["measurement_count"] == 0
    assert analysis["successful_count"] == 0
    assert analysis["statistics"]["pod_ready_observed"]["mean"] is None
    assert "No valid successful measurements" in (
        output_dir / visualization.HTML_NAME
    ).read_text(encoding="utf-8")


def test_one_sample_has_no_standard_deviation(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(
        input_dir,
        [measurement("one", 7)],
        phase19=False,
        phase20=False,
    )

    analysis = visualization.generate(input_dir, output_dir)

    stats = analysis["statistics"]["pod_ready_observed"]
    assert stats["count"] == 1
    assert stats["mean"] == 7
    assert stats["stdev"] is None


@pytest.mark.parametrize("bad_value", [-1, float("nan"), float("inf"), "bad", None, True])
def test_invalid_or_missing_measurements_are_excluded(tmp_path, bad_value):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    record = measurement("bad", 10)
    record["measurements_seconds"]["pod_ready_observed"] = bad_value
    write_json(input_dir / "phase18" / "phase18-bad.json", record)
    write_json(input_dir / "phase22-result.json", {})

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["statistics"]["pod_ready_observed"]["count"] == 0
    assert analysis["statistics"]["pod_ready_observed"]["mean"] is None
    assert (output_dir / visualization.HTML_NAME).is_file()


@pytest.mark.parametrize("status", ["PASS", "FAIL", "INCONCLUSIVE"])
def test_phase20_evaluation_status_and_reason_are_preserved(tmp_path, status):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [measurement("one", 10)])
    phase20_path = input_dir / "phase20" / "slo-evaluation.json"
    report = json.loads(phase20_path.read_text(encoding="utf-8"))
    report["overall_status"] = status
    report["metrics"]["pod_ready_observed"]["status"] = status
    report["metrics"]["pod_ready_observed"]["reason"] = "Source reason"
    write_json(phase20_path, report)

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["slo_evaluation"]["overall_status"] == status
    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )
    assert status in html_text
    assert "Source reason" in html_text


def test_html_escapes_untrusted_measurement_and_report_strings(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    record = measurement(
        "<script>alert(1)</script>",
        10,
        observed_at=None,
    )
    write_json(input_dir / "phase18" / "phase18-xss.json", record)
    write_json(input_dir / "phase22-result.json", {})
    phase20_path = input_dir / "phase20" / "slo-evaluation.json"
    write_json(phase20_path, {
        "overall_status": "PASS",
        "metrics": {
            metric: {
                "status": "PASS",
                "reason": "<img src=x onerror=alert(1)>",
                "threshold_seconds": 20,
            }
            for metric in visualization.METRICS
        },
    })

    visualization.generate(input_dir, output_dir)

    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )
    assert "<script>" not in html_text
    assert "&lt;script&gt;" in html_text
    assert "<img src=x" not in html_text
    assert "&lt;img" in html_text


def test_existing_output_directory_is_never_overwritten(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    input_dir.mkdir()
    output_dir.mkdir()
    sentinel = output_dir / "keep.txt"
    sentinel.write_text("untouched", encoding="utf-8")

    with pytest.raises(FileExistsError):
        visualization.generate(input_dir, output_dir)

    assert sentinel.read_text(encoding="utf-8") == "untouched"


def test_output_inside_input_is_rejected_after_path_resolution(tmp_path):
    input_dir = tmp_path / "phase22"
    input_dir.mkdir()
    output_dir = input_dir / "nested-analysis"

    with pytest.raises(ValueError, match="must not overlap"):
        visualization.generate(input_dir, output_dir)
    assert not output_dir.exists()


def test_offline_phase22_reports_can_use_external_phase18_directory(tmp_path):
    phase22_dir = tmp_path / "phase22"
    phase18_dir = tmp_path / "phase18"
    output_dir = tmp_path / "analysis"
    phase22_dir.mkdir()
    write_json(phase18_dir / "phase18-one.json", {
        **measurement("one", 9),
    })
    write_json(phase22_dir / "phase22-result.json", {"status": "PASS"})

    analysis = visualization.generate(
        phase22_dir,
        output_dir,
        phase18_dir,
    )

    assert analysis["measurement_count"] == 1
    assert analysis["statistics"]["pod_ready_observed"]["mean"] == 9


def test_phase19_summary_statistics_take_precedence(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [measurement("one", 10)])

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["statistics_source"] == "phase19"
    assert analysis["statistics"]["pod_ready_observed"]["mean"] == 12.5


def test_phase22_failure_status_is_visible_separately_from_phase20(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [measurement("one", 10)])
    write_json(input_dir / "phase22-result.json", {
        "status": "ERROR",
        "evaluation_status": "PASS",
        "partial_evaluation": True,
    })

    analysis = visualization.generate(input_dir, output_dir)
    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )

    assert analysis["phase22_result"]["status"] == "ERROR"
    assert analysis["slo_evaluation"]["overall_status"] == "PASS"
    assert "ERROR" in html_text
    assert "PASS" in html_text


def test_phase18_only_comparable_conditions_are_not_discarded(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(
        input_dir,
        [measurement("one", 10)],
        phase19=False,
        phase20=False,
    )

    analysis = visualization.generate(input_dir, output_dir)

    assert analysis["measurement_conditions"]["comparable"] is True


def test_phase22_subdirectory_output_can_be_created_only_when_enabled(
    tmp_path,
):
    input_dir = tmp_path / "phase22"
    output_dir = input_dir / "phase23"
    input_dir.mkdir()
    write_json(input_dir / "phase22-result.json", {"status": "PASS"})

    with pytest.raises(ValueError, match="must not overlap"):
        visualization.generate(input_dir, output_dir)

    visualization.generate(
        input_dir,
        output_dir,
        allow_output_inside_input=True,
    )

    assert (output_dir / visualization.HTML_NAME).is_file()


def test_partial_phase22_result_is_explained_in_html():
    analysis = {
        "measurement_count": 1,
        "successful_count": 1,
        "failed_count": 0,
        "ordering": "unknown",
        "measurements": [],
        "statistics": visualization.calculate_statistics([]),
        "statistics_source": "phase19",
        "measurement_conditions": {
            "comparable": False,
            "conditions": None,
            "reason": "conditions unknown",
        },
        "slo_evaluation": {
            "overall_status": "PASS",
            "metrics": {},
        },
        "phase22_result": {
            "status": "ERROR",
            "partial_evaluation": True,
        },
        "limitations": [],
    }

    html_text = visualization.render_html(analysis)

    assert "Partial evaluation" in html_text
    assert "not a full successful run" in html_text


def test_html_is_self_contained_without_external_assets(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [measurement("one", 10)])

    visualization.generate(input_dir, output_dir)

    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )
    assert "<svg" in html_text
    assert "https://" not in html_text
    assert "<script" not in html_text


def test_html_css_and_svg_labels_are_readable_and_responsive(tmp_path):
    input_dir = tmp_path / "phase22"
    output_dir = tmp_path / "analysis"
    write_phase22_artifacts(input_dir, [
        measurement("job-1", 10, "2026-10-28T01:00:00+00:00"),
    ])

    visualization.generate(input_dir, output_dir)

    html_text = (output_dir / visualization.HTML_NAME).read_text(
        encoding="utf-8"
    )
    assert "grid-template-columns:repeat(2,minmax(0,1fr))" in html_text
    assert "@media(max-width:700px)" in html_text
    assert ".grid{grid-template-columns:minmax(0,1fr)}" in html_text
    assert "white-space:pre-wrap" in html_text
    assert "overflow-wrap:anywhere" in html_text
    assert 'font-size="12"' in html_text
    assert 'font-size="13"' in html_text
    assert 'font-size="14"' in html_text
