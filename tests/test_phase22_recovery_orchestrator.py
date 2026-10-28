"""Tests for Phase 22 recovery orchestration."""

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "phase22-recovery-orchestrator.py"
CONFIG = ROOT / "config" / "phase20-slo.json"

spec = importlib.util.spec_from_file_location("phase22_orchestrator", SCRIPT)
orchestrator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orchestrator)


def conditions():
    return {
        "failure_scenario": "worker_pod_deletion",
        "measurement_method": "kubectl_polling_observation",
        "test_pause_seconds": 30,
        "job_recovery_includes_test_pause": True,
        "worker_readiness_probe": False,
    }


def record(job_id, value=1, *, include_conditions=True, status="passed"):
    data = {
        "job_id": job_id,
        "status": status,
    }
    if status == "passed":
        data["measurements_seconds"] = {
            "pod_recreated_observed": value,
            "pod_ready_observed": value,
            "job_recovered_observed": value,
            "ready_to_recovery": value,
        }
    if include_conditions:
        observed_conditions = conditions()
        data.update({
            "measurement_conditions": observed_conditions,
            "measurement_conditions_comparable": True,
            "measurement_conditions_reason": (
                "All required measurement conditions are known and identical."
            ),
            "measurement_conditions_observations": {
                key: {
                    "pod_name": name,
                    "collected_at": "2026-10-28T00:00:00+00:00",
                    "conditions": dict(observed_conditions),
                    "reasons": {},
                }
                for key, name in (
                    ("old_pod", "worker-old"),
                    ("replacement_pod", "worker-new"),
                )
            },
        })
    else:
        data.update({
            "measurement_conditions_comparable": False,
            "measurement_conditions_reason": (
                "measurement conditions are not verified comparable"
            ),
        })
    return data


def write_measurements(input_dir, *, value=1, include_conditions=True):
    input_dir.mkdir(parents=True)
    for index in range(3):
        (input_dir / f"phase18-job-{index}.json").write_text(
            json.dumps(record(
                f"job-{index}",
                value=value,
                include_conditions=include_conditions,
            )),
            encoding="utf-8",
        )


def test_offline_mode_runs_phase19_and_phase20(tmp_path):
    input_dir = tmp_path / "measurements"
    output_dir = tmp_path / "new-output"
    write_measurements(input_dir)

    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    assert exit_code == 0
    assert result["mode"] == "offline"
    assert result["status"] == "PASS"
    assert result["measurement_count"] == 3
    assert json.loads(
        (output_dir / "phase20/slo-evaluation.json").read_text(
            encoding="utf-8"
        )
    )["overall_status"] == "PASS"
    assert (output_dir / "phase19/recovery-results.csv").is_file()
    assert (output_dir / "phase19/recovery-report.md").is_file()
    assert not (output_dir / "phase23").exists()


@pytest.mark.parametrize(
    ("value", "include_conditions", "expected_status", "expected_exit"),
    [
        (1, True, "PASS", 0),
        (100, True, "FAIL", 1),
        (1, False, "INCONCLUSIVE", 2),
    ],
)
def test_offline_mode_propagates_evaluation(
    tmp_path,
    value,
    include_conditions,
    expected_status,
    expected_exit,
):
    input_dir = tmp_path / "measurements"
    output_dir = tmp_path / "new-output"
    write_measurements(
        input_dir,
        value=value,
        include_conditions=include_conditions,
    )

    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    assert exit_code == expected_exit
    assert result["status"] == expected_status


def test_missing_input_is_recorded_as_error(tmp_path):
    output_dir = tmp_path / "new-output"

    exit_code = orchestrator.main([
        "--input",
        str(tmp_path / "missing"),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    assert exit_code == 3
    assert result["status"] == "ERROR"
    assert "input directory does not exist" in result["execution_error"]


def test_existing_output_path_is_never_overwritten(tmp_path):
    input_dir = tmp_path / "measurements"
    output_dir = tmp_path / "existing-output"
    write_measurements(input_dir)
    output_dir.mkdir()
    sentinel = output_dir / "keep.json"
    sentinel.write_text('{"keep": true}\n', encoding="utf-8")

    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    assert exit_code == 3
    assert sentinel.read_text(encoding="utf-8") == '{"keep": true}\n'
    assert not (output_dir / orchestrator.RESULT_NAME).exists()


def test_offline_input_output_overlap_is_rejected_without_writing(tmp_path):
    input_dir = tmp_path / "measurements"
    write_measurements(input_dir)
    output_dir = input_dir / "reports"

    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    assert exit_code == 3
    assert not output_dir.exists()


def test_offline_input_output_overlap_through_input_symlink_is_rejected(
    tmp_path,
):
    input_dir = tmp_path / "measurements"
    write_measurements(input_dir)
    input_alias = tmp_path / "measurements-alias"
    input_alias.symlink_to(input_dir, target_is_directory=True)
    output_dir = input_dir / "reports"

    exit_code = orchestrator.main([
        "--input",
        str(input_alias),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    assert exit_code == 3
    assert not output_dir.exists()


def test_offline_input_output_overlap_through_output_symlink_is_rejected(
    tmp_path,
):
    input_dir = tmp_path / "measurements"
    write_measurements(input_dir)
    output_parent_alias = tmp_path / "measurements-alias"
    output_parent_alias.symlink_to(input_dir, target_is_directory=True)
    output_dir = output_parent_alias / "nested"

    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    assert exit_code == 3
    assert not (input_dir / "nested").exists()


def test_cluster_mode_requires_confirmation_context_and_namespace():
    with pytest.raises(SystemExit) as exc_info:
        orchestrator.main(["--mode", "cluster"])

    assert exc_info.value.code == 2


def test_cluster_run_count_is_bounded():
    with pytest.raises(SystemExit) as exc_info:
        orchestrator.main([
            "--mode",
            "cluster",
            "--confirm-cluster-run",
            "--expected-context",
            "kind-poc",
            "--namespace",
            "ai-mobility-poc",
            "--runs",
            str(orchestrator.MAX_CLUSTER_RUNS + 1),
        ])

    assert exc_info.value.code == 2


def fake_cluster_kubectl(
    *,
    current_context="kind-poc",
    configured_namespace="ai-mobility-poc",
    normal_replicas=0,
    normal_pods=None,
    test_replicas=1,
    test_pod_ready=True,
    pending="0\n",
):
    if normal_pods is None:
        normal_pods = []

    def run(context, *args):
        if args == ("config", "current-context"):
            return current_context
        if args == ("config", "view", "--minify", "-o", "json"):
            return json.dumps({
                "contexts": [{
                    "name": current_context,
                    "context": {"namespace": configured_namespace},
                }]
            })
        if args[:2] == ("get", "namespace"):
            return json.dumps({"metadata": {"name": args[2]}})
        if args[:6] == (
            "-n",
            "ai-mobility-poc",
            "get",
            "pods",
            "-l",
            "app=inference-worker",
        ):
            return json.dumps({"items": normal_pods})
        if args[:6] == (
            "-n",
            "ai-mobility-poc",
            "get",
            "pods",
            "-l",
            "app=phase17-worker",
        ):
            return json.dumps({
                "items": [{
                    "metadata": {"name": "phase17-worker-abc"},
                    "status": {
                        "conditions": [{
                            "type": "Ready",
                            "status": "True" if test_pod_ready else "False",
                        }]
                    },
                }]
            })
        if args[:4] == ("-n", "ai-mobility-poc", "get", "deployment"):
            name = args[4]
            replicas = normal_replicas if name == "inference-worker" else test_replicas
            return json.dumps({
                "metadata": {"name": name},
                "spec": {"replicas": replicas},
                "status": {
                    "replicas": replicas,
                    "readyReplicas": replicas,
                    "availableReplicas": replicas,
                },
            })
        if "XPENDING" in args:
            return pending
        raise AssertionError(f"Unexpected kubectl call: {context}, {args}")

    return run


def test_cluster_preflight_accepts_confirmed_safe_state():
    state = orchestrator.check_cluster_target(
        "kind-poc",
        "ai-mobility-poc",
        kubectl_fn=fake_cluster_kubectl(),
    )

    assert state["context"] == "kind-poc"
    assert state["namespace"] == "ai-mobility-poc"
    assert state["pending_count"] == 0
    assert state["verification_pod"] == "phase17-worker-abc"
    assert state["normal_worker_pods"] == []


@pytest.mark.parametrize(
    ("phase", "terminating"),
    [("Running", False), ("Pending", False), ("Running", True)],
)
def test_cluster_preflight_rejects_normal_worker_pod_with_zero_replicas(
    phase,
    terminating,
):
    metadata = {"name": "inference-worker-orphan"}
    if terminating:
        metadata["deletionTimestamp"] = "2026-10-28T00:00:00Z"
    runner = fake_cluster_kubectl(
        normal_pods=[{
            "metadata": metadata,
            "status": {"phase": phase},
        }]
    )

    with pytest.raises(RuntimeError, match="Normal inference-worker Pods"):
        orchestrator.check_cluster_target(
            "kind-poc",
            "ai-mobility-poc",
            kubectl_fn=runner,
        )


def test_cluster_preflight_accepts_zero_normal_worker_pods():
    runner = fake_cluster_kubectl(normal_replicas=0, normal_pods=[])

    state = orchestrator.check_cluster_target(
        "kind-poc",
        "ai-mobility-poc",
        kubectl_fn=runner,
    )

    assert state["normal_worker_pods"] == []


@pytest.mark.parametrize(
    ("runner", "message"),
    [
        (
            fake_cluster_kubectl(current_context="other-context"),
            "context mismatch",
        ),
        (
            fake_cluster_kubectl(configured_namespace="other-namespace"),
            "namespace mismatch",
        ),
        (
            fake_cluster_kubectl(pending="2\n"),
            "Pending must be empty",
        ),
        (
            fake_cluster_kubectl(normal_replicas=1),
            "Normal inference-worker must be fully stopped",
        ),
        (
            fake_cluster_kubectl(test_replicas=2),
            "Verification phase17-worker must have exactly one Ready replica",
        ),
        (
            fake_cluster_kubectl(test_pod_ready=False),
            "Expected exactly one Ready phase17-worker Pod",
        ),
    ],
)
def test_cluster_preflight_rejects_mismatched_target_or_worker_state(
    runner,
    message,
):
    with pytest.raises(RuntimeError, match=message):
        orchestrator.check_cluster_target(
            "kind-poc",
            "ai-mobility-poc",
            kubectl_fn=runner,
        )


def test_cluster_measurement_failure_is_persisted_without_success(tmp_path):
    output_dir = tmp_path / "new-output"
    output_dir.mkdir()
    phase18, _, _ = orchestrator.load_phase_modules()

    def fail_measurement(*args):
        raise RuntimeError("simulated measurement failure")

    result = orchestrator.run_cluster(
        phase18=phase18,
        output_dir=output_dir,
        runs=2,
        context="kind-poc",
        namespace="ai-mobility-poc",
        config_path=CONFIG,
        preflight=lambda context, namespace: {
            "context": context,
            "namespace": namespace,
        },
        measurement=fail_measurement,
        state_reader=lambda context, namespace: (
            {"redis_pending_count": 0},
            {},
        ),
    )

    assert result["status"] == "ERROR"
    assert result["measurement_count"] == 0
    assert "simulated measurement failure" in result["execution_error"]


def test_cluster_midrun_failure_is_saved_in_phase22_result(tmp_path, monkeypatch):
    output_dir = tmp_path / "new-output"
    failed_record = record("failed-job", status="failed")

    def safe_preflight(context, namespace):
        return {"context": context, "namespace": namespace}

    def failed_measurement(phase18_module, context, namespace, output_path):
        output_path.mkdir(parents=True, exist_ok=True)
        measurement_path = output_path / "phase18-failed-job.json"
        measurement_path.write_text(
            json.dumps(failed_record),
            encoding="utf-8",
        )
        return {
            "record": failed_record,
            "path": str(measurement_path),
            "error": "simulated mid-run failure",
            "stdout": "partial measurement output",
        }

    monkeypatch.setattr(orchestrator, "check_cluster_target", safe_preflight)
    monkeypatch.setattr(
        orchestrator,
        "run_phase18_measurement",
        failed_measurement,
    )
    monkeypatch.setattr(
        orchestrator,
        "collect_cluster_state",
        lambda context, namespace: ({"redis_pending_count": 1}, {}),
    )
    exit_code = orchestrator.main([
        "--mode",
        "cluster",
        "--expected-context",
        "kind-poc",
        "--namespace",
        "ai-mobility-poc",
        "--confirm-cluster-run",
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    assert exit_code == 3
    assert result["status"] == "ERROR"
    assert "simulated mid-run failure" in result["execution_error"]
    assert result["runs"][0]["status"] == "failed"
    assert result["failure_state"] == {"redis_pending_count": 1}
    assert (output_dir / "phase18/phase18-failed-job.json").is_file()


def test_cluster_partial_pass_keeps_overall_error(tmp_path):
    output_dir = tmp_path / "new-output"
    output_dir.mkdir()
    phase18, _, _ = orchestrator.load_phase_modules()
    calls = 0

    def preflight(context, namespace):
        nonlocal calls
        calls += 1
        if calls == 4:
            raise RuntimeError("Pending unexpectedly appeared")
        return {"context": context, "namespace": namespace}

    def measurement(phase18_module, context, namespace, output_path):
        run_number = calls
        return {
            "record": record(f"job-{run_number}"),
            "path": str(output_path / f"phase18-job-{run_number}.json"),
            "error": None,
            "stdout": "",
        }

    result = orchestrator.run_cluster(
        phase18=phase18,
        output_dir=output_dir,
        runs=4,
        context="kind-poc",
        namespace="ai-mobility-poc",
        config_path=CONFIG,
        preflight=preflight,
        measurement=measurement,
        state_reader=lambda context, namespace: (
            {
                "normal_worker_deployment": {"desired": 0},
                "normal_worker_pods": [],
                "verification_worker_deployment": {"desired": 1},
                "verification_worker_pods": [{"name": "phase17-worker", "ready": True}],
                "redis_pending_count": 0,
            },
            {},
        ),
    )

    assert result["status"] == "ERROR"
    assert result["evaluation_status"] == "PASS"
    assert result["measurement_count"] == 3
    assert result["partial_evaluation"] is True
    assert result["failure_state"]["redis_pending_count"] == 0
    assert "Pending unexpectedly appeared" in result["execution_error"]
    report = (
        output_dir / "phase20" / "slo-evaluation.md"
    ).read_text(encoding="utf-8")
    assert "Partial evaluation only" in report
    assert "overall Phase 22 status remains `ERROR`" in report


def test_cluster_failure_records_state_collection_errors_separately(tmp_path):
    output_dir = tmp_path / "new-output"
    output_dir.mkdir()
    phase18, _, _ = orchestrator.load_phase_modules()

    def fail_measurement(*args):
        raise RuntimeError("original measurement failure")

    def fail_state_collection(context, namespace):
        raise RuntimeError("state inspection unavailable")

    result = orchestrator.run_cluster(
        phase18=phase18,
        output_dir=output_dir,
        runs=1,
        context="kind-poc",
        namespace="ai-mobility-poc",
        config_path=CONFIG,
        preflight=lambda context, namespace: {"context": context},
        measurement=fail_measurement,
        state_reader=fail_state_collection,
    )

    assert result["status"] == "ERROR"
    assert "original measurement failure" in result["execution_error"]
    assert result["failure_state"] == {}
    assert result["failure_state_errors"] == {
        "state_collection": "state inspection unavailable"
    }


def test_cluster_failure_records_individual_state_read_errors():
    def kubectl_fn(context, *args):
        if "deployment" in args and "inference-worker" in args:
            raise RuntimeError("normal deployment unavailable")
        if "pods" in args and "app=inference-worker" in args:
            return json.dumps({"items": []})
        if "deployment" in args and "phase17-worker" in args:
            return json.dumps({"spec": {"replicas": 1}, "status": {}})
        if "pods" in args and "app=phase17-worker" in args:
            raise RuntimeError("verification Pod unavailable")
        if "XPENDING" in args:
            return "3\n"
        raise AssertionError(f"Unexpected kubectl call: {context}, {args}")

    state, errors = orchestrator.collect_cluster_state(
        "kind-poc",
        "ai-mobility-poc",
        kubectl_fn=kubectl_fn,
    )

    assert state["normal_worker_pods"] == []
    assert state["verification_worker_deployment"]["desired"] == 1
    assert state["redis_pending_count"] == 3
    assert errors == {
        "normal_worker_deployment": "normal deployment unavailable",
        "verification_worker_pods": "verification Pod unavailable",
    }


@pytest.mark.parametrize(
    ("value", "include_conditions", "expected_status"),
    [
        (1, True, "PASS"),
        (100, True, "FAIL"),
        (1, False, "INCONCLUSIVE"),
    ],
)
def test_offline_visualization_bundle_preserves_phase20_status(
    tmp_path,
    value,
    include_conditions,
    expected_status,
):
    input_dir = tmp_path / "measurements"
    output_dir = tmp_path / "integrated-output"
    write_measurements(
        input_dir,
        value=value,
        include_conditions=include_conditions,
    )

    exit_code = orchestrator.main([
        "--mode",
        "offline",
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
        "--include-visualization",
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    assert result["status"] == expected_status
    assert result["visualization"]["status"] == "generated"
    assert result["visualization"]["html"] == (
        "phase23/recovery-analysis.html"
    )
    assert (output_dir / "phase19/summary.json").is_file()
    assert (output_dir / "phase20/slo-evaluation.json").is_file()
    assert (output_dir / "phase23/recovery-analysis.html").is_file()
    assert (output_dir / "phase23/recovery-analysis.json").is_file()
    assert "phase23/recovery-analysis.html" in result["generated_artifacts"]
    assert exit_code == orchestrator.status_exit_code(expected_status)


def test_visualization_failure_changes_overall_pass_to_error(
    tmp_path,
    monkeypatch,
):
    input_dir = tmp_path / "measurements"
    output_dir = tmp_path / "integrated-output"
    write_measurements(input_dir)

    class FailedVisualization:
        @staticmethod
        def generate(*args, **kwargs):
            raise RuntimeError("simulated renderer failure")

    monkeypatch.setattr(
        orchestrator,
        "load_phase23_module",
        lambda: FailedVisualization,
    )
    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
        "--include-visualization",
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    assert exit_code == 3
    assert result["status"] == "ERROR"
    assert result["evaluation_status"] == "PASS"
    assert result["visualization"]["status"] == "failed"
    assert result["visualization_error"] == "simulated renderer failure"
    assert "phase19/summary.json" in result["generated_artifacts"]
    assert "phase20/slo-evaluation.json" in result["generated_artifacts"]


def test_cluster_partial_failure_visualization_is_marked_partial(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "cluster-output"
    calls = 0

    def preflight(context, namespace):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated preflight stop")
        return {"context": context, "namespace": namespace}

    def measured(phase18_module, context, namespace, measurements_dir):
        measurements_dir.mkdir(parents=True, exist_ok=True)
        path = measurements_dir / "phase18-job-1.json"
        path.write_text(json.dumps(record("job-1")), encoding="utf-8")
        return {
            "record": record("job-1"),
            "path": str(path),
            "error": None,
            "stdout": "",
        }

    monkeypatch.setattr(orchestrator, "check_cluster_target", preflight)
    monkeypatch.setattr(orchestrator, "run_phase18_measurement", measured)
    monkeypatch.setattr(
        orchestrator,
        "collect_cluster_state",
        lambda context, namespace: ({"redis_pending_count": 0}, {}),
    )
    exit_code = orchestrator.main([
        "--mode",
        "cluster",
        "--expected-context",
        "kind-poc",
        "--namespace",
        "ai-mobility-poc",
        "--confirm-cluster-run",
        "--runs",
        "2",
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
        "--include-visualization",
    ])

    result = json.loads(
        (output_dir / orchestrator.RESULT_NAME).read_text(encoding="utf-8")
    )
    html_text = (
        output_dir / "phase23/recovery-analysis.html"
    ).read_text(encoding="utf-8")
    assert exit_code == 3
    assert result["status"] == "ERROR"
    assert result["partial_evaluation"] is True
    assert result["visualization"]["status"] == "generated"
    assert "Partial evaluation" in html_text
    assert "not a full successful run" in html_text


def test_include_visualization_is_rejected_if_output_already_exists(
    tmp_path,
):
    input_dir = tmp_path / "measurements"
    output_dir = tmp_path / "existing-output"
    write_measurements(input_dir)
    output_dir.mkdir()
    sentinel = output_dir / "keep.txt"
    sentinel.write_text("unchanged", encoding="utf-8")

    exit_code = orchestrator.main([
        "--input",
        str(input_dir),
        "--output",
        str(output_dir),
        "--config",
        str(CONFIG),
        "--include-visualization",
    ])

    assert exit_code == 3
    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert not (output_dir / "phase23").exists()
