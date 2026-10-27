"""Tests for Phase 18 measurement-condition collection."""

from datetime import datetime
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "phase18-measure-recovery.py"
)

spec = importlib.util.spec_from_file_location("phase18_measure_recovery", SCRIPT)
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


def install_kubectl_mock(
    monkeypatch,
    *,
    pause_output,
    readiness_probe=True,
    pause_returncode=0,
):
    pod = {
        "spec": {
            "containers": [
                {
                    "name": "worker",
                    **(
                        {"readinessProbe": {"exec": {"command": ["true"]}}}
                        if readiness_probe
                        else {}
                    ),
                }
            ]
        }
    }

    def fake_kubectl(*args, check=True):
        if args[:2] == ("get", "pod"):
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps(pod),
                stderr="",
            )
        if args[0] == "exec":
            assert args[2:5] == ("-c", "worker", "--")
            return SimpleNamespace(
                returncode=pause_returncode,
                stdout=pause_output if pause_returncode == 0 else "",
                stderr=(
                    "" if pause_returncode == 0
                    else "container is not available"
                ),
            )
        raise AssertionError(f"Unexpected kubectl arguments: {args}")

    monkeypatch.setattr(measure, "kubectl", fake_kubectl)


def test_collects_runtime_pause_and_pod_readiness_probe(monkeypatch):
    install_kubectl_mock(monkeypatch, pause_output="30\n")

    conditions, reasons = measure.collect_measurement_conditions("worker-0")

    assert conditions == {
        "failure_scenario": "worker_pod_deletion",
        "measurement_method": "kubectl_polling_observation",
        "test_pause_seconds": 30.0,
        "job_recovery_includes_test_pause": True,
        "worker_readiness_probe": True,
    }
    assert reasons == {}


def test_condition_observation_records_collection_time(monkeypatch):
    install_kubectl_mock(monkeypatch, pause_output="30\n")

    observation = measure.collect_pod_measurement_conditions("worker-0")

    assert observation["pod_name"] == "worker-0"
    assert isinstance(
        datetime.fromisoformat(observation["collected_at"]),
        datetime,
    )
    assert observation["conditions"]["test_pause_seconds"] == 30.0


def test_missing_pause_environment_is_recorded_as_unknown(monkeypatch):
    install_kubectl_mock(
        monkeypatch,
        pause_output=f"{measure.MISSING_ENV_SENTINEL}\n",
    )

    conditions, reasons = measure.collect_measurement_conditions("worker-0")

    assert conditions["test_pause_seconds"] is None
    assert conditions["job_recovery_includes_test_pause"] is None
    assert TEST_PAUSE_ENV_NAME in reasons["test_pause_seconds"]
    assert "duration is unknown" in reasons["job_recovery_includes_test_pause"]


@pytest.mark.parametrize("readiness_probe", [True, False])
def test_readiness_probe_is_read_from_target_pod(monkeypatch, readiness_probe):
    install_kubectl_mock(
        monkeypatch,
        pause_output="0\n",
        readiness_probe=readiness_probe,
    )

    conditions, reasons = measure.collect_measurement_conditions("worker-0")

    assert conditions["worker_readiness_probe"] is readiness_probe
    assert conditions["job_recovery_includes_test_pause"] is False
    assert "worker_readiness_probe" not in reasons


@pytest.mark.parametrize("pause_output", ["invalid\n", "-1\n", "nan\n", "inf\n"])
def test_invalid_pause_environment_is_recorded_as_unknown(
    monkeypatch,
    pause_output,
):
    install_kubectl_mock(monkeypatch, pause_output=pause_output)

    conditions, reasons = measure.collect_measurement_conditions("worker-0")

    assert conditions["test_pause_seconds"] is None
    assert conditions["job_recovery_includes_test_pause"] is None
    assert "test_pause_seconds" in reasons
    assert "job_recovery_includes_test_pause" in reasons


def make_observation(conditions):
    return {
        "conditions": {
            "failure_scenario": "worker_pod_deletion",
            "measurement_method": "kubectl_polling_observation",
            "test_pause_seconds": 30.0,
            "job_recovery_includes_test_pause": True,
            "worker_readiness_probe": False,
            **conditions,
        },
        "reasons": {},
    }


def test_old_and_replacement_pod_conditions_match():
    old = make_observation({})
    replacement = make_observation({})

    comparable, reason = measure.compare_measurement_conditions(old, replacement)

    assert comparable is True
    assert "known and identical" in reason


def test_old_and_replacement_pod_pause_mismatch_is_incomparable():
    old = make_observation({})
    replacement = make_observation({"test_pause_seconds": 0.0})

    comparable, reason = measure.compare_measurement_conditions(old, replacement)

    assert comparable is False
    assert "test_pause_seconds" in reason


def test_old_and_replacement_pod_readiness_mismatch_is_incomparable():
    old = make_observation({})
    replacement = make_observation({"worker_readiness_probe": True})

    comparable, reason = measure.compare_measurement_conditions(old, replacement)

    assert comparable is False
    assert "worker_readiness_probe" in reason


def test_unknown_conditions_are_incomparable():
    old = make_observation({})
    replacement = make_observation({"test_pause_seconds": None})
    replacement["reasons"] = {
        "test_pause_seconds": "kubectl exec failed: Pod is unavailable."
    }

    comparable, reason = measure.compare_measurement_conditions(old, replacement)

    assert comparable is False
    assert "kubectl exec failed" in reason


def test_replacement_pod_exec_failure_is_recorded_as_unknown(monkeypatch):
    install_kubectl_mock(
        monkeypatch,
        pause_output="",
        pause_returncode=1,
    )

    conditions, reasons = measure.collect_measurement_conditions("replacement-0")

    assert conditions["test_pause_seconds"] is None
    assert conditions["job_recovery_includes_test_pause"] is None
    assert "container is not available" in reasons["test_pause_seconds"]


TEST_PAUSE_ENV_NAME = measure.TEST_PAUSE_ENV
