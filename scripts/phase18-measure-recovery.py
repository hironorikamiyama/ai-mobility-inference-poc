#!/usr/bin/env python3
"""Phase 18: Kubernetes Worker recovery timing."""

import json
import math
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

NS = "ai-mobility-poc"
APP = "phase17-worker"
STREAM = "inference_jobs"
GROUP = "inference_workers"
OUTPUT = Path("output/phase18")

POLL_INTERVAL = 0.2
CHECKPOINT_TIMEOUT = 25
POD_TIMEOUT = 120
RECOVERY_TIMEOUT = 150
TEST_PAUSE_ENV = "WORKER_TEST_PAUSE_BEFORE_SAVE_SECONDS"
MISSING_ENV_SENTINEL = "__PHASE18_MISSING_ENV__"
CONDITION_KEYS = (
    "failure_scenario",
    "measurement_method",
    "test_pause_seconds",
    "job_recovery_includes_test_pause",
    "worker_readiness_probe",
)


def run(*args, check=True):
    return subprocess.run(
        args,
        text=True,
        capture_output=True,
        check=check,
    )


def kubectl(*args, check=True):
    return run("kubectl", "-n", NS, *args, check=check)


def pod_names():
    result = kubectl(
        "get", "pods",
        "-l", f"app={APP}",
        "-o", "json",
    )
    items = json.loads(result.stdout)["items"]
    return [item["metadata"]["name"] for item in items]


def pod_ready(name):
    result = kubectl(
        "get", "pod", name, "-o", "json"
    )
    pod = json.loads(result.stdout)

    return any(
        condition["type"] == "Ready"
        and condition["status"] == "True"
        for condition in pod["status"].get("conditions", [])
    )


def collect_measurement_conditions(pod_name):
    conditions = {
        "failure_scenario": "worker_pod_deletion",
        "measurement_method": "kubectl_polling_observation",
        "test_pause_seconds": None,
        "job_recovery_includes_test_pause": None,
        "worker_readiness_probe": None,
    }
    reasons = {}

    try:
        pod_result = kubectl(
            "get", "pod", pod_name, "-o", "json", check=False
        )
    except OSError as exc:
        reasons["worker_readiness_probe"] = (
            "Could not read the target Pod container definition: "
            + str(exc)
        )
    else:
        if pod_result.returncode != 0:
            reasons["worker_readiness_probe"] = (
                "Could not read the target Pod container definition: "
                + pod_result.stderr.strip()
            )
        else:
            try:
                pod = json.loads(pod_result.stdout)
            except json.JSONDecodeError as exc:
                reasons["worker_readiness_probe"] = (
                    "Could not parse the target Pod container definition: "
                    + str(exc)
                )
            else:
                containers = pod.get("spec", {}).get("containers", [])
                worker = next(
                    (
                        container for container in containers
                        if container.get("name") == "worker"
                    ),
                    None,
                )
                if worker is None:
                    reasons["worker_readiness_probe"] = (
                        "The target Pod does not define a worker container."
                    )
                else:
                    conditions["worker_readiness_probe"] = (
                        "readinessProbe" in worker
                    )

    try:
        pause_result = kubectl(
            "exec",
            pod_name,
            "-c",
            "worker",
            "--",
            "python",
            "-c",
            (
                "import os; "
                f"print(os.environ.get({TEST_PAUSE_ENV!r}, "
                f"{MISSING_ENV_SENTINEL!r}))"
            ),
            check=False,
        )
    except OSError as exc:
        reasons["test_pause_seconds"] = (
            "Could not read the test-pause environment variable from the "
            "target Pod: " + str(exc)
        )
    else:
        if pause_result.returncode != 0:
            reasons["test_pause_seconds"] = (
                "Could not read the test-pause environment variable from the "
                "target Pod: " + pause_result.stderr.strip()
            )
        else:
            raw_pause = pause_result.stdout.strip()
            if raw_pause == MISSING_ENV_SENTINEL:
                reasons["test_pause_seconds"] = (
                    f"{TEST_PAUSE_ENV} is not set in the target Pod."
                )
            else:
                try:
                    pause_seconds = float(raw_pause)
                except ValueError:
                    reasons["test_pause_seconds"] = (
                        f"{TEST_PAUSE_ENV} is not a valid number: {raw_pause!r}."
                    )
                else:
                    if not math.isfinite(pause_seconds) or pause_seconds < 0:
                        reasons["test_pause_seconds"] = (
                            f"{TEST_PAUSE_ENV} must be finite and non-negative; "
                            f"got {raw_pause!r}."
                        )
                    else:
                        conditions["test_pause_seconds"] = pause_seconds
                        conditions["job_recovery_includes_test_pause"] = (
                            pause_seconds > 0
                        )

    if conditions["job_recovery_includes_test_pause"] is None:
        reasons["job_recovery_includes_test_pause"] = (
            "Cannot determine whether the job recovery includes the test pause "
            "because its effective duration is unknown."
        )

    return conditions, reasons


def collect_pod_measurement_conditions(pod_name):
    conditions, reasons = collect_measurement_conditions(pod_name)
    return {
        "pod_name": pod_name,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "conditions": conditions,
        "reasons": reasons,
    }


def compare_measurement_conditions(old_observation, new_observation):
    unknown = []
    observations = (
        ("old_pod", old_observation),
        ("replacement_pod", new_observation),
    )
    known_conditions = {}

    for label, observation in observations:
        conditions = observation.get("conditions", {})
        reasons = observation.get("reasons", {})
        if not isinstance(conditions, dict):
            unknown.append(f"{label}: conditions are missing")
            continue
        if not isinstance(reasons, dict):
            reasons = {}

        for key in CONDITION_KEYS:
            value = conditions.get(key)
            if key in ("failure_scenario", "measurement_method"):
                valid = isinstance(value, str) and bool(value.strip())
            elif key == "test_pause_seconds":
                try:
                    valid = (
                        isinstance(value, (int, float))
                        and not isinstance(value, bool)
                        and math.isfinite(float(value))
                        and value >= 0
                    )
                except OverflowError:
                    valid = False
            else:
                valid = isinstance(value, bool)
            if not valid:
                unknown.append(
                    f"{label}/{key}: "
                    f"{reasons.get(key, 'condition is missing or invalid')}"
                )
        known_conditions[label] = conditions

    if unknown:
        return False, "Measurement conditions are unknown: " + "; ".join(unknown)

    old_conditions = known_conditions["old_pod"]
    new_conditions = known_conditions["replacement_pod"]
    differences = [
        key for key in CONDITION_KEYS
        if (
            type(old_conditions[key]) is not type(new_conditions[key])
            or old_conditions[key] != new_conditions[key]
        )
    ]
    if differences:
        return (
            False,
            "Measurement conditions differ between old and replacement Pods: "
            + ", ".join(differences),
        )

    return True, "All required measurement conditions are known and identical."


def pod_logs(name):
    result = kubectl("logs", name)
    return result.stdout


def pending_count():
    result = kubectl(
        "exec", "deploy/redis", "--",
        "redis-cli", "--raw",
        "XPENDING", STREAM, GROUP,
    )
    return int(result.stdout.splitlines()[0])


def mongo_document(job_id):
    script = (
        "const d = db.inference_results.findOne("
        f"{{_id: '{job_id}'}});"
        "print(EJSON.stringify(d));"
    )
    result = kubectl(
        "exec", "mongo-0", "--",
        "mongosh", "--quiet", "ai_mobility",
        "--eval", script,
    )
    return json.loads(result.stdout.strip())


def wait_until(predicate, timeout, description):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result, time.monotonic()
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Timed out: {description}")


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    job_id = f"phase18-{uuid.uuid4().hex[:12]}"
    data = {
        "job_id": job_id,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "status": "started",
        "measurement_conditions_observations": {},
        "measurement_conditions_comparable": False,
        "measurement_conditions_reason": (
            "Replacement Pod measurement conditions have not been collected."
        ),
    }

    output_path = OUTPUT / f"{job_id}.json"

    try:
        # Validate the test environment before injecting a job.
        normal = kubectl(
            "get", "deployment", "inference-worker",
            "-o", "json",
        )
        normal_replicas = json.loads(normal.stdout)["spec"]["replicas"]
        if normal_replicas != 0:
            raise RuntimeError(
                "inference-worker must have 0 replicas"
            )

        deployment = kubectl(
            "get", "deployment", APP, "-o", "json"
        )
        if json.loads(deployment.stdout)["spec"]["replicas"] != 1:
            raise RuntimeError(
                "phase17-worker must have 1 replica"
            )

        if pending_count() != 0:
            raise RuntimeError(
                "Redis Pending must be empty"
            )

        names = pod_names()
        if len(names) != 1 or not pod_ready(names[0]):
            raise RuntimeError(
                "Expected one Ready phase17-worker Pod"
            )

        old_pod = names[0]
        data["old_pod"] = old_pod
        old_conditions = collect_pod_measurement_conditions(old_pod)
        data["measurement_conditions_observations"]["old_pod"] = old_conditions
        print(f"OLD_POD={old_pod}", flush=True)

        added = kubectl(
            "exec", "deploy/redis", "--",
            "redis-cli", "--raw",
            "XADD", STREAM, "*",
            "job_id", job_id,
            "input_value", "0.5",
        )
        stream_id = added.stdout.strip()
        data["stream_id"] = stream_id

        print(f"JOB_ID={job_id}", flush=True)
        print(f"STREAM_ID={stream_id}", flush=True)

        marker = (
            f"Before MongoDB save: job_id={job_id}, "
            f"stream_id={stream_id}"
        )

        wait_until(
            lambda: marker in pod_logs(old_pod),
            CHECKPOINT_TIMEOUT,
            "pre-save checkpoint",
        )

        # T0: immediately before requesting Pod deletion.
        t0 = time.monotonic()
        data["deletion_requested_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        kubectl(
            "delete", "pod", old_pod,
            "--grace-period=0",
            "--force",
            "--wait=false",
        )

        data["pending_after_delete"] = pending_count()
        data["mongo_after_delete"] = mongo_document(job_id)

        if data["pending_after_delete"] != 1:
            raise AssertionError(
                "Expected exactly one Pending message"
            )
        if data["mongo_after_delete"] is not None:
            raise AssertionError(
                "MongoDB document already exists"
            )

        # T1: replacement Pod first observed.
        new_pod, t1 = wait_until(
            lambda: next(
                (
                    name for name in pod_names()
                    if name != old_pod
                ),
                None,
            ),
            POD_TIMEOUT,
            "replacement Pod creation",
        )
        data["new_pod"] = new_pod
        print(f"NEW_POD={new_pod}", flush=True)

        # T2: replacement Pod first observed Ready.
        _, t2 = wait_until(
            lambda: pod_ready(new_pod),
            POD_TIMEOUT,
            "replacement Pod Ready",
        )

        new_conditions = collect_pod_measurement_conditions(new_pod)
        data["measurement_conditions_observations"][
            "replacement_pod"
        ] = new_conditions
        (
            data["measurement_conditions_comparable"],
            data["measurement_conditions_reason"],
        ) = compare_measurement_conditions(old_conditions, new_conditions)
        if data["measurement_conditions_comparable"]:
            data["measurement_conditions"] = old_conditions["conditions"]

        # T3: processing completion first observed in logs.
        processed_marker = f"Processed: job_id={job_id},"

        _, t3 = wait_until(
            lambda: processed_marker in pod_logs(new_pod),
            RECOVERY_TIMEOUT,
            "recovered job processing",
        )

        data["pending_final"] = pending_count()
        document = mongo_document(job_id)

        data["mongo_final"] = document
        data["recovery_log"] = next(
            (
                line for line in pod_logs(new_pod).splitlines()
                if processed_marker in line
            ),
            "",
        )

        data["measurements_seconds"] = {
            "pod_recreated_observed": round(t1 - t0, 3),
            "pod_ready_observed": round(t2 - t0, 3),
            "job_recovered_observed": round(t3 - t0, 3),
            "ready_to_recovery": round(t3 - t2, 3),
        }

        if data["pending_final"] != 0:
            raise AssertionError("Pending was not cleared")
        if document is None:
            raise AssertionError("MongoDB result is missing")
        if abs(float(document["score"]) - 0.5) > 1e-6:
            raise AssertionError("Unexpected inference score")
        if "ack=1" not in data["recovery_log"]:
            raise AssertionError("ACK was not confirmed")

        data["status"] = "passed"

        print("\n=== Phase 18 measurements ===")
        for key, value in data["measurements_seconds"].items():
            print(f"{key}: {value:.3f} s")

        print("RESULT=PASSED")

    except Exception as exc:
        data["status"] = "failed"
        data["error"] = str(exc)
        print(f"RESULT=FAILED: {exc}", flush=True)
        raise

    finally:
        output_path.write_text(
            json.dumps(
                data,
                ensure_ascii=False,
                indent=2,
                default=str,
            ) + "\n",
            encoding="utf-8",
        )
        print(f"OUTPUT={output_path}", flush=True)


if __name__ == "__main__":
    main()
