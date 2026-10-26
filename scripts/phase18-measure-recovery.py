#!/usr/bin/env python3
"""Phase 18: Kubernetes Worker recovery timing."""

import json
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


def run(*args, check=True):
    return subprocess.run(
        args,
        text=True,
        capture_output=True,
        check=check,
    )


def kubectl(*args):
    return run("kubectl", "-n", NS, *args)


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
