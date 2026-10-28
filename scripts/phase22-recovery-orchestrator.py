#!/usr/bin/env python3
"""Phase 22: Orchestrate recovery measurement and SLO evaluation."""

import argparse
import contextlib
import importlib.util
import io
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PHASE18_PATH = ROOT / "scripts" / "phase18-measure-recovery.py"
PHASE19_PATH = ROOT / "scripts" / "phase19-recovery-report.py"
PHASE20_PATH = ROOT / "scripts" / "phase20-slo-evaluation.py"
PHASE23_PATH = ROOT / "scripts" / "phase23-recovery-visualization.py"
DEFAULT_INPUT = Path("output/phase18")
DEFAULT_CONFIG = ROOT / "config" / "phase20-slo.json"
DEFAULT_OUTPUT = Path("output/phase22")
MAX_CLUSTER_RUNS = 10
RESULT_NAME = "phase22-result.json"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_phase_modules():
    return (
        load_module("phase22_phase18", PHASE18_PATH),
        load_module("phase22_phase19", PHASE19_PATH),
        load_module("phase22_phase20", PHASE20_PATH),
    )


def load_phase23_module():
    return load_module("phase22_phase23", PHASE23_PATH)


def write_json(path, value, *, create=False):
    mode = "x" if create else "w"
    with path.open(mode, encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2, allow_nan=False)
        file.write("\n")


def kubectl(context, *args):
    command = ["kubectl"]
    if context is not None:
        command.extend(["--context", context])
    command.extend(args)
    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"kubectl {' '.join(command[1:])} failed: "
            f"{result.stderr.strip()}"
        )
    return result.stdout


def deployment_replicas(deployment, name):
    spec = deployment.get("spec", {})
    status = deployment.get("status", {})
    return {
        "desired": spec.get("replicas", 1),
        "current": status.get("replicas", 0),
        "ready": status.get("readyReplicas", 0),
        "available": status.get("availableReplicas", 0),
        "name": name,
    }


def pod_is_ready(pod):
    return any(
        condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def worker_pod_summary(pod, *, include_ready=False):
    metadata = pod.get("metadata", {})
    status = pod.get("status", {})
    summary = {
        "name": metadata.get("name"),
        "phase": status.get("phase"),
        "terminating": metadata.get("deletionTimestamp") is not None,
    }
    if include_ready:
        summary["ready"] = pod_is_ready(pod)
    return summary


def collect_cluster_state(
    expected_context,
    namespace,
    *,
    kubectl_fn=kubectl,
):
    state = {}
    errors = {}

    def collect(key, callback):
        try:
            state[key] = callback()
        except Exception as exc:
            errors[key] = str(exc)

    def read_deployment(name):
        deployment = json.loads(kubectl_fn(
            expected_context,
            "-n",
            namespace,
            "get",
            "deployment",
            name,
            "-o",
            "json",
        ))
        return deployment_replicas(deployment, name)

    def read_pods(label, *, include_ready=False):
        pod_list = json.loads(kubectl_fn(
            expected_context,
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            label,
            "-o",
            "json",
        ))
        return [
            worker_pod_summary(pod, include_ready=include_ready)
            for pod in pod_list.get("items", [])
        ]

    def read_pending_count():
        output = kubectl_fn(
            expected_context,
            "-n",
            namespace,
            "exec",
            "deploy/redis",
            "--",
            "redis-cli",
            "--raw",
            "XPENDING",
            "inference_jobs",
            "inference_workers",
        )
        try:
            return int(output.splitlines()[0])
        except (IndexError, ValueError) as exc:
            raise RuntimeError(
                f"Could not parse Redis Pending count: {output!r}"
            ) from exc

    collect(
        "normal_worker_deployment",
        lambda: read_deployment("inference-worker"),
    )
    collect(
        "normal_worker_pods",
        lambda: read_pods("app=inference-worker"),
    )
    collect(
        "verification_worker_deployment",
        lambda: read_deployment("phase17-worker"),
    )
    collect(
        "verification_worker_pods",
        lambda: read_pods("app=phase17-worker", include_ready=True),
    )
    collect("redis_pending_count", read_pending_count)
    return state, errors


def check_cluster_target(
    expected_context,
    namespace,
    *,
    kubectl_fn=kubectl,
):
    current_context = kubectl_fn(None, "config", "current-context").strip()
    if current_context != expected_context:
        raise RuntimeError(
            f"Kubernetes context mismatch: expected {expected_context!r}, "
            f"current context is {current_context!r}."
        )

    kube_config = json.loads(kubectl_fn(
        expected_context,
        "config",
        "view",
        "--minify",
        "-o",
        "json",
    ))
    contexts = kube_config.get("contexts", [])
    if not contexts or contexts[0].get("name") != expected_context:
        raise RuntimeError(
            "The selected kubeconfig context does not match the confirmed context."
        )
    configured_namespace = contexts[0].get("context", {}).get("namespace")
    if configured_namespace and configured_namespace != namespace:
        raise RuntimeError(
            f"Kubernetes namespace mismatch: context selects "
            f"{configured_namespace!r}, requested namespace is {namespace!r}."
        )

    namespace_data = json.loads(kubectl_fn(
        expected_context,
        "get",
        "namespace",
        namespace,
        "-o",
        "json",
    ))
    if namespace_data.get("metadata", {}).get("name") != namespace:
        raise RuntimeError(
            f"Could not confirm target namespace {namespace!r}."
        )

    normal_deployment = deployment_replicas(
        json.loads(kubectl_fn(
            expected_context,
            "-n",
            namespace,
            "get",
            "deployment",
            "inference-worker",
            "-o",
            "json",
        )),
        "inference-worker",
    )
    if any(normal_deployment[key] != 0 for key in ("desired", "current", "ready", "available")):
        raise RuntimeError(
            "Normal inference-worker must be fully stopped; "
            f"observed {normal_deployment}."
        )

    normal_pod_list = json.loads(kubectl_fn(
        expected_context,
        "-n",
        namespace,
        "get",
        "pods",
        "-l",
        "app=inference-worker",
        "-o",
        "json",
    ))
    normal_pods = normal_pod_list.get("items", [])
    if normal_pods:
        raise RuntimeError(
            "Normal inference-worker Pods must be absent; "
            f"observed {[worker_pod_summary(pod) for pod in normal_pods]}."
        )

    test_deployment = deployment_replicas(
        json.loads(kubectl_fn(
            expected_context,
            "-n",
            namespace,
            "get",
            "deployment",
            "phase17-worker",
            "-o",
            "json",
        )),
        "phase17-worker",
    )
    if any(test_deployment[key] != 1 for key in ("desired", "current", "ready", "available")):
        raise RuntimeError(
            "Verification phase17-worker must have exactly one Ready replica; "
            f"observed {test_deployment}."
        )

    pod_list = json.loads(kubectl_fn(
        expected_context,
        "-n",
        namespace,
        "get",
        "pods",
        "-l",
        "app=phase17-worker",
        "-o",
        "json",
    ))
    pods = pod_list.get("items", [])
    if len(pods) != 1 or not pod_is_ready(pods[0]):
        raise RuntimeError(
            "Expected exactly one Ready phase17-worker Pod; "
            f"observed {len(pods)} Pod(s)."
        )

    pending_output = kubectl_fn(
        expected_context,
        "-n",
        namespace,
        "exec",
        "deploy/redis",
        "--",
        "redis-cli",
        "--raw",
        "XPENDING",
        "inference_jobs",
        "inference_workers",
    )
    try:
        pending_count = int(pending_output.splitlines()[0])
    except (IndexError, ValueError) as exc:
        raise RuntimeError(
            f"Could not parse Redis Pending count: {pending_output!r}"
        ) from exc
    if pending_count != 0:
        raise RuntimeError(
            f"Redis Pending must be empty before measurement; found {pending_count}."
        )

    return {
        "context": expected_context,
        "namespace": namespace,
        "normal_worker": normal_deployment,
        "normal_worker_pods": [],
        "verification_worker": test_deployment,
        "verification_pod": pods[0].get("metadata", {}).get("name"),
        "pending_count": pending_count,
    }


def run_phase18_measurement(phase18, context, namespace, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    before = set(output_dir.glob("phase18-*.json"))
    previous = (
        phase18.NS,
        phase18.OUTPUT,
        phase18.KUBECTL_CONTEXT,
    )
    phase18.NS = namespace
    phase18.OUTPUT = output_dir
    phase18.KUBECTL_CONTEXT = context
    captured = io.StringIO()
    error = None
    try:
        with contextlib.redirect_stdout(captured):
            phase18.main()
    except Exception as exc:
        error = str(exc)
    finally:
        (
            phase18.NS,
            phase18.OUTPUT,
            phase18.KUBECTL_CONTEXT,
        ) = previous

    created = sorted(set(output_dir.glob("phase18-*.json")) - before)
    if len(created) != 1:
        return {
            "record": None,
            "path": None,
            "error": error or (
                "Phase 18 did not produce exactly one new measurement JSON."
            ),
            "stdout": captured.getvalue(),
        }

    path = created[0]
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "record": None,
            "path": str(path),
            "error": error or f"Could not read Phase 18 result: {exc}",
            "stdout": captured.getvalue(),
        }

    if record.get("status") != "passed" and error is None:
        error = record.get("error", "Phase 18 measurement did not pass.")
    return {
        "record": record,
        "path": str(path),
        "error": error,
        "stdout": captured.getvalue(),
    }


def write_phase19_outputs(phase19, records, output_dir, summary):
    output_dir.mkdir(parents=True, exist_ok=False)
    write_json(output_dir / "summary.json", summary, create=True)
    phase19.write_csv(records, output_dir / "recovery-results.csv")
    phase19.write_markdown(summary, output_dir / "recovery-report.md")


def run_evaluation(records, output_dir, config_path):
    _, phase19, phase20 = load_phase_modules()
    summary = phase19.summarize(records)
    phase19_dir = output_dir / "phase19"
    write_phase19_outputs(phase19, records, phase19_dir, summary)

    config = phase20.load_config(config_path)
    report = phase20.evaluate(summary, config)
    phase20.write_reports(report, output_dir / "phase20")
    return summary, report


def run_offline(input_dir, output_dir, config_path):
    _, phase19, _ = load_phase_modules()
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Phase 18 input directory does not exist: {input_dir}"
        )
    records = phase19.load_records(input_dir)
    summary, report = run_evaluation(records, output_dir, config_path)
    return {
        "mode": "offline",
        "status": report["overall_status"],
        "measurement_count": len(records),
        "passed_runs": summary["passed_runs"],
        "failed_runs": summary["failed_runs"],
        "measurement_conditions_comparable": summary[
            "measurement_conditions_comparable"
        ],
        "measurement_conditions_reason": summary[
            "measurement_conditions_reason"
        ],
        "evaluation_report": "phase20/slo-evaluation.json",
    }


def run_cluster(
    *,
    phase18,
    output_dir,
    runs,
    context,
    namespace,
    config_path,
    preflight=None,
    measurement=None,
    state_reader=None,
):
    preflight = preflight or check_cluster_target
    measurement = measurement or run_phase18_measurement
    state_reader = state_reader or collect_cluster_state
    records = []
    run_results = []
    failure = None
    measurements_dir = output_dir / "phase18"

    for run_number in range(1, runs + 1):
        try:
            target = preflight(context, namespace)
            measured = measurement(
                phase18,
                context,
                namespace,
                measurements_dir,
            )
        except Exception as exc:
            failure = f"Run {run_number} preflight/measurement failed: {exc}"
            run_results.append({
                "run": run_number,
                "status": "failed",
                "error": failure,
            })
            break

        record = measured["record"]
        run_error = measured["error"]
        run_result = {
            "run": run_number,
            "status": (
                record.get("status", "failed")
                if isinstance(record, dict)
                else "failed"
            ),
            "preflight": target,
            "measurement_json": measured["path"],
            "stdout": measured["stdout"],
        }
        if run_error:
            run_result["error"] = run_error
            failure = f"Run {run_number} measurement failed: {run_error}"
        if isinstance(record, dict):
            records.append(record)
            run_result["job_id"] = record.get("job_id")
        run_results.append(run_result)
        if failure:
            break

    result = {
        "mode": "cluster",
        "runs": run_results,
        "measurement_count": len(records),
        "measurement_directory": "phase18",
    }
    if failure:
        result["execution_error"] = failure
        result["partial_evaluation"] = bool(records)
        try:
            failure_state, state_errors = state_reader(context, namespace)
        except Exception as exc:
            failure_state = {}
            state_errors = {"state_collection": str(exc)}
        result["failure_state"] = failure_state
        if state_errors:
            result["failure_state_errors"] = state_errors

    if records:
        try:
            summary, report = run_evaluation(
                records,
                output_dir,
                config_path,
            )
        except Exception as exc:
            result["status"] = "ERROR"
            result["execution_error"] = (
                f"Could not aggregate/evaluate measurements: {exc}"
            )
            return result
        result.update({
            "evaluation_status": report["overall_status"],
            "measurement_conditions_comparable": summary[
                "measurement_conditions_comparable"
            ],
            "measurement_conditions_reason": summary[
                "measurement_conditions_reason"
            ],
            "evaluation_report": "phase20/slo-evaluation.json",
        })
        if failure and records:
            report_path = output_dir / "phase20" / "slo-evaluation.md"
            with report_path.open("a", encoding="utf-8") as report_file:
                report_file.write(
                    "\n> **Partial evaluation only:** Phase 20 uses the "
                    "successful measurements collected before the run "
                    "failed. The overall Phase 22 status remains `ERROR`.\n"
                )

    if failure:
        result["status"] = "ERROR"
    elif records:
        result["status"] = result["evaluation_status"]
    else:
        result["status"] = "ERROR"
        result.setdefault(
            "execution_error",
            "No Phase 18 measurement result was produced.",
        )
    return result


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Run recovery report and SLO evaluation offline by default; "
            "cluster mode requires explicit confirmation."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("offline", "cluster"),
        default="offline",
        help="Execution mode (default: offline)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Directory containing Phase 18 JSON measurements",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="New output directory; existing paths are never overwritten",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Phase 20 SLO configuration JSON",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help=f"Number of cluster measurements (1-{MAX_CLUSTER_RUNS})",
    )
    parser.add_argument(
        "--expected-context",
        help="Required current Kubernetes context for cluster mode",
    )
    parser.add_argument(
        "--namespace",
        help="Required Kubernetes namespace for cluster mode",
    )
    parser.add_argument(
        "--confirm-cluster-run",
        action="store_true",
        help="Confirm that cluster mode may inject a worker Pod failure",
    )
    parser.add_argument(
        "--include-visualization",
        action="store_true",
        help="Also generate Phase 23 HTML/SVG and JSON reports",
    )
    return parser


def validate_args(parser, args):
    if args.runs < 1 or args.runs > MAX_CLUSTER_RUNS:
        parser.error(f"--runs must be between 1 and {MAX_CLUSTER_RUNS}")
    if args.mode == "cluster":
        if not args.confirm_cluster_run:
            parser.error(
                "cluster mode requires --confirm-cluster-run"
            )
        if not args.expected_context or not args.namespace:
            parser.error(
                "cluster mode requires --expected-context and --namespace"
            )
    elif (
        args.confirm_cluster_run
        or args.expected_context
        or args.namespace
    ):
        parser.error(
            "cluster confirmation/context/namespace options require "
            "--mode cluster"
        )


def status_exit_code(status):
    if status == "PASS":
        return 0
    if status == "FAIL":
        return 1
    if status == "INCONCLUSIVE":
        return 2
    return 3


def generated_artifacts(output_dir):
    candidates = (
        "phase18",
        "phase19/summary.json",
        "phase19/recovery-results.csv",
        "phase19/recovery-report.md",
        "phase20/slo-evaluation.json",
        "phase20/slo-evaluation.md",
        "phase23/recovery-analysis.html",
        "phase23/recovery-analysis.json",
    )
    return [
        path
        for path in candidates
        if (output_dir / path).exists()
    ]


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)

    output_dir = args.output
    if args.mode == "offline":
        input_path = args.input.resolve()
        output_path = output_dir.resolve()
        if (
            input_path == output_path
            or input_path in output_path.parents
            or output_path in input_path.parents
        ):
            print(
                "Offline input and output directories must not overlap.",
                file=sys.stderr,
            )
            return 3

    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        print(
            f"Output path already exists; refusing to overwrite: {output_dir}",
            file=sys.stderr,
        )
        return 3
    except OSError as exc:
        print(f"Could not create output directory {output_dir}: {exc}", file=sys.stderr)
        return 3

    result = {
        "report": "Phase 22 recovery orchestration",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "RUNNING",
        "mode": args.mode,
    }
    result_path = output_dir / RESULT_NAME
    write_json(result_path, result, create=True)

    try:
        if not args.config.is_file():
            raise FileNotFoundError(f"SLO config not found: {args.config}")

        if args.mode == "offline":
            result.update(
                run_offline(args.input, output_dir, args.config)
            )
        else:
            phase18, _, _ = load_phase_modules()
            result.update(run_cluster(
                phase18=phase18,
                output_dir=output_dir,
                runs=args.runs,
                context=args.expected_context,
                namespace=args.namespace,
                config_path=args.config,
            ))
    except Exception as exc:
        result.update({
            "status": "ERROR",
            "execution_error": str(exc),
        })
    if args.include_visualization:
        if args.mode == "offline" and result.get("status") in (
            "PASS",
            "FAIL",
            "INCONCLUSIVE",
        ):
            result["evaluation_status"] = result["status"]
        try:
            write_json(result_path, result)
            phase23 = load_phase23_module()
            phase23.generate(
                output_dir,
                output_dir / "phase23",
                args.input if args.mode == "offline" else None,
                allow_output_inside_input=True,
            )
            result["visualization"] = {
                "status": "generated",
                "html": "phase23/recovery-analysis.html",
                "analysis_json": "phase23/recovery-analysis.json",
            }
        except Exception as exc:
            result["visualization"] = {
                "status": "failed",
                "error": str(exc),
            }
            result["visualization_error"] = str(exc)
            if result.get("status") != "ERROR":
                result["execution_error"] = (
                    f"Phase 23 visualization failed: {exc}"
                )
            result["status"] = "ERROR"
        result["generated_artifacts"] = generated_artifacts(output_dir)
    result["completed_at"] = datetime.now(timezone.utc).isoformat()
    write_json(result_path, result)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"Result: {result_path}")
    return status_exit_code(result["status"])


if __name__ == "__main__":
    raise SystemExit(main())
