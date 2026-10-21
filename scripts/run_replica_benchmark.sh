#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="ai-mobility-poc"
REPLICAS="${1:?Usage: $0 <1|2> [runs]}"
RUNS="${2:-3}"
EXPECTED_JOBS=100
OUTPUT_DIR="output/replica-comparison/worker${REPLICAS}"
SUMMARY_FILE="${OUTPUT_DIR}/summary.csv"

if [[ "$REPLICAS" != "1" && "$REPLICAS" != "2" ]]; then
    echo "ERROR: replicas must be 1 or 2" >&2
    exit 1
fi

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: runs must be a positive integer" >&2
    exit 1
fi

for command in kubectl python; do
    command -v "$command" >/dev/null || {
        echo "ERROR: $command not found" >&2
        exit 1
    }
done

mkdir -p "$OUTPUT_DIR"

READY=$(kubectl -n "$NAMESPACE" get deployment \
    inference-worker \
    -o jsonpath='{.status.readyReplicas}')

DESIRED=$(kubectl -n "$NAMESPACE" get deployment \
    inference-worker \
    -o jsonpath='{.spec.replicas}')

if [[ "$READY" != "$REPLICAS" ||
      "$DESIRED" != "$REPLICAS" ]]; then
    echo "ERROR: expected $REPLICAS workers; desired=$DESIRED ready=${READY:-0}" >&2
    exit 1
fi

if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "run_id,replicas,completed,e2e_p95_ms,e2e_p99_ms,redis_wait_p95_ms,worker_p95_ms,mongo_p95_ms,mongo_max_ms" \
        > "$SUMMARY_FILE"
fi

collect_mongodb_stats() {
    local output_file="$1"

    kubectl -n "$NAMESPACE" exec mongo-0 -- \
        mongosh ai_mobility --quiet --eval \
        "$(cat scripts/mongodb/collect_stats.js)" \
        > "$output_file"

    python -m json.tool "$output_file" >/dev/null
}

check_pending() {
    local pending
    pending=$(kubectl -n "$NAMESPACE" exec \
        deployment/redis -- \
        redis-cli XPENDING inference_jobs inference_workers \
        | tr -d '\r[:space:]')

    [[ "$pending" == "0" ]]
}

for ((i=1; i<=RUNS; i++)); do
    RUN_ID="perf-r${REPLICAS}-$(date +%Y%m%d%H%M%S)-${i}-$$"
    JOB_NAME="bench-r${REPLICAS}-$(date +%H%M%S)-${i}-$$"

    export RUN_ID JOB_NAME
    export OUTPUT_DIR SUMMARY_FILE REPLICAS EXPECTED_JOBS

    echo
    echo "=== Run $i/$RUNS : $RUN_ID ==="

    if ! check_pending; then
        echo "ERROR: Redis Pending is not zero before run" >&2
        exit 1
    fi

    RUN_DIR="${OUTPUT_DIR}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    export RUN_DIR

    collect_mongodb_stats "${RUN_DIR}/mongodb-before.json"

    python - <<'PY'
import os
from pathlib import Path

source = Path("k8s/benchmark-producer-job.yaml").read_text(
    encoding="utf-8"
)

old = "name: benchmark-producer\n  namespace:"

assert source.count(old) == 1
assert source.count("perf-pipeline-manual") == 1

source = source.replace(
    old,
    f"name: {os.environ['JOB_NAME']}\n  namespace:",
    1,
).replace(
    "perf-pipeline-manual",
    os.environ["RUN_ID"],
    1,
)

# 実行したJob定義を保存して再現性を確保
Path(os.environ["RUN_DIR"], "job.yaml").write_text(
    source, encoding="utf-8"
)
PY

    kubectl apply -f "${RUN_DIR}/job.yaml"

    if ! kubectl -n "$NAMESPACE" wait \
        --for=condition=complete \
        "job/${JOB_NAME}" \
        --timeout=180s; then
        kubectl -n "$NAMESPACE" get pods \
            -l job-name="$JOB_NAME" -o wide || true
        kubectl -n "$NAMESPACE" logs \
            "job/${JOB_NAME}" || true
        echo "ERROR: Producer Job did not complete" >&2
        exit 1
    fi

    kubectl -n "$NAMESPACE" logs "job/${JOB_NAME}" \
        > "${RUN_DIR}/producer.json"

    # MongoDBへの100件保存を待つ
    SAVED=0
    for ((attempt=1; attempt<=60; attempt++)); do
        SAVED=$(kubectl -n "$NAMESPACE" exec mongo-0 -- \
            mongosh ai_mobility --quiet --eval \
            "db.inference_results.countDocuments({_id: {\$regex: '^${RUN_ID}-'}})" \
            | tr -d '\r[:space:]')

        if [[ "$SAVED" == "$EXPECTED_JOBS" ]]; then
            break
        fi

        sleep 1
    done

    if [[ "$SAVED" != "$EXPECTED_JOBS" ]]; then
        echo "ERROR: MongoDB saved $SAVED/$EXPECTED_JOBS" >&2
        exit 1
    fi

    # MongoDB保存後のACK完了を待つ
    PENDING_OK=false
    for ((attempt=1; attempt<=30; attempt++)); do
        if check_pending; then
            PENDING_OK=true
            break
        fi
        sleep 1
    done

    if [[ "$PENDING_OK" != "true" ]]; then
        echo "ERROR: Redis Pending did not reach zero" >&2
        exit 1
    fi

    collect_mongodb_stats "${RUN_DIR}/mongodb-after.json"

    python scripts/mongodb/analyze_stats.py \
    "${RUN_DIR}/mongodb-before.json" \
    "${RUN_DIR}/mongodb-after.json" \
    > "${RUN_DIR}/mongodb-delta.json"

    LOG_FILE="${RUN_DIR}/worker.log"
    : > "$LOG_FILE"

    # Pod別ログと処理件数を保存
    echo "pod,completed_jobs" > "${RUN_DIR}/workers.csv"

    mapfile -t PODS < <(
        kubectl -n "$NAMESPACE" get pods \
            -l app=inference-worker \
            -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
    )

    if [[ "${#PODS[@]}" -ne "$REPLICAS" ]]; then
        echo "ERROR: Worker Pod count changed during run" >&2
        exit 1
    fi

    for POD in "${PODS[@]}"; do
        POD_LOG="${RUN_DIR}/${POD}.log"

        kubectl -n "$NAMESPACE" logs "$POD" \
            --timestamps=true > "$POD_LOG"

        COUNT=$(grep -Fc \
            "Processed: job_id=${RUN_ID}-" \
            "$POD_LOG" || true)

        echo "${POD},${COUNT}" \
            >> "${RUN_DIR}/workers.csv"

        # Pod名を保持しながら統合
        grep -F "Processed: job_id=${RUN_ID}-" "$POD_LOG" \
            | sed "s/^/pod=${POD} /" \
            >> "$LOG_FILE" || true

        echo "  $POD : $COUNT jobs"
    done

    LOG_COUNT=$(grep -Fc \
        "Processed: job_id=${RUN_ID}-" \
        "$LOG_FILE" || true)

    if [[ "$LOG_COUNT" -ne "$EXPECTED_JOBS" ]]; then
        echo "ERROR: Processed log count=$LOG_COUNT" >&2
        exit 1
    fi

    python scripts/analyze_e2e.py \
        "$LOG_FILE" "$RUN_ID" "$EXPECTED_JOBS" \
        > "${RUN_DIR}/e2e.json"

    python scripts/analyze_dispatch.py \
        "$LOG_FILE" "$RUN_ID" \
        > "${RUN_DIR}/dispatch.json"

    python scripts/analyze_worker_breakdown.py \
        "$LOG_FILE" "$RUN_ID" \
        > "${RUN_DIR}/breakdown.json"

    python - <<'PY'
import csv
import json
import os
from pathlib import Path

directory = Path(os.environ["RUN_DIR"])

def read(name):
    return json.loads(
        (directory / name).read_text(encoding="utf-8")
    )

e2e = read("e2e.json")
dispatch = read("dispatch.json")
breakdown = read("breakdown.json")

mongodb = read("mongodb-delta.json")

# 必ず先に定義する
expected = int(os.environ["EXPECTED_JOBS"])

assert mongodb["connections_rejected_delta"] == 0, (
    "MongoDB connection rejections detected"
)

if mongodb["update_delta"] != expected:
    print(
        "WARNING: MongoDB update delta differs "
        f"from completed jobs: {mongodb['update_delta']} != {expected}"
    )

assert e2e["completed_jobs"] == expected
assert dispatch["completed_jobs"] == expected
assert breakdown["completed_jobs_with_breakdown"] == expected, (
    "Detailed metrics are missing. "
    "Check the Worker image and logs."
)

row = [
    os.environ["RUN_ID"],
    os.environ["REPLICAS"],
    expected,
    e2e["end_to_end_latency"]["p95_ms"],
    e2e["end_to_end_latency"]["p99_ms"],
    dispatch["redis_wait_latency"]["p95_ms"],
    e2e["worker_latency"]["p95_ms"],
    breakdown["mongo_save_ms"]["p95_ms"],
    breakdown["mongo_save_ms"]["max_ms"],
]

with Path(os.environ["SUMMARY_FILE"]).open(
    "a", encoding="utf-8", newline=""
) as f:
    csv.writer(f).writerow(row)

print(
    f"Completed: {expected}/{expected}, "
    f"E2E p95={row[3]:.3f}ms, "
    f"p99={row[4]:.3f}ms, "
    f"Mongo p95={row[7]:.3f}ms"
)
PY

    echo "Saved: $RUN_DIR"
done

echo
echo "All runs completed."
echo "Summary: $SUMMARY_FILE"
