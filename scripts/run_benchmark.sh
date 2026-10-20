#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="ai-mobility-poc"
EXPECTED=100
OUTPUT_DIR="output/benchmark-$(date +%Y%m%d%H%M%S)"

mkdir -p "$OUTPUT_DIR"

# 既存のPendingジョブがないことを確認
PENDING=$(kubectl -n "$NAMESPACE" exec deployment/redis -- \
  redis-cli --raw XPENDING inference_jobs inference_workers | head -1)

if [ "$PENDING" != "0" ]; then
  echo "ERROR: Pending jobs exist: $PENDING" >&2
  exit 1
fi

kubectl -n "$NAMESPACE" rollout status \
  deployment/inference-worker --timeout=120s

kubectl -n "$NAMESPACE" rollout status \
  statefulset/mongo --timeout=120s

for round in 1 2 3; do
  RUN_ID="perf-repeat-$(date +%Y%m%d%H%M%S)-r${round}"

  echo "=== Round ${round}/3: ${RUN_ID} ==="

  POD=$(kubectl -n "$NAMESPACE" get pods \
    -l app=inference-worker \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')

  # 100件投入
  kubectl -n "$NAMESPACE" exec deployment/redis -- \
    sh -c '
      i=1
      while [ "$i" -le 100 ]; do
        redis-cli XADD inference_jobs "*" \
          job_id "$1-$i" \
          input_value 0.5 >/dev/null || exit 1
        i=$((i + 1))
      done
    ' sh "$RUN_ID"

  # MongoDBへの100件保存を待つ（最大120秒）
  COMPLETE=0
  for attempt in $(seq 1 60); do
    COUNT=$(kubectl -n "$NAMESPACE" exec mongo-0 -- \
      mongosh ai_mobility --quiet --eval \
      "db.inference_results.countDocuments({_id: {\$regex: '^${RUN_ID}-'}})" \
      | tr -d '\r\n')

    if [ "$COUNT" = "$EXPECTED" ]; then
      COMPLETE=1
      break
    fi

    sleep 2
  done

  if [ "$COMPLETE" -ne 1 ]; then
    echo "ERROR: Timeout; MongoDB records: $COUNT / $EXPECTED" >&2
    exit 1
  fi

  # ACKまでの処理完了を確認
  PENDING=$(kubectl -n "$NAMESPACE" exec deployment/redis -- \
    redis-cli --raw XPENDING inference_jobs inference_workers | head -1)

  if [ "$PENDING" != "0" ]; then
    echo "ERROR: Pending jobs remain: $PENDING" >&2
    exit 1
  fi

  # 同じWorker Podからログを取得
  kubectl -n "$NAMESPACE" logs "$POD" \
    --timestamps=true > "$OUTPUT_DIR/${RUN_ID}.log"

  python scripts/analyze_e2e.py \
    "$OUTPUT_DIR/${RUN_ID}.log" "$RUN_ID" 100 \
    > "$OUTPUT_DIR/${RUN_ID}.json"

  # 集計ログも100件であることを確認
  python - "$OUTPUT_DIR/${RUN_ID}.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    report = json.load(f)

if report["completed_jobs"] != 100:
    raise SystemExit(
        f"ERROR: completed_jobs={report['completed_jobs']}"
    )

print(
    "Completed:",
    report["completed_jobs"],
    "E2E p95:",
    report["end_to_end_latency"]["p95_ms"],
    "ms",
)
PY

done

echo "Benchmark completed: $OUTPUT_DIR"
