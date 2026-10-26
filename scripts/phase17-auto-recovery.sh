#!/usr/bin/env bash
set -euo pipefail

NS=ai-mobility-poc
DEPLOYMENT=phase17-worker
JOB_ID="phase17-recovery-$(date +%Y%m%d%H%M%S)"
REDIS_STREAM=inference_jobs
REDIS_GROUP=inference_workers

get_pod() {
  kubectl get pods -n "$NS" \
    -l app=phase17-worker \
    -o jsonpath='{.items[0].metadata.name}'
}

echo "=== Phase 17: Deployment auto recovery ==="
echo "JOB_ID=$JOB_ID"

# 既存Workerとの競合を防ぐ
REPLICAS=$(kubectl get deployment inference-worker \
  -n "$NS" -o jsonpath='{.spec.replicas}')

if [ "$REPLICAS" != "0" ]; then
  echo "ERROR: existing Worker must have 0 replicas"
  exit 1
fi

OLD_POD=$(get_pod)
echo "OLD_POD=$OLD_POD"

kubectl wait -n "$NS" \
  --for=condition=Ready \
  "pod/$OLD_POD" --timeout=60s

# 初期状態を確認
PENDING=$(kubectl exec -n "$NS" deploy/redis -- \
  redis-cli XPENDING "$REDIS_STREAM" "$REDIS_GROUP" |
  head -n 1 | tr -d '\r')

if [ "$PENDING" != "0" ]; then
  echo "ERROR: existing Pending messages: $PENDING"
  exit 1
fi

# ジョブ投入
STREAM_ID=$(kubectl exec -n "$NS" deploy/redis -- \
  redis-cli XADD "$REDIS_STREAM" '*' \
  job_id "$JOB_ID" input_value 0.5 | tr -d '\r')

echo "STREAM_ID=$STREAM_ID"
echo "Waiting for checkpoint..."

# チェックポイントを最大25秒間確認
FOUND=0

for i in $(seq 1 100); do
  if kubectl logs -n "$NS" "$OLD_POD" 2>/dev/null |
     grep -Fq "Before MongoDB save: job_id=$JOB_ID, stream_id=$STREAM_ID"; then
    FOUND=1
    break
  fi
  sleep 0.1
done

if [ "$FOUND" -ne 1 ]; then
  echo "ERROR: checkpoint not detected"
  kubectl logs -n "$NS" "$OLD_POD" --tail=30 || true
  exit 1
fi

echo "Checkpoint reached."
echo "Deleting old Pod..."

kubectl delete pod "$OLD_POD" \
  -n "$NS" \
  --grace-period=0 \
  --force \
  --wait=false

echo "=== State immediately after deletion request ==="
kubectl exec -n "$NS" deploy/redis -- \
  redis-cli XPENDING "$REDIS_STREAM" "$REDIS_GROUP"

kubectl exec -n "$NS" mongo-0 -- \
  mongosh --quiet ai_mobility --eval \
  "printjson(db.inference_results.findOne({_id:'$JOB_ID'}))"

echo "=== Waiting for replacement Pod ==="

NEW_POD=""

for i in $(seq 1 120); do
  CANDIDATE=$(get_pod 2>/dev/null || true)

  if [ -n "$CANDIDATE" ] &&
     [ "$CANDIDATE" != "$OLD_POD" ]; then
    NEW_POD="$CANDIDATE"
    break
  fi

  sleep 1
done

if [ -z "$NEW_POD" ]; then
  echo "ERROR: replacement Pod not found"
  exit 1
fi

echo "NEW_POD=$NEW_POD"

kubectl wait -n "$NS" \
  --for=condition=Ready \
  "pod/$NEW_POD" --timeout=120s

echo "Waiting for recovered job..."

RECOVERED=0

for i in $(seq 1 150); do
  if kubectl logs -n "$NS" "$NEW_POD" 2>/dev/null |
     grep -F "Processed: job_id=$JOB_ID," |
     grep -Fq "stream_id=$STREAM_ID"; then
    RECOVERED=1
    break
  fi
  sleep 1
done

if [ "$RECOVERED" -ne 1 ]; then
  echo "ERROR: recovery was not confirmed"
  kubectl logs -n "$NS" "$NEW_POD" --tail=30 || true
  exit 1
fi

echo "=== Recovery log ==="
kubectl logs -n "$NS" "$NEW_POD" |
  grep -F "Processed: job_id=$JOB_ID,"

echo "=== Redis Pending ==="
kubectl exec -n "$NS" deploy/redis -- \
  redis-cli XPENDING "$REDIS_STREAM" "$REDIS_GROUP"

echo "=== MongoDB result ==="
kubectl exec -n "$NS" mongo-0 -- \
  mongosh --quiet ai_mobility --eval \
  "printjson(db.inference_results.findOne({_id:'$JOB_ID'}))"

echo "=== Deployment ==="
kubectl get deployment "$DEPLOYMENT" -n "$NS"

echo "=== Summary ==="
echo "JOB_ID=$JOB_ID"
echo "STREAM_ID=$STREAM_ID"
echo "OLD_POD=$OLD_POD"
echo "NEW_POD=$NEW_POD"
echo "Recovery log detected."
