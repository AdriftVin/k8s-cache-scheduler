#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-sched-bench}"
DEPLOY="${DEPLOY:-sched-bench}"
N="${N:-200}"


SCHED="${SCHED:-}"
RUNID="${RUNID:-run-$(date +%Y%m%d-%H%M%S)}"
APPID="${APPID:-$RUNID}"   

echo "[INFO] ns=$NS deploy=$DEPLOY N=$N scheduler=${SCHED:-default} run=$RUNID"

kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS" >/dev/null


cat > "/tmp/${DEPLOY}.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${DEPLOY}
  namespace: ${NS}
  labels:
    app: ${DEPLOY}
spec:
  replicas: 0
  selector:
    matchLabels:
      app: ${DEPLOY}
  template:
    metadata:
      labels:
        app: ${DEPLOY}
        bench-run: "${RUNID}"
        bench-scheduler: "${SCHED:-default}"
        applicationId: "${APPID}"
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.9
        imagePullPolicy: IfNotPresent
        resources:
          requests:
            cpu: "10m"
            memory: "16Mi"
YAML


kubectl -n "$NS" apply -f "/tmp/${DEPLOY}.yaml" >/dev/null

if [[ -n "$SCHED" ]]; then
  kubectl -n "$NS" patch deploy "$DEPLOY" --type='json' -p="[
    {\"op\":\"add\",\"path\":\"/spec/template/spec/schedulerName\",\"value\":\"$SCHED\"}
  ]" >/dev/null || true
else

  kubectl -n "$NS" patch deploy "$DEPLOY" --type='json' -p="[
    {\"op\":\"remove\",\"path\":\"/spec/template/spec/schedulerName\"}
  ]" >/dev/null 2>&1 || true
fi


kubectl -n "$NS" patch deploy "$DEPLOY" --type='merge' -p "{
  \"spec\": {\"template\": {\"metadata\": {\"labels\": {
    \"bench-run\": \"${RUNID}\",
    \"bench-scheduler\": \"${SCHED:-default}\",
    \"applicationId\": \"${APPID}\"
  }}}}
}" >/dev/null

echo "[INFO] scale to 0 (cleanup old pods)..."
kubectl -n "$NS" scale deploy "$DEPLOY" --replicas=0 >/dev/null

kubectl -n "$NS" wait pod -l app="$DEPLOY" --for=delete --timeout=15m >/dev/null 2>&1 || true

echo "[INFO] scale to $N..."
kubectl -n "$NS" scale deploy "$DEPLOY" --replicas="$N" >/dev/null

echo "[INFO] verifying pod count..."

for _ in $(seq 1 60); do
  created=$(kubectl -n "$NS" get pods -l app="$DEPLOY",bench-run="$RUNID" --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$created" -ge "$N" ]]; then break; fi
  sleep 2
done
created=$(kubectl -n "$NS" get pods -l app="$DEPLOY",bench-run="$RUNID" --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo "[INFO] created pods=$created expected=$N"
if [[ "$created" -lt "$N" ]]; then
  echo "[ERROR] Not enough pods created. Events:"
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -n 50 || true
  exit 1
fi

echo "[INFO] waiting PodScheduled..."
kubectl -n "$NS" wait pod -l app="$DEPLOY",bench-run="$RUNID" --for=condition=PodScheduled --timeout=30m >/dev/null

echo "[INFO] waiting Ready (best-effort)..."
kubectl -n "$NS" wait pod -l app="$DEPLOY",bench-run="$RUNID" --for=condition=Ready --timeout=45m >/dev/null || true

echo "[INFO] exporting pods json..."
kubectl -n "$NS" get pod -l app="$DEPLOY",bench-run="$RUNID" -o json > "pods_${RUNID}.json"

if kubectl top nodes >/dev/null 2>&1; then
  echo "[INFO] snapshot kubectl top nodes -> topnodes_${RUNID}.txt"
  kubectl top nodes > "topnodes_${RUNID}.txt" || true
fi

echo "[INFO] computing multi-dim stats..."
python3 - <<PY
import json, math, statistics
from datetime import datetime

def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def pct(xs, p):
    if not xs: return None
    xs = sorted(xs)
    k = (len(xs)-1) * p
    f = math.floor(k); c = math.ceil(k)
    if f == c: return xs[int(k)]
    return xs[f] + (xs[c]-xs[f]) * (k-f)

def jain(values):
    if not values: return None
    s = sum(values)
    if s == 0: return 1.0
    sq = sum(v*v for v in values)
    return (s*s) / (len(values) * sq) if sq > 0 else 1.0

def cpu_to_m(v):
    if v is None: return 0
    v = str(v).strip()
    if v.endswith("m"):
        return int(v[:-1])
    return int(float(v) * 1000)

def mem_to_mi(v):
    if v is None: return 0
    v = str(v).strip()
    units = {"Ki": 1/1024, "Mi": 1, "Gi": 1024, "Ti": 1024*1024}
    for u, mul in units.items():
        if v.endswith(u):
            return int(float(v[:-len(u)]) * mul)
    try:
        return int(int(v) / (1024*1024))
    except:
        return 0

data = json.load(open("pods_${RUNID}.json","r"))
items = data.get("items", [])
total = len(items)

sched_ms, ready_ms, post_ms = [], [], []
missing_sched = 0
missing_ready = 0

node_counts = {}
cpu_req_m = {}
mem_req_mi = {}

for it in items:
    cts = it["metadata"]["creationTimestamp"]
    node = it.get("spec", {}).get("nodeName") or "(none)"
    node_counts[node] = node_counts.get(node, 0) + 1

    creq = 0
    mreq = 0
    for c in it.get("spec", {}).get("containers", []):
        req = (c.get("resources") or {}).get("requests") or {}
        creq += cpu_to_m(req.get("cpu"))
        mreq += mem_to_mi(req.get("memory"))
    cpu_req_m[node] = cpu_req_m.get(node, 0) + creq
    mem_req_mi[node] = mem_req_mi.get(node, 0) + mreq

    sched_t = None
    ready_t = None
    for cond in it.get("status", {}).get("conditions", []):
        if cond.get("type") == "PodScheduled" and cond.get("status") == "True":
            sched_t = cond.get("lastTransitionTime")
        if cond.get("type") == "Ready" and cond.get("status") == "True":
            ready_t = cond.get("lastTransitionTime")

    if sched_t:
        sched_ms.append((parse(sched_t) - parse(cts)).total_seconds()*1000.0)
    else:
        missing_sched += 1

    if ready_t:
        ready_ms.append((parse(ready_t) - parse(cts)).total_seconds()*1000.0)
        if sched_t:
            post_ms.append((parse(ready_t) - parse(sched_t)).total_seconds()*1000.0)
    else:
        missing_ready += 1

def summarize(name, xs):
    if not xs:
        print(f"{name}: n=0")
        return
    print(f"{name}: n={len(xs)} mean={statistics.mean(xs):.2f} "
          f"p50={pct(xs,0.50):.2f} p90={pct(xs,0.90):.2f} p95={pct(xs,0.95):.2f} p99={pct(xs,0.99):.2f} max={max(xs):.2f}")

print(f"pods_total={total} scheduled={len(sched_ms)} ready={len(ready_ms)} missing_sched={missing_sched} missing_ready={missing_ready}")
summarize("schedule_ms (create->PodScheduled)", sched_ms)
summarize("ready_ms    (create->Ready)", ready_ms)
summarize("post_ms     (PodScheduled->Ready)", post_ms)

nodes = [n for n in node_counts.keys() if n != "(none)"]
counts = [node_counts[n] for n in nodes]
if counts:
    mean = statistics.mean(counts)
    stdev = statistics.pstdev(counts) if len(counts) > 1 else 0.0
    cv = (stdev/mean) if mean > 0 else 0.0
    print(f"placement_nodes={len(nodes)} pods_per_node: min={min(counts)} max={max(counts)} mean={mean:.2f} stdev={stdev:.2f} cv={cv:.3f} jain={jain(counts):.4f}")
    top = sorted(zip(nodes, counts), key=lambda x: x[1], reverse=True)[:5]
    print("top_nodes_by_pods:", "; ".join([f"{n}={c}" for n,c in top]))

cpu_vals = [cpu_req_m.get(n,0) for n in nodes]
mem_vals = [mem_req_mi.get(n,0) for n in nodes]
if cpu_vals:
    print(f"cpu_requests_m  per_node: min={min(cpu_vals)} max={max(cpu_vals)} mean={statistics.mean(cpu_vals):.2f} jain={jain(cpu_vals):.4f}")
if mem_vals:
    print(f"mem_requests_Mi per_node: min={min(mem_vals)} max={max(mem_vals)} mean={statistics.mean(mem_vals):.2f} jain={jain(mem_vals):.4f}")
PY

echo "[INFO] outputs: pods_${RUNID}.json (and maybe topnodes_${RUNID}.txt)"
