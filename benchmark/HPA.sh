#!/usr/bin/env bash
set -euo pipefail

# HPA + Fortio benchmark focused on "time-to-recover" under autoscaling,
# while also collecting scheduling/ready latencies for newly created pods.

# -----------------------------
# Config (override via env)
# -----------------------------
NS="${NS:-as-default}"
SCHEDULER_NAME="${SCHEDULER_NAME:-default-scheduler}"
RUNID_RAW="${RUNID:-eval-1}"
RUNID="$(echo "$RUNID_RAW" | tr -cs 'a-zA-Z0-9.-' '-' | sed 's/^-//; s/-$//')"

APP_NAME="${APP_NAME:-php-apache}"
APP_IMAGE="${APP_IMAGE:-registry.k8s.io/hpa-example}"
APP_PORT="${APP_PORT:-80}"
APP_REPLICAS_INIT="${APP_REPLICAS_INIT:-3}"

HPA_MIN="${HPA_MIN:-3}"
HPA_MAX="${HPA_MAX:-20}"
HPA_TARGET_CPU="${HPA_TARGET_CPU:-50}"  # averageUtilization

FORTIO_IMAGE="${FORTIO_IMAGE:-fortio/fortio:latest}"
QPS="${QPS:-0}"                 # 0 -> max
FORTIO_TIMEOUT="${FORTIO_TIMEOUT:-15s}"

# SLA definition for "recovered"
SLA_P99="${SLA_P99:-2.0}"        # seconds
SLA_ERR_PCT="${SLA_ERR_PCT:-1.0}" # percent

# FIND: increase concurrency until SLA violated
FIND_START="${FIND_START:-10}"
FIND_STEP="${FIND_STEP:-10}"
FIND_MAX="${FIND_MAX:-100}"
FIND_DURATION="${FIND_DURATION:-60s}"

# HOLD: fixed concurrency (found in FIND) in short windows
HOLD_WINDOW="${HOLD_WINDOW:-30s}"
HOLD_TOTAL="${HOLD_TOTAL:-10m}"
RECOVERY_STREAK="${RECOVERY_STREAK:-2}"  # N consecutive OK windows to declare recovered

# misc
WAIT_ROLLOUT_TIMEOUT="${WAIT_ROLLOUT_TIMEOUT:-10m}"
WAIT_ENDPOINTS_TIMEOUT="${WAIT_ENDPOINTS_TIMEOUT:-240}"
PROPAGATION_SLEEP="${PROPAGATION_SLEEP:-10}"
WARMUP_ENABLE="${WARMUP_ENABLE:-1}"
WARMUP_SECONDS="${WARMUP_SECONDS:-10}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"

OUTDIR="${OUTDIR:-./as_out}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUTDIR/$NS/$RUNID/$TS"
mkdir -p "$OUT"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# duration like 30s / 2m / 1h -> seconds
parse_duration_s() {
  local d="$1"
  if [[ "$d" =~ ^([0-9]+)([smh]?)$ ]]; then
    local n="${BASH_REMATCH[1]}"; local u="${BASH_REMATCH[2]}"; [[ -z "$u" ]] && u="s"
    case "$u" in
      s) echo "$n";;
      m) echo $((n*60));;
      h) echo $((n*3600));;
      *) echo 0;;
    esac
  else
    echo 0
  fi
}

wait_rollout() {
  log "[INFO] rollout status deploy/$1 (ns=$NS)"
  kubectl -n "$NS" rollout status "deploy/$1" --timeout="$WAIT_ROLLOUT_TIMEOUT"
}

wait_endpoints() {
  local svc="$1" timeout="$2"
  log "[INFO] waiting endpoints svc/$svc (timeout=${timeout}s)"
  local start now
  start=$(date +%s)
  while true; do
    local ep_count es_count
    ep_count="$(kubectl -n "$NS" get endpoints "$svc" -o jsonpath='{range .subsets[*].addresses[*]}1{end}' 2>/dev/null | wc -c | tr -d ' ' || true)"
    es_count="$(kubectl -n "$NS" get endpointslice -l "kubernetes.io/service-name=$svc" -o jsonpath='{range .items[*].endpoints[*]}{range .addresses[*]}1{end}{end}' 2>/dev/null | wc -c | tr -d ' ' || true)"
    if [[ "${ep_count:-0}" -gt 0 || "${es_count:-0}" -gt 0 ]]; then
      log "[INFO] endpoints ready (endpoints=$ep_count endpointslice=$es_count)"
      return 0
    fi
    now=$(date +%s)
    if (( now - start > timeout )); then
      log "[ERROR] endpoints not ready after ${timeout}s"
      return 1
    fi
    sleep 3
  done
}

# -----------------------------
# Deploy app + svc + HPA
# -----------------------------
log "[INFO] ns=$NS scheduler=$SCHEDULER_NAME run=$RUNID out=$OUT"

kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS" >/dev/null

cat > "$OUT/manifests.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${APP_NAME}
  namespace: ${NS}
  labels:
    app: ${APP_NAME}
    bench-run: ${RUNID}
    bench-scheduler: ${SCHEDULER_NAME}
spec:
  replicas: ${APP_REPLICAS_INIT}
  selector:
    matchLabels:
      app: ${APP_NAME}
  template:
    metadata:
      labels:
        app: ${APP_NAME}
        bench-run: ${RUNID}
        bench-scheduler: ${SCHEDULER_NAME}
    spec:
      schedulerName: ${SCHEDULER_NAME}
      containers:
      - name: ${APP_NAME}
        image: ${APP_IMAGE}
        ports:
        - containerPort: ${APP_PORT}
        resources:
          requests:
            cpu: "200m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "256Mi"
---
apiVersion: v1
kind: Service
metadata:
  name: ${APP_NAME}
  namespace: ${NS}
  labels:
    app: ${APP_NAME}
spec:
  type: ClusterIP
  selector:
    app: ${APP_NAME}
  ports:
  - port: 80
    targetPort: ${APP_PORT}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ${APP_NAME}
  namespace: ${NS}
  labels:
    app: ${APP_NAME}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ${APP_NAME}
  minReplicas: ${HPA_MIN}
  maxReplicas: ${HPA_MAX}
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: ${HPA_TARGET_CPU}
YAML

kubectl apply -f "$OUT/manifests.yaml" | tee "$OUT/apply.log" >/dev/null

wait_rollout "$APP_NAME"
sleep "$PROPAGATION_SLEEP"
wait_endpoints "$APP_NAME" "$WAIT_ENDPOINTS_TIMEOUT"

URL="http://${APP_NAME}"

if [[ "$WARMUP_ENABLE" == "1" ]]; then
  sleep "$WARMUP_SECONDS"
  log "[INFO] warmup curl x20 (best effort)"
  kubectl -n "$NS" run warmcurl --restart=Never --rm -i --image=curlimages/curl:8.5.0 -- \
    sh -lc "for i in \$(seq 1 20); do curl -fsS -m 3 ${URL}/ >/dev/null || true; sleep 0.2; done" \
    >/dev/null 2>&1 || true
fi

if ! kubectl -n "$NS" top pods >/dev/null 2>&1; then
  log "[WARN] 'kubectl top pods' failed -> metrics may be unavailable; HPA might not scale (FailedGetResourceMetric)."
fi

# -----------------------------
# Timelines (polling)
# -----------------------------
HPA_TIMELINE="$OUT/hpa_timeline.csv"
PODS_TIMELINE="$OUT/pods_timeline.csv"

echo "ts_utc,epoch,currentReplicas,desiredReplicas" > "$HPA_TIMELINE"
echo "ts_utc,epoch,totalPods,readyPods" > "$PODS_TIMELINE"

poll_hpa() {
  while true; do
    local ts epoch curr des
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    epoch="$(date +%s)"
    curr="$(kubectl -n "$NS" get hpa "$APP_NAME" -o jsonpath='{.status.currentReplicas}' 2>/dev/null || true)"
    des="$(kubectl -n "$NS" get hpa "$APP_NAME" -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || true)"
    echo "${ts},${epoch},${curr},${des}" >> "$HPA_TIMELINE"
    sleep "$POLL_INTERVAL"
  done
}

poll_pods() {
  while true; do
    local ts epoch total ready
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    epoch="$(date +%s)"
    total="$(kubectl -n "$NS" get pods -l app="$APP_NAME" --no-headers 2>/dev/null | wc -l | tr -d ' ' || true)"
    ready="$(kubectl -n "$NS" get pods -l app="$APP_NAME" -o jsonpath='{range .items[*]}{range .status.conditions[?(@.type=="Ready")]}{.status}{"\n"}{end}{end}' 2>/dev/null | grep -c '^True$' || true)"
    echo "${ts},${epoch},${total},${ready}" >> "$PODS_TIMELINE"
    sleep "$POLL_INTERVAL"
  done
}

POLL_PIDS=()
poll_hpa & POLL_PIDS+=("$!")
poll_pods & POLL_PIDS+=("$!")

cleanup() {
  for pid in "${POLL_PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

# -----------------------------
# Fortio job runner
# -----------------------------
JOB_BASE="loadgen-${RUNID}"
: > "$OUT/fortio.log"

make_job_yaml() {
  local job="$1" c="$2" dur="$3" out="$4"
  cat > "$out" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  namespace: ${NS}
  labels:
    app: loadgen
    bench-run: ${RUNID}
    bench-scheduler: ${SCHEDULER_NAME}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: loadgen
        bench-run: ${RUNID}
        bench-scheduler: ${SCHEDULER_NAME}
    spec:
      restartPolicy: Never
      containers:
      - name: fortio
        image: ${FORTIO_IMAGE}
        args:
          - load
          - -t
          - "${dur}"
          - -c
          - "${c}"
          - -qps
          - "${QPS}"
          - -timeout
          - "${FORTIO_TIMEOUT}"
          - -nocatchup
          - -allow-initial-errors
          - "${URL}"
YAML
}

wait_job() {
  local job="$1" timeout_s="$2"
  local deadline=$(( $(date +%s) + timeout_s ))
  while true; do
    local cond
    cond="$(kubectl -n "$NS" get job "$job" -o jsonpath='{range .status.conditions[*]}{.type}={.status}{" "}{end}' 2>/dev/null || true)"
    if echo "$cond" | grep -q 'Complete=True'; then return 0; fi
    if echo "$cond" | grep -q 'Failed=True'; then return 1; fi
    if (( $(date +%s) > deadline )); then return 1; fi
    sleep 3
  done
}

run_stage() {
  local job="$1" c="$2" dur="$3"
  local y="$OUT/${job}.yaml"
  kubectl -n "$NS" delete job "$job" --ignore-not-found >/dev/null 2>&1 || true
  make_job_yaml "$job" "$c" "$dur" "$y"
  kubectl apply -f "$y" >/dev/null
  log "[INFO] run $job (c=$c t=$dur)"
  if ! wait_job "$job" 1800; then
    log "[ERROR] job failed: $job"
    kubectl -n "$NS" logs "job/$job" --tail=2000 > "$OUT/fortio_${job}.log" 2>/dev/null || true
    return 1
  fi
  kubectl -n "$NS" logs "job/$job" --tail=2000 > "$OUT/fortio_${job}.log" 2>/dev/null || true
  {
    echo "===== ${job} (c=${c} t=${dur}) ====="
    cat "$OUT/fortio_${job}.log" || true
    echo
  } >> "$OUT/fortio.log"
  return 0
}

extract_metrics() {
  # output: calls qps p99 err_pct
  local f="$1"
  local ended calls qps p99 code200 err_pct
  ended="$(grep -m1 'Ended after' "$f" || true)"
  calls="$(echo "$ended" | sed -E 's/.*: ([0-9]+) calls.*/\1/' || true)"
  qps="$(echo "$ended" | sed -E 's/.*qps=([0-9.]+).*/\1/' || true)"
  p99="$(grep -m1 '^# target 99% ' "$f" | awk '{print $4}' || true)"
  code200="$(grep -m1 '^Code 200' "$f" | sed -E 's/Code 200 : ([0-9]+).*/\1/' || true)"
  [[ -z "${calls:-}" ]] && calls=0
  [[ -z "${qps:-}" ]] && qps=0
  [[ -z "${p99:-}" ]] && p99=0
  [[ -z "${code200:-}" ]] && code200=0
  err_pct="$(awk -v c="$calls" -v ok="$code200" 'BEGIN{ if(c<=0){print 0.0; exit} e=c-ok; if(e<0)e=0; printf("%.4f", (e*100.0)/c)}')"
  echo "$calls $qps $p99 $err_pct"
}

is_breach() {
  local p99="$1" err="$2"
  awk -v p="$p99" -v e="$err" -v sp="$SLA_P99" -v se="$SLA_ERR_PCT" 'BEGIN{ if(p>sp || e>se) print 1; else print 0 }'
}

is_ok() {
  local p99="$1" err="$2"
  awk -v p="$p99" -v e="$err" -v sp="$SLA_P99" -v se="$SLA_ERR_PCT" 'BEGIN{ if(p<=sp && e<=se) print 1; else print 0 }'
}

# -----------------------------
# FIND stage
# -----------------------------
FIND_CSV="$OUT/find.csv"
echo "idx,start_epoch,end_epoch,concurrency,duration,calls,qps,p99,err_pct,breach" > "$FIND_CSV"

log "[INFO] FIND: ramp concurrency until SLA violated (p99>${SLA_P99}s or err%>${SLA_ERR_PCT}%)"
idx=0
THRESH_C=""

for c in $(seq "$FIND_START" "$FIND_STEP" "$FIND_MAX"); do
  idx=$((idx+1))
  job="${JOB_BASE}-find-${idx}"
  start_epoch="$(date +%s)"
  if ! run_stage "$job" "$c" "$FIND_DURATION"; then
    break
  fi
  end_epoch="$(date +%s)"
  read -r calls qps p99 err_pct < <(extract_metrics "$OUT/fortio_${job}.log")
  breach="$(is_breach "$p99" "$err_pct")"
  echo "${idx},${start_epoch},${end_epoch},${c},${FIND_DURATION},${calls},${qps},${p99},${err_pct},${breach}" >> "$FIND_CSV"
  if [[ "$breach" == "1" ]]; then
    THRESH_C="$c"
    log "[INFO] FIND: SLA violated at c=$THRESH_C (p99=$p99 err%=$err_pct)"
    break
  else
    log "[INFO] FIND: OK at c=$c (p99=$p99 err%=$err_pct)"
  fi
done

if [[ -z "${THRESH_C:-}" ]]; then
  THRESH_C="$FIND_MAX"
  log "[WARN] FIND: no violation up to FIND_MAX=$FIND_MAX, using hold_concurrency=$THRESH_C"
fi

# -----------------------------
# HOLD stage
# -----------------------------
HOLD_CSV="$OUT/hold.csv"
echo "idx,start_epoch,end_epoch,concurrency,duration,calls,qps,p99,err_pct,sla_ok" > "$HOLD_CSV"

hold_total_s="$(parse_duration_s "$HOLD_TOTAL")"
window_s="$(parse_duration_s "$HOLD_WINDOW")"
if [[ "$hold_total_s" -le 0 || "$window_s" -le 0 ]]; then
  log "[ERROR] invalid HOLD_TOTAL=$HOLD_TOTAL or HOLD_WINDOW=$HOLD_WINDOW"
  exit 2
fi
nwin=$(( (hold_total_s + window_s - 1) / window_s ))

log "[INFO] HOLD: c=$THRESH_C window=$HOLD_WINDOW total=$HOLD_TOTAL (~$nwin windows)"
HOLD_START_EPOCH="$(date +%s)"

breach_seen=0
streak=0
TTR_SEC="-1"

for i in $(seq 1 "$nwin"); do
  job="${JOB_BASE}-hold-${i}"
  start_epoch="$(date +%s)"
  if ! run_stage "$job" "$THRESH_C" "$HOLD_WINDOW"; then
    break
  fi
  end_epoch="$(date +%s)"
  read -r calls qps p99 err_pct < <(extract_metrics "$OUT/fortio_${job}.log")
  ok="$(is_ok "$p99" "$err_pct")"
  echo "${i},${start_epoch},${end_epoch},${THRESH_C},${HOLD_WINDOW},${calls},${qps},${p99},${err_pct},${ok}" >> "$HOLD_CSV"

  if [[ "$ok" == "0" ]]; then
    breach_seen=1
    streak=0
  else
    if [[ "$breach_seen" == "1" ]]; then
      streak=$((streak+1))
      if [[ "$TTR_SEC" == "-1" && "$streak" -ge "$RECOVERY_STREAK" ]]; then
        TTR_SEC=$(( end_epoch - HOLD_START_EPOCH ))
        log "[INFO] RECOVERED at +${TTR_SEC}s (streak=${streak})"
      fi
    fi
  fi
done

# Determine TTR mode
TTR_MODE="recovered"
if [[ "$breach_seen" == "0" ]]; then
  # Service never violated SLA during HOLD, so "recovery time" is not applicable.
  TTR_MODE="no_degradation"
  TTR_SEC="0"
elif [[ "$TTR_SEC" == "-1" ]]; then
  # SLA was violated but did not recover within HOLD window.
  TTR_MODE="no_recovery_within_hold"
fi

# Peak metrics in HOLD (for headroom / context)
HOLD_MAX_P99="$(awk -F',' 'NR>1 && $8!="" {if($8+0>m)m=$8+0} END{if(m=="") print ""; else printf "%.6f", m}' "$HOLD_CSV" 2>/dev/null || true)"
HOLD_MAX_ERR_PCT="$(awk -F',' 'NR>1 && $9!="" {if($9+0>m)m=$9+0} END{if(m=="") print ""; else printf "%.6f", m}' "$HOLD_CSV" 2>/dev/null || true)"

# -----------------------------
# Collect state snapshots
# -----------------------------
kubectl -n "$NS" get deploy,svc,hpa,pods -o wide > "$OUT/state_after.txt" || true
kubectl -n "$NS" get events --sort-by=.lastTimestamp > "$OUT/events.txt" || true
kubectl top nodes > "$OUT/top_nodes.txt" 2>/dev/null || true
kubectl -n "$NS" top pods > "$OUT/top_pods.txt" 2>/dev/null || true

# -----------------------------
# Pod latency analysis for pods created after HOLD start
# -----------------------------
kubectl -n "$NS" get pods -l app="$APP_NAME" -o json > "$OUT/pods.json" || true

python3 - <<PY
import json, datetime, math
out = r"$OUT"
hold_start = int("$HOLD_START_EPOCH")

def ts(s):
    if not s:
        return None
    return int(datetime.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp())

with open(out + '/pods.json','r',encoding='utf-8') as f:
    data = json.load(f)

rows=[]
sched=[]
ready=[]
startup=[]

for pod in data.get('items',[]):
    name = pod['metadata']['name']
    cts = ts(pod['metadata'].get('creationTimestamp'))
    if cts is None or cts < hold_start:
        continue
    st = rt = None
    for c in (pod.get('status',{}).get('conditions',[]) or []):
        if c.get('type')=='PodScheduled' and c.get('status')=='True':
            st = ts(c.get('lastTransitionTime'))
        if c.get('type')=='Ready' and c.get('status')=='True':
            rt = ts(c.get('lastTransitionTime'))
    rows.append((name, cts, st, rt))
    if st is not None:
        sched.append(st-cts)
    if rt is not None:
        ready.append(rt-cts)
    if st is not None and rt is not None:
        startup.append(rt-st)

with open(out + '/pod_latencies.tsv','w',encoding='utf-8') as f:
    f.write('pod\tcreation_epoch\tscheduled_epoch\tready_epoch\tsched_latency_s\tready_latency_s\tstartup_after_sched_s\n')
    for name, cts, st, rt in rows:
        sl = '' if st is None else st-cts
        rl = '' if rt is None else rt-cts
        su = '' if (st is None or rt is None) else rt-st
        f.write(f"{name}\t{cts}\t{'' if st is None else st}\t{'' if rt is None else rt}\t{sl}\t{rl}\t{su}\n")

def q(xs,p):
    if not xs:
        return None
    xs=sorted(xs)
    k=max(1, math.ceil(p*len(xs)))
    return xs[k-1]

with open(out + '/pod_latency_summary.txt','w',encoding='utf-8') as f:
    for label, xs in [('sched_latency_s', sched), ('ready_latency_s', ready), ('startup_after_sched_s', startup)]:
        if not xs:
            f.write(f"{label}: n=0\n")
            continue
        f.write(f"{label}: n={len(xs)} min={min(xs)} p50={q(xs,0.5)} p90={q(xs,0.9)} p99={q(xs,0.99)} max={max(xs)}\n")
PY

# -----------------------------
# Summary
# -----------------------------
cat > "$OUT/summary.txt" <<TXT
ns=${NS}
scheduler=${SCHEDULER_NAME}
run=${RUNID}
out=${OUT}

sla_p99_s=${SLA_P99}
sla_err_pct=${SLA_ERR_PCT}

hold_start_epoch=${HOLD_START_EPOCH}
recovery_streak=${RECOVERY_STREAK}
ttr_mode=${TTR_MODE}
ttr_sec=${TTR_SEC}
hold_max_p99_s=${HOLD_MAX_P99}
hold_max_err_pct=${HOLD_MAX_ERR_PCT}

See:
  find.csv / hold.csv
  fortio.log
  hpa_timeline.csv / pods_timeline.csv
  pod_latency_summary.txt
TXT

log "[INFO] DONE. Artifacts at: $OUT"
log "[INFO] Key: summary.txt, fortio.log, find.csv, hold.csv, pod_latency_summary.txt"

