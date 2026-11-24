#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
backend=${1:-simulator}
case "$backend" in simulator|model) ;; *) echo "usage: scripts/compare.sh simulator|model" >&2; exit 2;; esac
port=${BENCH_PORT:-18080}
worker=${WORKER_URL:-http://127.0.0.1:8000}
model=${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}
mkdir -p results
out=$(mktemp -d "results/${backend}-XXXXXXXX")
base="http://127.0.0.1:$port"
pid=""
stop_gateway() {
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        pid=""
    fi
}
trap stop_gateway EXIT
trap 'exit 130' INT TERM
if curl -s --max-time 1 "$base/healthz" >/dev/null 2>&1; then echo "Port $port already in use. Set BENCH_PORT." >&2; exit 1; fi
for policy in fifo priority; do
    bin/gateway -listen "127.0.0.1:$port" -worker "$worker" -policy "$policy" -slots 1 >"$out/$policy.log" 2>&1 &
    pid=$!
    ready=0
    for ((i=0;i<50;i++)); do
        kill -0 "$pid" 2>/dev/null || break
        if curl -fsS --max-time 1 "$base/healthz" >/dev/null 2>&1; then ready=1; break; fi
        sleep 0.1
    done
    if [[ "$ready" != 1 ]] || ! kill -0 "$pid" 2>/dev/null; then cat "$out/$policy.log" >&2; exit 1; fi
    echo "Warming up $backend / $policy"
    bin/loadgen "$out/$policy-warmup.csv" "$base/v1/chat/completions" "$model" 4 10 >/dev/null
    echo "backend=$backend policy=$policy worker=$worker"
    bin/loadgen "$out/$policy.csv" "$base/v1/chat/completions" "$model" "${REQUESTS:-48}" "${SEED:-42}" "${TARGET_MS:-500}" | tee "$out/$policy.txt"
    stop_gateway
done
echo "Results in $out"
