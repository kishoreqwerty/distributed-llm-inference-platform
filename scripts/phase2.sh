#!/usr/bin/env bash
# Phase 2 benchmark (macOS only): FIFO and priority gateways with 1, 2 and 4 slots in front of the
# Mac worker. Each seed replays the same 6,000-request open-loop workload (20 req/s) for every
# configuration; configuration order rotates across seeds. Every measured run gets an identical
# warm-up first, and the worker's /metrics is sampled during the measured run.
#
# usage: scripts/phase2.sh
# Overrides: CONFIGS="fifo-1 ... priority-4"  SEEDS="42 43 44"  REQUESTS=6000  RATE=20  TARGET_MS=500
#            LOADGEN_CONCURRENCY=256  BENCH_PORT=18090  WORKER_URL=http://127.0.0.1:8000  MODEL=...
#            WARMUP_REQUESTS=100  GATEWAY_QUEUE=128  COOLDOWN_S=10  RESULTS_ROOT=results
#
# The script only starts and stops its own gateway on BENCH_PORT. It never signals the worker or any
# other process.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$(uname -s)" == Darwin ]] || { echo "phase2.sh is macOS-only" >&2; exit 2; }

configs=(${CONFIGS:-fifo-1 fifo-2 fifo-4 priority-1 priority-2 priority-4})
seeds=(${SEEDS:-42 43 44})
requests=${REQUESTS:-6000} rate=${RATE:-20} target_ms=${TARGET_MS:-500}
concurrency=${LOADGEN_CONCURRENCY:-256}
port=${BENCH_PORT:-18090}
worker=${WORKER_URL:-http://127.0.0.1:8000}; worker=${worker%/}
model=${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}
warmup_requests=${WARMUP_REQUESTS:-100} warmup_seed=7
queue=${GATEWAY_QUEUE:-128}
cooldown=${COOLDOWN_S:-10}
results_root=${RESULTS_ROOT:-results}
base="http://127.0.0.1:$port"

for c in "${configs[@]}"; do [[ "$c" =~ ^(fifo|priority)-[0-9]+$ ]] || { echo "bad config '$c' (want fifo-N or priority-N)" >&2; exit 2; }; done
for s in "${seeds[@]}"; do [[ "$s" =~ ^[0-9]+$ ]] || { echo "bad seed '$s'" >&2; exit 2; }; done
[[ -x bin/gateway && -x bin/loadgen ]] || { echo "missing bin/gateway or bin/loadgen: run 'make build'" >&2; exit 1; }
if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then echo "port $port is in use; set BENCH_PORT" >&2; exit 1; fi

payload=$(printf '{"model":"%s","stream":true,"temperature":0,"max_tokens":2,"messages":[{"role":"user","content":"Say hi."}]}' "$model")
probe_worker() {
    local body
    body=$(curl -sS -N --max-time 60 -H 'Content-Type: application/json' -d "$payload" "$worker/v1/chat/completions" 2>&1) \
        || { echo "worker probe failed: $body" >&2; return 1; }
    grep -q '^data: *\[DONE\]' <<<"$body" || { echo "worker probe did not finish with [DONE]" >&2; return 1; }
}
probe_worker || exit 1
metrics_status=$(curl -s -o /dev/null --max-time 3 -w '%{http_code}' "$worker/metrics" || true)
[[ "$metrics_status" == 200 ]] && metrics="available" || metrics="unavailable (HTTP ${metrics_status:-none})"
echo "Worker $worker OK; /metrics $metrics"

mkdir -p "$results_root"
out=$(mktemp -d "$results_root/phase2-$(date +%Y%m%d-%H%M%S)-XXXX")
{
    echo "date=$(date '+%Y-%m-%dT%H:%M:%S%z')"
    echo "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo none) git_dirty=$(git status --porcelain --untracked-files=no 2>/dev/null | grep -q . && echo yes || echo no)"
    echo "worker=$worker model=$model metrics=$metrics"
    echo "requests=$requests rate_rps=$rate target_ms=$target_ms client_concurrency=$concurrency"
    echo "configs=${configs[*]} seeds=${seeds[*]} gateway_queue=$queue gateway_port=$port"
    echo "warmup_requests=$warmup_requests warmup_seed=$warmup_seed warmup_rate_rps=$rate cooldown_s=$cooldown"
} >"$out/meta.txt"
echo "Results: $out"

gw_pid=""
stop_gateway() {
    if [[ -n "$gw_pid" ]]; then kill "$gw_pid" 2>/dev/null || true; wait "$gw_pid" 2>/dev/null || true; gw_pid=""; fi
}
trap stop_gateway EXIT
trap 'exit 130' INT TERM

start_gateway() { # policy slots logfile
    bin/gateway -listen "127.0.0.1:$port" -worker "$worker" -policy "$1" -slots "$2" -queue "$queue" >"$3" 2>&1 &
    gw_pid=$!
    for ((i = 0; i < 50; i++)); do
        kill -0 "$gw_pid" 2>/dev/null || break
        curl -fsS --max-time 1 "$base/healthz" >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    cat "$3" >&2; echo "gateway failed to start" >&2; return 1
}

# Wait until the worker reports no running requests (if exposed), else a fixed cool-down.
settle() {
    if [[ "$metrics" == available ]]; then
        for ((i = 0; i < 120; i++)); do
            running=$(curl -s --max-time 2 "$worker/metrics" | awk '$1 ~ /^vllm:num_requests_running([{]|$)/ {s += $NF} END {print s + 0}')
            [[ "$running" == 0 ]] && return 0
            sleep 0.5
        done
        echo "warning: worker still reports running requests after 60 s" >&2
    else
        sleep "$cooldown"
    fi
}

val() { # line_prefix key file -> value (or n/a)
    awk -v p="$1" -v k="$2" '$1 == p { for (i = 2; i <= NF; i++) if (index($i, k "=") == 1) { print substr($i, length(k) + 2); found = 1; exit } }
        END { if (!found) print "n/a" }' "$3"
}

tsv="$out/runs.tsv"
printf 'seed\torder\tpolicy\tslots\turgent_p95_ttft_ms\turgent_within_target\turgent_offered\turgent_within_pct\tcompletions_per_sec\tok\trejected\tfailed\tinterrupted\tlateness_p95_ms\tlateness_max_ms\twaited_for_client\tclient_saturated\trunning_max\trunning_mean\twaiting_max\tkv_cache_usage_max\tkv_cache_usage_mean\tloadgen_rc\n' >"$tsv"

n=${#configs[@]} s_count=${#seeds[@]}
for ((k = 0; k < s_count; k++)); do
    seed=${seeds[$k]}
    shift_by=$(( (k * n / s_count) % n ))
    for ((j = 0; j < n; j++)); do
        cfg=${configs[$(( (j + shift_by) % n ))]}
        policy=${cfg%-*} slots=${cfg##*-}
        order=$((j + 1))
        dir="$out/seed$seed/$(printf '%02d' "$order")-$cfg"
        mkdir -p "$dir"
        echo "=== seed=$seed order=$order policy=$policy slots=$slots ($(date +%H:%M:%S))"
        probe_worker || { echo "worker unhealthy before $cfg seed $seed; stopping" >&2; exit 1; }
        settle
        start_gateway "$policy" "$slots" "$dir/gateway.log"
        LOADGEN_CONCURRENCY=$concurrency bin/loadgen "$dir/warmup.csv" "$base/v1/chat/completions" "$model" \
            "$warmup_requests" "$warmup_seed" "$target_ms" "$rate" >"$dir/warmup.txt" 2>"$dir/warmup.err" || true
        settle
        set +e
        LOADGEN_CONCURRENCY=$concurrency LOADGEN_METRICS_URL="$worker/metrics" bin/loadgen "$dir/run.csv" \
            "$base/v1/chat/completions" "$model" "$requests" "$seed" "$target_ms" "$rate" >"$dir/summary.txt" 2>"$dir/loadgen.err"
        rc=$?
        set -e
        stop_gateway
        f="$dir/summary.txt"
        grep -q '^total ' "$f" || { echo "loadgen produced no summary (rc=$rc): $(head -3 "$dir/loadgen.err")" >&2; exit 1; }
        printf '%s\n' "$seed" "$order" "$policy" "$slots" \
            "$(val urgent p95_ttft_ms "$f")" "$(val urgent within_target "$f")" "$(val urgent offered "$f")" "$(val urgent within_target_pct "$f")" \
            "$(val total completions_per_sec "$f")" "$(val total ok "$f")" "$(val total rejected "$f")" "$(val total failed "$f")" "$(val total interrupted_streams "$f")" \
            "$(val dispatch lateness_p95_ms "$f")" "$(val dispatch lateness_max_ms "$f")" "$(val dispatch waited_for_client "$f")" "$(val dispatch client_saturated "$f")" \
            "$(val worker_metrics running_max "$f")" "$(val worker_metrics running_mean "$f")" "$(val worker_metrics waiting_max "$f")" \
            "$(val worker_metrics kv_cache_usage_max "$f")" "$(val worker_metrics kv_cache_usage_mean "$f")" "$rc" | paste -sd '\t' - >>"$tsv"
        tail -1 "$tsv" | awk -F'\t' '{printf "    urgent p95=%s ms within500=%s/%s  completions/s=%s  ok=%s rejected=%s failed=%s  lateness p95=%s ms saturated=%s  running_max=%s kv_max=%s\n", $5, $6, $7, $9, $10, $11, $12, $14, $17, $18, $21}'
    done
done
stop_gateway
echo
column -t -s $'\t' "$tsv" | tee "$out/runs.txt"
echo "Results: $out"
