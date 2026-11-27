#!/usr/bin/env bash
# Phase 3 routing comparison (macOS only): primary-only (Mac only), round-robin and load-aware
# gateways over the workers in a routing config. Each seed replays the same open-loop workload for
# every mode; mode order rotates across seeds. Every measured run gets an identical warm-up first.
#
# usage: scripts/phase3.sh
# Overrides: MODES="primary-only round-robin load-aware"  SEEDS="42"  REQUESTS=1200  RATE=20
#            TARGET_MS=500  POLICY=priority  CONFIG=configs/phase3-mac-rog.json  BENCH_PORT=18091
#            MODEL=...  LOADGEN_CONCURRENCY=256  WARMUP_REQUESTS=100  GATEWAY_QUEUE=128
#            COOLDOWN_S=10  RESULTS_ROOT=results
#
# Worker URLs, slots and time estimates come from CONFIG. The script only starts and stops its own
# gateway on BENCH_PORT. It never signals a worker or any other process.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$(uname -s)" == Darwin ]] || { echo "phase3.sh is macOS-only" >&2; exit 2; }

modes=(${MODES:-primary-only round-robin load-aware})
seeds=(${SEEDS:-42})
requests=${REQUESTS:-1200} rate=${RATE:-20} target_ms=${TARGET_MS:-500} policy=${POLICY:-priority}
config=${CONFIG:-configs/phase3-mac-rog.json}
concurrency=${LOADGEN_CONCURRENCY:-256}
port=${BENCH_PORT:-18091}
model=${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}
warmup_requests=${WARMUP_REQUESTS:-100} warmup_seed=7
queue=${GATEWAY_QUEUE:-128}
cooldown=${COOLDOWN_S:-10}
results_root=${RESULTS_ROOT:-results}
base="http://127.0.0.1:$port"

for m in "${modes[@]}"; do [[ "$m" =~ ^(primary-only|round-robin|load-aware)$ ]] || { echo "bad mode '$m'" >&2; exit 2; }; done
for s in "${seeds[@]}"; do [[ "$s" =~ ^[0-9]+$ ]] || { echo "bad seed '$s'" >&2; exit 2; }; done
[[ "$policy" == fifo || "$policy" == priority ]] || { echo "POLICY must be fifo or priority" >&2; exit 2; }
[[ -x bin/gateway && -x bin/loadgen ]] || { echo "missing bin/gateway or bin/loadgen: run 'make build'" >&2; exit 1; }
[[ -f "$config" ]] || { echo "missing routing config $config" >&2; exit 1; }
if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then echo "port $port is in use; set BENCH_PORT" >&2; exit 1; fi

count=$(plutil -extract workers raw -o - "$config")
names=() urls=()
for ((i = 0; i < count; i++)); do
    names+=("$(plutil -extract "workers.$i.name" raw -o - "$config")")
    u=$(plutil -extract "workers.$i.url" raw -o - "$config"); urls+=("${u%/}")
done

payload=$(printf '{"model":"%s","stream":true,"temperature":0,"max_tokens":2,"messages":[{"role":"user","content":"Say hi."}]}' "$model")
probe_workers() {
    local i body
    for ((i = 0; i < count; i++)); do
        body=$(curl -sS -N --max-time 60 -H 'Content-Type: application/json' -d "$payload" "${urls[$i]}/v1/chat/completions" 2>&1) \
            || { echo "worker ${names[$i]} probe failed: $body" >&2; return 1; }
        grep -q '^data: *\[DONE\]' <<<"$body" || { echo "worker ${names[$i]} probe did not finish with [DONE]" >&2; return 1; }
    done
}
probe_workers || exit 1
metrics=()
for ((i = 0; i < count; i++)); do
    code=$(curl -s -o /dev/null --max-time 3 -w '%{http_code}' "${urls[$i]}/metrics" || true)
    [[ "$code" == 200 ]] && metrics+=(available) || metrics+=("unavailable(HTTP_${code:-none})")
    echo "Worker ${names[$i]} ${urls[$i]} OK; /metrics ${metrics[$i]}"
done

mkdir -p "$results_root"
out=$(mktemp -d "$results_root/phase3-$(date +%Y%m%d-%H%M%S)-XXXX")
cp "$config" "$out/routing-config.json"
{
    echo "date=$(date '+%Y-%m-%dT%H:%M:%S%z')"
    echo "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo none) git_dirty=$(git status --porcelain --untracked-files=no 2>/dev/null | grep -q . && echo yes || echo no)"
    echo "config=$config model=$model"
    for ((i = 0; i < count; i++)); do echo "worker=${names[$i]} url=${urls[$i]} metrics=${metrics[$i]}"; done
    echo "requests=$requests rate_rps=$rate target_ms=$target_ms policy=$policy client_concurrency=$concurrency"
    echo "modes=${modes[*]} seeds=${seeds[*]} gateway_queue=$queue gateway_port=$port"
    echo "warmup_requests=$warmup_requests warmup_seed=$warmup_seed warmup_rate_rps=$rate cooldown_s=$cooldown"
} >"$out/meta.txt"
echo "Results: $out"

gw_pid=""
stop_gateway() {
    if [[ -n "$gw_pid" ]]; then kill "$gw_pid" 2>/dev/null || true; wait "$gw_pid" 2>/dev/null || true; gw_pid=""; fi
}
trap stop_gateway EXIT
trap 'exit 130' INT TERM

start_gateway() { # mode logfile
    bin/gateway -listen "127.0.0.1:$port" -route "$1" -workers "$config" -policy "$policy" -queue "$queue" >"$2" 2>&1 &
    gw_pid=$!
    for ((i = 0; i < 50; i++)); do
        kill -0 "$gw_pid" 2>/dev/null || break
        curl -fsS --max-time 1 "$base/healthz" >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    cat "$2" >&2; echo "gateway failed to start" >&2; return 1
}

# Wait until every worker that exposes vllm:num_requests_running reports 0; otherwise a fixed cool-down.
settle() {
    local i n running fixed=0
    for ((i = 0; i < count; i++)); do
        [[ "${metrics[$i]}" == available ]] || { fixed=1; continue; }
        for ((n = 0; n < 120; n++)); do
            running=$(curl -s --max-time 2 "${urls[$i]}/metrics" | awk '$1 ~ /^vllm:num_requests_running([{]|$)/ {s += $NF; f = 1} END {print f ? s + 0 : "missing"}') || running=missing
            [[ "$running" == 0 ]] && break
            [[ "$running" == missing ]] && { fixed=1; break; }
            sleep 0.5
        done
        ((n < 120)) || echo "warning: ${names[$i]} still reports running requests after 60 s" >&2
    done
    ((fixed == 0)) || sleep "$cooldown"
}

val() { # line_prefix key file -> value (or n/a)
    awk -v p="$1" -v k="$2" '$1 == p { for (i = 2; i <= NF; i++) if (index($i, k "=") == 1) { print substr($i, length(k) + 2); found = 1; exit } }
        END { if (!found) print "n/a" }' "$3"
}
by_worker() { # line_prefix file -> "mac=N rog=M" (legacy primary/backup zeros dropped)
    awk -v p="$1" '$1 == p { s = ""; for (i = 2; i <= NF; i++) if ($i != "primary=0" && $i != "backup=0") s = s (s ? " " : "") $i; print (s ? s : "none") }' "$2"
}

tsv="$out/runs.tsv"
printf 'seed\torder\tmode\turgent_p95_ttft_ms\turgent_within_target\turgent_offered\turgent_within_pct\tbackground_p95_ttft_ms\tcompletions_per_sec\tok\trejected\tfailed\tinterrupted\tok_by_worker\tfailed_by_worker\tlateness_p95_ms\tclient_saturated\tloadgen_rc\n' >"$tsv"

n=${#modes[@]} s_count=${#seeds[@]}
for ((k = 0; k < s_count; k++)); do
    seed=${seeds[$k]}
    shift_by=$(( (k * n / s_count) % n ))
    for ((j = 0; j < n; j++)); do
        mode=${modes[$(( (j + shift_by) % n ))]}
        order=$((j + 1))
        dir="$out/seed$seed/$(printf '%02d' "$order")-$mode"
        mkdir -p "$dir"
        echo "=== seed=$seed order=$order mode=$mode ($(date +%H:%M:%S))"
        probe_workers || { echo "a worker is unhealthy before $mode seed $seed; stopping" >&2; exit 1; }
        settle
        start_gateway "$mode" "$dir/gateway.log"
        LOADGEN_CONCURRENCY=$concurrency bin/loadgen "$dir/warmup.csv" "$base/v1/chat/completions" "$model" \
            "$warmup_requests" "$warmup_seed" "$target_ms" "$rate" >"$dir/warmup.txt" 2>"$dir/warmup.err" \
            || echo "warning: warm-up exited non-zero (see $dir/warmup.txt)" >&2
        settle
        set +e
        LOADGEN_CONCURRENCY=$concurrency LOADGEN_METRICS_URL="${urls[0]}/metrics" bin/loadgen "$dir/run.csv" \
            "$base/v1/chat/completions" "$model" "$requests" "$seed" "$target_ms" "$rate" >"$dir/summary.txt" 2>"$dir/loadgen.err"
        rc=$?
        set -e
        curl -s --max-time 2 "$base/debug/workers" >"$dir/workers.json" || true
        stop_gateway
        f="$dir/summary.txt"
        grep -q '^total ' "$f" || { echo "loadgen produced no summary (rc=$rc): $(head -3 "$dir/loadgen.err")" >&2; exit 1; }
        printf '%s\n' "$seed" "$order" "$mode" \
            "$(val urgent p95_ttft_ms "$f")" "$(val urgent within_target "$f")" "$(val urgent offered "$f")" "$(val urgent within_target_pct "$f")" \
            "$(val background p95_ttft_ms "$f")" "$(val total completions_per_sec "$f")" "$(val total ok "$f")" "$(val total rejected "$f")" \
            "$(val total failed "$f")" "$(val total interrupted_streams "$f")" "$(by_worker ok_by_worker "$f")" "$(by_worker failed_by_worker "$f")" \
            "$(val dispatch lateness_p95_ms "$f")" "$(val dispatch client_saturated "$f")" "$rc" | paste -sd '\t' - >>"$tsv"
        tail -1 "$tsv" | awk -F'\t' '{printf "    urgent p95=%s ms within=%s/%s  background p95=%s ms  ok=%s rejected=%s failed=%s  ok_by_worker: %s  saturated=%s\n", $4, $5, $6, $8, $10, $11, $12, $14, $17}'
    done
done
stop_gateway
echo
column -t -s $'\t' "$tsv" | tee "$out/runs.txt"
echo "Results: $out"
