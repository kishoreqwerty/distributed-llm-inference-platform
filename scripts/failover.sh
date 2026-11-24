#!/usr/bin/env bash
# Failover benchmark: 30 requests at 1 req/s (seed 42, 500 ms urgent target) through the running
# gateway. About 10 s into the workload, bin/loadgen sends SIGINT to the verified primary model
# server PID and measures shutdown-signal -> first backup content token on its own monotonic clock.
#
# usage: scripts/failover.sh PRIMARY_PID
# Overrides (for local testing): GATEWAY_URL PRIMARY_URL BACKUP_URL MODEL RESULTS_ROOT
# Exit: 0 run complete with backup content, 3 run complete but NO backup content,
#       4 primary did not exit after SIGINT, 1 aborted/precheck failed (nothing was signalled).
#
# This script never restarts anything and never signals any process itself. The only signal is the
# one bin/loadgen sends to PRIMARY_PID, after PRIMARY_PID is verified as the primary's listener.
set -euo pipefail
cd "$(dirname "$0")/.."

gateway=${GATEWAY_URL:-http://127.0.0.1:18082}
primary=${PRIMARY_URL:-http://127.0.0.1:8000}
backup=${BACKUP_URL:-http://127.0.0.1:18001}
model=${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}
requests=30 rate=1 seed=42 target_ms=500 signal_at_ms=10000

die() { echo "ABORT: $*" >&2; [[ -n "${out:-}" ]] && echo "ABORTED: $*" >"$out/status.txt"; exit 1; }

[[ $# -eq 1 && "$1" =~ ^[0-9]+$ && "$1" -gt 1 ]] || { echo "usage: scripts/failover.sh PRIMARY_PID" >&2; exit 2; }
pid=$1
[[ -x bin/loadgen ]] || { echo "bin/loadgen missing: run 'make bin/loadgen'" >&2; exit 1; }

results_root=${RESULTS_ROOT:-results}
mkdir -p "$results_root"
out=$(mktemp -d "$results_root/failover-$(date +%Y%m%d-%H%M%S)-XXXX")
echo "Run directory: $out"

hostport=${primary#*://}; hostport=${hostport%%/*}
phost=${hostport%:*}; pport=${hostport##*:}
[[ "$pport" =~ ^[0-9]+$ ]] || die "PRIMARY_URL must include an explicit port: $primary"

# PRIMARY_PID must be the one and only process listening on the primary's host:port.
verify_owner() {
    local owners
    owners=$({ lsof -nP -iTCP:"$pport" -sTCP:LISTEN -Fpn 2>/dev/null || true; } | awk -v want1="$phost:$pport" -v want2="*:$pport" '
        /^p/ {p = substr($0, 2)} /^n/ {a = substr($0, 2); if (a == want1 || a == want2) print p}' | sort -u)
    [[ -n "$owners" ]] || die "nothing is listening on $phost:$pport"
    [[ "$owners" == "$pid" ]] || die "PID $pid does not own $phost:$pport (listener PID(s): $(echo $owners))"
}
verify_owner
echo "Verified: PID $pid owns the listener on $phost:$pport"
{
    echo "date=$(date '+%Y-%m-%dT%H:%M:%S%z')"
    echo "gateway=$gateway primary=$primary backup=$backup model=$model"
    echo "requests=$requests rate_rps=$rate seed=$seed target_ms=$target_ms signal=SIGINT signal_at_ms=$signal_at_ms"
    echo "primary_pid=$pid primary_cmd=$(ps -o command= -p "$pid")"
} >"$out/meta.txt"

payload=$(printf '{"model":"%s","stream":true,"temperature":0,"max_tokens":2,"messages":[{"role":"user","content":"Say hi."}]}' "$model")
probe() { # name base_url [extra curl args...]
    local name=$1 url=$2; shift 2
    local code
    code=$(curl -sS -N --max-time 60 -o "$out/probe-$name.sse" -w '%{http_code}' \
        -H 'Content-Type: application/json' -H 'X-Request-Priority: urgent' "$@" -d "$payload" \
        "${url%/}/v1/chat/completions" 2>"$out/probe-$name.err") || die "$name probe failed: $(cat "$out/probe-$name.err")"
    [[ "$code" == 200 ]] || die "$name probe returned HTTP $code: $(head -c 300 "$out/probe-$name.sse")"
    grep -q '^data: *\[DONE\]' "$out/probe-$name.sse" || die "$name probe stream did not finish with [DONE]"
}
probe primary "$primary"
probe backup "$backup"
echo "Verified: primary and backup workers both stream a completion for model $model"
probe gateway "$gateway" -D "$out/probe-gateway.headers"
role=$(tr -d '\r' <"$out/probe-gateway.headers" | awk -F: 'tolower($1) == "x-inference-worker" {sub(/^[ \t]+/, "", $2); v = $2} END {print v}')
[[ "$role" == primary ]] || die "gateway probe was served by X-Inference-Worker='${role:-<absent>}', expected primary"
echo "Verified: gateway served the probe with X-Inference-Worker: primary"

verify_owner  # Re-check immediately before traffic; loadgen signals ~10 s later.
echo "Starting workload: $requests requests at $rate req/s; SIGINT to PID $pid at +${signal_at_ms} ms"
set +e
bin/loadgen "$out/requests.csv" "${gateway%/}/v1/chat/completions" "$model" "$requests" "$seed" "$target_ms" \
    "$rate" "$pid" "$signal_at_ms" >"$out/summary.txt" 2>"$out/loadgen.err"
rc=$?
set -e
cat "$out/summary.txt"
grep -q '^failover ' "$out/summary.txt" || die "loadgen exited $rc without a failover result: $(cat "$out/loadgen.err")"
[[ -s "$out/loadgen.err" ]] && echo "Per-request errors: $out/loadgen.err"
if grep -q 'primary_exited=NO' "$out/summary.txt"; then
    echo "COMPLETE_PRIMARY_STILL_RUNNING" >"$out/status.txt"; echo "Results: $out (primary did NOT exit after SIGINT)"; exit 4
fi
if grep -q '^failover shutdown_to_first_backup_content_ms=' "$out/summary.txt"; then
    echo "COMPLETE" >"$out/status.txt"; echo "Results: $out"; exit 0
fi
echo "COMPLETE_NO_BACKUP_CONTENT" >"$out/status.txt"; echo "Results: $out (no backup content after the signal)"; exit 3
