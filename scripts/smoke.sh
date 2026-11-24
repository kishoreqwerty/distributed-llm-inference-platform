#!/usr/bin/env bash
set -euo pipefail
curl -fsS -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-Request-Priority: urgent' \
  -d '{"model":"Qwen/Qwen2.5-0.5B-Instruct","stream":true,"max_tokens":12,"messages":[{"role":"user","content":"Explain a GPU in one sentence."}]}'
