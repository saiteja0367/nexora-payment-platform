#!/usr/bin/env bash
set -euo pipefail
HOST="${HOST:-http://localhost:8088}"
headers=(-H 'Content-Type: application/json' -H 'X-Merchant-ID: merchant_demo')
echo 'Authorize a payment:'
response=$(curl -fsS -X POST "$HOST/v1/payments/authorize" "${headers[@]}" -H 'Idempotency-Key: auth-001' -d '{"amount_cents":1250}')
echo "$response"
if command -v python3 >/dev/null; then
 id=$(printf '%s' "$response" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
else echo 'python3 required for parsing JSON'; exit 1;fi
printf 'Capture: ';curl -fsS -X POST "$HOST/v1/payments/capture" "${headers[@]}" -H 'Idempotency-Key: cap-001' -d "$(printf '{"payment_id":"%s"}' "$id")";echo
printf 'Partial refund: ';curl -fsS -X POST "$HOST/v1/payments/refund" "${headers[@]}" -H 'Idempotency-Key: refund-001' -d "$(printf '{"payment_id":"%s","amount_cents":250}' "$id")";echo
printf 'Audit: ';curl -fsS "$HOST/v1/audit";echo
