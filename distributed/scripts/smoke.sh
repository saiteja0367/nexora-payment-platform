#!/usr/bin/env bash
set -euo pipefail
API="${API:-http://localhost:8081}"
KEY="nexora-smoke-$(date +%s)-$$"
echo "Checking readiness..."
curl -fsS "$API/readyz"
echo
echo "Creating payment..."
FIRST=$(curl -fsS -X POST "$API/payments" -H "Content-Type: application/json" -H "Idempotency-Key: $KEY" -d '{"amount_cents":1250}')
echo "$FIRST"
echo "Replaying same idempotency key..."
SECOND=$(curl -fsS -X POST "$API/payments" -H "Content-Type: application/json" -H "Idempotency-Key: $KEY" -d '{"amount_cents":1250}')
[ "$FIRST" = "$SECOND" ] || { echo "Idempotency replay mismatch"; exit 1; }
echo "Checking metrics..."
curl -fsS "$API/metrics" | grep nexora_payment_requests_total
echo "Smoke test passed."
