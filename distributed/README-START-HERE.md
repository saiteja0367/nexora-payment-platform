# Nexora: Distributed services quick start

This is the **distributed extension** to the tested stdlib-only Go simulator at the repository root.

## Run

Prerequisites: Docker Engine / Docker Desktop with Compose, and sufficient RAM for Kafka, PostgreSQL, Tempo, Prometheus, Grafana.

```bash
cd distributed
docker compose up --build -d
docker compose ps
bash scripts/smoke.sh
```

Endpoints:
- REST API: http://localhost:8081 (POST /payments, GET /payments/{id}, GET /healthz, GET /readyz, GET /metrics)
- gRPC: localhost:9090, service `nexora.PaymentService`, methods `Create` and `Get`, using `google.protobuf.Struct` requests/responses. Example request: `{"idempotency_key":"abc","amount_cents":1250}`. For gRPC reflection use grpcurl only after enabling reflection; otherwise use a client that knows the service descriptor.
- Prometheus: http://localhost:9091
- Grafana: http://localhost:3000 (anonymous Viewer in local development)

To test redelivery, restart worker and publisher while sending requests. Verify ledger invariants in PostgreSQL. This setup has not been end-to-end tested in the artifact-building environment (Docker and module downloads unavailable there).

**Design boundaries:** The Kafka broker is single-node for local demonstration; not highly available. The worker retries malformed events indefinitely rather than sending them to a DLQ. The gRPC adapter uses dynamic protobuf Struct rather than generated typed messages; `proto/payment.proto` documents a proposed typed contract, not the live service. API metrics are process-local and reset on restart. The provided dashboard measures HTTP requests only. Authentication, TLS, rate limiting, durable retry policies, complete reconciliation/repair and production load benchmarks are not yet implemented. Do not describe these as finished features.
