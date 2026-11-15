# Nexora distributed extension (experimental)

This folder contains an **implemented multi-process Go service prototype**. It is a separate Go module from the original verified single-process simulator.

## Components

- `api`: HTTP payment creation; PostgreSQL transaction writes payment + outbox event.
- `publisher`: PostgreSQL advisory-lock leader election; polls outbox and publishes to Kafka; marks published only after broker acknowledgement. **At-least-once** delivery, not exactly-once.
- `worker`: Kafka consumer group, inbox deduplication, database transaction with balanced double-entry ledger, commits Kafka offset after DB commit.
- `reconciler`: periodic ledger invariant checks.
- `tempo` and `grafana`: OTLP HTTP traces with Grafana Tempo datasource (not yet a Prometheus metrics stack).
- `k8s/app.yaml`: Kubernetes API, worker, publisher deployments and API Service. Needs external DB/broker, built images, and a Secret.
- `scripts/chaos-worker.sh`: kills a Kubernetes worker pod and waits for recovery. **Not a network partition test.**

## Start

```bash
cd distributed
go mod tidy
docker compose up --build -d
curl -i -X POST http://localhost:8081/payments \
  -H 'Idempotency-Key: order-1' -H 'Content-Type: application/json' \
  -d '{"amount_cents":2500}'
# Query the returned payment ID after a few seconds
curl http://localhost:8081/payments/<id>
# Grafana Tempo datasource: http://localhost:3000
```

Run `docker compose down -v` to erase the lab database. The Kafka configuration is **single broker, development only** and has no high-availability guarantee.

## Failure semantics

The outbox and payment are inserted in one PostgreSQL transaction. Publisher leadership uses a **PostgreSQL advisory lock**, not Raft/Paxos consensus. Publishing can succeed before marking the event published; redelivery is therefore possible. The consumer records the event ID in an inbox and creates the ledger entries in the same database transaction; Kafka offsets are committed afterwards. This yields an **effectively-once ledger effect** for repeated delivery of the same event, not global exactly-once processing.

## Important limitations

- This extension has **not been compiled or integration-tested in this environment**: the Go dependencies could not be downloaded due to blocked network access, and Docker is unavailable. Validate with `go mod tidy`, `go test ./...`, and `docker compose up --build` locally.
- Kafka topic auto-creation is for development only; schema registry, DLQ, and retry policy are not included.
- Leader election is only for the publisher and assumes PostgreSQL availability. No general-purpose distributed consensus algorithm is implemented.
- Trace spans are basic and do not propagate context through Kafka yet. No Grafana dashboards or Prometheus metrics are configured.
- Kubernetes manifests are **starter manifests** and not a full HA cluster setup; the workers/publishers need proper graceful shutdown, probes, and integration tests.
- `chaos-worker.sh` simulates a worker crash, not network partitions, latency injection, or true network chaos.
- No authentication, authorization, TLS, financial compliance, or production safeguards. Never use real money.

## Validation checklist

1. Run `go mod tidy && go test ./... && go vet ./...`.
2. Start Compose, POST a payment, verify it transitions to `settled`.
3. Restart the publisher after Kafka publish but before marking the outbox row; confirm only one pair of ledger entries.
4. Scale workers to 3; check Kafka consumer group rebalances and idempotent ledger writes.
5. Inspect Grafana traces; add propagation and dashboards before claiming end-to-end tracing.
6. Run worker-pod chaos on a test cluster; verify recovery and reconciliation.
