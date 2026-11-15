# Architecture and correctness model

## Core invariants
1. Idempotency key reuse with a different amount is rejected.
2. Each payment and its outbox event are committed atomically.
3. A Kafka event may be redelivered. The inbox records event IDs in the same transaction as ledger updates.
4. For settled payments, two ledger entries sum to zero.
5. Publisher advisory locks reduce concurrent publication, but cannot remove duplicate delivery across crashes.

## Failure windows
| Failure | Expected behavior |
|---|---|
| API crash before commit | No payment or outbox event |
| API crash after commit | Payment and outbox event durable; client can retry same key |
| Publisher crash after Kafka ack, before marking published | Event can be republished; worker must deduplicate |
| Worker crash before database commit | Kafka offset not committed; event retried |
| Worker crash after database commit, before Kafka offset commit | Event retried; inbox/ledger prevent duplicate posting |

## Scope limitations
The demonstration uses one PostgreSQL instance and a one-broker Kafka setup, which are not highly available. Multi-region failover, realistic PSP integrations, production authentication, reconciliation against external provider statements, and PCI controls are out of scope of the current code. Kubernetes YAML is a deployment starting point, not a validated production deployment.
