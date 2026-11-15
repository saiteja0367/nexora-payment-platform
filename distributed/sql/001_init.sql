CREATE TABLE IF NOT EXISTS payments (
 id UUID PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
 amount_cents BIGINT NOT NULL CHECK(amount_cents > 0),
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','settled')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
 id UUID PRIMARY KEY, aggregate_id UUID NOT NULL REFERENCES payments(id),
 event_type TEXT NOT NULL, payload JSONB NOT NULL, UNIQUE(aggregate_id,event_type),
 published_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS inbox (
 event_id UUID PRIMARY KEY, processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ledger (
 payment_id UUID NOT NULL REFERENCES payments(id), account TEXT NOT NULL,
 amount_cents BIGINT NOT NULL, PRIMARY KEY(payment_id,account)
);

-- Horizontally scalable outbox delivery: short-lived row claims, no global leader.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claimed_by UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claim_until TIMESTAMPTZ;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS outbox_ready_idx ON outbox(next_attempt_at,created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS dead_letters (
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, message_offset BIGINT NOT NULL,
 payload BYTEA NOT NULL, reason TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(topic,partition_id,message_offset)
);
