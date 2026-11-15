package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Payment struct {
	ID          string `json:"id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
}
type Store struct{ DB *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}

// Create uses one DB transaction to commit the payment and its outbox event.
func (s *Store) Create(ctx context.Context, key string, amount int64) (Payment, error) {
	var p Payment
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return p, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `INSERT INTO payments(id,idempotency_key,amount_cents) VALUES(gen_random_uuid(),$1,$2) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING id::text,amount_cents,status`, key, amount).Scan(&p.ID, &p.AmountCents, &p.Status)
	if err != nil {
		return p, err
	}
	if p.AmountCents != amount {
		return p, errors.New("idempotency key reused with different amount")
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox(id,aggregate_id,event_type,payload) VALUES(gen_random_uuid(),$1::uuid,'PaymentRequested',jsonb_build_object('payment_id',$1::text,'amount_cents',$2::bigint)) ON CONFLICT(aggregate_id,event_type) DO NOTHING`, p.ID, amount)
	if err != nil {
		return p, err
	}
	return p, tx.Commit(ctx)
}
func (s *Store) Get(ctx context.Context, id string) (Payment, error) {
	var p Payment
	err := s.DB.QueryRow(ctx, `SELECT id::text,amount_cents,status FROM payments WHERE id=$1::uuid`, id).Scan(&p.ID, &p.AmountCents, &p.Status)
	return p, err
}

// Settle is idempotent even under Kafka redelivery; inbox and ledger commit together.
func (s *Store) Settle(ctx context.Context, eventID, paymentID string, amount int64) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO inbox(event_id) VALUES($1::uuid) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	var actual int64
	err = tx.QueryRow(ctx, `SELECT amount_cents FROM payments WHERE id=$1::uuid FOR UPDATE`, paymentID).Scan(&actual)
	if err != nil {
		return err
	}
	if actual != amount {
		return fmt.Errorf("amount mismatch")
	}
	_, err = tx.Exec(ctx, `INSERT INTO ledger(payment_id,account,amount_cents) VALUES($1::uuid,'customer',-$2),($1::uuid,'merchant',$2) ON CONFLICT DO NOTHING`, paymentID, amount)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE payments SET status='settled' WHERE id=$1::uuid`, paymentID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PublishPending claims a bounded batch using SKIP LOCKED. Multiple publishers
// can run concurrently without holding a database transaction across Kafka I/O.
// A crash after Kafka acknowledgement can cause redelivery: consumers MUST dedupe.
func (s *Store) PublishPending(ctx context.Context, publish func(context.Context, string, []byte) error) error {
	claimID := ""
	if err := s.DB.QueryRow(ctx, `SELECT gen_random_uuid()::text`).Scan(&claimID); err != nil {
		return err
	}
	rows, err := s.DB.Query(ctx, `WITH selected AS (
        SELECT id FROM outbox WHERE published_at IS NULL
          AND next_attempt_at <= now()
          AND (claim_until IS NULL OR claim_until < now())
        ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
    ) UPDATE outbox o SET claimed_by=$1::uuid, claim_until=now()+interval '60 seconds',
        attempts=o.attempts+1 FROM selected
        WHERE o.id=selected.id RETURNING o.id::text,o.payload`, claimID)
	if err != nil {
		return err
	}
	type event struct {
		id   string
		data []byte
	}
	batch := make([]event, 0, 100)
	for rows.Next() {
		var e event
		var raw json.RawMessage
		if err = rows.Scan(&e.id, &raw); err != nil {
			rows.Close()
			return err
		}
		e.data = append([]byte(nil), raw...)
		batch = append(batch, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var firstErr error
	for _, e := range batch {
		pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		publishErr := publish(pubCtx, e.id, e.data)
		cancel()
		if publishErr != nil {
			// Capped exponential backoff calculated in SQL; claim ownership
			// prevents a delayed publisher from changing another claim.
			_, updateErr := s.DB.Exec(ctx, `UPDATE outbox SET claimed_by=NULL,claim_until=NULL,
              next_attempt_at=now()+make_interval(secs => LEAST(300, power(2,LEAST(attempts,8))::int))
              WHERE id=$1::uuid AND claimed_by=$2::uuid AND published_at IS NULL`, e.id, claimID)
			if firstErr == nil {
				firstErr = publishErr
			}
			if updateErr != nil && firstErr == nil {
				firstErr = updateErr
			}
			continue
		}
		_, err = s.DB.Exec(ctx, `UPDATE outbox SET published_at=now(),claimed_by=NULL,claim_until=NULL
           WHERE id=$1::uuid AND claimed_by=$2::uuid AND published_at IS NULL`, e.id, claimID)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
func (s *Store) Audit(ctx context.Context) ([]string, error) {
	rows, err := s.DB.Query(ctx, `SELECT p.id::text FROM payments p LEFT JOIN ledger l ON l.payment_id=p.id WHERE p.status='settled' GROUP BY p.id HAVING count(l.account)<>2 OR coalesce(sum(l.amount_cents),0)<>0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bad []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		bad = append(bad, id)
	}
	return bad, rows.Err()
}

var _ = pgx.ErrNoRows

// RecordDeadLetter durably quarantines poison events before committing their offsets.
func (s *Store) RecordDeadLetter(ctx context.Context, topic string, partition int, offset int64, payload []byte, reason string) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO dead_letters(topic,partition_id,message_offset,payload,reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, topic, partition, offset, payload, reason)
	return err
}
