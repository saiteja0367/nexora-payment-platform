package main

import (
	"context"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/events"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/store"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"log"
	"os"
	"time"
)

func main() {
	ctx := context.Background()
	db, err := store.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.DB.Close()
	shutdown, err := events.Trace(ctx, "ledger-worker")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(ctx)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{os.Getenv("KAFKA_BROKER")}, Topic: "payments", GroupID: "ledger-workers", MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second})
	defer r.Close()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		id := ""
		for _, h := range m.Headers {
			if h.Key == "event_id" {
				id = string(h.Value)
			}
		}
		e, err := events.Decode(m.Value)
		if err != nil || id == "" {
			if dlqErr := db.RecordDeadLetter(ctx, m.Topic, m.Partition, m.Offset, m.Value, "invalid payment event"); dlqErr != nil {
				log.Printf("DLQ persistence failed: %v", dlqErr)
				time.Sleep(time.Second)
				continue
			}
			if err = r.CommitMessages(ctx, m); err != nil {
				log.Printf("invalid event commit failed: %v", err)
			}
			continue
		}
		c, span := otel.Tracer("nexora").Start(ctx, "settle-payment")
		err = db.Settle(c, id, e.PaymentID, e.AmountCents)
		span.End()
		if err != nil {
			log.Printf("settlement failed: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if err = r.CommitMessages(ctx, m); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}
