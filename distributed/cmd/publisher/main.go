package main

import (
	"context"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/events"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/store"
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
	shutdown, err := events.Trace(ctx, "outbox-publisher")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(ctx)
	publisher := events.NewPublisher(os.Getenv("KAFKA_BROKER"), "payments")
	defer publisher.Close()
	for {
		err = db.PublishPending(ctx, publisher.Publish)
		if err != nil {
			log.Printf("publish: %v", err)
		}
		time.Sleep(time.Second)
	}
}
