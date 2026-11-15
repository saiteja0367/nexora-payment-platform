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
	shutdown, err := events.Trace(ctx, "reconciler")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(ctx)
	for {
		bad, err := db.Audit(ctx)
		if err != nil {
			log.Printf("audit error: %v", err)
		} else if len(bad) > 0 {
			log.Printf("ledger invariant violations: %v", bad)
		}
		time.Sleep(5 * time.Second)
	}
}
