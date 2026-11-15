package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/events"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/rpc"
	"github.com/saiteja0367/nexora-payment-platform/distributed/internal/store"
	"go.opentelemetry.io/otel"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	ctx := context.Background()
	db, err := store.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.DB.Close()
	shutdown, err := events.Trace(ctx, "payment-api")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(ctx)
	go func() {
		if err := rpc.Serve(":9090", &rpc.Handler{DB: db}); err != nil {
			log.Printf("grpc server: %v", err)
		}
	}()
	var created atomic.Uint64
	var errorsTotal atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.DB.Ping(r.Context()); err != nil {
			http.Error(w, "not ready", 503)
			return
		}
		w.Write([]byte("ready"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# TYPE nexora_payment_requests_total counter\nnexora_payment_requests_total{result=\"accepted\"} %d\nnexora_payment_requests_total{result=\"rejected\"} %d\n", created.Load(), errorsTotal.Load())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("nexora").Start(r.Context(), "create-payment")
		defer span.End()
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if key == "" {
			errorsTotal.Add(1)
			http.Error(w, "Idempotency-Key required", 400)
			return
		}
		var req struct {
			AmountCents int64 `json:"amount_cents"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil || req.AmountCents <= 0 {
			errorsTotal.Add(1)
			http.Error(w, "positive amount_cents required", 400)
			return
		}
		p, err := db.Create(ctx, key, req.AmountCents)
		if err != nil {
			errorsTotal.Add(1)
			http.Error(w, err.Error(), 409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		created.Add(1)
		w.WriteHeader(202)
		json.NewEncoder(w).Encode(p)
	})
	mux.HandleFunc("GET /payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		p, err := db.Get(r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, "not found", 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p)
	})
	log.Fatal((&http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
