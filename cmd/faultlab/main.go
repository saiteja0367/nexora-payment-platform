package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/saiteja0367/nexora-payment-platform/internal/engine"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	db := flag.String("journal", "./data/journal.jsonl", "durable append-only journal path")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()
	e, err := engine.Open(*db)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Amount int64 `json:"amount_cents"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
			write(w, 400, map[string]string{"error": "invalid JSON"})
			return
		}
		p, reused, err := e.Create(strings.TrimSpace(r.Header.Get("Idempotency-Key")), body.Amount)
		if err != nil {
			write(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if reused {
			write(w, 200, p)
		} else {
			write(w, 202, p)
		}
	})
	mux.HandleFunc("GET /payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		p, ok := e.Get(r.PathValue("id"))
		if !ok {
			write(w, 404, map[string]string{"error": "not found"})
			return
		}
		write(w, 200, p)
	})
	mux.HandleFunc("POST /payments/{id}/process", func(w http.ResponseWriter, r *http.Request) {
		fault := r.URL.Query().Get("fault")
		if fault != "" && fault != "before-commit" && fault != "after-commit" && fault != "reject" {
			write(w, 400, map[string]string{"error": "invalid fault"})
			return
		}
		outcome, err := e.Process(r.PathValue("id"), fault)
		if err != nil {
			write(w, 404, map[string]string{"error": err.Error()})
			return
		}
		write(w, 200, map[string]string{"outcome": outcome})
	})
	mux.HandleFunc("POST /faults/duplicate-credit/{id}", func(w http.ResponseWriter, r *http.Request) {
		if err := e.InjectCorruption(r.PathValue("id")); err != nil {
			write(w, 404, map[string]string{"error": err.Error()})
			return
		}
		write(w, 200, map[string]string{"injected": "unbalanced-ledger-credit"})
	})
	mux.HandleFunc("GET /reconcile", func(w http.ResponseWriter, r *http.Request) { write(w, 200, e.Reconcile()) })
	mux.HandleFunc("GET /journal", func(w http.ResponseWriter, r *http.Request) { write(w, 200, e.Journal()) })
	mux.HandleFunc("GET /pending", func(w http.ResponseWriter, r *http.Request) { write(w, 200, e.Pending()) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		rep := e.Reconcile()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "nexora_payments_total %d\nnexora_settled_total %d\nnexora_pending_total %d\nnexora_failed_total %d\n", rep.Payments, rep.Settled, rep.Pending, rep.Failed)
	})
	log.Printf("Nexora on %s (journal=%s)", *addr, *db)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Print(err)
		os.Exit(1)
	}
}
