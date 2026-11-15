// nexora-demo is a dependency-free HTTP demonstration of the payment state machine.
// It deliberately uses in-memory state and is not the PostgreSQL/Kafka distributed service.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/saiteja0367/nexora-payment-platform/internal/payments"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type request struct {
	Amount    int64  `json:"amount_cents"`
	PaymentID string `json:"payment_id"`
}

func main() {
	svc := payments.New()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/v1/payments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 3 {
			http.Error(w, "use /v1/payments/{authorize|capture|void|refund}", 404)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var in request
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		merchant, key := r.Header.Get("X-Merchant-ID"), r.Header.Get("Idempotency-Key")
		var p payments.Payment
		var err error
		switch parts[2] {
		case "authorize":
			p, err = svc.Authorize(merchant, key, in.Amount)
		case "capture":
			p, err = svc.Capture(merchant, key, in.PaymentID)
		case "void":
			p, err = svc.Void(merchant, key, in.PaymentID)
		case "refund":
			p, err = svc.Refund(merchant, key, in.PaymentID, in.Amount)
		default:
			http.Error(w, "unknown action", 404)
			return
		}
		if err != nil {
			code := 400
			if errors.Is(err, payments.ErrConflict) {
				code = 409
			} else if errors.Is(err, payments.ErrNotFound) {
				code = 404
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p)
	})
	mux.HandleFunc("/v1/audit", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Audit(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"balanced":true}`)
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8088"
	}
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("Nexora demo listening on :%s (in-memory only)", port)
	log.Fatal(server.ListenAndServe())
}
