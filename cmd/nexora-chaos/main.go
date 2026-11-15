// nexora-chaos runs a deterministic local correctness experiment.
// It does not simulate real Kafka, network partitions, or database crashes.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/saiteja0367/nexora-payment-platform/internal/payments"
	"os"
	"sync"
)

type report struct {
	Scenario      string `json:"scenario"`
	Attempts      int    `json:"attempts"`
	LedgerEntries int    `json:"ledger_entries"`
	Balanced      bool   `json:"balanced"`
	Error         string `json:"error,omitempty"`
}

func main() {
	svc := payments.New()
	p, err := svc.Authorize("merchant_demo", "auth-1", 10000)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := []string{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := svc.Capture("merchant_demo", "capture-1", p.ID)
			if e != nil {
				mu.Lock()
				errors = append(errors, e.Error())
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	auditErr := svc.Audit()
	entries := svc.Ledger()
	ok := auditErr == nil && len(entries) == 2 && len(errors) == 0
	r := report{Scenario: "concurrent duplicate capture", Attempts: 100, LedgerEntries: len(entries), Balanced: ok}
	if !ok {
		r.Error = fmt.Sprintf("audit=%v, errors=%v", auditErr, errors)
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
	if !ok {
		os.Exit(1)
	}
}
