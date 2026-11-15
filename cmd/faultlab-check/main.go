// nexora-check runs reproducible failure experiments and exits nonzero on invariant violations.
package main

import (
	"fmt"
	"github.com/saiteja0367/nexora-payment-platform/internal/engine"
	"os"
	"path/filepath"
)

func main() {
	dir, err := os.MkdirTemp("", "nexora-check-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "journal.jsonl")
	e, err := engine.Open(path)
	must(err)
	p, _, err := e.Create("order-1", 2500)
	must(err)
	out, err := e.Process(p.ID, "after-commit")
	must(err)
	assert(out == "simulated-crash-after-commit-before-ack", "lost ack")
	e, err = engine.Open(path)
	must(err)
	out, err = e.Process(p.ID, "")
	must(err)
	assert(out == "already-settled", "idempotent replay")
	r := e.Reconcile()
	assert(r.Balanced && r.Settled == 1 && r.LedgerPostings == 2 && len(r.DuplicateSettlements) == 0, "balanced replay")
	fmt.Println("PASS: commit -> lost ack -> restart -> redelivery -> exactly one balanced settlement")
	q, _, err := e.Create("order-2", 900)
	must(err)
	_, err = e.Process(q.ID, "before-commit")
	must(err)
	e, err = engine.Open(path)
	must(err)
	out, err = e.Process(q.ID, "")
	must(err)
	assert(out == "settled", "before-commit retry")
	fmt.Println("PASS: crash before commit -> restart -> safe retry")
	must(e.InjectCorruption(p.ID))
	r = e.Reconcile()
	assert(!r.Balanced && len(r.DuplicateSettlements) == 1, "corruption detected")
	fmt.Println("PASS: injected duplicate ledger credit -> reconciliation detects corruption")
}
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func assert(ok bool, label string) {
	if !ok {
		fmt.Fprintln(os.Stderr, "FAIL:", label)
		os.Exit(1)
	}
}
