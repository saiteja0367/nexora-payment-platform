package engine

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func fresh(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestCrashAfterCommitReplayAndRedelivery(t *testing.T) {
	e := fresh(t)
	p, _, err := e.Create("order-1", 1200)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := e.Process(p.ID, "after-commit")
	if err != nil || outcome != "simulated-crash-after-commit-before-ack" {
		t.Fatalf("%s %v", outcome, err)
	}
	recovered, err := Open(e.path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err = recovered.Process(p.ID, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	r := recovered.Reconcile()
	if r.Settled != 1 || r.LedgerPostings != 2 || !r.Balanced || len(r.DuplicateSettlements) != 0 {
		t.Fatalf("bad reconciliation: %+v", r)
	}
}
func TestCrashBeforeCommitRetry(t *testing.T) {
	e := fresh(t)
	p, _, _ := e.Create("key", 500)
	e.Process(p.ID, "before-commit")
	recovered, err := Open(e.path)
	if err != nil {
		t.Fatal(err)
	}
	recovered.Process(p.ID, "")
	r := recovered.Reconcile()
	if r.Settled != 1 || r.LedgerPostings != 2 {
		t.Fatalf("%+v", r)
	}
}
func TestIdempotencyConcurrency(t *testing.T) {
	e := fresh(t)
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := e.Create("same", 500)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if e.Reconcile().Payments != 1 {
		t.Fatal("duplicate payment created")
	}
	if _, _, err := e.Create("same", 600); err == nil {
		t.Fatal("expected idempotency conflict")
	}
}
func TestReconciliationDetectsCorruption(t *testing.T) {
	e := fresh(t)
	p, _, _ := e.Create("k", 700)
	e.Process(p.ID, "")
	e.InjectCorruption(p.ID)
	r := e.Reconcile()
	if r.Balanced || len(r.DuplicateSettlements) != 1 {
		t.Fatalf("failed to detect corruption: %+v", r)
	}
}
func TestJournalRejectsCorruption(t *testing.T) {
	e := fresh(t)
	e.Create("k", 1)
	if err := os.WriteFile(e.path, []byte("invalid-json\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(e.path); err == nil {
		t.Fatal("expected journal corruption error")
	}
}
