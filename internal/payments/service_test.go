package payments

import (
	"errors"
	"sync"
	"testing"
)

func TestLifecycle(t *testing.T) {
	s := New()
	p, e := s.Authorize("m1", "auth", 1000)
	if e != nil {
		t.Fatal(e)
	}
	same, e := s.Authorize("m1", "auth", 1000)
	if e != nil || same.ID != p.ID {
		t.Fatal("idempotency failed")
	}
	if _, e = s.Authorize("m1", "auth", 999); !errors.Is(e, ErrConflict) {
		t.Fatalf("want conflict: %v", e)
	}
	if _, e = s.Capture("m1", "cap", p.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Capture("m1", "cap", p.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Refund("m1", "r1", p.ID, 300); e != nil {
		t.Fatal(e)
	}
	final, e := s.Refund("m1", "r2", p.ID, 700)
	if e != nil || final.Status != Refunded {
		t.Fatalf("refund: %v %v", final, e)
	}
	if _, e = s.Refund("m1", "r3", p.ID, 1); !errors.Is(e, ErrInvalid) {
		t.Fatal("over-refund accepted")
	}
	if len(s.Ledger()) != 6 {
		t.Fatal("duplicate ledger postings")
	}
	if e = s.Audit(); e != nil {
		t.Fatal(e)
	}
}
func TestVoidAndTenantIsolation(t *testing.T) {
	s := New()
	p, _ := s.Authorize("a", "k", 50)
	if _, e := s.Get("b", p.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal("tenant leak")
	}
	if _, e := s.Capture("b", "cap", p.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal("cross tenant capture")
	}
	if _, e := s.Void("a", "v", p.ID); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Capture("a", "cap", p.ID); !errors.Is(e, ErrInvalid) {
		t.Fatal("captured void payment")
	}
	if e := s.Audit(); e != nil {
		t.Fatal(e)
	}
}
func TestConcurrentIdempotency(t *testing.T) {
	s := New()
	p, _ := s.Authorize("m", "auth", 500)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := s.Capture("m", "capture", p.ID); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if len(s.Ledger()) != 2 {
		t.Fatal("double posting")
	}
	if e := s.Audit(); e != nil {
		t.Fatal(e)
	}
}
func TestDifferentMerchantsSameKey(t *testing.T) {
	s := New()
	a, _ := s.Authorize("a", "key", 10)
	b, _ := s.Authorize("b", "key", 20)
	if a.ID == b.ID {
		t.Fatal("key not merchant scoped")
	}
}
