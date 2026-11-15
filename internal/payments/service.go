// Package payments implements a deterministic, concurrency-safe payment lifecycle.
// This in-memory reference model is for local experiments, NOT real money or durability.
package payments

import (
	"errors"
	"fmt"
	"sync"
)

type Status string

const (
	Authorized        Status = "authorized"
	Captured          Status = "captured"
	Voided            Status = "voided"
	PartiallyRefunded Status = "partially_refunded"
	Refunded          Status = "refunded"
)

var (
	ErrNotFound = errors.New("payment not found")
	ErrConflict = errors.New("idempotency key conflict")
	ErrInvalid  = errors.New("invalid payment transition")
)

type Payment struct {
	ID             string `json:"id"`
	Merchant       string `json:"merchant"`
	Amount         int64  `json:"amount_cents"`
	RefundedAmount int64  `json:"refunded_cents"`
	Status         Status `json:"status"`
}
type Entry struct {
	PaymentID string `json:"payment_id"`
	Operation string `json:"operation"`
	Account   string `json:"account"`
	Amount    int64  `json:"amount_cents"`
}
type result struct {
	Fingerprint string
	Payment     Payment
}
type Service struct {
	mu       sync.Mutex
	next     uint64
	payments map[string]Payment
	keys     map[string]result
	entries  []Entry
}

func New() *Service {
	return &Service{payments: make(map[string]Payment), keys: make(map[string]result)}
}
func (s *Service) operation(merchant, key, fingerprint string, fn func() (Payment, error)) (Payment, error) {
	if merchant == "" || key == "" {
		return Payment{}, fmt.Errorf("%w: merchant and idempotency key required", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	scoped := merchant + "\x00" + key
	if r, ok := s.keys[scoped]; ok {
		if r.Fingerprint != fingerprint {
			return Payment{}, ErrConflict
		}
		return r.Payment, nil
	}
	p, err := fn()
	if err != nil {
		return Payment{}, err
	}
	s.keys[scoped] = result{fingerprint, p}
	return p, nil
}
func (s *Service) Authorize(merchant, key string, amount int64) (Payment, error) {
	return s.operation(merchant, key, fmt.Sprintf("authorize:%d", amount), func() (Payment, error) {
		if amount <= 0 {
			return Payment{}, ErrInvalid
		}
		s.next++
		p := Payment{ID: fmt.Sprintf("pay_%08d", s.next), Merchant: merchant, Amount: amount, Status: Authorized}
		s.payments[p.ID] = p
		return p, nil
	})
}
func (s *Service) Capture(merchant, key, id string) (Payment, error) {
	return s.operation(merchant, key, "capture:"+id, func() (Payment, error) {
		p, err := s.lookup(merchant, id)
		if err != nil {
			return Payment{}, err
		}
		if p.Status != Authorized {
			return Payment{}, ErrInvalid
		}
		p.Status = Captured
		s.payments[id] = p
		s.post(id, "capture", "customer", -p.Amount, "merchant", p.Amount)
		return p, nil
	})
}
func (s *Service) Void(merchant, key, id string) (Payment, error) {
	return s.operation(merchant, key, "void:"+id, func() (Payment, error) {
		p, err := s.lookup(merchant, id)
		if err != nil {
			return Payment{}, err
		}
		if p.Status != Authorized {
			return Payment{}, ErrInvalid
		}
		p.Status = Voided
		s.payments[id] = p
		return p, nil
	})
}
func (s *Service) Refund(merchant, key, id string, amount int64) (Payment, error) {
	return s.operation(merchant, key, fmt.Sprintf("refund:%s:%d", id, amount), func() (Payment, error) {
		p, err := s.lookup(merchant, id)
		if err != nil {
			return Payment{}, err
		}
		if (p.Status != Captured && p.Status != PartiallyRefunded) || amount <= 0 || amount > p.Amount-p.RefundedAmount {
			return Payment{}, ErrInvalid
		}
		p.RefundedAmount += amount
		if p.RefundedAmount == p.Amount {
			p.Status = Refunded
		} else {
			p.Status = PartiallyRefunded
		}
		s.payments[id] = p
		s.post(id, "refund", "merchant", -amount, "customer", amount)
		return p, nil
	})
}
func (s *Service) lookup(merchant, id string) (Payment, error) {
	p, ok := s.payments[id]
	if !ok || p.Merchant != merchant {
		return Payment{}, ErrNotFound
	}
	return p, nil
}
func (s *Service) Get(merchant, id string) (Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookup(merchant, id)
}
func (s *Service) post(id, op, a string, neg int64, b string, pos int64) {
	s.entries = append(s.entries, Entry{id, op, a, neg}, Entry{id, op, b, pos})
}
func (s *Service) Ledger() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Entry(nil), s.entries...)
}

// Audit verifies journal balance and payment/ledger consistency.
func (s *Service) Audit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	type totals struct{ customer, merchant int64 }
	balances := map[string]totals{}
	for _, e := range s.entries {
		t := balances[e.PaymentID]
		switch e.Account {
		case "customer":
			t.customer += e.Amount
		case "merchant":
			t.merchant += e.Amount
		default:
			return fmt.Errorf("unknown account %s", e.Account)
		}
		balances[e.PaymentID] = t
	}
	for id, p := range s.payments {
		t := balances[id]
		if t.customer+t.merchant != 0 {
			return fmt.Errorf("unbalanced payment %s", id)
		}
		expected := int64(0)
		if p.Status == Captured || p.Status == PartiallyRefunded || p.Status == Refunded {
			expected = p.Amount - p.RefundedAmount
		}
		if t.merchant != expected {
			return fmt.Errorf("ledger mismatch for %s: got %d expected %d", id, t.merchant, expected)
		}
	}
	return nil
}
