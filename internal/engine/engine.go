package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Event is a durable append-only journal record. All amounts are integer cents.
type Event struct {
	Seq       uint64    `json:"seq"`
	At        time.Time `json:"at"`
	Kind      string    `json:"kind"`
	PaymentID string    `json:"payment_id"`
	Key       string    `json:"key,omitempty"`
	Amount    int64     `json:"amount_cents,omitempty"`
	Note      string    `json:"note,omitempty"`
}
type Payment struct {
	ID       string `json:"id"`
	Amount   int64  `json:"amount_cents"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}
type Posting struct {
	PaymentID string `json:"payment_id"`
	Account   string `json:"account"`
	Delta     int64  `json:"delta_cents"`
}
type Report struct {
	Payments             int      `json:"payments"`
	Settled              int      `json:"settled"`
	Failed               int      `json:"failed"`
	Pending              int      `json:"pending"`
	LedgerPostings       int      `json:"ledger_postings"`
	Balanced             bool     `json:"balanced"`
	MissingSettlements   []string `json:"missing_settlements"`
	DuplicateSettlements []string `json:"duplicate_settlements"`
	OrphanSettlements    []string `json:"orphan_settlements"`
}
type Engine struct {
	mu       sync.Mutex
	path     string
	journal  []Event
	payments map[string]*Payment
	keys     map[string]string
	postings []Posting
	seq      uint64
}

func Open(path string) (*Engine, error) {
	e := &Engine{path: path, payments: map[string]*Payment{}, keys: map[string]string{}}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(data) > 0 {
		lines := splitLines(data)
		for i, line := range lines {
			var ev Event
			if err := json.Unmarshal(line, &ev); err != nil {
				return nil, fmt.Errorf("corrupt journal line %d: %w", i+1, err)
			}
			if ev.Seq != e.seq+1 {
				return nil, fmt.Errorf("journal sequence gap at line %d", i+1)
			}
			e.apply(ev)
		}
	}
	return e, nil
}
func splitLines(data []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			if i > start {
				out = append(out, data[start:i])
			}
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, data[start:])
	}
	return out
}
func (e *Engine) append(kind, id, key string, amount int64, note string) error {
	ev := Event{Seq: e.seq + 1, At: time.Now().UTC(), Kind: kind, PaymentID: id, Key: key, Amount: amount, Note: note}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(e.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(data, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	e.apply(ev)
	return nil
}
func (e *Engine) apply(ev Event) {
	e.seq = ev.Seq
	e.journal = append(e.journal, ev)
	switch ev.Kind {
	case "created":
		e.payments[ev.PaymentID] = &Payment{ID: ev.PaymentID, Amount: ev.Amount, Status: "pending"}
		e.keys[ev.Key] = ev.PaymentID
	case "attempt":
		e.payments[ev.PaymentID].Attempts++
	case "settled":
		e.payments[ev.PaymentID].Status = "settled"
		e.postings = append(e.postings, Posting{ev.PaymentID, "receivable", -ev.Amount}, Posting{ev.PaymentID, "cash", ev.Amount})
	case "failed":
		e.payments[ev.PaymentID].Status = "failed"
	case "corrupt-credit":
		e.postings = append(e.postings, Posting{ev.PaymentID, "cash", ev.Amount})
	}
}
func (e *Engine) Create(key string, amount int64) (Payment, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if key == "" || amount <= 0 {
		return Payment{}, false, errors.New("nonempty key and positive amount required")
	}
	if id, ok := e.keys[key]; ok {
		p := *e.payments[id]
		if p.Amount != amount {
			return Payment{}, false, errors.New("idempotency key conflict")
		}
		return p, true, nil
	}
	id := fmt.Sprintf("pay_%08d", e.seq+1)
	if err := e.append("created", id, key, amount, ""); err != nil {
		return Payment{}, false, err
	}
	return *e.payments[id], false, nil
}
func (e *Engine) Get(id string) (Payment, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.payments[id]
	if !ok {
		return Payment{}, false
	}
	return *p, true
}
func (e *Engine) Pending() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var ids []string
	for id, p := range e.payments {
		if p.Status == "pending" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// Process models at-least-once delivery. A crash after durable settlement but before ack is harmless on redelivery.
func (e *Engine) Process(id, fault string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.payments[id]
	if !ok {
		return "", errors.New("payment not found")
	}
	if p.Status == "settled" {
		return "already-settled", nil
	}
	if p.Status == "failed" {
		return "already-failed", nil
	}
	if err := e.append("attempt", id, "", 0, ""); err != nil {
		return "", err
	}
	if fault == "before-commit" {
		return "simulated-crash-before-commit", nil
	}
	if fault == "reject" {
		if err := e.append("failed", id, "", 0, "processor rejected"); err != nil {
			return "", err
		}
		return "failed", nil
	}
	if err := e.append("settled", id, "", p.Amount, ""); err != nil {
		return "", err
	}
	if fault == "after-commit" {
		return "simulated-crash-after-commit-before-ack", nil
	}
	return "settled", nil
}
func (e *Engine) InjectCorruption(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.payments[id]
	if !ok {
		return errors.New("payment not found")
	}
	return e.append("corrupt-credit", id, "", p.Amount, "fault-injection-only")
}
func (e *Engine) Reconcile() Report {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := Report{Payments: len(e.payments), LedgerPostings: len(e.postings), Balanced: true, MissingSettlements: []string{}, DuplicateSettlements: []string{}, OrphanSettlements: []string{}}
	counts := map[string]int{}
	sums := map[string]int64{}
	for _, p := range e.postings {
		counts[p.PaymentID]++
		sums[p.PaymentID] += p.Delta
	}
	for id, p := range e.payments {
		switch p.Status {
		case "settled":
			r.Settled++
			if counts[id] < 2 {
				r.MissingSettlements = append(r.MissingSettlements, id)
			}
			if counts[id] > 2 {
				r.DuplicateSettlements = append(r.DuplicateSettlements, id)
			}
		case "failed":
			r.Failed++
		default:
			r.Pending++
		}
		if counts[id] > 0 && p.Status != "settled" {
			r.OrphanSettlements = append(r.OrphanSettlements, id)
		}
		if sums[id] != 0 {
			r.Balanced = false
		}
	}
	sort.Strings(r.MissingSettlements)
	sort.Strings(r.DuplicateSettlements)
	sort.Strings(r.OrphanSettlements)
	return r
}
func (e *Engine) Journal() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Event(nil), e.journal...)
}
