// Package provider simulates payment-provider responses for resilience testing.
// It never connects to a real payment network.
package provider

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("simulated provider unavailable")
var ErrTimeout = errors.New("simulated provider timeout")

type Outcome string

const (
	Approved Outcome = "approved"
	Declined Outcome = "declined"
)

type Config struct {
	Delay        time.Duration
	FailEvery    uint64
	DeclineEvery uint64
}
type Simulator struct {
	mu      sync.Mutex
	cfg     Config
	calls   uint64
	results map[string]Outcome
}

func New(c Config) *Simulator { return &Simulator{cfg: c, results: make(map[string]Outcome)} }

// Charge uses a stable merchant/request key, so a retry cannot create another simulated charge.
func (s *Simulator) Charge(ctx context.Context, merchant, key string) (Outcome, error) {
	if merchant == "" || key == "" {
		return "", errors.New("merchant and key required")
	}
	scoped := merchant + "\x00" + key
	s.mu.Lock()
	if result, ok := s.results[scoped]; ok {
		s.mu.Unlock()
		return result, nil
	}
	s.calls++
	call := s.calls
	cfg := s.cfg
	s.mu.Unlock()
	if cfg.Delay > 0 {
		select {
		case <-time.After(cfg.Delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if cfg.FailEvery > 0 && call%cfg.FailEvery == 0 {
		return "", ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	result := Approved
	if cfg.DeclineEvery > 0 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(scoped))
		if h.Sum64()%cfg.DeclineEvery == 0 {
			result = Declined
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.results[scoped]; ok {
		return prior, nil
	}
	s.results[scoped] = result
	return result, nil
}
