package primitives

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker open")

// Breaker protects a dependency with closed/open/half-open states.
type Breaker struct {
	mu                  sync.Mutex
	failures, threshold int
	cooldown            time.Duration
	opened              time.Time
	probe               bool
	now                 func() time.Time
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now}
}
func (b *Breaker) Execute(fn func() error) error {
	b.mu.Lock()
	if !b.opened.IsZero() {
		if b.now().Sub(b.opened) < b.cooldown || b.probe {
			b.mu.Unlock()
			return ErrCircuitOpen
		}
		b.probe = true
	}
	b.mu.Unlock()
	err := fn()
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.failures = 0
		b.opened = time.Time{}
		b.probe = false
		return nil
	}
	b.failures++
	if b.failures >= b.threshold {
		b.opened = b.now()
	}
	b.probe = false
	return err
}
