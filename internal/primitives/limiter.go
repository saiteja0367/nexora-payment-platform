package primitives

import (
	"sync"
	"time"
)

// Limiter is a per-process token bucket. Distributed enforcement needs shared state.
type Limiter struct {
	mu                     sync.Mutex
	tokens, capacity, rate float64
	last                   time.Time
	now                    func() time.Time
}

func NewLimiter(capacity int, ratePerSecond float64) *Limiter {
	if capacity < 1 {
		capacity = 1
	}
	if ratePerSecond <= 0 {
		ratePerSecond = 1
	}
	return &Limiter{tokens: float64(capacity), capacity: float64(capacity), rate: ratePerSecond, last: time.Now(), now: time.Now}
}
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.rate
		if l.tokens > l.capacity {
			l.tokens = l.capacity
		}
	}
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
