package primitives

import (
	"errors"
	"testing"
	"time"
)

func TestRingStableAndMovement(t *testing.T) {
	a := NewRing([]string{"a", "b", "c"}, 100)
	b := NewRing([]string{"a", "b", "c", "d"}, 100)
	moved := 0
	for i := 0; i < 1000; i++ {
		k := string(rune(i + 100))
		if a.Owner(k) != a.Owner(k) {
			t.Fatal("unstable")
		}
		if a.Owner(k) != b.Owner(k) {
			moved++
		}
	}
	if moved == 0 || moved > 500 {
		t.Fatalf("unexpected reassignment %d", moved)
	}
}
func TestCircuitBreaker(t *testing.T) {
	b := NewBreaker(2, time.Hour)
	bad := func() error { return errors.New("down") }
	_ = b.Execute(bad)
	_ = b.Execute(bad)
	if !errors.Is(b.Execute(func() error { return nil }), ErrCircuitOpen) {
		t.Fatal("expected open")
	}
	b.mu.Lock()
	b.opened = time.Now().Add(-2 * time.Hour)
	b.mu.Unlock()
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}
func TestTokenBucket(t *testing.T) {
	l := NewLimiter(2, 1)
	if !l.Allow() || !l.Allow() || l.Allow() {
		t.Fatal("burst wrong")
	}
	l.mu.Lock()
	l.last = l.last.Add(-2 * time.Second)
	l.mu.Unlock()
	if !l.Allow() {
		t.Fatal("refill failed")
	}
}
