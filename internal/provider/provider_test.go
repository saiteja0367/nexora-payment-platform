package provider

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIdempotentCharge(t *testing.T) {
	s := New(Config{})
	for i := 0; i < 20; i++ {
		o, e := s.Charge(context.Background(), "m", "k")
		if e != nil || o != Approved {
			t.Fatalf("%s %v", o, e)
		}
	}
	if s.calls != 1 {
		t.Fatal(s.calls)
	}
}
func TestFaults(t *testing.T) {
	s := New(Config{FailEvery: 2})
	_, _ = s.Charge(context.Background(), "m", "a")
	_, err := s.Charge(context.Background(), "m", "b")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
func TestDeadline(t *testing.T) {
	s := New(Config{Delay: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := s.Charge(ctx, "m", "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
