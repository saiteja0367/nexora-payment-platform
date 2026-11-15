package overload

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestBoundedAdmission(t *testing.T) {
	g := New(2)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Run(context.Background(), func(context.Context) error { started <- struct{}{}; <-release; return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	<-started
	<-started
	if err := g.Run(context.Background(), func(context.Context) error { t.Error("must reject"); return nil }); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("got %v", err)
	}
	if g.InFlight() != 2 || g.Rejected() != 1 {
		t.Fatal("invalid gate metrics")
	}
	close(release)
	wg.Wait()
	if g.InFlight() != 0 {
		t.Fatal("leaked permit")
	}
}
