package reliability

import (
	"math/rand"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := 2 * time.Second
	for i := 0; i < 100; i++ {
		d := ExponentialBackoff(i, base, max, nil)
		if d < base || d > max {
			t.Fatalf("out of range %v", d)
		}
		j := ExponentialBackoff(i, base, max, rand.New(rand.NewSource(int64(i))))
		if j < 0 || j > d {
			t.Fatalf("invalid jitter %v > %v", j, d)
		}
	}
	if ExponentialBackoff(3, 0, max, nil) != 0 {
		t.Fatal("zero base")
	}
}
