// Package reliability provides bounded retry timing primitives.
package reliability

import (
	"math/rand"
	"time"
)

// ExponentialBackoff computes a capped delay; jitter should be sampled with a per-worker RNG.
func ExponentialBackoff(attempt int, base, max time.Duration, rng *rand.Rand) time.Duration {
	if base <= 0 || max <= 0 {
		return 0
	}
	if base > max {
		base = max
	}
	delay := base
	for i := 0; i < attempt && delay < max; i++ {
		if delay > max/2 {
			delay = max
		} else {
			delay *= 2
		}
	}
	if rng == nil {
		return delay
	}
	return time.Duration(rng.Int63n(int64(delay) + 1))
}
