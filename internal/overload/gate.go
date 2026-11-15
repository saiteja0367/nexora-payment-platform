// Package overload provides bounded admission for expensive operations.
package overload

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrOverloaded = errors.New("capacity exhausted: retry later")

type Gate struct {
	tokens   chan struct{}
	rejected atomic.Uint64
}

func New(capacity int) *Gate {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	return &Gate{tokens: make(chan struct{}, capacity)}
}
func (g *Gate) Run(ctx context.Context, fn func(context.Context) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case g.tokens <- struct{}{}:
		defer func() { <-g.tokens }()
		return fn(ctx)
	default:
		g.rejected.Add(1)
		return ErrOverloaded
	}
}
func (g *Gate) InFlight() int    { return len(g.tokens) }
func (g *Gate) Rejected() uint64 { return g.rejected.Load() }
