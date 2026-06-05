package hutch

import (
	"math"
	"math/rand"
	"time"
)

// Backoff describes an exponential backoff schedule with optional jitter, used
// by drivers when reconnecting to a broker.
type Backoff struct {
	Min    time.Duration // delay before the first retry
	Max    time.Duration // cap on the delay
	Factor float64       // multiplier per attempt (>= 1)
	Jitter bool          // randomize within [d/2, d] to avoid thundering herds
}

// DefaultBackoff returns a sensible schedule: 1s, 2s, 4s … capped at 30s, with
// jitter.
func DefaultBackoff() Backoff {
	return Backoff{Min: time.Second, Max: 30 * time.Second, Factor: 2, Jitter: true}
}

// ForAttempt returns the delay for a 1-based attempt number.
func (b Backoff) ForAttempt(attempt int) time.Duration {
	min := b.Min
	if min <= 0 {
		min = time.Second
	}
	max := b.Max
	if max <= 0 {
		max = 30 * time.Second
	}
	factor := b.Factor
	if factor < 1 {
		factor = 2
	}
	if attempt < 1 {
		attempt = 1
	}

	d := float64(min) * math.Pow(factor, float64(attempt-1))
	if math.IsInf(d, 1) || d > float64(max) {
		d = float64(max)
	}
	wait := time.Duration(d)
	if b.Jitter && wait > 1 {
		half := wait / 2
		wait = half + time.Duration(rand.Int63n(int64(half)+1))
	}
	return wait
}
