package hutch

import (
	"testing"
	"time"
)

func TestBackoffForAttempt(t *testing.T) {
	b := Backoff{Min: time.Second, Max: 8 * time.Second, Factor: 2, Jitter: false}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 8 * time.Second}, // capped
	}
	for _, c := range cases {
		if got := b.ForAttempt(c.attempt); got != c.want {
			t.Errorf("ForAttempt(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffJitterWithinBounds(t *testing.T) {
	b := Backoff{Min: 4 * time.Second, Max: 4 * time.Second, Factor: 2, Jitter: true}
	for i := 0; i < 1000; i++ {
		d := b.ForAttempt(3) // base capped at 4s, jitter => [2s, 4s]
		if d < 2*time.Second || d > 4*time.Second {
			t.Fatalf("jitter out of bounds: %v", d)
		}
	}
}
