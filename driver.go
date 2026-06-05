package hutch

import (
	"context"
	"errors"
)

// ErrClosed is returned by operations on a Pool, Publisher, or driver that has
// already been closed.
var ErrClosed = errors.New("hutch: closed")

// Message is a single unit of work yielded by a [Subscriber]. The worker that
// receives it must eventually call exactly one of Ack or Nack — the [Pool] does
// this automatically based on the handler result and the configured
// [ErrorPolicy].
type Message interface {
	// Body returns the raw payload bytes.
	Body() []byte

	// Ack acknowledges successful processing; the broker removes the message.
	Ack() error

	// Nack signals failure. If requeue is true the broker should redeliver the
	// message; if false it should drop it (or route it to a dead-letter
	// destination, where the broker supports one).
	Nack(requeue bool) error
}

// Subscriber is the consume side of a broker driver.
//
// Subscribe delivers messages from queue on the returned channel, keeping at
// most prefetch messages in flight (delivered but not yet acked) at a time —
// the mechanism that lets multiple replicas share a queue fairly. A driver is
// expected to handle its own connection resilience: the returned channel should
// stay open across transient reconnects and close only when ctx is cancelled or
// the driver is closed.
type Subscriber interface {
	Subscribe(ctx context.Context, queue string, prefetch int) (<-chan Message, error)
	Close() error
}

// Producer is the publish side of a broker driver. Implementations must be safe
// for concurrent use.
type Producer interface {
	Publish(ctx context.Context, queue string, body []byte) error
	Close() error
}
