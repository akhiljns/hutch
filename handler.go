package hutch

import (
	"context"
	"encoding/json"
	"fmt"
)

// HandlerFunc processes a single message. Returning nil acks the message;
// returning an error applies the queue's [ErrorPolicy]. The context carries the
// per-message timeout (see [HandlerTimeout]) and is cancelled when the pool
// shuts down, so long-running handlers should respect it.
type HandlerFunc func(ctx context.Context, msg Message) error

// Handle adapts a typed handler into a [HandlerFunc] by JSON-decoding the
// message body into T. A decode failure is returned as an error (and so follows
// the queue's ErrorPolicy — typically [Drop], since a malformed payload can
// never succeed on retry).
//
//	pool.Handle("orders", hutch.Handle(func(ctx context.Context, o Order) error {
//	    return process(ctx, o)
//	}))
func Handle[T any](fn func(ctx context.Context, v T) error) HandlerFunc {
	return func(ctx context.Context, m Message) error {
		var v T
		if err := json.Unmarshal(m.Body(), &v); err != nil {
			return fmt.Errorf("hutch: decode %T: %w", v, err)
		}
		return fn(ctx, v)
	}
}
