package hutch

import (
	"context"
	"encoding/json"
)

// Publisher is a thin, broker-agnostic convenience wrapper over a [Producer].
// It adds JSON encoding; the resilience and batching/pooling live in the driver.
type Publisher struct {
	prod Producer
}

// NewPublisher wraps a Producer driver.
func NewPublisher(prod Producer) *Publisher {
	return &Publisher{prod: prod}
}

// Publish sends raw bytes to queue.
func (p *Publisher) Publish(ctx context.Context, queue string, body []byte) error {
	return p.prod.Publish(ctx, queue, body)
}

// PublishJSON JSON-encodes v and sends it to queue.
func (p *Publisher) PublishJSON(ctx context.Context, queue string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.prod.Publish(ctx, queue, body)
}

// Close closes the underlying producer.
func (p *Publisher) Close() error {
	return p.prod.Close()
}
