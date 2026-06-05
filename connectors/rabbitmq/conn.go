package rabbitmq

import (
	"context"
	"sync"
	"time"

	"github.com/akhiljns/hutch"

	amqp "github.com/rabbitmq/amqp091-go"
)

// connManager owns a single AMQP connection and re-dials it (with backoff,
// honoring a context) when it drops. It is shared by the consumer-forwarding
// loop and the publisher pool within a single driver instance.
type connManager struct {
	url     string
	backoff hutch.Backoff
	log     hutch.Logger

	mu     sync.Mutex
	conn   *amqp.Connection
	closed bool
}

func dialManager(url string, backoff hutch.Backoff, log hutch.Logger) (*connManager, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return &connManager{url: url, backoff: backoff, log: log, conn: conn}, nil
}

func (m *connManager) connection() *amqp.Connection {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn
}

// ensure returns a live connection, re-dialing with backoff until it succeeds,
// ctx is cancelled, or the manager is closed.
func (m *connManager) ensure(ctx context.Context) (*amqp.Connection, error) {
	for attempt := 1; ; attempt++ {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, hutch.ErrClosed
		}
		if m.conn != nil && !m.conn.IsClosed() {
			c := m.conn
			m.mu.Unlock()
			return c, nil
		}
		m.mu.Unlock()

		conn, err := amqp.Dial(m.url)
		if err == nil {
			m.mu.Lock()
			if m.closed {
				m.mu.Unlock()
				_ = conn.Close()
				return nil, hutch.ErrClosed
			}
			old := m.conn
			m.conn = conn
			m.mu.Unlock()
			if old != nil && !old.IsClosed() {
				_ = old.Close()
			}
			if attempt > 1 {
				m.log.Printf("rabbitmq: reconnected (attempt %d)", attempt)
			}
			return conn, nil
		}

		m.log.Printf("rabbitmq: dial failed (attempt %d): %v", attempt, err)
		select {
		case <-time.After(m.backoff.ForAttempt(attempt)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (m *connManager) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.conn != nil && !m.conn.IsClosed() {
		return m.conn.Close()
	}
	return nil
}
