// Package memory is an in-process hutch driver backed by Go channels. It needs
// no external broker, which makes it ideal for tests, local development, and
// examples. A single Broker implements both hutch.Subscriber and
// hutch.Producer, sharing its queues between publishers and subscribers.
package memory

import (
	"context"
	"sync"

	"github.com/akhiljns/hutch"
)

// Broker is an in-memory message broker. The zero value is not usable; call New.
type Broker struct {
	mu     sync.Mutex
	queues map[string]*queue
	closed bool
}

// New returns an empty in-memory broker.
func New() *Broker {
	return &Broker{queues: make(map[string]*queue)}
}

var (
	_ hutch.Subscriber = (*Broker)(nil)
	_ hutch.Producer   = (*Broker)(nil)
)

func (b *Broker) getQueue(name string) (*queue, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, false
	}
	q, ok := b.queues[name]
	if !ok {
		q = newQueue()
		b.queues[name] = q
	}
	return q, true
}

// Publish enqueues body on the named queue.
func (b *Broker) Publish(_ context.Context, name string, body []byte) error {
	q, ok := b.getQueue(name)
	if !ok {
		return hutch.ErrClosed
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	q.push(cp)
	return nil
}

// Subscribe streams messages from the named queue, keeping at most prefetch in
// flight. The channel closes when ctx is cancelled or the broker is closed.
func (b *Broker) Subscribe(ctx context.Context, name string, prefetch int) (<-chan hutch.Message, error) {
	q, ok := b.getQueue(name)
	if !ok {
		return nil, hutch.ErrClosed
	}
	if prefetch < 1 {
		prefetch = 1
	}
	out := make(chan hutch.Message)
	go q.deliver(ctx, out, prefetch)
	return out, nil
}

// Close stops all subscriptions and rejects further use.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for _, q := range b.queues {
		q.close()
	}
	return nil
}

// queue is an unbounded FIFO with a condition variable for blocking pops.
type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  [][]byte
	closed bool
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue) push(body []byte) {
	q.mu.Lock()
	if !q.closed {
		q.items = append(q.items, body)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

// pop blocks until an item is available, the queue is closed, or ctx is done.
func (q *queue) pop(ctx context.Context) ([]byte, bool) {
	// Wake the cond waiter if ctx is cancelled.
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		if q.closed || ctx.Err() != nil {
			return nil, false
		}
		q.cond.Wait()
	}
	body := q.items[0]
	q.items = q.items[1:]
	return body, true
}

func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *queue) deliver(ctx context.Context, out chan<- hutch.Message, prefetch int) {
	defer close(out)
	sem := make(chan struct{}, prefetch)
	for {
		// Acquire an in-flight slot (this is the prefetch bound).
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		body, ok := q.pop(ctx)
		if !ok {
			<-sem
			return
		}

		m := &message{q: q, body: body, sem: sem}
		select {
		case out <- m:
		case <-ctx.Done():
			<-sem
			return
		}
	}
}

// message settles exactly once; the in-flight slot is released on settle.
type message struct {
	q    *queue
	body []byte
	sem  chan struct{}
	once sync.Once
}

func (m *message) Body() []byte { return m.body }

func (m *message) Ack() error {
	m.once.Do(func() { <-m.sem })
	return nil
}

func (m *message) Nack(requeue bool) error {
	m.once.Do(func() {
		if requeue {
			m.q.push(m.body)
		}
		<-m.sem
	})
	return nil
}
