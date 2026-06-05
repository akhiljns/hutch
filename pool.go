package hutch

import (
	"context"
	"errors"
	"sync"
)

// Pool is a broker-agnostic consumer worker-pool. It draws messages from a
// [Subscriber], fans them out to a configurable number of workers per queue,
// bounds in-flight work via prefetch, applies an [ErrorPolicy] on failure, and
// drains gracefully on shutdown.
//
// A Pool is safe for concurrent use. Create one per Subscriber.
type Pool struct {
	sub Subscriber
	cfg poolConfig

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// NewPool builds a Pool over the given Subscriber.
func NewPool(sub Subscriber, opts ...Option) *Pool {
	cfg := defaultPoolConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.log == nil {
		cfg.log = nopLogger{}
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{sub: sub, cfg: cfg, ctx: ctx, cancel: cancel}
}

// Handle starts a worker pool for queue. It may be called for multiple queues on
// the same Pool. It returns once the subscription is established and workers are
// running; it does not block.
func (p *Pool) Handle(queue string, h HandlerFunc, opts ...QueueOption) error {
	if queue == "" {
		return errors.New("hutch: queue name is required")
	}
	if h == nil {
		return errors.New("hutch: handler is required")
	}

	qc := p.cfg.queueDefaults()
	for _, o := range opts {
		o(&qc)
	}
	if qc.workers < 1 {
		qc.workers = 1
	}
	if qc.prefetch < 1 {
		// The default that makes horizontal scaling work: cap in-flight at the
		// worker count so every worker can be busy without hoarding the backlog.
		qc.prefetch = qc.workers
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	p.mu.Unlock()

	msgs, err := p.sub.Subscribe(p.ctx, queue, qc.prefetch)
	if err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	// Spawn workers while holding the lock so wg.Add can never race Close's Wait.
	for i := 0; i < qc.workers; i++ {
		p.wg.Add(1)
		go p.worker(queue, msgs, h, qc)
	}
	p.mu.Unlock()

	p.cfg.log.Printf("hutch: handling %q (workers=%d prefetch=%d policy=%s)",
		queue, qc.workers, qc.prefetch, qc.errorPolicy)
	return nil
}

func (p *Pool) worker(queue string, msgs <-chan Message, h HandlerFunc, qc queueConfig) {
	defer p.wg.Done()
	for {
		select {
		case m, ok := <-msgs:
			if !ok {
				return
			}
			p.process(queue, m, h, qc)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) process(queue string, m Message, h HandlerFunc, qc queueConfig) {
	ctx := p.ctx
	if qc.handlerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, qc.handlerTimeout)
		defer cancel()
	}

	if err := h(ctx, m); err != nil {
		p.cfg.log.Printf("hutch: handler error on %q: %v", queue, err)
		if qc.onError != nil {
			qc.onError(m, err)
		}
		switch qc.errorPolicy {
		case Requeue:
			ackErr(p.cfg.log, queue, m.Nack(true))
		case Reject:
			ackErr(p.cfg.log, queue, m.Nack(false))
		default: // Drop
			ackErr(p.cfg.log, queue, m.Ack())
		}
		return
	}

	ackErr(p.cfg.log, queue, m.Ack())
}

func ackErr(log Logger, queue string, err error) {
	if err != nil {
		log.Printf("hutch: settle failed on %q: %v", queue, err)
	}
}

// Close stops accepting new messages and waits for in-flight handlers to finish,
// bounded by ctx. Messages still in flight when ctx expires are abandoned and
// will be redelivered by the broker after restart (at-least-once). Close is
// idempotent.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Tell the driver to stop delivering; this closes the message channels so
	// idle workers exit.
	subErr := p.sub.Close()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cfg.log.Printf("hutch: all workers drained")
	case <-ctx.Done():
		p.cfg.log.Printf("hutch: drain deadline exceeded; abandoning in-flight messages")
	}

	p.cancel() // release any workers still parked in handlers that honor ctx
	return subErr
}
