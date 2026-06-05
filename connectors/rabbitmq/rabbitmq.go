// Package rabbitmq is a hutch driver for RabbitMQ (AMQP 0-9-1), built on
// github.com/rabbitmq/amqp091-go.
//
// It encodes the production lessons hutch is built around:
//
//   - Consumers set a per-consumer prefetch (QoS) so work distributes fairly
//     across replicas instead of one pod hoarding the queue.
//   - A worker pool shares ONE channel/consumer rather than one channel per
//     worker, keeping channel usage flat as you scale (RabbitMQ caps channels
//     per connection and per user).
//   - Consumers reconnect automatically with backoff, transparently re-attaching
//     the delivery stream.
//   - Publishing uses a SEPARATE connection from consuming, with a small pool of
//     channels, so a consumer-side disruption can never starve producers.
//
// Use NewSubscriber and NewProducer separately — each opens its own connection,
// which is what gives you publish/consume isolation.
package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akhiljns/hutch"

	amqp "github.com/rabbitmq/amqp091-go"
)

var errChannelClosed = errors.New("rabbitmq: channel closed")

const defaultPublisherChannels = 3

// Option configures a driver.
type Option func(*config)

type config struct {
	backoff           hutch.Backoff
	log               hutch.Logger
	publisherChannels int
	declareQueues     bool
}

func newConfig(opts ...Option) config {
	c := config{
		backoff:           hutch.DefaultBackoff(),
		log:               nopLogger{},
		publisherChannels: defaultPublisherChannels,
		declareQueues:     true,
	}
	for _, o := range opts {
		o(&c)
	}
	if c.log == nil {
		c.log = nopLogger{}
	}
	if c.publisherChannels < 1 {
		c.publisherChannels = 1
	}
	return c
}

// WithBackoff sets the reconnect backoff schedule.
func WithBackoff(b hutch.Backoff) Option { return func(c *config) { c.backoff = b } }

// WithLogger sets the logger (default: no-op).
func WithLogger(l hutch.Logger) Option { return func(c *config) { c.log = l } }

// WithPublisherChannels sets the producer's channel-pool size (default 3).
func WithPublisherChannels(n int) Option { return func(c *config) { c.publisherChannels = n } }

// WithDeclareQueues controls whether the producer declares a durable queue on
// first publish (default true). Disable it if queues are provisioned elsewhere.
func WithDeclareQueues(declare bool) Option { return func(c *config) { c.declareQueues = declare } }

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// message adapts an amqp.Delivery to hutch.Message.
type message struct{ d amqp.Delivery }

func (m *message) Body() []byte            { return m.d.Body }
func (m *message) Ack() error              { return m.d.Ack(false) }
func (m *message) Nack(requeue bool) error { return m.d.Nack(false, requeue) }

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

type subscriber struct {
	cm  *connManager
	log hutch.Logger

	root   context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
}

// NewSubscriber opens a consumer connection to the broker.
func NewSubscriber(url string, opts ...Option) (hutch.Subscriber, error) {
	cfg := newConfig(opts...)
	cm, err := dialManager(url, cfg.backoff, cfg.log)
	if err != nil {
		return nil, err
	}
	root, cancel := context.WithCancel(context.Background())
	return &subscriber{cm: cm, log: cfg.log, root: root, cancel: cancel}, nil
}

func (s *subscriber) Subscribe(ctx context.Context, queue string, prefetch int) (<-chan hutch.Message, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, hutch.ErrClosed
	}
	s.wg.Add(1)
	s.mu.Unlock()

	if prefetch < 1 {
		prefetch = 1
	}
	// Stop the forwarding loop when either the caller's ctx or the subscriber is
	// cancelled.
	rctx, rcancel := mergedContext(ctx, s.root)
	out := make(chan hutch.Message)

	go func() {
		defer s.wg.Done()
		defer rcancel()
		defer close(out)
		s.run(rctx, queue, prefetch, out)
	}()
	return out, nil
}

func (s *subscriber) run(ctx context.Context, queue string, prefetch int, out chan<- hutch.Message) {
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := s.cm.ensure(ctx)
		if err != nil {
			return // ctx cancelled or subscriber closed
		}
		if err := s.consumeOnce(ctx, conn, queue, prefetch, out); err != nil {
			s.log.Printf("rabbitmq: %q consume interrupted (%v); reconnecting", queue, err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

// consumeOnce attaches a single channel + consumer and forwards deliveries until
// the channel/connection drops (returns an error to trigger reconnect) or ctx is
// cancelled (returns nil to stop).
func (s *subscriber) consumeOnce(ctx context.Context, conn *amqp.Connection, queue string, prefetch int, out chan<- hutch.Message) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Per-consumer prefetch: the key to fair distribution across replicas.
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	s.log.Printf("rabbitmq: consuming %q (prefetch %d)", queue, prefetch)

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return errChannelClosed
			}
			select {
			case out <- &message{d: d}:
			case <-ctx.Done():
				return nil
			}
		case err := <-closed:
			if err != nil {
				return err
			}
			return errChannelClosed
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *subscriber) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.cancel()  // stop all forwarding loops
	s.wg.Wait() // wait for them to close their channels
	return s.cm.close()
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

type producer struct {
	cm            *connManager
	log           hutch.Logger
	size          int
	declareQueues bool

	next uint64
	mu   sync.Mutex
	chans    []*amqp.Channel
	declared map[string]bool
}

// NewProducer opens a publisher connection to the broker (separate from any
// consumer connection).
func NewProducer(url string, opts ...Option) (hutch.Producer, error) {
	cfg := newConfig(opts...)
	cm, err := dialManager(url, cfg.backoff, cfg.log)
	if err != nil {
		return nil, err
	}
	return &producer{
		cm:            cm,
		log:           cfg.log,
		size:          cfg.publisherChannels,
		declareQueues: cfg.declareQueues,
		chans:         make([]*amqp.Channel, cfg.publisherChannels),
		declared:      make(map[string]bool),
	}, nil
}

func (p *producer) Publish(ctx context.Context, queue string, body []byte) error {
	if _, err := p.cm.ensure(ctx); err != nil {
		return err
	}
	if err := p.ensureDeclared(queue); err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ { // one retry for a stale channel
		if err := ctx.Err(); err != nil {
			return err
		}
		ch, idx, err := p.acquire()
		if err != nil {
			lastErr = err
			continue
		}
		err = ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		})
		if err == nil {
			return nil
		}
		lastErr = err
		p.discard(idx, ch)
	}
	return lastErr
}

func (p *producer) ensureDeclared(queue string) error {
	if !p.declareQueues {
		return nil
	}
	p.mu.Lock()
	done := p.declared[queue]
	p.mu.Unlock()
	if done {
		return nil
	}

	ch, err := p.cm.connection().Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	p.mu.Lock()
	p.declared[queue] = true
	p.mu.Unlock()
	return nil
}

func (p *producer) acquire() (*amqp.Channel, int, error) {
	idx := int(atomic.AddUint64(&p.next, 1)-1) % p.size
	p.mu.Lock()
	defer p.mu.Unlock()
	if ch := p.chans[idx]; ch != nil && !ch.IsClosed() {
		return ch, idx, nil
	}
	conn := p.cm.connection()
	if conn == nil || conn.IsClosed() {
		return nil, idx, errors.New("rabbitmq: no publisher connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, idx, err
	}
	p.chans[idx] = ch
	return ch, idx, nil
}

func (p *producer) discard(idx int, bad *amqp.Channel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.chans[idx] == bad {
		_ = bad.Close()
		p.chans[idx] = nil
	}
}

func (p *producer) Close() error {
	p.mu.Lock()
	for i, ch := range p.chans {
		if ch != nil {
			_ = ch.Close()
			p.chans[i] = nil
		}
	}
	p.mu.Unlock()
	return p.cm.close()
}

// mergedContext returns a context cancelled when either parent is done.
func mergedContext(a, b context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(a)
	stop := context.AfterFunc(b, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
