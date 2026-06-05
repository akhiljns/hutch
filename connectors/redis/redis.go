// Package redis is a hutch driver backed by Redis Streams and consumer groups,
// built on github.com/redis/go-redis/v9.
//
// Why streams (and not lists / pub-sub)? A queue driver for hutch must be able
// to Ack and Nack individual messages, and to redeliver work that a crashed
// consumer never finished. Redis Streams give exactly that:
//
//   - XADD publishes; a consumer group (XREADGROUP) load-balances new entries
//     across every consumer in the group — the same fair fan-out you get from a
//     RabbitMQ queue, so adding replicas adds throughput.
//   - Each delivered-but-unacked entry sits in the group's Pending Entries List
//     (PEL). XACK removes it; that is the ack.
//   - prefetch is honored by reading at most as many entries as there are free
//     in-flight slots (COUNT) and never reading more until some are settled —
//     so one replica can't hoard the backlog.
//   - Entries left pending by a dead consumer are reclaimed (XAUTOCLAIM) after
//     they go idle and redelivered, which is the Redis equivalent of RabbitMQ
//     redelivering unacked messages when a channel drops.
//
// Like the rabbitmq driver, NewSubscriber and NewProducer open independent
// clients, so a consumer-side storm can't starve your producers. go-redis
// manages its own connection pool and reconnects per command, so there is no
// separate connection manager here; the read loop only backs off on errors.
package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/akhiljns/hutch"

	redis "github.com/redis/go-redis/v9"
)

const (
	defaultGroup        = "hutch"
	defaultBlock        = 5 * time.Second
	defaultClaimMinIdle = 30 * time.Second
	bodyField           = "data"
)

// Option configures a driver.
type Option func(*config)

type config struct {
	backoff      hutch.Backoff
	log          hutch.Logger
	group        string
	consumer     string
	block        time.Duration
	claimMinIdle time.Duration
	maxLen       int64
}

func newConfig(opts ...Option) config {
	c := config{
		backoff:      hutch.DefaultBackoff(),
		log:          nopLogger{},
		group:        defaultGroup,
		consumer:     defaultConsumerName(),
		block:        defaultBlock,
		claimMinIdle: defaultClaimMinIdle,
	}
	for _, o := range opts {
		o(&c)
	}
	if c.log == nil {
		c.log = nopLogger{}
	}
	if c.group == "" {
		c.group = defaultGroup
	}
	if c.consumer == "" {
		c.consumer = defaultConsumerName()
	}
	if c.block <= 0 {
		c.block = defaultBlock
	}
	return c
}

// WithBackoff sets the backoff schedule used when a stream read errors.
func WithBackoff(b hutch.Backoff) Option { return func(c *config) { c.backoff = b } }

// WithLogger sets the logger (default: no-op).
func WithLogger(l hutch.Logger) Option { return func(c *config) { c.log = l } }

// WithGroup sets the consumer-group name (default "hutch"). All replicas that
// should share a queue must use the same group.
func WithGroup(name string) Option { return func(c *config) { c.group = name } }

// WithConsumerName sets this instance's consumer name within the group (default
// "<hostname>-<pid>"). It only needs to be unique per process.
func WithConsumerName(name string) Option { return func(c *config) { c.consumer = name } }

// WithBlock sets how long a single XREADGROUP call blocks waiting for new
// entries before looping (default 5s). It bounds shutdown latency, not throughput.
func WithBlock(d time.Duration) Option { return func(c *config) { c.block = d } }

// WithClaimMinIdle sets how long an entry must sit unacked before another
// consumer reclaims and redelivers it (default 30s). Set it comfortably above
// your handler timeout so in-progress work isn't reclaimed. Zero disables
// reclaiming.
func WithClaimMinIdle(d time.Duration) Option { return func(c *config) { c.claimMinIdle = d } }

// WithMaxLen caps stream length on publish using approximate trimming (XADD
// MAXLEN ~). Acked messages are deleted regardless; this is a backstop against a
// producer outrunning consumers. Default 0 (unbounded).
func WithMaxLen(n int64) Option { return func(c *config) { c.maxLen = n } }

func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "consumer"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// dial parses a redis:// URL and verifies connectivity before returning.
func dial(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

type subscriber struct {
	rdb *redis.Client
	cfg config

	root   context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
}

// NewSubscriber opens a consumer client to Redis. url is a standard redis URL,
// e.g. "redis://localhost:6379/0".
func NewSubscriber(url string, opts ...Option) (hutch.Subscriber, error) {
	rdb, err := dial(url)
	if err != nil {
		return nil, err
	}
	root, cancel := context.WithCancel(context.Background())
	return &subscriber{rdb: rdb, cfg: newConfig(opts...), root: root, cancel: cancel}, nil
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

	// Start the group at "0" so it picks up the entire existing backlog, not just
	// entries published after it connects — work-queue semantics, like a RabbitMQ
	// queue. MKSTREAM creates the stream if needed; BUSYGROUP means it already exists.
	if err := s.rdb.XGroupCreateMkStream(ctx, queue, s.cfg.group, "0").Err(); err != nil &&
		!isBusyGroup(err) {
		s.mu.Lock()
		s.wg.Done()
		s.mu.Unlock()
		return nil, err
	}

	rctx, rcancel := mergedContext(ctx, s.root)
	out := make(chan hutch.Message)
	sem := make(chan struct{}, prefetch)

	go func() {
		defer s.wg.Done()
		defer rcancel()
		defer close(out)
		var wg sync.WaitGroup
		if s.cfg.claimMinIdle > 0 {
			wg.Add(1)
			go func() { defer wg.Done(); s.reclaimLoop(rctx, queue, sem, out) }()
		}
		s.readLoop(rctx, queue, prefetch, sem, out)
		wg.Wait()
	}()
	return out, nil
}

// readLoop pulls new entries (XREADGROUP ... >) up to the number of free
// in-flight slots and forwards them, backing off on transient errors.
func (s *subscriber) readLoop(ctx context.Context, queue string, prefetch int, sem chan struct{}, out chan<- hutch.Message) {
	s.cfg.log.Printf("redis: consuming %q (group %q, prefetch %d)", queue, s.cfg.group, prefetch)
	fails := 0
	for {
		if ctx.Err() != nil {
			return
		}
		// Block until at least one slot is free, then grab as many more as are
		// immediately available so we can read a batch in one round trip.
		n, ok := acquire(ctx, sem, prefetch)
		if !ok {
			return
		}

		res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.cfg.group,
			Consumer: s.cfg.consumer,
			Streams:  []string{queue, ">"},
			Count:    int64(n),
			Block:    s.cfg.block,
		}).Result()
		if err != nil {
			release(sem, n) // give the slots back; nothing was delivered
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue // block timeout with no messages, or shutting down
			}
			fails++
			s.cfg.log.Printf("redis: read %q failed (attempt %d): %v", queue, fails, err)
			if !sleep(ctx, s.cfg.backoff.ForAttempt(fails)) {
				return
			}
			continue
		}
		fails = 0

		delivered := 0
		for _, st := range res {
			for _, x := range st.Messages {
				if s.forward(ctx, queue, x, sem, out) {
					delivered++
				}
			}
		}
		// Release any slots we acquired but didn't fill.
		release(sem, n-delivered)
	}
}

// reclaimLoop periodically takes ownership of entries that have been pending
// (delivered but unacked) longer than claimMinIdle — i.e. orphaned by a crashed
// consumer — and redelivers them.
func (s *subscriber) reclaimLoop(ctx context.Context, queue string, sem chan struct{}, out chan<- hutch.Message) {
	t := time.NewTicker(s.cfg.claimMinIdle)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		start := "0-0"
		for {
			msgs, next, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   queue,
				Group:    s.cfg.group,
				Consumer: s.cfg.consumer,
				MinIdle:  s.cfg.claimMinIdle,
				Start:    start,
				Count:    100,
			}).Result()
			if err != nil {
				if ctx.Err() == nil {
					s.cfg.log.Printf("redis: reclaim %q failed: %v", queue, err)
				}
				break
			}
			for _, x := range msgs {
				if _, ok := acquire(ctx, sem, 1); !ok {
					return
				}
				if !s.forward(ctx, queue, x, sem, out) {
					release(sem, 1)
				}
			}
			if next == "0-0" || next == "" {
				break // scanned the whole PEL
			}
			start = next
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// forward wraps an entry as a hutch.Message and hands it to a worker. The caller
// must already hold one sem slot, which the message releases when settled. It
// returns false (and settles the slot itself) if the entry can't be delivered.
func (s *subscriber) forward(ctx context.Context, queue string, x redis.XMessage, sem chan struct{}, out chan<- hutch.Message) bool {
	body, ok := x.Values[bodyField]
	if !ok {
		// Tombstone left by XAUTOCLAIM for an entry deleted from the stream:
		// clear it from the PEL and skip.
		_ = s.rdb.XAck(ctx, queue, s.cfg.group, x.ID).Err()
		return false
	}
	m := &message{
		rdb:    s.rdb,
		stream: queue,
		group:  s.cfg.group,
		id:     x.ID,
		body:   []byte(toString(body)),
		sem:    sem,
		maxLen: s.cfg.maxLen,
	}
	select {
	case out <- m:
		return true
	case <-ctx.Done():
		return false
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

	s.cancel()
	s.wg.Wait()
	return s.rdb.Close()
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

type producer struct {
	rdb    *redis.Client
	maxLen int64
}

// NewProducer opens a publisher client to Redis (independent of any consumer
// client). url is a standard redis URL.
func NewProducer(url string, opts ...Option) (hutch.Producer, error) {
	rdb, err := dial(url)
	if err != nil {
		return nil, err
	}
	cfg := newConfig(opts...)
	return &producer{rdb: rdb, maxLen: cfg.maxLen}, nil
}

func (p *producer) Publish(ctx context.Context, queue string, body []byte) error {
	args := &redis.XAddArgs{
		Stream: queue,
		Values: map[string]any{bodyField: body},
	}
	if p.maxLen > 0 {
		args.MaxLen = p.maxLen
		args.Approx = true
	}
	return p.rdb.XAdd(ctx, args).Err()
}

func (p *producer) Close() error { return p.rdb.Close() }

// ---------------------------------------------------------------------------
// message
// ---------------------------------------------------------------------------

// message adapts a stream entry to hutch.Message. It settles exactly once and
// releases its in-flight slot on settle. Ack and Reject (Nack false) remove the
// entry; Requeue (Nack true) re-adds the body at the tail for redelivery.
type message struct {
	rdb    *redis.Client
	stream string
	group  string
	id     string
	body   []byte
	sem    chan struct{}
	maxLen int64
	once   sync.Once
}

func (m *message) Body() []byte { return m.body }

func (m *message) Ack() error {
	var err error
	m.once.Do(func() {
		err = m.settle(false)
	})
	return err
}

func (m *message) Nack(requeue bool) error {
	var err error
	m.once.Do(func() {
		if requeue {
			// Append a fresh copy at the tail, then clear the original from the
			// PEL. (A stream entry is immutable and stays put; re-adding is how
			// you get a redelivery without leaving it pending forever.)
			args := &redis.XAddArgs{Stream: m.stream, Values: map[string]any{bodyField: m.body}}
			if m.maxLen > 0 {
				args.MaxLen = m.maxLen
				args.Approx = true
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if e := m.rdb.XAdd(ctx, args).Err(); e != nil {
				err = e
				return // leave it pending; reclaim will retry it later
			}
		}
		err = m.settle(true)
	})
	return err
}

// settle acknowledges the entry, deletes it from the stream to keep the stream
// bounded, and frees the in-flight slot.
func (m *message) settle(dropping bool) error {
	defer func() { <-m.sem }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.rdb.XAck(ctx, m.stream, m.group, m.id).Err(); err != nil {
		return err
	}
	// Best-effort cleanup; the ack is what matters for at-least-once delivery.
	_ = m.rdb.XDel(ctx, m.stream, m.id).Err()
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// acquire blocks until at least one slot is free (or ctx is done), then takes up
// to max slots without blocking. It returns the number acquired (>=1) and true,
// or 0 and false if ctx was cancelled first.
func acquire(ctx context.Context, sem chan struct{}, max int) (int, bool) {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return 0, false
	}
	n := 1
	for n < max {
		select {
		case sem <- struct{}{}:
			n++
		default:
			return n, true
		}
	}
	return n, true
}

func release(sem chan struct{}, n int) {
	for i := 0; i < n; i++ {
		<-sem
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func isBusyGroup(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}

// toString accepts the string or []byte that go-redis may hand back for a field.
func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprint(v)
	}
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
