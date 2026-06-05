package hutch

import "time"

const (
	defaultWorkers        = 10
	defaultHandlerTimeout = 30 * time.Second
)

// poolConfig holds pool-wide defaults that each queue inherits unless overridden
// with a QueueOption.
type poolConfig struct {
	log            Logger
	workers        int
	prefetch       int // 0 => default to the worker count per queue
	handlerTimeout time.Duration
	errorPolicy    ErrorPolicy
}

func defaultPoolConfig() poolConfig {
	return poolConfig{
		log:            nopLogger{},
		workers:        defaultWorkers,
		prefetch:       0,
		handlerTimeout: defaultHandlerTimeout,
		errorPolicy:    Drop,
	}
}

// Option configures a [Pool].
type Option func(*poolConfig)

// WithLogger sets the logger (default: no-op). *log.Logger satisfies it.
func WithLogger(l Logger) Option {
	return func(c *poolConfig) {
		if l != nil {
			c.log = l
		}
	}
}

// WithWorkers sets the default number of worker goroutines per queue.
func WithWorkers(n int) Option { return func(c *poolConfig) { c.workers = n } }

// WithPrefetch sets the default max in-flight messages per queue. When unset (or
// 0) it defaults to the queue's worker count, which keeps every worker busy
// without hoarding the backlog from other replicas.
func WithPrefetch(n int) Option { return func(c *poolConfig) { c.prefetch = n } }

// WithHandlerTimeout sets the default per-message handler timeout (0 disables).
func WithHandlerTimeout(d time.Duration) Option {
	return func(c *poolConfig) { c.handlerTimeout = d }
}

// WithErrorPolicy sets the default policy applied when a handler errors.
func WithErrorPolicy(p ErrorPolicy) Option { return func(c *poolConfig) { c.errorPolicy = p } }

// queueConfig is the resolved configuration for one Handle call.
type queueConfig struct {
	workers        int
	prefetch       int
	handlerTimeout time.Duration
	errorPolicy    ErrorPolicy
	onError        func(Message, error)
}

// QueueOption overrides pool defaults for a single [Pool.Handle] call.
type QueueOption func(*queueConfig)

// Workers overrides the worker count for this queue.
func Workers(n int) QueueOption { return func(c *queueConfig) { c.workers = n } }

// Prefetch overrides the max in-flight messages for this queue.
func Prefetch(n int) QueueOption { return func(c *queueConfig) { c.prefetch = n } }

// HandlerTimeout overrides the per-message timeout for this queue (0 disables).
func HandlerTimeout(d time.Duration) QueueOption {
	return func(c *queueConfig) { c.handlerTimeout = d }
}

// OnError overrides the [ErrorPolicy] for this queue.
func OnError(p ErrorPolicy) QueueOption { return func(c *queueConfig) { c.errorPolicy = p } }

// OnErrorFunc registers a callback invoked with every failed message and its
// error (before the ErrorPolicy is applied) — handy for metrics or capturing
// dropped payloads.
func OnErrorFunc(fn func(Message, error)) QueueOption {
	return func(c *queueConfig) { c.onError = fn }
}

func (p poolConfig) queueDefaults() queueConfig {
	return queueConfig{
		workers:        p.workers,
		prefetch:       p.prefetch,
		handlerTimeout: p.handlerTimeout,
		errorPolicy:    p.errorPolicy,
	}
}
