# hutch 🐰

A broker-agnostic **worker-pool for queue consumers** in Go.

`hutch` separates the orchestration that *every* queue consumer needs — a pool of
workers, a bounded number of in-flight messages, a configurable ack/nack policy,
per-message timeouts, and graceful draining on shutdown — from the broker
itself. Brokers plug in behind two small interfaces, so the same consumer code
runs on RabbitMQ, Redis, or (with a ~150-line driver) anything else.

```go
sub, _ := rabbitmq.NewSubscriber("amqp://guest:guest@localhost:5672/")
pool := hutch.NewPool(sub, hutch.WithWorkers(20))

pool.Handle("orders", hutch.Handle(func(ctx context.Context, o Order) error {
    return process(ctx, o)
}))
defer pool.Close(context.Background())
```

---

## Why another queue library?

`hutch` is small on purpose, but it bakes in lessons that are easy to get wrong
and expensive to learn in production:

| Lesson | What hutch does |
| --- | --- |
| **Bounded prefetch is what makes consumers scale.** Without a per-consumer in-flight cap, one replica greedily buffers the whole backlog and new replicas starve — adding pods does nothing. | Every queue gets a prefetch limit, defaulting to its worker count. Work spreads fairly across all replicas. |
| **Be channel-light.** Opening one broker channel per worker exhausts per-connection/per-user channel limits the instant you scale out. | A pool of N workers shares **one** channel/consumer. Channel usage stays flat as you add workers or replicas. |
| **Isolate publishing from consuming.** A consumer-side storm shouldn't be able to take down the producers your request path depends on. | The RabbitMQ driver publishes over a **separate connection** from consuming. |
| **Fail predictably.** Aggressive retry-to-DLQ floods; never-acking leaks unacked messages forever. | Explicit [`Drop`](#error-policies) / `Requeue` / `Reject` policy. The default (`Drop`) never piles up unacked or floods a DLQ. |
| **Drain on shutdown.** | `Pool.Close(ctx)` stops taking new work and waits for in-flight handlers, bounded by your context. |
| **Reconnect transparently.** | Drivers re-dial with exponential backoff + jitter and re-attach the delivery stream; the message channel survives reconnects. |

These come from running a high-throughput notification consumer across many
replicas — including an incident where a per-worker-channel design exhausted the
broker's channel quota and took down publishing. `hutch` is the design that
replaced it.

---

## Install

```sh
go get github.com/akhiljns/hutch
```

Drivers live in subpackages so you only pull the client you use:

- `github.com/akhiljns/hutch/connectors/rabbitmq` — RabbitMQ (AMQP 0-9-1)
- `github.com/akhiljns/hutch/connectors/redis` — Redis Streams + consumer groups
- `github.com/akhiljns/hutch/connectors/memory` — in-process, zero-dependency (tests/local dev)

---

## Architecture

```
        your handler
             │
        ┌────▼─────┐     Options: Workers, Prefetch, HandlerTimeout, ErrorPolicy
        │  Pool    │     • fan-out to N workers   • bounded in-flight (prefetch)
        │ (engine) │     • ack/nack policy        • graceful drain
        └────┬─────┘
   Subscriber │ Producer        ← tiny broker-agnostic interfaces
        ┌─────▼──────┐
        │   driver   │   rabbitmq · redis · memory · (kafka, nats, …)
        └─────┬──────┘
          the broker
```

The **engine** (`hutch`) is pure Go with no broker dependencies. Each **driver**
implements `Subscriber` and/or `Producer` and owns the broker-specific bits
(connections, prefetch/QoS, reconnection, ack semantics).

```go
type Message interface {
    Body() []byte
    Ack() error
    Nack(requeue bool) error
}

type Subscriber interface {
    Subscribe(ctx context.Context, queue string, prefetch int) (<-chan Message, error)
    Close() error
}

type Producer interface {
    Publish(ctx context.Context, queue string, body []byte) error
    Close() error
}
```

---

## Usage

### Consume

```go
sub, err := rabbitmq.NewSubscriber(url, rabbitmq.WithLogger(log.Default()))
if err != nil { log.Fatal(err) }

pool := hutch.NewPool(sub,
    hutch.WithWorkers(20),          // default workers per queue
    hutch.WithLogger(log.Default()),
)

// Typed handler: JSON body decoded into Order automatically.
pool.Handle("orders", hutch.Handle(func(ctx context.Context, o Order) error {
    return process(ctx, o)
}), hutch.OnError(hutch.Reject))

// Raw handler: full control over the message.
pool.Handle("audit", func(ctx context.Context, m hutch.Message) error {
    return store(ctx, m.Body())
}, hutch.Workers(4), hutch.Prefetch(8))

// On shutdown: stop consuming and drain in-flight, bounded by ctx.
ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()
pool.Close(ctx)
```

### Publish

```go
prod, _ := rabbitmq.NewProducer(url)   // separate connection from the consumer
pub := hutch.NewPublisher(prod)

pub.PublishJSON(ctx, "orders", Order{ID: 1, Item: "widget"})
pub.Publish(ctx, "audit", []byte("..."))
defer pub.Close()
```

### No broker? Use the in-memory driver

```go
broker := memory.New()              // implements both Subscriber and Producer
pub  := hutch.NewPublisher(broker)
pool := hutch.NewPool(broker)
```

### Redis

The Redis driver is a drop-in swap — same pool, same handlers, just a different
subscriber/producer:

```go
sub, _ := redis.NewSubscriber("redis://localhost:6379/0")
prod, _ := redis.NewProducer("redis://localhost:6379/0")  // separate client
pool := hutch.NewPool(sub, hutch.WithWorkers(20))
```

It's built on **Redis Streams + consumer groups**, the only Redis primitive that
gives per-message acks and redelivery (lists and pub/sub don't):

- `XADD` publishes; `XREADGROUP` load-balances entries across every replica in
  the group, so adding pods adds throughput.
- `prefetch` is honored by reading at most as many entries as there are free
  in-flight slots — no single replica hoards the backlog.
- `Ack`/`Reject` remove the entry (`XACK`+`XDEL`); `Requeue` re-adds it at the
  tail for redelivery.
- Entries left pending by a crashed consumer are reclaimed (`XAUTOCLAIM`) once
  idle and redelivered — the Redis analogue of RabbitMQ redelivering unacked
  messages when a channel drops.

The group starts at `0`, so a consumer picks up the whole existing backlog, not
just messages published after it connects.

Runnable examples: [`examples/memory`](./examples/memory) (no broker needed),
[`examples/rabbitmq`](./examples/rabbitmq), and [`examples/redis`](./examples/redis).

---

## Configuration

Pool-wide defaults (`hutch.With*`) are inherited by every queue and can be
overridden per `Handle` call (`hutch.Workers`, `hutch.Prefetch`, …):

| Option | Default | Meaning |
| --- | --- | --- |
| `WithWorkers(n)` / `Workers(n)` | 10 | worker goroutines per queue |
| `WithPrefetch(n)` / `Prefetch(n)` | = workers | max in-flight messages per queue (per replica) |
| `WithHandlerTimeout(d)` / `HandlerTimeout(d)` | 30s | per-message timeout (0 disables) |
| `WithErrorPolicy(p)` / `OnError(p)` | `Drop` | what to do when a handler errors |
| `OnErrorFunc(fn)` | — | callback for every failed message (metrics, capture) |
| `WithLogger(l)` | no-op | anything with `Printf`, e.g. `log.Default()` |

RabbitMQ driver options: `WithBackoff`, `WithLogger`, `WithPublisherChannels`
(default 3), `WithDeclareQueues` (default true).

Redis driver options: `WithGroup` (consumer group, default `"hutch"`),
`WithConsumerName` (default `<hostname>-<pid>`), `WithClaimMinIdle` (reclaim
entries idle longer than this from crashed consumers, default 30s; set above your
handler timeout), `WithBlock` (read block, default 5s), `WithMaxLen` (approximate
stream cap, default unbounded), `WithBackoff`, `WithLogger`.

### Error policies

| Policy | Behavior | Use when |
| --- | --- | --- |
| `Drop` (default) | ack the failed message | "process or move on"; pair with `OnErrorFunc` to capture |
| `Requeue` | nack + redeliver | failures are transient (⚠️ add your own attempt cap — a poison message loops forever) |
| `Reject` | nack, no redeliver | you have a dead-letter destination configured on the queue |

---

## Writing a driver (Kafka, Redis, NATS, …)

Implement `Subscriber` and/or `Producer`. The engine handles workers, prefetch
fan-out, ack policy, timeouts, and draining — your driver only deals with the
broker. Two responsibilities matter:

1. **Honor `prefetch`.** Keep at most `prefetch` messages outstanding per
   `Subscribe`. On RabbitMQ that's `basic.qos`; on Redis it's how many you pull
   before acking; on Kafka it maps to in-flight/`max.poll.records`.
2. **Hide reconnection.** Keep the returned channel open across transient
   broker disruptions; close it only when `ctx` is cancelled or `Close` is
   called.

Sketch:

```go
type Driver struct { /* client, config */ }

func (d *Driver) Subscribe(ctx context.Context, queue string, prefetch int) (<-chan hutch.Message, error) {
    out := make(chan hutch.Message)
    go d.run(ctx, queue, prefetch, out) // pull, wrap as hutch.Message, forward; reconnect on loss
    return out, nil
}

func (d *Driver) Publish(ctx context.Context, queue string, body []byte) error { /* ... */ }
func (d *Driver) Close() error { /* ... */ }
```

See [`memory`](./memory) for a complete ~150-line reference driver,
[`rabbitmq`](./rabbitmq) for a production-grade AMQP one (QoS, channel-light
forwarding, reconnect, isolated publisher pool), and [`redis`](./redis) for a
streams-based one (consumer groups, prefetch via batched reads, idle-entry
reclaim).

> Kafka and NATS drivers aren't bundled yet — they pull heavy clients and need a
> live broker to test honestly. The interface above is all it takes; PRs welcome.

---

## Status & caveats

- Concurrent acks from multiple workers share one channel; the RabbitMQ client
  serializes channel writes internally, so this is safe (and is what keeps
  channel counts flat). Per-message ordering is **not** guaranteed across
  workers — use a single worker per queue if you need it.
- `Drop` is lossy by design. Use `OnErrorFunc` to record what you drop, or
  switch to `Reject` with a dead-letter queue.
- Tested with `go test ./...`: the engine against the in-memory driver, and the
  Redis driver against an in-process Redis ([miniredis](https://github.com/alicebob/miniredis)).
  The RabbitMQ driver is exercised via its example against a real broker.

## License

[MIT](./LICENSE)
