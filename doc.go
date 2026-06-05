// Package hutch is a broker-agnostic worker-pool for queue consumers in Go.
//
// It separates the orchestration that every queue consumer needs — a pool of
// workers, a bounded number of in-flight messages, a configurable
// success/failure (ack/nack) policy, per-message timeouts, and graceful
// draining on shutdown — from the broker-specific transport. Brokers plug in
// behind two small interfaces ([Subscriber] and [Producer]); ship with a
// RabbitMQ driver and an in-memory driver, and add Kafka/Redis/etc. by
// implementing the same interfaces.
//
// # Why it exists
//
// hutch encodes a set of hard-won production lessons:
//
//   - Bounded prefetch is what makes consumers scale horizontally. Without a
//     per-consumer in-flight limit, one replica greedily buffers the whole
//     backlog and newly-added replicas starve. The [Pool] always sets a
//     prefetch (defaulting to the worker count) so work spreads fairly across
//     every replica.
//   - Be channel-light. A pool of N workers shares one broker channel/consumer,
//     not one channel per worker — opening a channel per worker exhausts broker
//     channel limits the moment you scale out.
//   - Isolate publishing from consuming. The RabbitMQ driver publishes over a
//     separate connection so a consumer-side disruption can never starve the
//     producers that your request path depends on.
//   - Fail predictably. Choose [Drop], [Requeue], or [Reject] explicitly; the
//     default ([Drop]) never lets failed messages pile up as unacked or flood a
//     dead-letter queue.
//   - Drain on shutdown. [Pool.Close] stops accepting new work and waits for
//     in-flight handlers to finish, bounded by the context you pass.
//
// # Quick start
//
//	sub, _ := rabbitmq.NewSubscriber("amqp://guest:guest@localhost:5672/")
//	pool := hutch.NewPool(sub, hutch.WithLogger(log.Default()))
//	pool.Handle("orders", hutch.Handle(func(ctx context.Context, o Order) error {
//	    return process(ctx, o)
//	}), hutch.Workers(20))
//	defer pool.Close(context.Background())
package hutch
