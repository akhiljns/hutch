// Command redis-example wires a hutch worker pool and publisher to Redis Streams
// with isolated clients and graceful shutdown. It needs a running Redis:
//
//	REDIS_URL=redis://localhost:6379/0 go run ./examples/redis
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akhiljns/hutch"
	hredis "github.com/akhiljns/hutch/connectors/redis"
)

type Order struct {
	ID   int    `json:"id"`
	Item string `json:"item"`
}

func main() {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}

	// Separate clients for consuming and publishing == isolation.
	sub, err := hredis.NewSubscriber(url, hredis.WithLogger(log.Default()))
	if err != nil {
		log.Fatalf("subscriber: %v", err)
	}
	prod, err := hredis.NewProducer(url, hredis.WithLogger(log.Default()))
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	pub := hutch.NewPublisher(prod)

	pool := hutch.NewPool(sub,
		hutch.WithLogger(log.Default()),
		hutch.WithWorkers(20), // prefetch defaults to 20 to match
	)
	if err := pool.Handle("orders", hutch.Handle(func(_ context.Context, o Order) error {
		log.Printf("processing order %d (%s)", o.ID, o.Item)
		return nil
	}), hutch.OnError(hutch.Requeue)); err != nil {
		log.Fatalf("handle: %v", err)
	}

	for i := 1; i <= 5; i++ {
		if err := pub.PublishJSON(context.Background(), "orders", Order{ID: i, Item: "widget"}); err != nil {
			log.Printf("publish: %v", err)
		}
	}

	// Run until SIGINT/SIGTERM, then drain.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	log.Println("shutting down...")
	_ = pool.Close(ctx) // stop consuming, drain in-flight
	_ = pub.Close()
}
