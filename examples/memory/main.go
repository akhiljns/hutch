// Command memory-example runs a hutch worker pool against the in-memory driver,
// so it needs no broker. Run it with:
//
//	go run ./examples/memory
package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/akhiljns/hutch"
	"github.com/akhiljns/hutch/connectors/memory"
)

type Order struct {
	ID   int    `json:"id"`
	Item string `json:"item"`
}

func main() {
	broker := memory.New()

	pub := hutch.NewPublisher(broker)
	pool := hutch.NewPool(broker,
		hutch.WithLogger(log.Default()),
		hutch.WithWorkers(4),
	)

	var processed int64
	if err := pool.Handle("orders", hutch.Handle(func(_ context.Context, o Order) error {
		log.Printf("processing order %d (%s)", o.ID, o.Item)
		atomic.AddInt64(&processed, 1)
		return nil
	})); err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 10; i++ {
		if err := pub.PublishJSON(context.Background(), "orders", Order{ID: i, Item: "widget"}); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pool.Close(ctx)
	_ = pub.Close()

	log.Printf("done: processed %d orders", atomic.LoadInt64(&processed))
}
