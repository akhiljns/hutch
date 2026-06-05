package redis_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhiljns/hutch"
	hredis "github.com/akhiljns/hutch/connectors/redis"

	"github.com/alicebob/miniredis/v2"
)

// newBroker starts an in-process miniredis and returns its URL.
func newBroker(t *testing.T) string {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return "redis://" + mr.Addr()
}

func TestProcessesAll(t *testing.T) {
	url := newBroker(t)

	prod, err := hredis.NewProducer(url)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()
	pub := hutch.NewPublisher(prod)

	const n = 50
	for i := 0; i < n; i++ {
		if err := pub.PublishJSON(context.Background(), "jobs", i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	sub, err := hredis.NewSubscriber(url, hredis.WithBlock(50*time.Millisecond))
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	defer sub.Close()

	var got int64
	done := make(chan struct{})
	pool := hutch.NewPool(sub, hutch.WithWorkers(8))
	if err := pool.Handle("jobs", func(_ context.Context, _ hutch.Message) error {
		if atomic.AddInt64(&got, 1) == n {
			close(done)
		}
		return nil
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out; processed %d of %d", atomic.LoadInt64(&got), n)
	}
	_ = pool.Close(context.Background())
}

func TestRequeueRedelivers(t *testing.T) {
	url := newBroker(t)

	prod, _ := hredis.NewProducer(url)
	defer prod.Close()
	_ = hutch.NewPublisher(prod).Publish(context.Background(), "jobs", []byte("x"))

	sub, err := hredis.NewSubscriber(url, hredis.WithBlock(50*time.Millisecond))
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	defer sub.Close()

	var attempts int64
	done := make(chan struct{})
	pool := hutch.NewPool(sub, hutch.WithWorkers(1))
	_ = pool.Handle("jobs", func(_ context.Context, _ hutch.Message) error {
		if atomic.AddInt64(&attempts, 1) == 1 {
			return errors.New("transient") // Requeue -> re-added at tail -> redelivered
		}
		close(done)
		return nil
	}, hutch.OnError(hutch.Requeue))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message was not redelivered after Requeue")
	}
	_ = pool.Close(context.Background())
}

func TestDropDoesNotRedeliver(t *testing.T) {
	url := newBroker(t)

	prod, _ := hredis.NewProducer(url)
	defer prod.Close()
	_ = hutch.NewPublisher(prod).Publish(context.Background(), "jobs", []byte("x"))

	sub, err := hredis.NewSubscriber(url, hredis.WithBlock(50*time.Millisecond))
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	defer sub.Close()

	var attempts, captured int64
	pool := hutch.NewPool(sub, hutch.WithWorkers(1))
	_ = pool.Handle("jobs", func(_ context.Context, _ hutch.Message) error {
		atomic.AddInt64(&attempts, 1)
		return errors.New("boom")
	}, // default policy is Drop (ack)
		hutch.OnErrorFunc(func(hutch.Message, error) { atomic.AddInt64(&captured, 1) }),
	)

	time.Sleep(500 * time.Millisecond)
	_ = pool.Close(context.Background())

	if a := atomic.LoadInt64(&attempts); a != 1 {
		t.Fatalf("attempts = %d, want 1 (Drop must not redeliver)", a)
	}
	if c := atomic.LoadInt64(&captured); c != 1 {
		t.Fatalf("OnErrorFunc called %d times, want 1", c)
	}
}
