package hutch_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akhiljns/hutch"
	"github.com/akhiljns/hutch/connectors/memory"
)

func TestPoolProcessesAll(t *testing.T) {
	broker := memory.New()
	defer broker.Close()

	pub := hutch.NewPublisher(broker)
	const n = 200
	for i := 0; i < n; i++ {
		if err := pub.PublishJSON(context.Background(), "jobs", i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	var got int64
	done := make(chan struct{})
	pool := hutch.NewPool(broker, hutch.WithWorkers(8))
	err := pool.Handle("jobs", func(_ context.Context, _ hutch.Message) error {
		if atomic.AddInt64(&got, 1) == n {
			close(done)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out; processed %d of %d", atomic.LoadInt64(&got), n)
	}
	_ = pool.Close(context.Background())
}

func TestRequeueRedelivers(t *testing.T) {
	broker := memory.New()
	defer broker.Close()

	_ = hutch.NewPublisher(broker).Publish(context.Background(), "jobs", []byte("x"))

	var attempts int64
	done := make(chan struct{})
	pool := hutch.NewPool(broker, hutch.WithWorkers(1))
	_ = pool.Handle("jobs", func(_ context.Context, _ hutch.Message) error {
		if atomic.AddInt64(&attempts, 1) == 1 {
			return errors.New("transient") // requeued -> redelivered
		}
		close(done)
		return nil
	}, hutch.OnError(hutch.Requeue))

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("message was not redelivered after Requeue")
	}
	_ = pool.Close(context.Background())
}

func TestDropDoesNotRedeliver(t *testing.T) {
	broker := memory.New()
	defer broker.Close()

	_ = hutch.NewPublisher(broker).Publish(context.Background(), "jobs", []byte("x"))

	var attempts, captured int64
	pool := hutch.NewPool(broker, hutch.WithWorkers(1))
	_ = pool.Handle("jobs", func(_ context.Context, _ hutch.Message) error {
		atomic.AddInt64(&attempts, 1)
		return errors.New("boom")
	}, // default policy is Drop
		hutch.OnErrorFunc(func(hutch.Message, error) { atomic.AddInt64(&captured, 1) }),
	)

	time.Sleep(250 * time.Millisecond)
	_ = pool.Close(context.Background())

	if a := atomic.LoadInt64(&attempts); a != 1 {
		t.Fatalf("attempts = %d, want 1 (Drop must not redeliver)", a)
	}
	if c := atomic.LoadInt64(&captured); c != 1 {
		t.Fatalf("OnErrorFunc called %d times, want 1", c)
	}
}

func TestTypedHandlerDecodeError(t *testing.T) {
	broker := memory.New()
	defer broker.Close()

	// invalid JSON for an int -> Handle returns a decode error -> Drop (default)
	_ = hutch.NewPublisher(broker).Publish(context.Background(), "jobs", []byte("not-json"))

	var decodeErr int64
	pool := hutch.NewPool(broker, hutch.WithWorkers(1))
	_ = pool.Handle("jobs", hutch.Handle(func(_ context.Context, _ int) error {
		t.Error("handler should not run on undecodable payload")
		return nil
	}), hutch.OnErrorFunc(func(_ hutch.Message, _ error) { atomic.AddInt64(&decodeErr, 1) }))

	time.Sleep(250 * time.Millisecond)
	_ = pool.Close(context.Background())

	if atomic.LoadInt64(&decodeErr) != 1 {
		t.Fatalf("expected 1 decode error, got %d", decodeErr)
	}
}
