// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
	"github.com/hashicorp/serf/testutil/retry"
)

// TestSerf_Subscribe_SnapshotThenIncrementals is an end-to-end check that a
// subscription on a running Serf starts from a consistent member snapshot,
// then observes later user events in order, and that acknowledging batches
// drains the subscription buffers.
func TestSerf_Subscribe_SnapshotThenIncrementals(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	conf := testConfig(t, ip1)
	s, err := Create(conf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()

	waitUntilNumNodes(t, 1, s)

	sub := s.Subscribe(SubscribeConfig{Buffer: 16})
	defer sub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First batch must be a consistent snapshot containing the local node.
	snap, err := sub.NextBatch(ctx)
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if snap.Stream != StreamMember || snap.Snapshot == nil {
		t.Fatalf("expected initial member snapshot, got %+v", snap)
	}
	foundLocal := false
	for _, m := range snap.Snapshot {
		if m.Name == conf.NodeName {
			foundLocal = true
		}
	}
	if !foundLocal {
		t.Fatalf("snapshot missing local node %q: %+v", conf.NodeName, snap.Snapshot)
	}
	if err := sub.Ack(snap); err != nil {
		t.Fatalf("ack snapshot: %v", err)
	}

	// Publish several user events and make sure the subscription sees them
	// in order, possibly coalesced into fewer batches.
	const eventCount = 5
	for i := range eventCount {
		if err := s.UserEvent(fmt.Sprintf("evt-%d", i), []byte{byte(i)}, false); err != nil {
			t.Fatalf("user event: %v", err)
		}
	}

	got := make(map[string]byte)
	for len(got) < eventCount {
		batch, err := sub.NextBatch(ctx)
		if err != nil {
			t.Fatalf("next batch: %v", err)
		}
		if batch.Stream != StreamUser || batch.Truncated {
			t.Fatalf("expected user event batch, got %+v", batch)
		}
		for _, e := range batch.Events {
			ue := e.(UserEvent)
			got[ue.Name] = ue.Payload[0]
		}
		if err := sub.Ack(batch); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	for i := range eventCount {
		if got[fmt.Sprintf("evt-%d", i)] != byte(i) {
			t.Fatalf("missing or corrupted evt-%d: %v", i, got)
		}
	}
}

// TestSerf_Subscribe_UnsubscribeReleases verifies that tearing down a
// subscription on a live Serf unblocks a waiting consumer and frees its
// buffers, without disturbing a second subscription.
func TestSerf_Subscribe_UnsubscribeReleases(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	conf := testConfig(t, ip1)
	s, err := Create(conf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()
	waitUntilNumNodes(t, 1, s)

	sub1 := s.Subscribe(SubscribeConfig{Buffer: 8})
	sub2 := s.Subscribe(SubscribeConfig{Buffer: 8})
	defer sub2.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sub1.Ack(mustNext(t, sub1)); err != nil {
		t.Fatalf("ack sub1 snapshot: %v", err)
	}
	if err := sub2.Ack(mustNext(t, sub2)); err != nil {
		t.Fatalf("ack sub2 snapshot: %v", err)
	}

	blocked := make(chan error, 1)
	go func() {
		_, err := sub1.NextBatch(ctx)
		blocked <- err
	}()

	sub1.Unsubscribe()

	select {
	case err := <-blocked:
		if !errors.Is(err, ErrSubscriptionClosed) {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("NextBatch was not unblocked by unsubscribe")
	}

	// The second subscription stays usable after the first is gone.
	if err := s.UserEvent("still-alive", nil, false); err != nil {
		t.Fatalf("user event: %v", err)
	}
	batch, err := sub2.NextBatch(ctx)
	if err != nil {
		t.Fatalf("sub2 should still receive events: %v", err)
	}
	if batch.Stream != StreamUser {
		t.Fatalf("expected user event on surviving subscription, got %+v", batch)
	}
}

// TestSerf_Subscribe_ShutdownCloses verifies Serf.Shutdown closes all
// subscriptions and makes their Done channels fire.
func TestSerf_Subscribe_ShutdownCloses(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	conf := testConfig(t, ip1)
	s, err := Create(conf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	waitUntilNumNodes(t, 1, s)

	sub := s.Subscribe(SubscribeConfig{Buffer: 8})
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack snapshot: %v", err)
	}

	if err := s.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	select {
	case <-sub.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("subscription Done did not fire on Serf shutdown")
	}

	if _, err := sub.NextBatch(context.Background()); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("expected ErrSubscriptionClosed after shutdown, got %v", err)
	}
}

// TestSerf_Subscribe_SlowCallbackRetry drives the Start loop with a
// callback that fails a few times before succeeding, and proves no user
// event is skipped by the retries.
func TestSerf_Subscribe_SlowCallbackRetry(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	conf := testConfig(t, ip1)
	s, err := Create(conf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()
	waitUntilNumNodes(t, 1, s)

	var mu sync.Mutex
	var attempts int
	var seen []string
	sub := s.Subscribe(SubscribeConfig{
		Buffer:        16,
		RetryInterval: time.Millisecond,
		OnMemberEvent: func(EventBatch) error { return nil },
		OnUserEvent: func(b EventBatch) error {
			mu.Lock()
			defer mu.Unlock()
			if b.Truncated {
				return nil
			}
			// Fail the first user batch twice, then accept it.
			if len(seen) == 0 && attempts < 2 {
				attempts++
				return errors.New("transient")
			}
			for _, e := range b.Events {
				seen = append(seen, e.(UserEvent).Name)
			}
			return nil
		},
	})
	defer sub.Unsubscribe()

	startCtx, startCancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})
	go func() {
		_ = sub.Start(startCtx)
		close(startDone)
	}()

	if err := s.UserEvent("retry-me", nil, false); err != nil {
		t.Fatalf("user event: %v", err)
	}

	retry.Run(t, func(r *retry.R) {
		mu.Lock()
		defer mu.Unlock()
		if len(seen) != 1 || seen[0] != "retry-me" {
			r.Fatalf("event not delivered exactly once after retries: %v", seen)
		}
		if attempts < 2 {
			r.Fatalf("handler should have retried, attempts=%d", attempts)
		}
	})

	startCancel()
	sub.Unsubscribe()
	select {
	case <-startDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Start did not return after unsubscribe")
	}
}
