// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// newTestEventManager builds an eventManager whose member snapshots come
// from an in-memory store, so the snapshot/sequence consistency logic can
// be driven deterministically without a running Serf node.
func newTestEventManager() (*eventManager, *[]Member) {
	var mu sync.Mutex
	members := []Member{}
	mgr := newEventManager(func() []Member {
		mu.Lock()
		defer mu.Unlock()
		out := make([]Member, len(members))
		copy(out, members)
		return out
	})
	mgr.testMembers = &members
	mgr.testMu = &mu
	return mgr, &members
}

// addMember mutates the snapshot store the way the Serf member lock would
// protect in production, then publishes the matching member event.
func addMember(mgr *eventManager, members *[]Member, name string, status MemberStatus) {
	mgr.testMu.Lock()
	*members = append(*members, Member{Name: name, Status: status})
	mgr.testMu.Unlock()
	mgr.publishMember(MemberEvent{
		Type:    EventMemberJoin,
		Members: []Member{{Name: name, Status: status}},
	})
}

func makeUserEvent(name string) UserEvent {
	return UserEvent{Name: name}
}

// mustNext calls NextBatch and fails the test on error.
func mustNext(t *testing.T, sub *Subscription) EventBatch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	batch, err := sub.NextBatch(ctx)
	if err != nil {
		t.Fatalf("NextBatch failed: %v", err)
	}
	return batch
}

// TestSubscribe_InitialMemberSnapshot verifies requirement 1: every
// subscription begins with a consistent member snapshot labeled with the
// current sequence before it sees any incremental event.
func TestSubscribe_InitialMemberSnapshot(t *testing.T) {
	mgr, members := newTestEventManager()

	// Some state exists before the subscription opens.
	addMember(mgr, members, "alice", StatusAlive)
	addMember(mgr, members, "bob", StatusAlive)

	sub := mgr.subscribe(SubscribeConfig{Buffer: 8})
	defer sub.Unsubscribe()

	batch := mustNext(t, sub)
	if batch.Stream != StreamMember || batch.Snapshot == nil {
		t.Fatalf("expected initial member snapshot, got %+v", batch)
	}
	if len(batch.Snapshot) != 2 {
		t.Fatalf("expected 2 members in snapshot, got %d", len(batch.Snapshot))
	}
	if batch.Seq != 2 {
		t.Fatalf("snapshot should be labeled with member seq 2, got %d", batch.Seq)
	}
	if err := sub.Ack(batch); err != nil {
		t.Fatalf("ack snapshot: %v", err)
	}

	// An event published after the snapshot is delivered incrementally and
	// has a sequence strictly greater than the snapshot label.
	addMember(mgr, members, "carol", StatusAlive)
	batch = mustNext(t, sub)
	if batch.Snapshot != nil {
		t.Fatalf("expected incremental event, got another snapshot")
	}
	if batch.Stream != StreamMember || len(batch.Events) != 1 {
		t.Fatalf("expected one member event, got %+v", batch)
	}
	if batch.Seq != 3 {
		t.Fatalf("incremental event seq should be 3, got %d", batch.Seq)
	}
}

// TestSubscribe_MemberOrdering verifies member events are delivered in the
// order they were published.
func TestSubscribe_MemberOrdering(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 16})
	defer sub.Unsubscribe()

	snap := mustNext(t, sub)
	if err := sub.Ack(snap); err != nil {
		t.Fatalf("ack: %v", err)
	}

	const n = 10
	for i := range n {
		addMember(mgr, members, fmt.Sprintf("node-%d", i), StatusAlive)
	}

	// Several events may be coalesced into one batch, so keep draining
	// until all ten have been observed, preserving delivery order.
	var got []string
	for len(got) < n {
		batch := mustNext(t, sub)
		if batch.Stream != StreamMember || batch.Snapshot != nil {
			t.Fatalf("unexpected batch: %+v", batch)
		}
		for _, e := range batch.Events {
			me := e.(MemberEvent)
			got = append(got, me.Members[0].Name)
		}
		if err := sub.Ack(batch); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	want := make([]string, n)
	for i := range n {
		want[i] = fmt.Sprintf("node-%d", i)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("member order mismatch\nwant: %v\ngot:  %v", want, got)
	}
}

// TestSubscribe_UserOrderingAndIndependentStreams verifies requirement 4:
// user events keep their own order and are delivered on a separate stream
// rather than merged with member events.
func TestSubscribe_UserOrderingAndIndependentStreams(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 16})
	defer sub.Unsubscribe()

	// Drain the initial snapshot so member events become available.
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Interleave publishes on both streams.
	mgr.publishUser(makeUserEvent("u1"))
	addMember(mgr, members, "alice", StatusAlive)
	mgr.publishUser(makeUserEvent("u2"))
	mgr.publishUser(makeUserEvent("u3"))
	addMember(mgr, members, "bob", StatusAlive)
	mgr.publishUser(makeUserEvent("u4"))

	var memberNames, userNames []string
	for len(memberNames) < 2 || len(userNames) < 4 {
		batch := mustNext(t, sub)
		switch batch.Stream {
		case StreamMember:
			if len(batch.Snapshot) != 0 || batch.Truncated {
				t.Fatalf("unexpected non-event member batch: %+v", batch)
			}
			for _, e := range batch.Events {
				memberNames = append(memberNames, e.(MemberEvent).Members[0].Name)
			}
		case StreamUser:
			if batch.Truncated {
				t.Fatalf("did not expect truncation")
			}
			for _, e := range batch.Events {
				userNames = append(userNames, e.(UserEvent).Name)
			}
		}
		if err := sub.Ack(batch); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	if fmt.Sprint(memberNames) != fmt.Sprint([]string{"alice", "bob"}) {
		t.Fatalf("member stream order wrong: %v", memberNames)
	}
	if fmt.Sprint(userNames) != fmt.Sprint([]string{"u1", "u2", "u3", "u4"}) {
		t.Fatalf("user stream order wrong: %v", userNames)
	}
}

// TestSubscribe_RetrySameBatch verifies requirement 2: a failed handler
// does not advance the position and the exact same batch is redelivered,
// including when newer events arrive during the retries.
func TestSubscribe_RetrySameBatch(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 32})
	defer sub.Unsubscribe()
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack snapshot: %v", err)
	}

	addMember(mgr, members, "a", StatusAlive)
	addMember(mgr, members, "b", StatusAlive)

	first := mustNext(t, sub)
	if len(first.Events) != 2 || first.Seq != 2 {
		t.Fatalf("unexpected first batch: %+v", first)
	}

	// Newer events published while the first batch is still unacknowledged
	// must not be folded into its redelivery.
	addMember(mgr, members, "c", StatusAlive)
	addMember(mgr, members, "d", StatusAlive)

	// Redelivery returns the identical frozen batch.
	second := mustNext(t, sub)
	if second.Seq != first.Seq || len(second.Events) != 2 {
		t.Fatalf("redelivery changed the batch:\nfirst:  %+v\nsecond: %+v", first, second)
	}
	for i := range first.Events {
		if !reflect.DeepEqual(first.Events[i], second.Events[i]) {
			t.Fatalf("redelivered event %d differs", i)
		}
	}

	if err := sub.Ack(second); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// The events held back during retries form the next batch.
	third := mustNext(t, sub)
	if len(third.Events) != 2 || third.Seq != 4 {
		t.Fatalf("expected held-back batch c,d got %+v", third)
	}
}

// TestSubscribe_StartHandlerRetry drives the built-in Start loop and
// confirms an erroring handler eventually succeeds on the same batch.
func TestSubscribe_StartHandlerRetry(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{
		Buffer:        8,
		RetryInterval: time.Millisecond,
	})
	defer sub.Unsubscribe()

	var attempts int
	var mu sync.Mutex
	sub.onMember = func(batch EventBatch) error {
		mu.Lock()
		defer mu.Unlock()
		if batch.Snapshot != nil {
			return nil // accept snapshots immediately
		}
		attempts++
		if attempts < 3 {
			return errors.New("transient failure")
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = sub.Start(ctx) }()

	// Wait for the initial snapshot to be consumed.
	waitUntil(t, func() bool {
		mgr.mu.Lock()
		done := !sub.member.needsSnapshot
		mgr.mu.Unlock()
		return done
	}, "snapshot consumed")

	addMember(mgr, members, "alice", StatusAlive)

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 3
	}, "handler retried until success")

	mgr.mu.Lock()
	remaining := len(sub.member.buf.events)
	mgr.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("buffer should be drained after success, has %d events", remaining)
	}
}

// TestSubscribe_MemberTruncationRebuildsSnapshot verifies requirement 3:
// when the member buffer fills, the subscription gets an explicit
// truncation followed by a fresh consistent snapshot covering the gap.
func TestSubscribe_MemberTruncationRebuildsSnapshot(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 4})
	defer sub.Unsubscribe()
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack initial snapshot: %v", err)
	}

	// Fill the buffer without consuming, then overflow it.
	for i := range 6 {
		addMember(mgr, members, fmt.Sprintf("n%d", i), StatusAlive)
	}

	batch := mustNext(t, sub)
	if batch.Stream != StreamMember || batch.Snapshot == nil {
		t.Fatalf("expected truncation-driven snapshot, got %+v", batch)
	}
	if !batch.Truncated {
		t.Fatalf("rebuilt snapshot must explicitly report truncation")
	}
	// The rebuilt snapshot reflects every member published so far.
	if len(batch.Snapshot) != 6 {
		t.Fatalf("snapshot should contain 6 members, got %d", len(batch.Snapshot))
	}
	if batch.Seq != mgr.memberSeq {
		t.Fatalf("snapshot seq %d should equal current member seq %d", batch.Seq, mgr.memberSeq)
	}
	if err := sub.Ack(batch); err != nil {
		t.Fatalf("ack rebuilt snapshot: %v", err)
	}

	// Events after the resynchronization are delivered normally and line
	// up exactly after the snapshot (no gap, no overlap).
	addMember(mgr, members, "after", StatusAlive)
	next := mustNext(t, sub)
	if next.Snapshot != nil || next.Seq != batch.Seq+1 {
		t.Fatalf("expected event seq %d after snapshot, got %+v", batch.Seq+1, next)
	}
}

// TestSubscribe_MemberTruncationSnapshotRetryable verifies a truncation-driven
// snapshot keeps being redelivered, with its truncation marker intact, until
// it is explicitly acknowledged.
func TestSubscribe_MemberTruncationSnapshotRetryable(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 2})
	defer sub.Unsubscribe()
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack initial snapshot: %v", err)
	}

	for i := range 4 {
		addMember(mgr, members, fmt.Sprintf("n%d", i), StatusAlive)
	}

	first := mustNext(t, sub)
	if first.Snapshot == nil || !first.Truncated {
		t.Fatalf("expected truncation snapshot, got %+v", first)
	}

	// Without an Ack the identical resynchronization point is redelivered.
	second := mustNext(t, sub)
	if second.Snapshot == nil || !second.Truncated {
		t.Fatalf("redelivery must remain a truncation snapshot, got %+v", second)
	}

	if err := sub.Ack(second); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Once acknowledged, no further snapshot is produced without a new
	// truncation; the stream is idle until another event arrives.
	addMember(mgr, members, "fresh", StatusAlive)
	next := mustNext(t, sub)
	if next.Snapshot != nil || next.Truncated {
		t.Fatalf("expected plain event after acknowledged snapshot, got %+v", next)
	}
}

// TestSubscribe_UserTruncationExplicit verifies the user stream signals an
// explicit, retryable truncation batch when its buffer overflows.
func TestSubscribe_UserTruncationExplicit(t *testing.T) {
	mgr, _ := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 3})
	defer sub.Unsubscribe()
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack initial snapshot: %v", err)
	}

	for i := range 5 {
		mgr.publishUser(makeUserEvent(fmt.Sprintf("e%d", i)))
	}

	batch := mustNext(t, sub)
	if batch.Stream != StreamUser || !batch.Truncated {
		t.Fatalf("expected user truncation batch, got %+v", batch)
	}

	// The truncation batch itself is retryable until acknowledged.
	redelivered := mustNext(t, sub)
	if !redelivered.Truncated || redelivered.Seq != batch.Seq {
		t.Fatalf("truncation must redeliver unchanged, got %+v", redelivered)
	}
	if err := sub.Ack(redelivered); err != nil {
		t.Fatalf("ack truncation: %v", err)
	}

	// Events published after the truncation point resume delivery.
	mgr.publishUser(makeUserEvent("post-truncation"))
	next := mustNext(t, sub)
	if next.Truncated || len(next.Events) != 1 {
		t.Fatalf("expected a fresh user event, got %+v", next)
	}
	if next.Events[0].(UserEvent).Name != "post-truncation" {
		t.Fatalf("unexpected event after truncation: %+v", next.Events[0])
	}
}

// TestSubscribe_UnsubscribeReleasesBuffers verifies requirement 5:
// unsubscribing frees the buffers, removes the subscription and unblocks
// waiting consumers.
func TestSubscribe_UnsubscribeReleasesBuffers(t *testing.T) {
	mgr, members := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 8})

	addMember(mgr, members, "alice", StatusAlive)
	sub.Unsubscribe()

	// Idempotent.
	sub.Unsubscribe()

	select {
	case <-sub.Done():
	case <-time.After(time.Second):
		t.Fatalf("Done channel not closed after unsubscribe")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := sub.NextBatch(ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("expected ErrSubscriptionClosed, got %v", err)
	}
	if err := sub.Ack(EventBatch{}); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("expected Ack to fail with ErrSubscriptionClosed, got %v", err)
	}

	mgr.mu.Lock()
	_, registered := mgr.subscriptions[sub]
	bufFreed := sub.member.buf.events == nil && sub.user.buf.events == nil
	mgr.mu.Unlock()
	if registered {
		t.Fatalf("subscription still registered with manager")
	}
	if !bufFreed {
		t.Fatalf("buffers not released")
	}
}

// TestSubscribe_ManagerCloseUnblocks verifies shutting the manager down
// (as Serf.Shutdown does) unblocks every waiting subscriber.
func TestSubscribe_ManagerCloseUnblocks(t *testing.T) {
	mgr, _ := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 8})
	defer sub.Unsubscribe()

	// Clear the immediately-available initial snapshot so the next call
	// genuinely blocks waiting for an event.
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack snapshot: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := sub.NextBatch(context.Background())
		result <- err
	}()

	// Give the goroutine time to park, then close the manager.
	time.Sleep(20 * time.Millisecond)
	mgr.close()

	select {
	case err := <-result:
		if !errors.Is(err, ErrSubscriptionClosed) {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("waiting subscriber was not unblocked by close")
	}
}

// TestSubscribe_IndependentSubscriptions confirms a slow consumer only
// truncates its own view and never affects a second subscription or the
// producing side.
func TestSubscribe_IndependentSubscriptions(t *testing.T) {
	mgr, members := newTestEventManager()
	slow := mgr.subscribe(SubscribeConfig{Buffer: 2})
	fast := mgr.subscribe(SubscribeConfig{Buffer: 16})
	defer slow.Unsubscribe()
	defer fast.Unsubscribe()

	// Drain both initial snapshots.
	if err := fast.Ack(mustNext(t, fast)); err != nil {
		t.Fatalf("fast ack: %v", err)
	}
	if err := slow.Ack(mustNext(t, slow)); err != nil {
		t.Fatalf("slow ack: %v", err)
	}

	// Overflow the slow subscription's tiny buffer while the fast one
	// keeps up.
	for i := range 6 {
		addMember(mgr, members, fmt.Sprintf("n%d", i), StatusAlive)
	}

	// Fast subscriber receives a clean ordered stream with no truncation.
	seen := 0
	for seen < 6 {
		batch := mustNext(t, fast)
		if batch.Snapshot != nil {
			t.Fatalf("fast subscriber should never be truncated")
		}
		seen += len(batch.Events)
		if err := fast.Ack(batch); err != nil {
			t.Fatalf("fast ack: %v", err)
		}
	}
	if seen != 6 {
		t.Fatalf("fast subscriber saw %d events, want 6", seen)
	}

	// Slow subscriber is resynchronized through a snapshot instead.
	batch := mustNext(t, slow)
	if batch.Snapshot == nil {
		t.Fatalf("slow subscriber expected truncation snapshot, got %+v", batch)
	}
}

// TestSubscribe_ContextCancel confirms NextBatch returns promptly when the
// caller context is canceled while no events are available.
func TestSubscribe_ContextCancel(t *testing.T) {
	mgr, _ := newTestEventManager()
	sub := mgr.subscribe(SubscribeConfig{Buffer: 8})
	defer sub.Unsubscribe()
	if err := sub.Ack(mustNext(t, sub)); err != nil {
		t.Fatalf("ack: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := sub.NextBatch(ctx)
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("NextBatch did not honor context cancellation")
	}
}

// waitUntil polls condition until it holds or the test times out.
func waitUntil(t *testing.T, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
