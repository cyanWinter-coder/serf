// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrSubscriptionClosed is returned by NextBatch and Ack when the
// subscription has been unsubscribed or its Serf instance has shut down.
var ErrSubscriptionClosed = errors.New("serf: subscription closed")

// EventStream identifies one of the independent, ordered event streams
// that a Subscription consumes. Member events and user events live on
// separate streams so that the order within each stream is preserved
// without the two streams being merged or re-ordered relative to one
// another.
type EventStream int

const (
	// StreamMember carries MemberEvent values (join, leave, failed,
	// update, reap). Every member event is delivered in the order Serf
	// observed it.
	StreamMember EventStream = iota

	// StreamUser carries UserEvent values. User events keep their own
	// order, independent of the member stream.
	StreamUser
)

func (s EventStream) String() string {
	switch s {
	case StreamMember:
		return "member"
	case StreamUser:
		return "user"
	default:
		return "unknown"
	}
}

// defaultEventSubscribeBuffer is the default capacity of the per-stream
// buffer handed to each subscription. It bounds the memory a single slow
// consumer can hold before its stream is truncated.
const defaultEventSubscribeBuffer = 256

// defaultSubscribeRetryInterval is how long the built-in subscription loop
// waits before re-delivering a batch whose handler returned an error.
const defaultSubscribeRetryInterval = 50 * time.Millisecond

// EventBatch is the unit of delivery and acknowledgement for a
// Subscription.
//
// Each batch is one of the following:
//
//   - A member snapshot: Snapshot is non-nil. This is a consistent
//     point-in-time member snapshot, delivered either as the first batch
//     of the member stream or after the member buffer was truncated. The
//     snapshot reflects all member events up through Seq, and subsequent
//     member events have sequences strictly greater than Seq. A snapshot
//     caused by an overflow additionally has Truncated set, so the
//     consumer can tell that intermediate member events were collapsed
//     into the snapshot rather than delivered incrementally.
//   - An event batch: Events contains one or more events. The batch is
//     delivered again, unchanged, until the handler succeeds (callback
//     mode) or Ack is called (manual mode); a failed handler therefore
//     never advances the stream position.
//   - A user truncation batch: Truncated is true and both Snapshot and
//     Events are empty. The user event buffer overflowed before the
//     previous batch was acknowledged, so events are missing. User events
//     cannot be reconstructed; the caller must re-synchronize out of band.
//     The stream resumes with events later than Seq once the truncation
//     batch is acknowledged.
type EventBatch struct {
	// Stream is the stream this batch belongs to.
	Stream EventStream

	// Seq is the sequence number of the last event represented by this
	// batch. For an event batch it is the sequence of the final event in
	// Events; for a snapshot it is the sequence of the last member event
	// incorporated into the snapshot; for a user truncation it is the
	// sequence of the last event that was discarded.
	Seq uint64

	// Events contains the ordered events of an event batch. It is empty
	// for snapshot and truncation batches.
	Events []Event

	// Snapshot, when non-nil, is a consistent point-in-time copy of the
	// cluster members at Seq.
	Snapshot []Member

	// Truncated reports that events on this stream were dropped because
	// the subscription's buffer reached its limit. On the member stream it
	// accompanies a rebuilt Snapshot; on the user stream it is a standalone
	// notification with no recoverable events.
	Truncated bool
}

// SubscribeConfig configures a new Subscription.
type SubscribeConfig struct {
	// Buffer is the maximum number of unacknowledged events buffered per
	// stream for this subscription. When the limit is reached the oldest
	// buffered events are discarded and the stream is truncated. Zero
	// selects the default.
	Buffer int

	// RetryInterval is the delay between redelivery attempts when an
	// event handler returns an error. Zero selects the default.
	RetryInterval time.Duration

	// OnMemberEvent, when set, receives every batch on the member stream,
	// including the initial member snapshot. Returning an error causes the
	// whole batch to be retried after RetryInterval.
	OnMemberEvent func(batch EventBatch) error

	// OnUserEvent, when set, receives every batch on the user stream,
	// including truncation notifications. Returning an error causes the
	// whole batch to be retried after RetryInterval.
	OnUserEvent func(batch EventBatch) error
}

// streamBuffer is the per-subscription, per-stream FIFO of events that
// have been published but not yet acknowledged. All access is serialized
// by the owning eventManager's lock.
type streamBuffer struct {
	events []bufferedEvent
}

// bufferedEvent pairs an event with the global sequence number it was
// published with on its stream.
type bufferedEvent struct {
	seq   uint64
	event Event
}

// Subscription is an at-least-once view over Serf's member and user
// events. Each subscription starts from a consistent member snapshot and
// then receives incremental events. Events are only removed from the
// subscription buffer once the batch covering them has been acknowledged,
// either implicitly by a successful handler or explicitly via Ack.
//
// A Subscription is not safe for concurrent use of NextBatch/Ack: a
// single goroutine should drive it (the goroutine started by Start, or
// the caller's own loop). Multiple independent Subscriptions should be
// created instead.
type Subscription struct {
	manager *eventManager

	retryInterval time.Duration
	onMember      func(EventBatch) error
	onUser        func(EventBatch) error

	mu     sync.Mutex
	closed bool
	doneCh chan struct{}

	// notify is signaled (non-blocking) whenever new events are queued,
	// a truncation is recorded, or the subscription is shut down.
	notify chan struct{}

	member streamState
	user   streamState
}

// streamState is the per-stream delivery state of a Subscription. It is
// only touched while the eventManager lock is held, except for reads of
// doneCh/notify which are safe concurrently.
type streamState struct {
	buf streamBuffer

	// nextSeq is the sequence number of the next event the consumer has
	// not acknowledged. Events below nextSeq have been acknowledged.
	nextSeq uint64

	// deliverSeq freezes the currently delivered, unacknowledged event
	// batch: it is the sequence of its last event, or zero when no event
	// batch is outstanding. Events newer than deliverSeq stay buffered and
	// are not folded into a redelivery, so a failed handler retries the
	// exact same batch it first saw.
	deliverSeq uint64

	// needsSnapshot is set when the stream must deliver a fresh member
	// snapshot before any further member events. It is set at creation and
	// whenever the member buffer is truncated. snapshotSeq labels the
	// currently pending snapshot and is only meaningful while needsSnapshot
	// is true. snapshotFromTruncation marks that the pending snapshot was
	// scheduled by a buffer overflow rather than being the initial one, so
	// the delivered batch can report the truncation explicitly.
	needsSnapshot          bool
	snapshotSeq            uint64
	snapshotFromTruncation bool

	// truncated records an overflow on the user stream that the consumer
	// has not been notified of yet. It carries the sequence of the last
	// discarded event.
	truncatedSeq uint64
	truncated    bool
}

// eventManager fans Serf events out to active Subscriptions. Each
// subscription owns independent per-stream buffers, so a slow or failing
// consumer only truncates its own view and never blocks Serf or other
// subscribers.
type eventManager struct {
	// memberSnapshot returns a consistent point-in-time copy of the
	// cluster members. It is invoked with the manager lock held and must
	// not call back into the manager.
	memberSnapshot func() []Member

	// memberSeq and userSeq are the global per-stream sequence counters.
	// The member sequence also labels member snapshots so that a snapshot
	// and the events that follow it line up without a gap.
	memberSeq uint64
	userSeq   uint64

	mu            sync.Mutex
	subscriptions map[*Subscription]struct{}
	closed        bool

	// testMu and testMembers back the deterministic member snapshot used
	// by unit tests; they are nil for a real Serf instance.
	testMu      *sync.Mutex
	testMembers *[]Member
}

// newEventManager builds an eventManager. snapshotFn is used to build
// consistent member snapshots; it must take the Serf member lock.
func newEventManager(snapshotFn func() []Member) *eventManager {
	return &eventManager{
		memberSnapshot: snapshotFn,
		subscriptions:  make(map[*Subscription]struct{}),
	}
}

// subscribe registers a new subscription. The subscription starts with a
// pending member snapshot labeled with the current member sequence, so it
// observes a consistent view followed by every later member event.
func (m *eventManager) subscribe(conf SubscribeConfig) *Subscription {
	buffer := conf.Buffer
	if buffer <= 0 {
		buffer = defaultEventSubscribeBuffer
	}
	retry := conf.RetryInterval
	if retry <= 0 {
		retry = defaultSubscribeRetryInterval
	}

	sub := &Subscription{
		manager:       m,
		retryInterval: retry,
		onMember:      conf.OnMemberEvent,
		onUser:        conf.OnUserEvent,
		doneCh:        make(chan struct{}),
		notify:        make(chan struct{}, 1),
	}
	sub.member.buf.events = make([]bufferedEvent, 0, buffer)
	sub.user.buf.events = make([]bufferedEvent, 0, buffer)
	sub.member.needsSnapshot = true

	m.mu.Lock()
	defer m.mu.Unlock()

	// Label the initial snapshot with the current member sequence while
	// we hold the same lock that publishes member events. No member event
	// can be inserted in between, which guarantees the snapshot and the
	// first incremental event line up without a gap or overlap.
	sub.member.nextSeq = m.memberSeq + 1
	sub.member.needsSnapshot = true
	sub.member.snapshotSeq = m.memberSeq
	sub.user.nextSeq = m.userSeq + 1
	m.subscriptions[sub] = struct{}{}
	return sub
}

// publishMember fans a member event out to every subscription and
// advances the global member sequence. It must be called while the Serf
// member lock is held so that the event cannot race a member snapshot.
func (m *eventManager) publishMember(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.memberSeq++
	seq := m.memberSeq
	for sub := range m.subscriptions {
		sub.enqueueLocked(StreamMember, seq, e)
	}
}

// publishUser fans a user event out to every subscription and advances
// the global user sequence.
func (m *eventManager) publishUser(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.userSeq++
	seq := m.userSeq
	for sub := range m.subscriptions {
		sub.enqueueLocked(StreamUser, seq, e)
	}
}

// enqueueLocked appends an event to the named stream buffer, applying the
// bounded-buffer truncation policy when the buffer is full. The caller
// must hold the manager lock.
func (s *Subscription) enqueueLocked(stream EventStream, seq uint64, e Event) {
	st := s.stateFor(stream)

	// A pending snapshot or an unacknowledged truncation already implies a
	// resynchronization point; events published in the meantime would only
	// be discarded again, so skip buffering until the consumer catches up.
	if stream == StreamMember && st.needsSnapshot {
		return
	}
	if stream == StreamUser && st.truncated {
		st.truncatedSeq = seq
		return
	}

	if len(st.buf.events) >= cap(st.buf.events) {
		s.truncateLocked(stream, seq)
		return
	}
	st.buf.events = append(st.buf.events, bufferedEvent{seq: seq, event: e})
	s.signalLocked()
}

// truncateLocked records that the stream buffer overflowed. The member
// stream is resynchronized through a fresh snapshot; the user stream
// raises an explicit truncation batch. An already delivered, unacknowledged
// batch (the events up to deliverSeq) is never discarded: only the newer,
// not-yet-delivered events behind it are dropped. The caller must hold the
// manager lock.
func (s *Subscription) truncateLocked(stream EventStream, seq uint64) {
	st := s.stateFor(stream)
	if st.deliverSeq > 0 {
		kept := st.buf.events[:0]
		for _, be := range st.buf.events {
			if be.seq <= st.deliverSeq {
				kept = append(kept, be)
			}
		}
		st.buf.events = kept
	} else {
		st.buf.events = st.buf.events[:0]
	}
	switch stream {
	case StreamMember:
		st.needsSnapshot = true
		st.snapshotFromTruncation = true
		st.nextSeq = seq + 1
	case StreamUser:
		st.truncated = true
		st.truncatedSeq = seq
		st.nextSeq = seq + 1
	}
	s.signalLocked()
}

// stateFor returns the streamState for the given stream.
func (s *Subscription) stateFor(stream EventStream) *streamState {
	if stream == StreamMember {
		return &s.member
	}
	return &s.user
}

// signalLocked wakes a blocked NextBatch without ever blocking the
// publisher. Pending signals coalesce in the one-element channel.
func (s *Subscription) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// unsubscribe removes the subscription, releases its buffers and wakes
// any goroutine blocked in NextBatch. It is safe to call multiple times.
func (m *eventManager) unsubscribe(s *Subscription) {
	m.mu.Lock()
	if _, ok := m.subscriptions[s]; ok {
		delete(m.subscriptions, s)
	}
	closed := s.markClosedLocked()
	m.mu.Unlock()

	if closed {
		close(s.doneCh)
	}
}

// markClosedLocked flips the subscription to closed and returns whether
// the caller should close doneCh. The caller must hold the manager lock.
func (s *Subscription) markClosedLocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.closed = true
	s.member.buf.events = nil
	s.user.buf.events = nil
	s.signalLocked()
	return true
}

// close shuts the manager down and releases every subscription. It is
// invoked from Serf.Shutdown.
func (m *eventManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for sub := range m.subscriptions {
		if sub.markClosedLocked() {
			close(sub.doneCh)
		}
		delete(m.subscriptions, sub)
	}
}

// isClosed reports whether the subscription has been unsubscribed or its
// manager has been shut down.
func (s *Subscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// NextBatch blocks until the next deliverable batch is available, the
// context is canceled, or the subscription is torn down. Member events
// are drained before user events when both are ready; the two streams are
// never merged into one batch and each retains its own order.
//
// The returned batch must be acknowledged with Ack once it has been
// processed successfully. A batch that is not acknowledged is delivered
// again on the next call rather than being skipped.
func (s *Subscription) NextBatch(ctx context.Context) (EventBatch, error) {
	for {
		if s.isClosed() {
			return EventBatch{}, ErrSubscriptionClosed
		}

		batch, ok := s.takeBatch(ctx)
		if ok {
			return batch, nil
		}
		if ctx.Err() != nil {
			return EventBatch{}, ctx.Err()
		}
	}
}

// takeBatch performs one non-blocking delivery attempt, falling back to
// the notify channel when nothing is ready.
func (s *Subscription) takeBatch(ctx context.Context) (EventBatch, bool) {
	s.manager.mu.Lock()

	// Member stream is serviced first: snapshots, then truncation-driven
	// snapshots, then buffered events.
	if batch, ok := s.nextMemberBatchLocked(); ok {
		s.manager.mu.Unlock()
		return batch, true
	}
	if batch, ok := s.nextUserBatchLocked(); ok {
		s.manager.mu.Unlock()
		return batch, true
	}

	// Nothing ready: park until a publish, truncation, shutdown or the
	// caller's context wakes us. Register the context wake-up while the
	// manager lock is held so a concurrent publish cannot be missed.
	notify := s.notify
	var wake <-chan struct{}
	if ctx.Done() != nil {
		wake = ctx.Done()
	}
	s.manager.mu.Unlock()

	select {
	case <-notify:
	case <-wake:
	case <-s.doneCh:
	}
	return EventBatch{}, false
}

// nextMemberBatchLocked assembles the next member-stream batch. The
// caller must hold the manager lock.
func (s *Subscription) nextMemberBatchLocked() (EventBatch, bool) {
	st := &s.member

	if st.needsSnapshot {
		// Rebuild the snapshot on every redelivery until it is
		// acknowledged, so a failed consumer retries a consistent batch
		// rather than silently skipping the resynchronization point. The
		// snapshot is built while holding the manager lock, which every
		// member publisher also holds; the snapshot callback takes the
		// Serf member lock in the same order as publishMember, so the
		// snapshot is consistent with the sequence it is labeled with.
		seq := s.manager.memberSeq
		st.snapshotSeq = seq
		members := s.manager.memberSnapshot()
		return EventBatch{
			Stream:    StreamMember,
			Seq:       seq,
			Snapshot:  members,
			Truncated: st.snapshotFromTruncation,
		}, true
	}

	if len(st.buf.events) > 0 {
		return drainBatchLocked(StreamMember, st), true
	}
	return EventBatch{}, false
}

// nextUserBatchLocked assembles the next user-stream batch. The caller
// must hold the manager lock.
func (s *Subscription) nextUserBatchLocked() (EventBatch, bool) {
	st := &s.user

	if st.truncated {
		// Keep reporting the truncation until it is acknowledged; an
		// unacknowledged truncation must be retried like any other batch.
		return EventBatch{
			Stream:    StreamUser,
			Seq:       st.truncatedSeq,
			Truncated: true,
		}, true
	}

	if len(st.buf.events) > 0 {
		return drainBatchLocked(StreamUser, st), true
	}
	return EventBatch{}, false
}

// drainBatchLocked packages the outstanding, unacknowledged events of one
// stream into a single batch. The batch boundary is frozen on first
// delivery through deliverSeq: later publishes stay in the buffer and are
// excluded from redeliveries, so a failed consumer retries the exact same
// batch. Nothing is removed from the buffer here; removal only happens on
// Ack. The caller must hold the manager lock.
func drainBatchLocked(stream EventStream, st *streamState) EventBatch {
	if st.deliverSeq == 0 {
		// First delivery of this batch: freeze it at the current tail.
		st.deliverSeq = st.buf.events[len(st.buf.events)-1].seq
	}

	events := make([]Event, 0, len(st.buf.events))
	for _, be := range st.buf.events {
		if be.seq > st.deliverSeq {
			break
		}
		events = append(events, be.event)
	}
	return EventBatch{
		Stream: stream,
		Seq:    st.deliverSeq,
		Events: events,
	}
}

// Ack acknowledges a batch previously returned by NextBatch. All events
// up to and including batch.Seq on batch.Stream are removed from the
// subscription buffer and will not be redelivered. Acknowledging a
// snapshot or a truncation batch simply clears that resynchronization
// point.
//
// Ack never moves the stream position backwards: acknowledging an older
// batch after a newer one has already been acknowledged is a no-op.
func (s *Subscription) Ack(batch EventBatch) error {
	if s.isClosed() {
		return ErrSubscriptionClosed
	}

	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()

	st := s.stateFor(batch.Stream)

	switch {
	case batch.Snapshot != nil:
		// Only clear the resynchronization point if this snapshot is still
		// the current one. A redelivered snapshot can carry a newer label,
		// in which case an acknowledgement of the older batch must not
		// clear the pending snapshot.
		if st.needsSnapshot && st.snapshotSeq == batch.Seq {
			st.needsSnapshot = false
			st.snapshotFromTruncation = false
		}
	case batch.Truncated:
		// Only clear a still-pending truncation for this exact point. A
		// later truncation may already have superseded it.
		if st.truncated && st.truncatedSeq == batch.Seq {
			st.truncated = false
		}
	default:
		// Acknowledging an older event batch after a newer one has already
		// been acknowledged is a no-op and must never move nextSeq back.
		if batch.Seq < st.nextSeq {
			return nil
		}
		// Drop every buffered event up to and including batch.Seq and
		// release the frozen delivery boundary so the next batch can be
		// formed from the remaining buffered events.
		kept := st.buf.events[:0]
		for _, be := range st.buf.events {
			if be.seq > batch.Seq {
				kept = append(kept, be)
			}
		}
		st.buf.events = kept
		st.deliverSeq = 0
	}
	// Only advance the stream position. It must never move backwards, so a
	// stale acknowledgement is ignored when the position is already ahead.
	if batch.Seq+1 > st.nextSeq {
		st.nextSeq = batch.Seq + 1
	}
	return nil
}

// Unsubscribe tears the subscription down, releasing its per-stream
// buffers and unblocking a concurrent NextBatch. It is safe to call
// multiple times and from any goroutine, including an event handler.
func (s *Subscription) Unsubscribe() {
	s.manager.unsubscribe(s)
}

// Done returns a channel that is closed when the subscription has been
// unsubscribed or its Serf instance has shut down.
func (s *Subscription) Done() <-chan struct{} {
	return s.doneCh
}

// Start runs the configured event handlers until the subscription is
// unsubscribed, the context is canceled, or Serf shuts down. Each handler
// is invoked with one batch at a time and the batch is only acknowledged
// after the handler returns nil; an error triggers a redelivery of the
// same batch after RetryInterval. It returns nil on clean shutdown and
// the last handler error otherwise.
func (s *Subscription) Start(ctx context.Context) error {
	for {
		batch, err := s.NextBatch(ctx)
		if err != nil {
			if err == ErrSubscriptionClosed {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		handler := s.onMember
		if batch.Stream == StreamUser {
			handler = s.onUser
		}
		if handler == nil {
			// No handler registered for this stream: acknowledge and
			// move on so it cannot stall the other stream.
			_ = s.Ack(batch)
			continue
		}

		if err := handler(batch); err != nil {
			// Redeliver the same batch after the retry interval. The
			// unacknowledged batch stays buffered, so its position does not
			// advance and no event is skipped.
			select {
			case <-time.After(s.retryInterval):
			case <-ctx.Done():
				return ctx.Err()
			case <-s.doneCh:
				return nil
			}
			continue
		}
		if err := s.Ack(batch); err != nil {
			return err
		}
	}
}
