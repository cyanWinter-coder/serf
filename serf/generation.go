// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Generation is the opaque instance generation of a node. Every fresh process
// for a node name should use a different generation. Unlike the Lamport time
// and the failure-detector sequence number, it identifies *which instance* of
// a named node produced a message.
type Generation = uint64

// Sequence is the per-generation ordering number used by alive, suspect and
// dead transitions.
type Sequence = uint64

// MemberIdentity is the stable identity of a member. The node name remains the
// user-visible identifier; Generation distinguishes successive instances that
// reuse the same name and address. Addresses are transport details and are
// intentionally not part of identity.
type MemberIdentity struct {
	Name       string
	Generation Generation
}

// String renders the identity as name@generation.
func (id MemberIdentity) String() string {
	if id.Generation == 0 {
		return id.Name
	}
	return id.Name + "@" + strconv.FormatUint(id.Generation, 10)
}

// key returns the internal map key for an identity.
func (id MemberIdentity) key() string {
	return id.Name + "\x00" + strconv.FormatUint(id.Generation, 10)
}

// TransitionState is the failure-detector state committed by a Transition.
type TransitionState string

const (
	// TransitionAlive commits an alive/refutation transition.
	TransitionAlive TransitionState = "alive"

	// TransitionSuspect commits a suspect transition.
	TransitionSuspect TransitionState = "suspect"

	// TransitionDead commits a dead transition.
	TransitionDead TransitionState = "dead"
)

// valid reports whether the state is one of the supported transitions.
func (s TransitionState) valid() bool {
	switch s {
	case TransitionAlive, TransitionSuspect, TransitionDead:
		return true
	default:
		return false
	}
}

// rank orders states that may share a sequence number. A higher rank may
// replace a lower rank at the same sequence, but not vice versa.
func (s TransitionState) rank() int {
	switch s {
	case TransitionAlive:
		return 1
	case TransitionSuspect:
		return 2
	case TransitionDead:
		return 3
	default:
		return 0
	}
}

// Transition is a single generation- and sequence-ordered alive, suspect or
// dead state transition.
type Transition struct {
	Name       string
	Generation Generation
	Sequence   Sequence
	State      TransitionState
}

// MemberTransition is an alternate descriptive name for Transition.
type MemberTransition = Transition

// StateTransition is an alternate descriptive name for Transition.
type StateTransition = Transition

// Identity returns the identity that produced the transition.
func (t Transition) Identity() MemberIdentity {
	return MemberIdentity{Name: t.Name, Generation: t.Generation}
}

// dedupKey returns the idempotency key for an exact transition.
func (t Transition) dedupKey() string {
	return t.Name + "\x00" +
		strconv.FormatUint(t.Generation, 10) + "\x00" +
		strconv.FormatUint(t.Sequence, 10) + "\x00" +
		string(t.State)
}

// RetransmitItem is one message waiting for generation-scoped retransmission.
type RetransmitItem struct {
	Sequence Sequence
	Message  []byte
}

// RetransmitQueue deduplicates outbound messages by sequence and can be
// discarded as a unit when a member generation leaves or is removed.
type RetransmitQueue struct {
	mu    sync.Mutex
	items []RetransmitItem
	seen  map[Sequence]struct{}
}

// NewRetransmitQueue creates an empty retransmission queue.
func NewRetransmitQueue() *RetransmitQueue {
	return &RetransmitQueue{seen: make(map[Sequence]struct{})}
}

// Enqueue adds an item. It returns false when the sequence is already queued.
func (q *RetransmitQueue) Enqueue(sequence Sequence, msg []byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.seen[sequence]; ok {
		return false
	}
	q.seen[sequence] = struct{}{}
	q.items = append(q.items, RetransmitItem{Sequence: sequence, Message: msg})
	return true
}

// Items returns a copy of the queued items.
func (q *RetransmitQueue) Items() []RetransmitItem {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]RetransmitItem, len(q.items))
	copy(out, q.items)
	return out
}

// Len returns the number of queued items.
func (q *RetransmitQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Reset discards all queued items.
func (q *RetransmitQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = nil
	q.seen = make(map[Sequence]struct{})
}

// GenerationManager tracks the current generation of named members and makes
// state transitions idempotent and fencing-aware. Its zero value is not ready
// for use; call NewGenerationManager.
type GenerationManager struct {
	mu sync.Mutex

	// current is the newest accepted generation for each node name.
	current map[string]Generation

	// addresses records the transport address currently claimed by an
	// identity. It is only an index for restart detection; identity never
	// contains an address.
	addresses map[string]MemberIdentity

	// owners indexes all identity keys used for a node name so cleanup can
	// reach every generation-scoped runtime object.
	owners map[string]map[string]struct{}

	// seen is the exact-transition idempotency set.
	seen map[string]struct{}

	// last is the highest ordered transition committed for an identity.
	last map[string]Transition

	// committed retains committed transitions for tests and diagnostics.
	committed []Transition

	timers map[string]*time.Timer
	queues map[string]*RetransmitQueue
}

// NewGenerationManager creates an empty generation manager.
func NewGenerationManager() *GenerationManager {
	return &GenerationManager{
		current:   make(map[string]Generation),
		addresses: make(map[string]MemberIdentity),
		owners:    make(map[string]map[string]struct{}),
		seen:      make(map[string]struct{}),
		last:      make(map[string]Transition),
		timers:    make(map[string]*time.Timer),
		queues:    make(map[string]*RetransmitQueue),
	}
}

// Join records an identity as the current instance for its name. A lower
// generation is rejected. Equal generations are idempotent. Generation zero is
// treated as a legacy/unknown generation and never moves the fence backwards.
func (m *GenerationManager) Join(id MemberIdentity) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.joinLocked(id, "")
}

// ObserveJoin is an alias for Join.
func (m *GenerationManager) ObserveJoin(id MemberIdentity) bool {
	return m.Join(id)
}

// JoinWithAddress records the identity and its transport address. The address
// does not participate in identity comparisons.
func (m *GenerationManager) JoinWithAddress(id MemberIdentity, address string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.joinLocked(id, address)
}

// joinLocked is the lock-held implementation of Join.
func (m *GenerationManager) joinLocked(id MemberIdentity, address string) bool {
	if id.Name == "" {
		return false
	}

	if id.Generation != 0 {
		if cur, ok := m.current[id.Name]; ok && cur != 0 && id.Generation < cur {
			return false
		}
		m.current[id.Name] = id.Generation
	} else if _, ok := m.current[id.Name]; !ok {
		m.current[id.Name] = 0
	}

	key := id.key()
	if m.owners[id.Name] == nil {
		m.owners[id.Name] = make(map[string]struct{})
	}
	m.owners[id.Name][key] = struct{}{}

	if address != "" {
		m.addresses[address] = id
	}
	return true
}

// Restart records a new generation joining from the same transport address.
// The address index is updated and the new identity becomes current. It
// returns false if the supplied generation is older than the current one.
func (m *GenerationManager) Restart(id MemberIdentity, address string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.joinLocked(id, address) {
		return false
	}

	// A restart replaces runtime state for every previous generation of the
	// same name. Deduplication history is retained so duplicate old messages
	// cannot become visible again.
	for key := range m.owners[id.Name] {
		if key == id.key() {
			continue
		}
		m.cleanupKeyLocked(key)
	}
	return true
}

// CurrentGeneration returns the current generation for a name.
func (m *GenerationManager) CurrentGeneration(name string) (Generation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.current[name]
	return g, ok
}

// IsCurrent reports whether id may affect current state. Zero generations are
// accepted as legacy/unknown messages unless an explicit non-zero generation
// is being compared directly.
func (m *GenerationManager) IsCurrent(name string, generation Generation) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.current[name]
	if !ok || generation == 0 || cur == 0 {
		return true
	}
	return generation >= cur
}

// Accept is an alias for IsCurrent.
func (m *GenerationManager) Accept(name string, generation Generation) bool {
	return m.IsCurrent(name, generation)
}

// IdentityForAddress returns the identity most recently bound to a transport
// address. Callers should treat this as a transport index only.
func (m *GenerationManager) IdentityForAddress(address string) (MemberIdentity, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.addresses[address]
	return id, ok
}

// Commit records an alive, suspect or dead transition. It returns true exactly
// once for a valid, current transition. Duplicate and stale/out-of-order
// transitions return false and have no effect.
func (m *GenerationManager) Commit(t Transition) bool {
	if !t.State.valid() || t.Name == "" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	id := t.Identity()
	cur, known := m.current[t.Name]
	if known && t.Generation != 0 && cur != 0 && t.Generation < cur {
		return false
	}

	// An alive message can establish a generation. Suspect/dead messages for
	// an unknown name are still accepted by this low-level helper; Serf's
	// higher-level handlers preserve the historical unknown-node behavior.
	m.joinLocked(id, "")

	if _, ok := m.seen[t.dedupKey()]; ok {
		return false
	}

	key := id.key()
	if last, ok := m.last[key]; ok {
		if t.Sequence < last.Sequence {
			return false
		}
		if t.Sequence == last.Sequence && t.State.rank() <= last.State.rank() {
			return false
		}
	}

	m.seen[t.dedupKey()] = struct{}{}
	m.last[key] = t
	m.committed = append(m.committed, t)
	return true
}

// CommitTransition is an alias for Commit.
func (m *GenerationManager) CommitTransition(t Transition) bool {
	return m.Commit(t)
}

// CommitAlive commits an alive transition.
func (m *GenerationManager) CommitAlive(id MemberIdentity, sequence Sequence) bool {
	return m.Commit(Transition{
		Name:       id.Name,
		Generation: id.Generation,
		Sequence:   sequence,
		State:      TransitionAlive,
	})
}

// CommitSuspect commits a suspect transition.
func (m *GenerationManager) CommitSuspect(id MemberIdentity, sequence Sequence) bool {
	return m.Commit(Transition{
		Name:       id.Name,
		Generation: id.Generation,
		Sequence:   sequence,
		State:      TransitionSuspect,
	})
}

// CommitDead commits a dead transition.
func (m *GenerationManager) CommitDead(id MemberIdentity, sequence Sequence) bool {
	return m.Commit(Transition{
		Name:       id.Name,
		Generation: id.Generation,
		Sequence:   sequence,
		State:      TransitionDead,
	})
}

// Committed returns a copy of all transitions committed in order.
func (m *GenerationManager) Committed() []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Transition, len(m.committed))
	copy(out, m.committed)
	return out
}

// State returns the most recently committed transition state for an identity.
func (m *GenerationManager) State(id MemberIdentity) (TransitionState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	transition, ok := m.last[id.key()]
	return transition.State, ok
}

// LastSequence returns the highest committed sequence for an identity.
func (m *GenerationManager) LastSequence(id MemberIdentity) (Sequence, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	transition, ok := m.last[id.key()]
	return transition.Sequence, ok
}

// StartDetection starts or replaces the detection timer for an identity.
func (m *GenerationManager) StartDetection(id MemberIdentity, timeout time.Duration, fn func()) {
	key := id.key()

	m.mu.Lock()
	if old := m.timers[key]; old != nil {
		old.Stop()
	}

	var timer *time.Timer
	timer = time.AfterFunc(timeout, func() {
		m.mu.Lock()
		if current := m.timers[key]; current != timer {
			m.mu.Unlock()
			return
		}
		delete(m.timers, key)
		m.mu.Unlock()

		if fn != nil {
			fn()
		}
	})
	m.timers[key] = timer
	m.mu.Unlock()
}

// CancelDetection stops and removes an identity's detection timer.
func (m *GenerationManager) CancelDetection(id MemberIdentity) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.timers[id.key()]
	if !ok {
		return false
	}
	stopped := t.Stop()
	delete(m.timers, id.key())
	return stopped
}

// HasDetectionTimer reports whether a detection timer is registered.
func (m *GenerationManager) HasDetectionTimer(id MemberIdentity) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.timers[id.key()]
	return ok
}

// RetransmitQueue returns the generation-scoped retransmission queue, creating
// it if necessary.
func (m *GenerationManager) RetransmitQueue(id MemberIdentity) *RetransmitQueue {
	key := id.key()

	m.mu.Lock()
	defer m.mu.Unlock()

	q, ok := m.queues[key]
	if !ok {
		q = NewRetransmitQueue()
		m.queues[key] = q
	}
	return q
}

// EnqueueRetransmit appends a retransmission item to an identity's queue.
func (m *GenerationManager) EnqueueRetransmit(id MemberIdentity, sequence Sequence, msg []byte) bool {
	return m.RetransmitQueue(id).Enqueue(sequence, msg)
}

// HasRetransmitQueue reports whether a generation-scoped queue remains.
func (m *GenerationManager) HasRetransmitQueue(id MemberIdentity) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.queues[id.key()]
	return ok
}

// ClearRetransmit resets and removes an identity's retransmission queue.
func (m *GenerationManager) ClearRetransmit(id MemberIdentity) {
	m.mu.Lock()
	q, ok := m.queues[id.key()]
	if ok {
		delete(m.queues, id.key())
	}
	m.mu.Unlock()

	if ok {
		q.Reset()
	}
}

// CleanupIdentity cancels timers and clears queues for one exact identity.
func (m *GenerationManager) CleanupIdentity(id MemberIdentity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupKeyLocked(id.key())
}

// CleanupName cancels timers and clears queues for every generation of name.
func (m *GenerationManager) CleanupName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.owners[name] {
		m.cleanupKeyLocked(key)
	}
}

// cleanupKeyLocked releases runtime resources associated with an identity key.
// Deduplication and current-generation state are intentionally retained.
func (m *GenerationManager) cleanupKeyLocked(key string) {
	if t, ok := m.timers[key]; ok {
		t.Stop()
		delete(m.timers, key)
	}
	if q, ok := m.queues[key]; ok {
		q.Reset()
		delete(m.queues, key)
	}
}

// Close stops every detection timer and clears all retransmission queues.
func (m *GenerationManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, t := range m.timers {
		t.Stop()
		delete(m.timers, key)
	}
	for key, q := range m.queues {
		q.Reset()
		delete(m.queues, key)
	}
}

// lastInstanceGeneration makes generations strictly increase within a process
// even when two instances are created within the same wall-clock nanosecond.
var lastInstanceGeneration atomic.Uint64

// nextInstanceGeneration returns a fresh, process-monotonic generation based
// on wall-clock time so successive processes normally start on a higher value.
func nextInstanceGeneration(now time.Time) Generation {
	candidate := uint64(now.UnixNano())
	for {
		prev := lastInstanceGeneration.Load()
		next := candidate
		if next <= prev {
			next = prev + 1
		}
		if lastInstanceGeneration.CompareAndSwap(prev, next) {
			return next
		}
	}
}
