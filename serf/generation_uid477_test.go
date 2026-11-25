// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/testutil"
)

func TestGenerationManager_RestartFencesOldInstance_uid477(t *testing.T) {
	mgr := NewGenerationManager()
	oldID := MemberIdentity{Name: "node", Generation: 1}
	newID := MemberIdentity{Name: "node", Generation: 2}

	if !mgr.JoinWithAddress(oldID, "10.0.0.1:7946") {
		t.Fatal("initial generation should be accepted")
	}
	if !mgr.IsCurrent("node", 1) {
		t.Fatal("initial generation should be current")
	}

	// A delayed old-generation message after the restart must be fenced.
	if !mgr.Restart(newID, "10.0.0.1:7946") {
		t.Fatal("new generation should replace the old one")
	}
	if mgr.IsCurrent("node", 1) {
		t.Fatal("old generation must no longer be current")
	}
	if !mgr.IsCurrent("node", 2) {
		t.Fatal("new generation should be current")
	}
	if mgr.Join(oldID) {
		t.Fatal("old generation join must be rejected")
	}
	if id, ok := mgr.IdentityForAddress("10.0.0.1:7946"); !ok || id != newID {
		t.Fatalf("address index should point to new identity: %#v %v", id, ok)
	}
}

func TestGenerationManager_TransitionsAreIdempotentAndOrdered_uid477(t *testing.T) {
	mgr := NewGenerationManager()
	id := MemberIdentity{Name: "node", Generation: 7}
	if !mgr.Join(id) {
		t.Fatal("join failed")
	}

	// Duplicate alive/suspect/dead deliveries commit only once.
	if !mgr.CommitAlive(id, 1) {
		t.Fatal("first alive should commit")
	}
	if mgr.CommitAlive(id, 1) {
		t.Fatal("duplicate alive must not commit again")
	}
	if !mgr.CommitSuspect(id, 2) {
		t.Fatal("suspect should commit")
	}
	if mgr.CommitSuspect(id, 2) {
		t.Fatal("duplicate suspect must not commit again")
	}
	if !mgr.CommitDead(id, 3) {
		t.Fatal("dead should commit")
	}
	if mgr.CommitDead(id, 3) {
		t.Fatal("duplicate dead must not commit again")
	}

	committed := mgr.Committed()
	if len(committed) != 3 {
		t.Fatalf("expected exactly 3 commits, got %d: %#v", len(committed), committed)
	}

	// Out-of-order older transitions cannot move the state backwards.
	if mgr.CommitSuspect(id, 2) {
		t.Fatal("stale suspect should not commit after dead")
	}
	if mgr.CommitAlive(id, 0) {
		t.Fatal("older alive should not commit")
	}

	// A higher sequence alive is a valid refutation/restart within the same
	// generation and is allowed to establish alive again.
	if !mgr.CommitAlive(id, 4) {
		t.Fatal("newer alive/refutation should commit")
	}
}

func TestGenerationManager_OutOfOrderSequence_uid477(t *testing.T) {
	mgr := NewGenerationManager()
	id := MemberIdentity{Name: "node", Generation: 1}
	mgr.Join(id)

	// Dead arrives first ...
	if !mgr.CommitDead(id, 5) {
		t.Fatal("dead seq 5 should commit")
	}
	// ... followed by delayed lower-sequence messages and duplicates.
	if mgr.CommitSuspect(id, 4) {
		t.Fatal("lower sequence suspect must not commit")
	}
	if mgr.CommitAlive(id, 4) {
		t.Fatal("lower sequence alive must not commit")
	}
	if mgr.CommitDead(id, 5) {
		t.Fatal("duplicate dead must not commit")
	}
	if got := mgr.Committed(); len(got) != 1 || got[0].State != TransitionDead {
		t.Fatalf("only the first dead should be committed: %#v", got)
	}
}

func TestGenerationManager_CleanupTimersAndQueues_uid477(t *testing.T) {
	mgr := NewGenerationManager()
	id := MemberIdentity{Name: "node", Generation: 3}
	mgr.Join(id)

	fired := make(chan struct{}, 1)
	mgr.StartDetection(id, time.Hour, func() {
		fired <- struct{}{}
	})
	if !mgr.HasDetectionTimer(id) {
		t.Fatal("detection timer should be registered")
	}
	if !mgr.EnqueueRetransmit(id, 1, []byte("hello")) {
		t.Fatal("first retransmit item should enqueue")
	}
	if mgr.EnqueueRetransmit(id, 1, []byte("duplicate")) {
		t.Fatal("duplicate sequence should not enqueue")
	}
	if q := mgr.RetransmitQueue(id); q.Len() != 1 {
		t.Fatalf("expected one queued item, got %d", q.Len())
	}

	mgr.CleanupIdentity(id)
	if mgr.HasDetectionTimer(id) {
		t.Fatal("detection timer should be removed")
	}
	if mgr.HasRetransmitQueue(id) {
		t.Fatal("retransmission queue should be removed")
	}

	// Replacing a timer and cleaning the generation must stop the old timer.
	mgr.StartDetection(id, 10*time.Millisecond, func() {
		fired <- struct{}{}
	})
	mgr.CleanupIdentity(id)
	select {
	case <-fired:
		t.Fatal("cleaned detection timer fired")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestSerf_GenerationRestartFencesMessagesAndEvents_uid477(t *testing.T) {
	ip, returnFn := testutil.TakeIP()
	defer returnFn()

	cfg := testConfig(t, ip)
	cfg.Generation = 42
	s, err := Create(cfg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()

	eventCh := make(chan Event, 32)
	s.config.EventCh = eventCh

	if s.Generation() != 42 {
		t.Fatalf("bad local generation: %d", s.Generation())
	}

	addr := net.IPv4(10, 0, 0, 10)
	oldID := MemberIdentity{Name: "restart", Generation: 1}
	newID := MemberIdentity{Name: "restart", Generation: 2}
	old := &memberState{
		Member: Member{
			Name:       "restart",
			Generation: 1,
			Addr:       addr,
			Port:       7946,
			Status:     StatusAlive,
		},
		statusLTime: 10,
	}

	s.memberLock.Lock()
	s.members["restart"] = old
	s.memberLock.Unlock()
	s.generations.JoinWithAddress(oldID, net.JoinHostPort(addr.String(), "7946"))
	s.generations.StartDetection(oldID, time.Hour, func() {})
	s.generations.EnqueueRetransmit(oldID, 1, []byte("old"))

	// New instance joins with a new generation, even with a lower Lamport
	// time than an old delayed message might have.
	join := &messageJoin{Node: "restart", Generation: 2, LTime: 11}
	if !s.handleNodeJoinIntent(join) {
		t.Fatal("new generation join should be accepted/rebroadcast")
	}

	s.memberLock.RLock()
	member := s.members["restart"]
	if member == nil || member.Generation != 2 {
		t.Fatalf("new generation not installed: %#v", member)
	}
	if member.Status != StatusAlive {
		t.Fatalf("new instance should remain alive state, got %v", member.Status)
	}
	s.memberLock.RUnlock()

	if s.generations.HasDetectionTimer(oldID) {
		t.Fatal("old generation detection timer should be cleaned on restart")
	}
	if s.generations.HasRetransmitQueue(oldID) {
		t.Fatal("old generation retransmission queue should be cleaned on restart")
	}
	if current, ok := s.generations.IdentityForAddress(net.JoinHostPort(addr.String(), "7946")); !ok || current != newID {
		t.Fatalf("same address should map to new generation: %#v %v", current, ok)
	}

	// Delayed old-generation join/leave/suspect/alive/dead messages fail.
	if s.handleNodeJoinIntent(&messageJoin{Node: "restart", Generation: 1, LTime: 100}) {
		t.Fatal("old generation join must not rebroadcast")
	}
	if s.handleNodeLeaveIntent(&messageLeave{Node: "restart", Generation: 1, LTime: 100}) {
		t.Fatal("old generation leave must not rebroadcast")
	}
	if s.HandleSuspect(oldID, 100) || s.HandleAlive(oldID, 101) || s.HandleDead(oldID, 102) {
		t.Fatal("old generation transitions must not commit")
	}

	s.memberLock.RLock()
	if s.members["restart"].Status != StatusAlive {
		t.Fatalf("old messages changed new instance state: %v", s.members["restart"].Status)
	}
	s.memberLock.RUnlock()

	// Duplicate new-generation transitions commit once.
	if !s.HandleAlive(newID, 1) || s.HandleAlive(newID, 1) {
		t.Fatal("alive must commit exactly once")
	}
	if !s.HandleSuspect(newID, 2) || s.HandleSuspect(newID, 2) {
		t.Fatal("suspect must commit exactly once")
	}
	if !s.HandleDead(newID, 3) || s.HandleDead(newID, 3) {
		t.Fatal("dead must commit exactly once")
	}

	// The memberlist join event publishes the current generation once.
	node := &memberlist.Node{Name: "restart", Addr: addr, Port: 7946}
	s.handleNodeJoin(node)
	select {
	case e := <-eventCh:
		me, ok := e.(MemberEvent)
		if !ok || me.Type != EventMemberJoin || len(me.Members) != 1 {
			t.Fatalf("unexpected event: %#v", e)
		}
		if got := me.Members[0].Generation; got != 2 {
			t.Fatalf("event generation = %d, want 2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("missing join event")
	}

	// An old-generation event/intent racing after the replacement must not
	// publish another member event.
	s.memberLock.Lock()
	s.recentIntents["restart"] = nodeIntent{
		Type:       messageJoinType,
		Generation: 1,
		LTime:      99,
	}
	s.memberLock.Unlock()
	s.handleNodeJoin(node)
	select {
	case e := <-eventCh:
		t.Fatalf("stale generation event was published: %#v", e)
	default:
	}

	// Re-register runtime objects for the current instance, then verify the
	// memberlist leave/dead path cleans them.
	s.generations.StartDetection(newID, time.Hour, func() {})
	s.generations.EnqueueRetransmit(newID, 1, []byte("new"))
	s.handleNodeLeave(node)
	if s.generations.HasDetectionTimer(newID) {
		t.Fatal("leave should clean detection timer")
	}
	if s.generations.HasRetransmitQueue(newID) {
		t.Fatal("leave should clean retransmission queue")
	}
	select {
	case e := <-eventCh:
		me := e.(MemberEvent)
		if me.Type != EventMemberFailed {
			t.Fatalf("expected failed event, got %v", me.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("missing failed event")
	}
}

func TestSerf_LeaveAndForcedDeleteCleanRuntime_uid477(t *testing.T) {
	ip, returnFn := testutil.TakeIP()
	defer returnFn()

	s, err := Create(testConfig(t, ip))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()
	s.config.EventCh = make(chan Event, 32)

	id := MemberIdentity{Name: "cleanup", Generation: 9}
	m := &memberState{
		Member: Member{
			Name:       "cleanup",
			Generation: 9,
			Status:     StatusAlive,
		},
	}

	s.memberLock.Lock()
	s.members["cleanup"] = m
	s.memberLock.Unlock()
	s.generations.Join(id)
	s.generations.StartDetection(id, time.Hour, func() {})
	s.generations.EnqueueRetransmit(id, 1, nil)

	s.handleNodeLeave(&memberlist.Node{Name: "cleanup"})
	if s.generations.HasDetectionTimer(id) {
		t.Fatal("node leave did not cancel detection timer")
	}
	if s.generations.HasRetransmitQueue(id) {
		t.Fatal("node leave did not clear retransmission queue")
	}

	// Forced erase also cancels a pending prune timer.
	pruneID := MemberIdentity{Name: "prune", Generation: 11}
	prune := &memberState{
		Member: Member{Name: "prune", Generation: 11, Status: StatusLeaving},
	}
	s.config.BroadcastTimeout = time.Hour
	s.config.LeavePropagateDelay = time.Hour

	s.memberLock.Lock()
	s.members["prune"] = prune
	s.handlePrune(prune)
	if !s.hasPruneTimer(pruneID.key()) {
		s.memberLock.Unlock()
		t.Fatal("prune timer was not registered")
	}
	s.eraseNode(prune)
	timerOK := s.hasPruneTimer(pruneID.key())
	_, memberOK := s.members["prune"]
	s.memberLock.Unlock()

	if timerOK {
		t.Fatal("erase did not cancel prune timer")
	}
	if memberOK {
		t.Fatal("erase did not remove member")
	}
}

func TestGenerationMessages_RoundTrip_uid477(t *testing.T) {
	join := &messageJoin{Node: "node", LTime: 9, Generation: 3}
	raw, err := encodeMessage(messageJoinType, join, false)
	if err != nil {
		t.Fatal(err)
	}
	var decodedJoin messageJoin
	if err := decodeMessage(raw[1:], &decodedJoin); err != nil {
		t.Fatal(err)
	}
	if decodedJoin.Generation != 3 || decodedJoin.Node != "node" || decodedJoin.LTime != 9 {
		t.Fatalf("bad join decode: %#v", decodedJoin)
	}

	leave := &messageLeave{Node: "node", LTime: 10, Generation: 3, Prune: true}
	raw, err = encodeMessage(messageLeaveType, leave, false)
	if err != nil {
		t.Fatal(err)
	}
	var decodedLeave messageLeave
	if err := decodeMessage(raw[1:], &decodedLeave); err != nil {
		t.Fatal(err)
	}
	if decodedLeave.Generation != 3 || !decodedLeave.Prune {
		t.Fatalf("bad leave decode: %#v", decodedLeave)
	}
}

func TestSerf_OldPruneTimerCannotEraseNewGeneration_uid477(t *testing.T) {
	ip, returnFn := testutil.TakeIP()
	defer returnFn()

	cfg := testConfig(t, ip)
	cfg.BroadcastTimeout = time.Millisecond
	cfg.LeavePropagateDelay = 0
	s, err := Create(cfg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s.Shutdown()
	s.config.EventCh = make(chan Event, 32)

	name := "reused"
	old := &memberState{
		Member: Member{Name: name, Generation: 1, Status: StatusLeaving},
	}
	replacement := &memberState{
		Member: Member{Name: name, Generation: 2, Status: StatusAlive},
	}

	s.memberLock.Lock()
	s.members[name] = old
	s.handlePrune(old)
	// Simulate the new instance joining before the old prune timer fires.
	s.members[name] = replacement
	s.memberLock.Unlock()

	time.Sleep(30 * time.Millisecond)

	s.memberLock.RLock()
	current, ok := s.members[name]
	s.memberLock.RUnlock()
	if !ok || current.Generation != 2 {
		t.Fatalf("old prune timer erased/replaced new generation: ok=%v current=%#v", ok, current)
	}
}
