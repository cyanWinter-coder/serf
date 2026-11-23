// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSnapshotter(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	outCh := make(chan Event, 64)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write some user events
	ue := UserEvent{
		LTime: 42,
		Name:  "bar",
	}
	inCh <- ue

	// Write some queries
	q := &Query{
		LTime: 50,
		Name:  "uptime",
	}
	inCh <- q

	// Write some member events
	clock.Witness(100)
	meJoin := MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	meFail := MemberEvent{
		Type: EventMemberFailed,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	inCh <- meJoin
	inCh <- meFail
	inCh <- meJoin

	// Check these get passed through
	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, ue) {
			t.Fatalf("expected user event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, q) {
			t.Fatalf("expected query event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, meJoin) {
			t.Fatalf("expected member event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, meFail) {
			t.Fatalf("expected member event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	select {
	case e := <-outCh:
		if !reflect.DeepEqual(e, meJoin) {
			t.Fatalf("expected member event: %#v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastClock() != 100 {
		t.Fatalf("bad clock %d", snap.LastClock())
	}
	if snap.LastEventClock() != 42 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}
	if snap.LastQueryClock() != 50 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	prev := snap.AliveNodes()
	if len(prev) != 1 {
		t.Fatalf("expected alive: %#v", prev)
	}
	if prev[0].Name != "foo" {
		t.Fatalf("bad name: %#v", prev[0])
	}
	if prev[0].Addr != "127.0.0.1:5000" {
		t.Fatalf("bad addr: %#v", prev[0])
	}

	// Close the snapshotter.
	close(stopCh)
	snap.Wait()

	// Open the snapshotter, make sure nothing dies reading with coordinates
	// disabled.
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	close(stopCh)
	snap.Wait()
}

func TestSnapshotter_forceCompact(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// Create a very low limit
	inCh, snap, err := NewSnapshotter(td+"snap", 1024, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write lots of user events
	for i := range 1024 {
		ue := UserEvent{
			LTime: LamportTime(i),
		}
		inCh <- ue
	}

	// Write lots of queries
	for i := range 1024 {
		q := &Query{
			LTime: LamportTime(i),
		}
		inCh <- q
	}

	// Wait for drain
	for len(inCh) > 0 {
		time.Sleep(20 * time.Millisecond)
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastEventClock() != 1023 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}

	if snap.LastQueryClock() != 1023 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	close(stopCh)
	snap.Wait()
}

func TestSnapshotter_leave(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write a user event
	ue := UserEvent{
		LTime: 42,
		Name:  "bar",
	}
	inCh <- ue

	// Write a query
	q := &Query{
		LTime: 50,
		Name:  "uptime",
	}
	inCh <- q

	// Write some member events
	clock.Witness(100)
	meJoin := MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	inCh <- meJoin

	// Wait for drain
	for len(inCh) > 0 {
		time.Sleep(20 * time.Millisecond)
	}

	// Leave the cluster!
	snap.Leave()

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastClock() != 0 {
		t.Fatalf("bad clock %d", snap.LastClock())
	}
	if snap.LastEventClock() != 0 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}
	if snap.LastQueryClock() != 0 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	prev := snap.AliveNodes()
	if len(prev) != 0 {
		t.Fatalf("expected none alive: %#v", prev)
	}
}

func TestSnapshotter_leave_rejoin(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Write a user event
	ue := UserEvent{
		LTime: 42,
		Name:  "bar",
	}
	inCh <- ue

	// Write a query
	q := &Query{
		LTime: 50,
		Name:  "uptime",
	}
	inCh <- q

	// Write some member events
	clock.Witness(100)
	meJoin := MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{
			{
				Name: "foo",
				Addr: []byte{127, 0, 0, 1},
				Port: 5000,
			},
		},
	}
	inCh <- meJoin

	// Wait for drain
	for len(inCh) > 0 {
		time.Sleep(20 * time.Millisecond)
	}

	// Leave the cluster!
	snap.Leave()

	// Close the snapshoter
	close(stopCh)
	snap.Wait()

	// Open the snapshoter
	stopCh = make(chan struct{})
	_, snap, err = NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the values
	if snap.LastClock() != 100 {
		t.Fatalf("bad clock %d", snap.LastClock())
	}
	if snap.LastEventClock() != 42 {
		t.Fatalf("bad clock %d", snap.LastEventClock())
	}
	if snap.LastQueryClock() != 50 {
		t.Fatalf("bad clock %d", snap.LastQueryClock())
	}

	prev := snap.AliveNodes()
	if len(prev) == 0 {
		t.Fatalf("expected alive: %#v", prev)
	}
}

func TestSnapshotter_slowDiskNotBlockingEventCh(t *testing.T) {
	t.Skip("Flaky test")
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	outCh := make(chan Event, 1024)
	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// We need enough events to be much more than the buffers used which are size
	// 1024. This number processes easily within the 500ms we allow below on my
	// host provided there is no disk IO on the path (I verified that by just
	// returning early in tryAppend using the old blocking code). The new async
	// method should pass without disabling disk writes too!
	numEvents := 10000

	// Write lots of member updates (way bigger than our chan buffers)
	startCh := make(chan struct{})
	go func() {
		<-startCh
		for i := range numEvents {
			e := MemberEvent{
				Type: EventMemberJoin,
				Members: []Member{
					{
						Name: fmt.Sprintf("foo%d", i),
						Addr: []byte{127, 0, byte((i / 256) % 256), byte(i % 256)},
						Port: 5000,
					},
				},
			}
			if i%10 == 0 {
				// 1/10 events is a leave
				e.Type = EventMemberLeave
			}
			inCh <- e
			// Pace ourselves - if we just throw these out as fast as possible the
			// read loop below can't keep up and we end up dropping messages due to
			// backpressure. But we need to still send them all in well less than the
			// timeout, 10k messages at 1 microsecond should take 10 ms minimum. In
			// practice it's quite a bit more to actually process and because the
			// buffer here blocks.
			time.Sleep(1 * time.Microsecond)
		}
	}()

	// Wait for them all to process through and it should be in a lot less time
	// than if the disk IO was in serial. This was verified by running this test
	// against the old serial implementation and seeing it never come close to
	// passing on my laptop with an SSD. It's not the most robust thing ever but
	// it's at least a sanity check that we are non-blocking now, and it passes
	// reliably at least on my machine. I typically see this complete in around
	// 115ms on my machine so this should give plenty of headroom for slower CI
	// environments while still being low enough that actual disk IO would
	// reliably blow it.
	deadline := time.After(500 * time.Millisecond)
	numRecvd := 0
	start := time.Now()
	for numRecvd < numEvents {
		select {
		// Now we are ready to listen, start the generator goroutine off
		case startCh <- struct{}{}:
			continue
		case <-outCh:
			numRecvd++
		case <-deadline:
			t.Fatalf("timed out after %s waiting for messages blocked on fake disk IO? "+
				"got %d of %d", time.Since(start), numRecvd, numEvents)
		}
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()
}

func TestSnapshotter_blockedUpstreamNotBlockingMemberlist(t *testing.T) {
	td := t.TempDir()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// OutCh is unbuffered simulating a slow upstream
	outCh := make(chan Event)

	inCh, snap, err := NewSnapshotter(td+"snap", snapshotSizeLimit, true,
		logger, clock, outCh, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// We need enough events to be more than the internal buffer sizes
	numEvents := 3000

	// Send some updates
	for i := range numEvents {
		e := MemberEvent{
			Type: EventMemberJoin,
			Members: []Member{
				{
					Name: fmt.Sprintf("foo%d", i),
					Addr: []byte{127, 0, byte((i / 256) % 256), byte(i % 256)},
					Port: 5000,
				},
			},
		}
		if i%10 == 0 {
			// 1/10 events is a leave
			e.Type = EventMemberLeave
		}
		select {
		case inCh <- e:
		default:
			t.Fatalf("inCh should never block")
		}
		// Allow just the tiniest time so that the runtime can schedule the
		// goroutine that's reading this even if they are both on the same physical
		// core (like in CI).
		time.Sleep(1 * time.Microsecond)
	}

	// Close the snapshoter
	close(stopCh)
	snap.Wait()
}

// newTestSnapshotter wires up the minimal scaffolding shared by the tests
// below. The returned cleanup closes the snapshotter when the test ends.
func newTestSnapshotter(t *testing.T, path string,
	memberSnap func() []pinnedMember,
	memberGeneration func(name string) (LamportTime, bool)) (chan<- Event, *Snapshotter, chan struct{}) {
	t.Helper()

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "test["+t.Name()+"]: ", log.LstdFlags)
	inCh, snap, err := NewSnapshotter(path, snapshotSizeLimit, false,
		logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	snap.memberSnap = memberSnap
	snap.memberGeneration = memberGeneration

	t.Cleanup(func() {
		select {
		case <-stopCh:
		default:
			close(stopCh)
		}
		snap.Wait()
	})
	return inCh, snap, stopCh
}

// TestSnapshotter_versionHeader verifies a brand-new snapshot starts with the
// version header and that a rewritten file keeps it after a full lifecycle.
func TestSnapshotter_versionHeader(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	inCh, snap, stopCh := newTestSnapshotter(t, path, nil, nil)
	inCh <- UserEvent{LTime: 7, Name: "evt"}
	for len(inCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	close(stopCh)
	snap.Wait()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.HasPrefix(string(contents), snapshotHeader) {
		t.Fatalf("snapshot missing version header, got: %q", string(contents))
	}

	// Reopening must succeed and recognize the versioned format.
	_, snap2, stopCh2 := newTestSnapshotter(t, path, nil, nil)
	defer close(stopCh2)
	if snap2.LastEventClock() != 7 {
		t.Fatalf("bad event clock after reopen: %d", snap2.LastEventClock())
	}
}

// TestSnapshotter_legacyV0Replay makes sure a headerless snapshot produced by
// an older release still recovers with lenient, warning-only parsing.
func TestSnapshotter_legacyV0Replay(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	// Legacy format: no header, no member generation suffix, and a line the
	// current parser does not recognize that must only produce a warning.
	legacy := strings.Join([]string{
		"alive: old-node 127.0.0.1:5000",
		"clock: 11",
		"event-clock: 5",
		"some-old-unknown-line: xyz",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(legacy), 0644); err != nil {
		t.Fatalf("err: %v", err)
	}

	_, snap, stopCh := newTestSnapshotter(t, path, nil, nil)
	defer close(stopCh)

	prev := snap.AliveNodes()
	if len(prev) != 1 || prev[0].Name != "old-node" || prev[0].Addr != "127.0.0.1:5000" {
		t.Fatalf("legacy alive nodes not recovered: %#v", prev)
	}
	if snap.LastClock() != 11 || snap.LastEventClock() != 5 {
		t.Fatalf("legacy clocks not recovered: %d/%d", snap.LastClock(), snap.LastEventClock())
	}
}

// TestSnapshotter_corruptRejected covers the recovery-side contract: a
// damaged versioned snapshot must fail startup instead of yielding an empty
// cluster, and the bad file must be left in place for inspection.
func TestSnapshotter_corruptRejected(t *testing.T) {
	cases := []struct {
		name     string
		contents string
		want     error
	}{
		{
			name: "truncated final line",
			contents: strings.Join([]string{
				strings.TrimSpace(snapshotHeader),
				"alive: foo 127.0.0.1:5000 gen=3",
				"clock: 1", // missing trailing newline
			}, "\n"),
			want: errSnapshotCorrupt,
		},
		{
			// A header-like line missing its newline cannot be a valid v1
			// header; it is treated as an unknown future version rather than
			// being silently parsed as legacy content.
			name:     "truncated header",
			contents: strings.TrimSpace(snapshotHeader) + "garbage",
			want:     errSnapshotVersion,
		},
		{
			name: "unknown line",
			contents: strings.Join([]string{
				strings.TrimSpace(snapshotHeader),
				"mystery: data",
				"",
			}, "\n"),
			want: errSnapshotCorrupt,
		},
		{
			name: "bad clock",
			contents: strings.Join([]string{
				strings.TrimSpace(snapshotHeader),
				"clock: not-a-number",
				"",
			}, "\n"),
			want: errSnapshotCorrupt,
		},
		{
			name: "missing generation",
			contents: strings.Join([]string{
				strings.TrimSpace(snapshotHeader),
				"alive: foo 127.0.0.1:5000",
				"",
			}, "\n"),
			want: errSnapshotCorrupt,
		},
		{
			name: "generation moves backwards",
			contents: strings.Join([]string{
				strings.TrimSpace(snapshotHeader),
				"alive: foo 127.0.0.1:5000 gen=9",
				"alive: foo 127.0.0.1:5000 gen=4",
				"",
			}, "\n"),
			want: errSnapshotCorrupt,
		},
		{
			name: "future version",
			contents: strings.Join([]string{
				"# serf-snapshot-v99",
				"",
			}, "\n"),
			want: errSnapshotVersion,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			td := t.TempDir()
			path := filepath.Join(td, "snap")
			if err := os.WriteFile(path, []byte(tc.contents), 0644); err != nil {
				t.Fatalf("err: %v", err)
			}

			clock := new(LamportClock)
			stopCh := make(chan struct{})
			logger := log.New(os.Stderr, "", log.LstdFlags)
			_, _, err := NewSnapshotter(path, snapshotSizeLimit, false,
				logger, clock, nil, stopCh)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got: %v", tc.want, err)
			}
			close(stopCh)

			// The corrupt file must still be there: recovery never mutates
			// or deletes the snapshot it could not read.
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("corrupt snapshot was removed: %v", err)
			}
		})
	}
}

// TestSnapshotter_memberGenerationRoundTrip writes join events for the same
// node at increasing generations and verifies the persisted generations
// survive a replay, including the write-side guard against a lower-generation
// join moving a node backwards.
func TestSnapshotter_memberGenerationRoundTrip(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	var genMu sync.Mutex
	generations := map[string]LamportTime{}
	genFn := func(name string) (LamportTime, bool) {
		genMu.Lock()
		defer genMu.Unlock()
		g, ok := generations[name]
		return g, ok
	}

	inCh, snap, stopCh := newTestSnapshotter(t, path, nil, genFn)

	join := func(name string, gen LamportTime, port int) {
		genMu.Lock()
		generations[name] = gen
		genMu.Unlock()
		inCh <- MemberEvent{
			Type: EventMemberJoin,
			Members: []Member{{
				Name: name,
				Addr: []byte{127, 0, 0, 1},
				Port: uint16(port),
			}},
		}
	}
	join("foo", 3, 5000)
	join("bar", 8, 5001)
	for len(inCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}

	// A stale join (lower generation than already recorded) must not move
	// foo's generation backwards or overwrite its current address.
	join("foo", 2, 5999)
	for len(inCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}

	close(stopCh)
	snap.Wait()

	// The stream goroutine has exited by now, so reading the snapshotter's
	// in-memory maps is race-free. The stale join must not have moved the
	// generation backwards or changed the recorded address.
	if got := snap.aliveGenerations["foo"]; got != 3 {
		t.Fatalf("in-memory generation moved backwards: %d, want 3", got)
	}
	if got := snap.aliveNodes["foo"]; got != "127.0.0.1:5000" {
		t.Fatalf("in-memory address overwritten by stale join: %q", got)
	}

	_, snap2, stopCh2 := newTestSnapshotter(t, path, nil, genFn)
	defer close(stopCh2)

	if got := snap2.aliveGenerations["foo"]; got != 3 {
		t.Fatalf("bad foo generation: %d, want 3", got)
	}
	if got := snap2.aliveGenerations["bar"]; got != 8 {
		t.Fatalf("bad bar generation: %d, want 8", got)
	}
	prev := snap2.AliveNodes()
	if len(prev) != 2 {
		t.Fatalf("bad alive nodes: %#v", prev)
	}
}

// TestSnapshotter_pinnedCommitOnShutdown verifies the final shutdown commit
// persists the member set captured at shutdown, including its generations,
// rather than whatever partial state the streamed events happened to reach.
func TestSnapshotter_pinnedCommitOnShutdown(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	pinned := []pinnedMember{
		{Name: "alpha", Addr: "127.0.0.1:7001", Generation: 12},
		{Name: "beta", Addr: "127.0.0.1:7002", Generation: 15},
	}
	inCh, snap, stopCh := newTestSnapshotter(t, path,
		func() []pinnedMember { return pinned }, nil)

	// Stream an event for a node that is NOT in the pinned shutdown set.
	inCh <- MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{{
			Name: "stale",
			Addr: []byte{127, 0, 0, 1},
			Port: 9999,
		}},
	}
	for len(inCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}

	close(stopCh)
	snap.Wait()

	_, snap2, stopCh2 := newTestSnapshotter(t, path, nil, nil)
	defer close(stopCh2)

	names := map[string]bool{}
	for _, n := range snap2.AliveNodes() {
		names[n.Name] = true
	}
	if !names["alpha"] || !names["beta"] {
		t.Fatalf("pinned members missing: %#v", names)
	}
	if names["stale"] {
		t.Fatalf("stale streamed member must not survive the pinned commit")
	}
	if snap2.aliveGenerations["alpha"] != 12 || snap2.aliveGenerations["beta"] != 15 {
		t.Fatalf("pinned generations lost: %#v", snap2.aliveGenerations)
	}
}

// TestSnapshotter_atomicCommitKeepsPreviousVersion forces the final commit's
// temporary-file creation to fail by removing write permission on the
// directory. The previous snapshot must remain intact and recoverable.
func TestSnapshotter_atomicCommitKeepsPreviousVersion(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	inCh, snap, stopCh := newTestSnapshotter(t, path, nil, nil)
	inCh <- MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{{
			Name: "durable",
			Addr: []byte{127, 0, 0, 1},
			Port: 6000,
		}},
	}
	for len(inCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}

	// Block creation of the .compact temporary file.
	if err := os.Chmod(td, 0555); err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(td, 0755) })

	close(stopCh)
	snap.Wait()

	// No temporary file may be left behind.
	if _, err := os.Stat(path + tmpExt); !os.IsNotExist(err) {
		t.Fatalf("temporary snapshot file left behind: %v", err)
	}

	// The original snapshot must still parse and contain the prior member.
	_, snap2, stopCh2 := newTestSnapshotter(t, path, nil, nil)
	defer close(stopCh2)
	prev := snap2.AliveNodes()
	if len(prev) != 1 || prev[0].Name != "durable" {
		t.Fatalf("previous snapshot lost after failed commit: %#v", prev)
	}
}

// TestSnapshotter_noTempFileAfterCompaction drives a forced compaction and
// asserts the normal success path cleans up via an atomic rename, with no
// .compact file lingering.
func TestSnapshotter_noTempFileAfterCompaction(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	clock := new(LamportClock)
	stopCh := make(chan struct{})
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// Tiny limit forces repeated compactions while streaming.
	inCh, snap, err := NewSnapshotter(path, 1024, false, logger, clock, nil, stopCh)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := range 512 {
		inCh <- UserEvent{LTime: LamportTime(i), Name: "e"}
	}
	for len(inCh) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	close(stopCh)
	snap.Wait()

	if _, err := os.Stat(path + tmpExt); !os.IsNotExist(err) {
		t.Fatalf("compaction temp file left behind: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot missing after compaction: %v", err)
	}
}

// TestSnapshotter_leaveIdempotent hammers Leave from many goroutines and
// asserts the leave marker is recorded once and the resulting snapshot still
// resets recovery to an empty cluster.
func TestSnapshotter_leaveIdempotent(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "snap")

	inCh, snap, stopCh := newTestSnapshotter(t, path, nil, nil)
	inCh <- MemberEvent{
		Type: EventMemberJoin,
		Members: []Member{{
			Name: "foo",
			Addr: []byte{127, 0, 0, 1},
			Port: 5000,
		}},
	}
	for len(inCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			snap.Leave()
		})
	}
	wg.Wait()

	close(stopCh)
	snap.Wait()

	// Exactly one leave line must be present.
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := strings.Count(string(contents), "\nleave\n"); got != 1 {
		t.Fatalf("expected exactly one leave marker, got %d: %q", got, string(contents))
	}

	_, snap2, stopCh2 := newTestSnapshotter(t, path, nil, nil)
	defer close(stopCh2)
	if len(snap2.AliveNodes()) != 0 {
		t.Fatalf("leave snapshot must recover no nodes: %#v", snap2.AliveNodes())
	}
}

// TestSnapshotter_concurrentWritesAndShutdown floods events while shutdown
// races in from another goroutine. It must not panic, must finish Wait, and
// the resulting snapshot must replay cleanly as an all-or-nothing commit.
func TestSnapshotter_concurrentWritesAndShutdown(t *testing.T) {
	td := t.TempDir()

	for iter := range 8 {
		path := filepath.Join(td, fmt.Sprintf("snap-%d", iter))
		clock := new(LamportClock)
		stopCh := make(chan struct{})
		logger := log.New(os.Stderr, "", log.LstdFlags)
		inCh, snap, err := NewSnapshotter(path, 4096, false, logger, clock, nil, stopCh)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 2000 {
				select {
				case <-stopCh:
					return
				default:
				}
				select {
				case inCh <- UserEvent{LTime: LamportTime(i), Name: "flood"}:
				case <-stopCh:
					return
				}
			}
		})
		wg.Go(func() {
			close(stopCh)
		})

		wg.Wait()
		snap.Wait()

		// Recovery on whatever landed must be well-formed.
		stopCh2 := make(chan struct{})
		_, snap2, err := NewSnapshotter(path, snapshotSizeLimit, false,
			logger, clock, nil, stopCh2)
		if err != nil {
			t.Fatalf("replay after raced shutdown failed (must be all-or-nothing): %v", err)
		}
		close(stopCh2)
		snap2.Wait()
	}
}

// TestParseAliveLine exercises the versioned/legacy line parser directly,
// including node names containing spaces.
func TestParseAliveLine(t *testing.T) {
	cases := []struct {
		info     string
		strict   bool
		wantName string
		wantAddr string
		wantGen  LamportTime
		wantErr  bool
	}{
		{"node-a 127.0.0.1:5000 gen=7", true, "node-a", "127.0.0.1:5000", 7, false},
		{"node with spaces 10.0.0.1:80 gen=2", true, "node with spaces", "10.0.0.1:80", 2, false},
		{"legacy-node 127.0.0.1:5000", false, "legacy-node", "127.0.0.1:5000", 0, false},
		{"legacy-node 127.0.0.1:5000", true, "", "", 0, true},
		{"node-a 127.0.0.1:5000 gen=bad", true, "", "", 0, true},
		{"node-a gen=3", true, "", "", 0, true}, // gen consumed, no address left
		{"garbage", true, "", "", 0, true},      // no space-separated fields at all
	}

	for _, tc := range cases {
		name, addr, gen, err := parseAliveLine(tc.info, tc.strict)
		if (err != nil) != tc.wantErr {
			t.Fatalf("parseAliveLine(%q) err = %v, wantErr = %v", tc.info, err, tc.wantErr)
		}
		if err != nil {
			continue
		}
		if name != tc.wantName || addr != tc.wantAddr || gen != tc.wantGen {
			t.Fatalf("parseAliveLine(%q) = (%q, %q, %d), want (%q, %q, %d)",
				tc.info, name, addr, gen, tc.wantName, tc.wantAddr, tc.wantGen)
		}
	}
}
