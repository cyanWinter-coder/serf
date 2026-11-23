// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/hashicorp/go-metrics"
)

/*
Serf supports using a "snapshot" file that contains various
transactional data that is used to help Serf recover quickly
and gracefully from a failure. We append member events, as well
as the latest clock values to the file during normal operation,
and periodically checkpoint and roll over the file. During a restore,
we can replay the various member events to recall a list of known
nodes to re-join, as well as restore our clock values to avoid replaying
old events.
*/

const (
	// flushInterval is how often we force a flush of the snapshot file
	flushInterval = 500 * time.Millisecond

	// clockUpdateInterval is how often we fetch the current lamport time of the cluster and write to the snapshot file
	clockUpdateInterval = 500 * time.Millisecond

	// tmpExt is the extention we use for the temporary file during compaction
	tmpExt = ".compact"

	// snapshotErrorRecoveryInterval is how often we attempt to recover from
	// errors writing to the snapshot file.
	snapshotErrorRecoveryInterval = 30 * time.Second

	// eventChSize is the size of the event buffers between Serf and the
	// consuming application. If this is exhausted we will block Serf and Memberlist.
	eventChSize = 2048

	// shutdownFlushTimeout is the time limit to write pending events to the snapshot during a shutdown
	shutdownFlushTimeout = 250 * time.Millisecond

	// snapshotBytesPerNode is an estimated bytes per node to snapshot
	snapshotBytesPerNode = 128

	// snapshotCompactionThreshold is the threshold we apply to
	// the snapshot size estimate (nodes * bytes per node) before compacting.
	snapshotCompactionThreshold = 2

	// snapshotHeader is the first line of every versioned snapshot. It pins
	// the on-disk format so that recovery can reject files from the future
	// instead of silently interpreting them with the wrong grammar. The
	// version number in the header is the snapshot format version; legacy
	// headerless files are treated as version 0.
	snapshotHeader = "# serf-snapshot-v1\n"
)

var (
	// errSnapshotCorrupt is returned when a snapshot cannot be replayed.
	// NewSnapshotter wraps this error; callers must fail startup rather than
	// boot with an empty member list, since the previous good snapshot file
	// is left untouched on disk.
	errSnapshotCorrupt = errors.New("corrupt snapshot")

	// errSnapshotVersion is returned when a snapshot advertises a format
	// version that this build does not know how to read.
	errSnapshotVersion = errors.New("unsupported snapshot version")
)

// Snapshotter is responsible for ingesting events and persisting
// them to disk, and providing a recovery mechanism at start time.
type Snapshotter struct {
	// aliveNodes maps node name to its last known address.
	aliveNodes map[string]string

	// aliveGenerations maps node name to the member generation (the join
	// Lamport time) of the entry in aliveNodes. Recovery uses it to ignore
	// stale entries for a node that was superseded by a newer incarnation.
	aliveGenerations map[string]LamportTime

	clock          *LamportClock
	fh             *os.File
	buffered       *bufio.Writer
	inCh           <-chan Event
	streamCh       chan Event
	lastFlush      time.Time
	lastClock      LamportTime
	lastEventClock LamportTime
	lastQueryClock LamportTime
	leaveCh        chan struct{}

	// leaving is set once a leave has been recorded; all writes after that
	// point are suppressed. An atomic keeps the stream hot path lock-free.
	leaving atomic.Bool

	logger           *log.Logger
	minCompactSize   int64
	path             string
	offset           int64
	outCh            chan<- Event
	rejoinAfterLeave bool
	shutdownCh       <-chan struct{}
	waitCh           chan struct{}

	// commitOnce makes sure the final shutdown commit runs at most once, so
	// a leave marker and a plain shutdown commit can never be written twice
	// or interleave with a background compaction.
	commitOnce sync.Once

	// memberSnap returns the point-in-time member set (name, address and
	// generation) to pin into the final shutdown commit. It is optional:
	// snapshotter-only tests leave it nil and rely on streamed events.
	memberSnap func() []pinnedMember

	// memberGeneration looks up the current member generation for a node so
	// streamed join events can record which incarnation they belong to.
	memberGeneration func(name string) (LamportTime, bool)

	lastAttemptedCompaction time.Time
	metricLabels            []metrics.Label
}

// pinnedMember is the immutable view of one member captured when shutdown
// begins. No member event arriving after the pin can change it, so the final
// commit can never contain a member that was already stale at shutdown.
type pinnedMember struct {
	Name       string
	Addr       string
	Generation LamportTime
}

// PreviousNode is used to represent the previously known alive nodes
type PreviousNode struct {
	Name string
	Addr string
}

func (p PreviousNode) String() string {
	return fmt.Sprintf("%s: %s", p.Name, p.Addr)
}

// NewSnapshotter creates a new Snapshotter that records events up to a
// max byte size before rotating the file. It can also be used to
// recover old state. Snapshotter works by reading an event channel it returns,
// passing through to an output channel, and persisting relevant events to disk.
// Setting rejoinAfterLeave makes leave not clear the state, and can be used
// if you intend to rejoin the same cluster after a leave.
func NewSnapshotter(path string,
	minCompactSize int,
	rejoinAfterLeave bool,
	logger *log.Logger,
	clock *LamportClock,
	outCh chan<- Event,
	shutdownCh <-chan struct{}) (chan<- Event, *Snapshotter, error) {
	inCh := make(chan Event, eventChSize)
	streamCh := make(chan Event, eventChSize)

	// Try to open the file
	fh, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open snapshot: %v", err)
	}

	// Determine the offset
	info, err := fh.Stat()
	if err != nil {
		fh.Close()
		return nil, nil, fmt.Errorf("failed to stat snapshot: %v", err)
	}
	offset := info.Size()

	// Create the snapshotter
	snap := &Snapshotter{
		aliveNodes:       make(map[string]string),
		aliveGenerations: make(map[string]LamportTime),
		clock:            clock,
		fh:               fh,
		buffered:         bufio.NewWriter(fh),
		inCh:             inCh,
		streamCh:         streamCh,
		lastClock:        0,
		lastEventClock:   0,
		lastQueryClock:   0,
		leaveCh:          make(chan struct{}),
		logger:           logger,
		minCompactSize:   int64(minCompactSize),
		path:             path,
		offset:           offset,
		outCh:            outCh,
		rejoinAfterLeave: rejoinAfterLeave,
		shutdownCh:       shutdownCh,
		waitCh:           make(chan struct{}),
	}

	// Recover the last known state. A failure here means the on-disk file is
	// corrupt or from an unsupported format version: fail startup instead of
	// silently booting into an empty cluster. The file itself is untouched.
	if err := snap.replay(); err != nil {
		fh.Close()
		return nil, nil, fmt.Errorf("failed to recover snapshot: %w", err)
	}

	// Brand-new files get a version header so that every snapshot written by
	// this build is self-describing. The write is buffered; if the process
	// dies before the first flush there is simply no file to read next time.
	if offset == 0 {
		n, err := snap.buffered.WriteString(snapshotHeader)
		if err != nil {
			fh.Close()
			return nil, nil, fmt.Errorf("failed to write snapshot header: %v", err)
		}
		snap.offset += int64(n)
	}

	// Start handling new commands
	go snap.teeStream()
	go snap.stream()
	return inCh, snap, nil
}

// LastClock returns the last known clock time
func (s *Snapshotter) LastClock() LamportTime {
	return s.lastClock
}

// LastEventClock returns the last known event clock time
func (s *Snapshotter) LastEventClock() LamportTime {
	return s.lastEventClock
}

// LastQueryClock returns the last known query clock time
func (s *Snapshotter) LastQueryClock() LamportTime {
	return s.lastQueryClock
}

// AliveNodes returns the last known alive nodes
func (s *Snapshotter) AliveNodes() []*PreviousNode {
	// Copy the previously known
	previous := make([]*PreviousNode, 0, len(s.aliveNodes))
	for name, addr := range s.aliveNodes {
		previous = append(previous, &PreviousNode{name, addr})
	}

	// Randomize the order, prevents hot shards
	for i := range previous {
		j := rand.Intn(i + 1)
		previous[i], previous[j] = previous[j], previous[i]
	}
	return previous
}

// Wait is used to wait until the snapshotter finishes shut down
func (s *Snapshotter) Wait() {
	<-s.waitCh
}

// Leave is used to remove known nodes to prevent a restart from
// causing a join. Otherwise nodes will re-join after leaving!
//
// It is safe to call repeatedly and from multiple goroutines: the leave
// marker is delivered at most once. If shutdown has already started the
// call is a no-op and the shutdown commit decides the persisted contents.
func (s *Snapshotter) Leave() {
	if !s.leaving.CompareAndSwap(false, true) {
		return
	}
	select {
	case s.leaveCh <- struct{}{}:
	case <-s.shutdownCh:
	}
}

// teeStream is a long running routine that is used to copy events
// to the output channel and the internal event handler.
func (s *Snapshotter) teeStream() {
	flushEvent := func(e Event) {
		// Forward to the internal stream, do not block
		select {
		case s.streamCh <- e:
		default:
		}

		// Forward the event immediately, do not block
		if s.outCh != nil {
			select {
			case s.outCh <- e:
			default:
			}
		}
	}

OUTER:
	for {
		select {
		case e := <-s.inCh:
			flushEvent(e)
		case <-s.shutdownCh:
			break OUTER
		}
	}

	// Drain any remaining events before exiting
	for {
		select {
		case e := <-s.inCh:
			flushEvent(e)
		default:
			return
		}
	}
}

// stream is a long running routine that is used to handle events
func (s *Snapshotter) stream() {
	clockTicker := time.NewTicker(clockUpdateInterval)
	defer clockTicker.Stop()

	// flushEvent is used to handle writing out an event
	flushEvent := func(e Event) {
		// Stop recording events after a leave is issued
		if s.leaving.Load() {
			return
		}
		switch typed := e.(type) {
		case MemberEvent:
			s.processMemberEvent(typed)
		case UserEvent:
			s.processUserEvent(typed)
		case *Query:
			s.processQuery(typed)
		default:
			s.logger.Printf("[ERR] serf: Unknown event to snapshot: %#v", e)
		}
	}

	for {
		select {
		case <-s.leaveCh:
			// leaving was already marked by Leave(); just make sure nothing
			// else can sneak in behind us.
			s.leaving.Store(true)

			// If we plan to re-join, keep our state
			if !s.rejoinAfterLeave {
				s.aliveNodes = make(map[string]string)
				s.aliveGenerations = make(map[string]LamportTime)
			}
			s.tryAppend("leave\n")
			if err := s.buffered.Flush(); err != nil {
				s.logger.Printf("[ERR] serf: failed to flush leave to snapshot: %v", err)
			}
			if err := s.fh.Sync(); err != nil {
				s.logger.Printf("[ERR] serf: failed to sync leave to snapshot: %v", err)
			}

		case e := <-s.streamCh:
			flushEvent(e)

		case <-clockTicker.C:
			s.updateClock()

		case <-s.shutdownCh:
			// Exactly one final commit, regardless of how Shutdown was
			// invoked or whether a leave raced it.
			s.commitOnce.Do(s.commitFinal)
			close(s.waitCh)
			return
		}
	}
}

// commitFinal performs the single shutdown commit. The persisted contents
// depend on whether a leave was recorded, but the commit itself runs once:
//
//   - leave without rejoin: a leave marker is already on disk; the final
//     commit just drains and flushes pending lines.
//   - leave with rejoin, or plain shutdown: the currently visible member set
//     is pinned (including member generations), all clocks are checkpointed,
//     and the whole file is rewritten atomically so a crash mid-write leaves
//     the previous snapshot intact.
func (s *Snapshotter) commitFinal() {
	// Setup a timeout
	flushTimeout := time.After(shutdownFlushTimeout)

	// Snapshot the clock before anything else can mutate it.
	s.updateClock()

	// Clear out the buffers
FLUSH:
	for {
		select {
		case e := <-s.streamCh:
			if s.leaving.Load() {
				continue
			}
			switch typed := e.(type) {
			case MemberEvent:
				s.processMemberEvent(typed)
			case UserEvent:
				s.processUserEvent(typed)
			case *Query:
				s.processQuery(typed)
			}
		case <-flushTimeout:
			break FLUSH
		default:
			break FLUSH
		}
	}

	// Pin the last visible member set and its intent version before writing
	// anything. A leave that cleared the member set takes precedence; with
	// rejoinAfterLeave the pinned set is still meaningful.
	if !s.leaving.Load() && s.memberSnap != nil {
		s.pinMembers(s.memberSnap())
	}

	// Rewrite the full state atomically. If anything fails before the rename
	// the previous snapshot stays in place, so recovery always sees either
	// the old or the new complete file.
	if err := s.compact(); err != nil {
		s.logger.Printf("[ERR] serf: failed to commit final snapshot, keeping previous version: %v", err)
		// compact() re-opens the previous file on failure; only flush if it
		// managed to do so, otherwise there is nothing left to write to.
		if s.fh != nil && s.buffered != nil {
			if err := s.buffered.Flush(); err != nil {
				s.logger.Printf("[ERR] serf: failed to flush snapshot: %v", err)
			}
		}
	}
	if s.fh != nil {
		if err := s.fh.Sync(); err != nil {
			s.logger.Printf("[ERR] serf: failed to sync snapshot: %v", err)
		}
		s.fh.Close()
		s.fh = nil
	}
}

// pinMembers replaces the streamed member view with the shutdown-time member
// set. The final commit must describe exactly what was visible when shutdown
// began, so streamed entries that are not part of the pinned set are dropped
// rather than merged in.
func (s *Snapshotter) pinMembers(pinned []pinnedMember) {
	alive := make(map[string]string, len(pinned))
	generations := make(map[string]LamportTime, len(pinned))
	for _, m := range pinned {
		alive[m.Name] = m.Addr
		generations[m.Name] = m.Generation
	}
	s.aliveNodes = alive
	s.aliveGenerations = generations
}

// processMemberEvent is used to handle a single member event
func (s *Snapshotter) processMemberEvent(e MemberEvent) {
	switch e.Type {
	case EventMemberJoin:
		for _, mem := range e.Members {
			addr := net.TCPAddr{IP: mem.Addr, Port: int(mem.Port)}
			addrStr := addr.String()

			// Record the member generation (its join Lamport time). The
			// member state lookup is optional; snapshotter-only tests do not
			// wire one up and default to generation zero.
			generation := LamportTime(0)
			if s.memberGeneration != nil {
				if gen, ok := s.memberGeneration(mem.Name); ok {
					generation = gen
				}
			}

			// Never move a node's generation backwards. Events for one node
			// are ordered by Lamport time, so a lower generation means the
			// join belongs to an older incarnation that has already been
			// superseded: skip the whole entry, including its address.
			if current, ok := s.aliveGenerations[mem.Name]; ok && current > generation {
				continue
			}

			s.aliveNodes[mem.Name] = addrStr
			s.aliveGenerations[mem.Name] = generation
			s.tryAppend(fmt.Sprintf("alive: %s %s gen=%d\n", mem.Name, addrStr, generation))
		}

	case EventMemberLeave:
		fallthrough
	case EventMemberFailed:
		for _, mem := range e.Members {
			delete(s.aliveNodes, mem.Name)
			delete(s.aliveGenerations, mem.Name)
			s.tryAppend(fmt.Sprintf("not-alive: %s\n", mem.Name))
		}
	}
	s.updateClock()
}

// updateClock is called periodically to check if we should udpate our
// clock value. This is done after member events but should also be done
// periodically due to race conditions with join and leave intents
func (s *Snapshotter) updateClock() {
	lastSeen := s.clock.Time() - 1
	if lastSeen > s.lastClock {
		s.lastClock = lastSeen
		s.tryAppend(fmt.Sprintf("clock: %d\n", s.lastClock))
	}
}

// processUserEvent is used to handle a single user event
func (s *Snapshotter) processUserEvent(e UserEvent) {
	// Ignore old clocks
	if e.LTime <= s.lastEventClock {
		return
	}
	s.lastEventClock = e.LTime
	s.tryAppend(fmt.Sprintf("event-clock: %d\n", e.LTime))
}

// processQuery is used to handle a single query event
func (s *Snapshotter) processQuery(q *Query) {
	// Ignore old clocks
	if q.LTime <= s.lastQueryClock {
		return
	}
	s.lastQueryClock = q.LTime
	s.tryAppend(fmt.Sprintf("query-clock: %d\n", q.LTime))
}

// tryAppend will invoke append line but will not return an error
func (s *Snapshotter) tryAppend(l string) {
	if err := s.appendLine(l); err != nil {
		s.logger.Printf("[ERR] serf: Failed to update snapshot: %v", err)
		now := time.Now()
		if now.Sub(s.lastAttemptedCompaction) > snapshotErrorRecoveryInterval {
			s.lastAttemptedCompaction = now
			s.logger.Printf("[INFO] serf: Attempting compaction to recover from error...")
			err = s.compact()
			if err != nil {
				s.logger.Printf("[ERR] serf: Compaction failed, will reattempt after %v: %v", snapshotErrorRecoveryInterval, err)
			} else {
				s.logger.Printf("[INFO] serf: Finished compaction, successfully recovered from error state")
			}
		}
	}
}

// appendLine is used to append a line to the existing log
func (s *Snapshotter) appendLine(l string) error {
	defer metrics.MeasureSinceWithLabels([]string{"serf", "snapshot", "appendLine"}, time.Now(), s.metricLabels)

	n, err := s.buffered.WriteString(l)
	if err != nil {
		return err
	}

	// Check if we should flush
	now := time.Now()
	if now.Sub(s.lastFlush) > flushInterval {
		s.lastFlush = now
		if err := s.buffered.Flush(); err != nil {
			return err
		}
	}

	// Check if a compaction is necessary
	s.offset += int64(n)
	if s.offset > s.snapshotMaxSize() {
		return s.compact()
	}
	return nil
}

// snapshotMaxSize computes the maximum size and is used to force periodic compaction.
func (s *Snapshotter) snapshotMaxSize() int64 {
	nodes := int64(len(s.aliveNodes))
	estSize := nodes * snapshotBytesPerNode
	threshold := max(
		// Apply a minimum threshold to avoid frequent compaction
		estSize*snapshotCompactionThreshold, s.minCompactSize)
	return threshold
}

// Compact is used to compact the snapshot once it is too large.
//
// The compaction is crash-safe: the new contents are written to a temporary
// file, flushed and fsynced first, and only then swapped into place with a
// single atomic rename. Any failure before the rename leaves the previous
// snapshot file untouched and re-opens its handle so the snapshotter keeps
// working.
func (s *Snapshotter) compact() error {
	defer metrics.MeasureSinceWithLabels([]string{"serf", "snapshot", "compact"}, time.Now(), s.metricLabels)

	// Write the complete new snapshot to a sibling temporary file.
	newPath := s.path + tmpExt
	fh, err := os.OpenFile(newPath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new snapshot: %v", err)
	}

	// On any error path the temp file must not linger and the original file
	// handle must be restored before returning.
	committed := false
	defer func() {
		if !committed {
			fh.Close()
			if err := os.Remove(newPath); err != nil && !os.IsNotExist(err) {
				s.logger.Printf("[WARN] serf: failed to remove abandoned snapshot temp file %s: %v", newPath, err)
			}
			s.reopenAfterFailure()
		}
	}()

	buf := bufio.NewWriter(fh)

	// Every rewritten snapshot carries the current format version.
	if _, err := buf.WriteString(snapshotHeader); err != nil {
		return fmt.Errorf("failed to write snapshot header: %v", err)
	}

	// Persist the leave marker ahead of the member list so recovery resets
	// before applying anything, matching the append-only file ordering.
	leaving := s.leaving.Load() && !s.rejoinAfterLeave
	if leaving {
		if _, err := buf.WriteString("leave\n"); err != nil {
			return fmt.Errorf("failed to write leave marker: %v", err)
		}
	}

	// Write out the live nodes with their member generation. After a leave
	// without rejoin the member set has been cleared and nothing else may be
	// persisted, or recovery would resurrect the cluster we deliberately left.
	var offset int64
	if !leaving {
		for name, addr := range s.aliveNodes {
			line := fmt.Sprintf("alive: %s %s gen=%d\n", name, addr, s.aliveGenerations[name])
			n, err := buf.WriteString(line)
			if err != nil {
				return fmt.Errorf("failed to write alive node: %v", err)
			}
			offset += int64(n)
		}

		// Write out the clocks
		for _, entry := range []struct {
			prefix string
			value  LamportTime
		}{
			{"clock", s.lastClock},
			{"event-clock", s.lastEventClock},
			{"query-clock", s.lastQueryClock},
		} {
			line := fmt.Sprintf("%s: %d\n", entry.prefix, entry.value)
			n, err := buf.WriteString(line)
			if err != nil {
				return fmt.Errorf("failed to write %s: %v", entry.prefix, err)
			}
			offset += int64(n)
		}
	}

	// Flush the new snapshot and force it onto disk before the rename, so a
	// crash can never expose an empty or partially written replacement.
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush new snapshot: %v", err)
	}
	if err := fh.Sync(); err != nil {
		return fmt.Errorf("failed to fsync new snapshot: %v", err)
	}
	if err := fh.Close(); err != nil {
		return fmt.Errorf("failed to close new snapshot: %v", err)
	}

	// The old handle must be closed before the rename: Windows refuses to
	// rename onto a path that still has an open handle. Errors here only mean
	// buffered data may not have made it out; the file itself stays valid.
	if s.buffered != nil {
		_ = s.buffered.Flush()
		s.buffered = nil
	}
	if s.fh != nil {
		_ = s.fh.Close()
		s.fh = nil
	}

	// Single atomic swap. On both Unix (rename(2)) and Windows (MoveFileEx
	// with replace) this either installs the complete new file or leaves the
	// old one in place; there is no remove-then-rename window to lose the
	// snapshot in.
	if err := os.Rename(newPath, s.path); err != nil {
		return fmt.Errorf("failed to install new snapshot: %v", err)
	}
	committed = true

	// Best effort: make sure the directory entry itself is durable. Not all
	// platforms support syncing an open directory, so ignore failures.
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}

	// Re-open the freshly installed snapshot for further appends.
	reopened, err := os.OpenFile(s.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open compacted snapshot: %v", err)
	}
	s.fh = reopened
	s.buffered = bufio.NewWriter(reopened)
	s.offset = offset
	s.lastFlush = time.Now()
	return nil
}

// reopenAfterFailure restores a usable file handle after a failed compaction.
// The original snapshot was never renamed away, so reopening its path gives
// back the previous complete version. A failure here is fatal to the writer
// and is surfaced on the next append attempt.
func (s *Snapshotter) reopenAfterFailure() {
	if s.fh != nil {
		return
	}
	fh, err := os.OpenFile(s.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		s.logger.Printf("[ERR] serf: failed to reopen snapshot after failed compaction: %v", err)
		return
	}
	s.fh = fh
	s.buffered = bufio.NewWriter(fh)
}

// replay is used to seek to reset our internal state by replaying
// the snapshot file. It is used at initialization time to read old
// state.
//
// Files carrying a version header are replayed strictly: an unrecognized
// line, an unparseable clock or a member generation that moves backwards
// makes replay fail so the caller refuses to start instead of booting an
// empty cluster. Headerless files are treated as legacy version 0 and keep
// the historical lenient parsing so upgrades from older releases work.
func (s *Snapshotter) replay() error {
	// Seek to the beginning
	if _, err := s.fh.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(s.fh)

	// The first line selects the parsing mode. A version header enables
	// strict validation; anything else means a legacy version-0 file.
	firstLine, readErr := reader.ReadString('\n')
	if readErr == io.EOF && firstLine == "" {
		// Empty file, nothing to recover.
		return s.seekToEnd()
	} else if readErr != nil && readErr != io.EOF {
		return fmt.Errorf("failed to read snapshot header: %w", readErr)
	}

	// versioned selects strict (v1+) parsing once a recognized header has
	// been seen; headerless files stay in lenient legacy mode.
	versioned := false
	switch {
	case firstLine == snapshotHeader:
		// A header must end with a newline; anything shorter is a torn
		// write of the header itself.
		if readErr == io.EOF {
			return fmt.Errorf("%w: truncated snapshot header", errSnapshotCorrupt)
		}
		versioned = true

	case strings.HasPrefix(firstLine, "# serf-snapshot-v"):
		// A header from a build newer than this one: do not guess its
		// grammar, fail loudly instead.
		return fmt.Errorf("%w: %q", errSnapshotVersion, strings.TrimSpace(firstLine))

	default:
		// A headerless first line means a legacy version-0 file. If the
		// file ended right there without a newline, treat it leniently the
		// way the historical parser did.
		line := strings.TrimRight(firstLine, "\n")
		if line != "" {
			if err := s.replayLine(line, false); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return s.seekToEnd()
		}
	}

	// Read each remaining line.
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("failed to read snapshot: %w", readErr)
		}

		// A line without its terminating newline is a torn write. In a
		// versioned snapshot that is corruption; legacy files tolerate it
		// the way they always have.
		if readErr == io.EOF && line != "" {
			if versioned {
				return fmt.Errorf("%w: truncated final line %q", errSnapshotCorrupt, strings.TrimSpace(line))
			}
			break
		}
		if readErr == io.EOF {
			break
		}

		// Skip the newline and replay the line.
		if err := s.replayLine(line[:len(line)-1], versioned); err != nil {
			return err
		}
	}

	return s.seekToEnd()
}

// seekToEnd positions the snapshot file handle for new appends.
func (s *Snapshotter) seekToEnd() error {
	if _, err := s.fh.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// replayLine applies one snapshot line to the in-memory state. In strict
// (versioned) mode every parse failure aborts recovery; legacy lines only
// log a warning and are skipped.
func (s *Snapshotter) replayLine(line string, strict bool) error {
	// failf reports a parse failure according to the parsing mode. The
	// sentinel is attached with %w so callers can use errors.Is to detect
	// corruption regardless of the explanatory message.
	failf := func(format string, args ...any) error {
		msg := fmt.Sprintf(format, args...)
		if strict {
			return fmt.Errorf("%w: %s", errSnapshotCorrupt, msg)
		}
		s.logger.Printf("[WARN] serf: %s", msg)
		return nil
	}

	// Switch on the prefix
	if after, ok := strings.CutPrefix(line, "alive: "); ok {
		name, addr, gen, err := parseAliveLine(after, strict)
		if err != nil {
			return failf("failed to parse alive entry %q: %v", line, err)
		}

		// Validate the member generation: the snapshot is append-only, so
		// successive alive entries for one node must never move backwards.
		// A lower generation means an older incarnation was recorded after
		// a newer one, which cannot happen in an intact file.
		if current, ok := s.aliveGenerations[name]; ok && gen < current {
			return failf("member %s generation moved backwards: %d < %d", name, gen, current)
		}
		s.aliveNodes[name] = addr
		s.aliveGenerations[name] = gen
		return nil

	} else if after, ok := strings.CutPrefix(line, "not-alive: "); ok {
		name := after
		delete(s.aliveNodes, name)
		delete(s.aliveGenerations, name)
		return nil

	} else if after, ok := strings.CutPrefix(line, "clock: "); ok {
		timeInt, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			return failf("failed to convert clock time: %v", err)
		}
		s.lastClock = LamportTime(timeInt)
		return nil

	} else if after, ok := strings.CutPrefix(line, "event-clock: "); ok {
		timeInt, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			return failf("failed to convert event clock time: %v", err)
		}
		s.lastEventClock = LamportTime(timeInt)
		return nil

	} else if after, ok := strings.CutPrefix(line, "query-clock: "); ok {
		timeInt, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			return failf("failed to convert query clock time: %v", err)
		}
		s.lastQueryClock = LamportTime(timeInt)
		return nil

	} else if strings.HasPrefix(line, "coordinate: ") {
		return nil // Ignores any coordinate persistence from old snapshots, serf should re-converge

	} else if line == "leave" {
		// Ignore a leave if we plan on re-joining
		if s.rejoinAfterLeave {
			s.logger.Printf("[INFO] serf: Ignoring previous leave in snapshot")
			return nil
		}
		s.aliveNodes = make(map[string]string)
		s.aliveGenerations = make(map[string]LamportTime)
		s.lastClock = 0
		s.lastEventClock = 0
		s.lastQueryClock = 0
		return nil

	} else if strings.HasPrefix(line, "#") {
		// Skip comment lines (the version header is consumed before replay).
		return nil
	}

	return failf("unrecognized snapshot line: %v", line)
}

// parseAliveLine splits the payload of an "alive:" line into node name,
// address and member generation. Versioned lines always carry a gen=<n>
// suffix; legacy lines do not and report generation zero. Node names may
// contain spaces, so splitting is done from the right.
func parseAliveLine(info string, strict bool) (name, addr string, gen LamportTime, err error) {
	// The address is always the last whitespace-separated field and never
	// contains a space. Split it off first.
	addrIdx := strings.LastIndex(info, " ")
	if addrIdx == -1 {
		return "", "", 0, fmt.Errorf("missing address")
	}
	addr = info[addrIdx+1:]
	head := info[:addrIdx]

	// In versioned files the address is followed by a generation, so the
	// field we just split off must parse as gen=<n>.
	if genField, ok := strings.CutPrefix(addr, "gen="); ok {
		genInt, parseErr := strconv.ParseUint(genField, 10, 64)
		if parseErr != nil {
			return "", "", 0, fmt.Errorf("invalid generation %q: %v", genField, parseErr)
		}
		gen = LamportTime(genInt)

		// With the generation consumed, the real address is the last field
		// of the remaining head.
		innerIdx := strings.LastIndex(head, " ")
		if innerIdx == -1 {
			return "", "", 0, fmt.Errorf("failed to parse address")
		}
		name = head[:innerIdx]
		addr = head[innerIdx+1:]
		return name, addr, gen, nil
	}

	if strict {
		return "", "", 0, fmt.Errorf("missing member generation")
	}

	// Legacy two-field line "<name> <addr>": the first split already
	// separated the address, so head is the full node name (which may itself
	// contain spaces).
	return head, addr, 0, nil
}
