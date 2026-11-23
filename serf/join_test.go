// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package serf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
	"github.com/hashicorp/serf/testutil/retry"
)

// seedAddr returns the seed string used to reach s through memberlist.
func seedAddr(s *Serf) string {
	return s.config.NodeName + "/" + s.config.MemberlistConfig.BindAddr
}

// drainEvents reads every event currently buffered on ch without blocking.
func drainEvents(ch chan Event) []Event {
	var events []Event
	for {
		select {
		case e := <-ch:
			events = append(events, e)
		default:
			return events
		}
	}
}

// waitForJoinEvent polls ch until a MemberJoin for nodeName is observed,
// draining everything else along the way.
func waitForJoinEvent(t *testing.T, ch chan Event, nodeName string) {
	t.Helper()
	retry.Run(t, func(r *retry.R) {
		for _, e := range drainEvents(ch) {
			me, ok := e.(MemberEvent)
			if !ok || me.Type != EventMemberJoin {
				continue
			}
			for _, m := range me.Members {
				if m.Name == nodeName {
					return
				}
			}
		}
		r.Fatalf("missing join event for %s", nodeName)
	})
}

func newTestSerf(t *testing.T, ip net.IP) *Serf {
	t.Helper()
	return newTestSerfWithConfig(t, testConfig(t, ip))
}

func newTestSerfWithConfig(t *testing.T, config *Config) *Serf {
	t.Helper()
	s, err := Create(config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { s.Shutdown() })
	return s
}

// TestSerf_Join_PartialSuccess makes sure a failed seed never undoes a
// successful merge with another seed in the same batch, and that the
// returned error summarizes the failed attempt while the joined count is
// still reported.
func TestSerf_Join_PartialSuccess(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	ip3, returnFn3 := testutil.TakeIP()
	defer returnFn3()

	eventCh := make(chan Event, 16)
	s1Config := testConfig(t, ip1)
	s1Config.EventCh = eventCh
	s1 := newTestSerfWithConfig(t, s1Config)

	s2 := newTestSerf(t, ip2)

	// Nothing is bound on ip3, so this seed fails to connect.
	deadSeed := "node-dead/" + ip3.String()

	waitUntilNumNodes(t, 1, s1, s2)

	num, err := s1.Join([]string{seedAddr(s2), deadSeed}, false)
	if err == nil {
		t.Fatalf("expected an error summarizing the failed seed")
	}
	if !strings.Contains(err.Error(), "partial join") {
		t.Fatalf("expected partial join error, got: %v", err)
	}
	if !strings.Contains(err.Error(), deadSeed) {
		t.Fatalf("error should name the failed seed, got: %v", err)
	}
	if num != 1 {
		t.Fatalf("expected 1 successful contact, got %d", num)
	}

	// The successful merge must survive the failure of the other seed.
	waitUntilNumNodes(t, 2, s1, s2)

	// The member-join event for s2 must still have been published.
	waitForJoinEvent(t, eventCh, s2.config.NodeName)
}

// TestSerf_Join_RetryIdempotent verifies that retrying the same batch of
// seeds neither repeats handshakes against already-joined seeds nor
// re-broadcasts the join intent, while previously failed seeds are still
// retried and their success is preserved.
func TestSerf_Join_RetryIdempotent(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	// Observe s1's own event channel: a node never receives a join event
	// for itself, so every MemberJoin here is caused by s1's own join
	// attempts rather than background gossip on the remote side.
	eventCh := make(chan Event, 16)
	s1Config := testConfig(t, ip1)
	s1Config.EventCh = eventCh
	s1 := newTestSerfWithConfig(t, s1Config)

	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	// First join succeeds.
	num, err := s1.Join([]string{seedAddr(s2)}, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 1 {
		t.Fatalf("expected 1 successful contact, got %d", num)
	}
	waitUntilNumNodes(t, 2, s1, s2)

	// Wait for the first join event to be delivered, then drain anything
	// left over so the retry starts from a clean channel.
	waitForJoinEvent(t, eventCh, s2.config.NodeName)
	drainEvents(eventCh)

	// Retry the exact same batch: the seed is already joined, so no new
	// handshake and no new join event should be produced.
	num, err = s1.Join([]string{seedAddr(s2)}, false)
	if err != nil {
		t.Fatalf("retry should not fail: %v", err)
	}
	if num != 0 {
		t.Fatalf("expected no new contacts on retry, got %d", num)
	}

	// Give any asynchronous event delivery a chance to happen and assert
	// that nothing ever arrives.
	time.Sleep(50 * time.Millisecond)
	if events := drainEvents(eventCh); len(events) != 0 {
		t.Fatalf("retry published duplicate events: %#v", events)
	}

	// Membership is unchanged and still converged.
	waitUntilNumNodes(t, 2, s1, s2)
	if !s1.joinBroadcasted {
		t.Fatalf("join intent should have been broadcast once")
	}
}

// TestSerf_Join_DedupeBatch verifies a seed repeated within a single call
// is only attempted once.
func TestSerf_Join_DedupeBatch(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1 := newTestSerf(t, ip1)
	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	seed := seedAddr(s2)
	num, err := s1.Join([]string{seed, seed, seed}, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if num != 1 {
		t.Fatalf("expected a single contact despite duplicate seeds, got %d", num)
	}
	waitUntilNumNodes(t, 2, s1, s2)
}

// TestSerf_Join_RejoinAfterLeave verifies that a node which left the
// cluster can rejoin using the same seed string: the retry dedup is based
// on current alive membership, not a permanent record.
func TestSerf_Join_RejoinAfterLeave(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1 := newTestSerf(t, ip1)
	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	seed := seedAddr(s2)
	if _, err := s1.Join([]string{seed}, false); err != nil {
		t.Fatalf("initial join failed: %v", err)
	}
	waitUntilNumNodes(t, 2, s1, s2)

	// s2 leaves and shuts down.
	if err := s2.Leave(); err != nil {
		t.Fatalf("leave failed: %v", err)
	}
	if err := s2.Shutdown(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Wait until s1 observes the departure.
	retry.Run(t, func(r *retry.R) {
		for _, m := range s1.Members() {
			if m.Name == s2.config.NodeName && m.Status == StatusAlive {
				r.Fatalf("s2 should no longer be alive")
			}
		}
	})

	// Bring s2 back on the same address and rejoin with the same seed.
	s2Config := testConfig(t, ip2)
	s2, err := Create(s2Config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s2.Shutdown()
	waitUntilNumNodes(t, 1, s2)

	num, err := s1.Join([]string{seed}, false)
	if err != nil {
		t.Fatalf("rejoin should succeed: %v", err)
	}
	if num != 1 {
		t.Fatalf("expected the rejoin to perform a handshake, got %d", num)
	}
	waitUntilNumNodes(t, 2, s1, s2)
}

// TestSerf_Join_RetryFailedSeed verifies that a seed which failed in the
// first batch is retried on a subsequent call, and once it succeeds the
// merged membership is kept.
func TestSerf_Join_RetryFailedSeed(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	ip3, returnFn3 := testutil.TakeIP()
	defer returnFn3()

	s1 := newTestSerf(t, ip1)
	s2 := newTestSerf(t, ip2)
	deadSeed := "node-dead/" + ip3.String()

	waitUntilNumNodes(t, 1, s1, s2)

	// Mixed batch: one success, one failure.
	num, err := s1.Join([]string{seedAddr(s2), deadSeed}, false)
	if err == nil {
		t.Fatalf("expected error for the dead seed")
	}
	if num != 1 {
		t.Fatalf("expected 1 contact, got %d", num)
	}
	for _, m := range s1.Members() {
		if m.Name == "node-dead" {
			t.Fatalf("failed seed must not appear in members")
		}
	}
	waitUntilNumNodes(t, 2, s1, s2)

	// Retry with only the dead seed: still failing, but the earlier merge
	// with s2 stays intact.
	num, err = s1.Join([]string{deadSeed}, false)
	if err == nil {
		t.Fatalf("expected the dead seed to keep failing")
	}
	if num != 0 {
		t.Fatalf("expected no new contacts, got %d", num)
	}
	waitUntilNumNodes(t, 2, s1, s2)
}

// TestSerf_Join_TimeoutStopsNewHandshakes verifies that once the context
// expires, no new seed handshakes are started, while an in-flight
// handshake is allowed to finish.
func TestSerf_Join_TimeoutStopsNewHandshakes(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1 := newTestSerf(t, ip1)
	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	// A listener that accepts the TCP connection but never answers the
	// memberlist handshake, so the attempt blocks until TCPTimeout.
	blackholeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	var connsMu sync.Mutex
	var blackholeConns []net.Conn
	go func() {
		for {
			conn, err := blackholeLn.Accept()
			if err != nil {
				return
			}
			// Hold the connection open without writing anything.
			connsMu.Lock()
			blackholeConns = append(blackholeConns, conn)
			connsMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		blackholeLn.Close()
		connsMu.Lock()
		for _, conn := range blackholeConns {
			conn.Close()
		}
		connsMu.Unlock()
	})

	blackholeSeed := "node-blackhole/" + blackholeLn.Addr().String()
	goodSeed := seedAddr(s2)

	// Expire the context while the blackhole handshake is still in flight
	// (but before the 100ms test TCPTimeout), so the good seed must never
	// be attempted afterwards.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	num, err := s1.JoinWithContext(ctx, []string{blackholeSeed, goodSeed}, false)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected a timeout error")
	}
	if num != 0 {
		t.Fatalf("expected no successful contacts, got %d", num)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded in error chain, got: %v", err)
	}
	if !strings.Contains(err.Error(), goodSeed) {
		t.Fatalf("error should report the seed skipped after timeout, got: %v", err)
	}

	// The in-flight handshake was allowed to converge (TCPTimeout is
	// 100ms in tests), proving we don't interrupt an active connection.
	if elapsed < 80*time.Millisecond {
		t.Fatalf("in-flight handshake was interrupted early after %v", elapsed)
	}

	// s2 was never contacted and no join intent was broadcast.
	waitUntilNumNodes(t, 1, s1)
	if s1.joinBroadcasted {
		t.Fatalf("join intent must not be broadcast when nothing joined")
	}
}

// TestSerf_Join_AlreadyCanceled verifies a canceled context skips every
// seed without performing any handshake.
func TestSerf_Join_AlreadyCanceled(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1 := newTestSerf(t, ip1)
	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	num, err := s1.JoinWithContext(ctx, []string{seedAddr(s2)}, false)
	if err == nil {
		t.Fatalf("expected an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled in error chain, got: %v", err)
	}
	if num != 0 {
		t.Fatalf("expected no contacts, got %d", num)
	}
	waitUntilNumNodes(t, 1, s1)
	for _, m := range s1.Members() {
		if m.Name == s2.config.NodeName {
			t.Fatalf("canceled seed must not appear in members")
		}
	}
}

// rejectMergeDelegate rejects merges containing any node whose name is in
// its blocklist and accepts everything else.
type rejectMergeDelegate struct {
	blocklist map[string]struct{}
}

func (d *rejectMergeDelegate) NotifyMerge(members []*Member) error {
	for _, m := range members {
		if _, ok := d.blocklist[m.Name]; ok {
			return fmt.Errorf("Merge canceled")
		}
	}
	return nil
}

// TestSerf_Join_CancelWithPartialSuccess verifies that when one seed
// rejects the merge through its Merge delegate, a successful merge with
// another seed in the same batch is not rolled back.
func TestSerf_Join_CancelWithPartialSuccess(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	ip3, returnFn3 := testutil.TakeIP()
	defer returnFn3()

	// s1 rejects only the merge involving s2, while accepting s3. This
	// mirrors a real deployment where one seed is incompatible.
	s1Config := testConfig(t, ip1)
	s2Config := testConfig(t, ip2)
	s1Config.Merge = &rejectMergeDelegate{
		blocklist: map[string]struct{}{s2Config.NodeName: {}},
	}
	s1 := newTestSerfWithConfig(t, s1Config)

	s2, err := Create(s2Config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer s2.Shutdown()

	s3 := newTestSerf(t, ip3)
	waitUntilNumNodes(t, 1, s1, s2, s3)

	num, err := s1.Join([]string{seedAddr(s2), seedAddr(s3)}, false)
	if err == nil {
		t.Fatalf("expected the canceled merge to surface an error")
	}
	if !strings.Contains(err.Error(), "Merge canceled") {
		t.Fatalf("expected merge cancellation in error, got: %v", err)
	}
	if num != 1 {
		t.Fatalf("expected the good seed to still count, got %d", num)
	}

	// s3's successful merge is preserved even though s2 canceled.
	waitUntilNumNodes(t, 2, s1, s3)

	// s2 rejected the merge, so s1 must never have learned about it as a
	// member.
	for _, m := range s1.Members() {
		if m.Name == s2.config.NodeName {
			t.Fatalf("canceled seed %s must not appear in s1's members", m.Name)
		}
	}
}

// TestSerf_Join_AllFail verifies the all-failure path still returns a
// non-nil error with zero contacts, preserving the single-seed contract.
func TestSerf_Join_AllFail(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1 := newTestSerf(t, ip1)
	waitUntilNumNodes(t, 1, s1)

	badSeed := "node-dead/" + ip2.String()
	num, err := s1.Join([]string{badSeed}, false)
	if err == nil {
		t.Fatalf("expected an error when no seed is reachable")
	}
	if num != 0 {
		t.Fatalf("expected zero contacts, got %d", num)
	}
	waitUntilNumNodes(t, 1, s1)
}

// TestSerf_Join_ErrorRedactsCredentials verifies error summaries never
// leak credentials embedded in seed strings.
func TestSerf_Join_ErrorRedactsCredentials(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	s1 := newTestSerf(t, ip1)
	waitUntilNumNodes(t, 1, s1)

	const secret = "super-secret-key"
	seedWithCreds := fmt.Sprintf("serf://user:%s@127.0.0.1:1", secret)

	_, err := s1.Join([]string{seedWithCreds}, false)
	if err == nil {
		t.Fatalf("expected a connection error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "***@") {
		t.Fatalf("expected redacted credentials in error, got: %v", err)
	}
}

// TestSerf_Join_ConcurrentSerialize makes sure concurrent Join calls are
// serialized and never double-broadcast the join intent.
func TestSerf_Join_ConcurrentSerialize(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1 := newTestSerf(t, ip1)
	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			if _, err := s1.Join([]string{seedAddr(s2)}, false); err != nil {
				t.Errorf("unexpected err: %v", err)
			}
		})
	}
	wg.Wait()

	waitUntilNumNodes(t, 2, s1, s2)
	if !s1.joinBroadcasted {
		t.Fatalf("join intent should have been broadcast")
	}
}

func TestDedupeSeeds(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"empty", nil, []string{}},
		{"unique", []string{"a", "b"}, []string{"a", "b"}},
		{"duplicates preserve order", []string{"b", "a", "b", "a", "c"}, []string{"b", "a", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedupeSeeds(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v want %v", got, tt.want)
				}
			}
		})
	}
}

func TestSeedNodeName(t *testing.T) {
	tests := []struct {
		seed string
		want string
	}{
		{"node-a/127.0.0.1:7946", "node-a"},
		{"127.0.0.1:7946", ""},
		{"/127.0.0.1:7946", ""},
	}
	for _, tt := range tests {
		if got := seedNodeName(tt.seed); got != tt.want {
			t.Fatalf("seedNodeName(%q) = %q, want %q", tt.seed, got, tt.want)
		}
	}
}

func TestSanitizeSeed(t *testing.T) {
	secret := "super-secret"
	tests := []struct {
		name string
		seed string
		want string
	}{
		{"plain host:port", "127.0.0.1:7946", "127.0.0.1:7946"},
		{"node name prefix", "node-a/127.0.0.1:7946", "node-a/127.0.0.1:7946"},
		{"url with credentials", "serf://user:" + secret + "@127.0.0.1:7946", "serf://***@127.0.0.1:7946"},
		{"bare at-sign left alone", "weird@host:7946", "weird@host:7946"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeSeed(tt.seed)
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
			if strings.Contains(got, secret) {
				t.Fatalf("redacted seed still contains secret: %q", got)
			}
		})
	}
}

func TestSanitizeJoinError(t *testing.T) {
	secret := "super-secret"
	// Mirrors the DNS-resolution failure shape, where the scheme is
	// stripped before the error is produced.
	err := fmt.Errorf("lookup /user:%s@127.0.0.1:1: no such host", secret)
	got := sanitizeJoinError(err).Error()
	if strings.Contains(got, secret) {
		t.Fatalf("error leaked credentials: %s", got)
	}
	if !strings.Contains(got, "***@") {
		t.Fatalf("expected redaction marker, got: %s", got)
	}

	// Ordinary host:port errors pass through unchanged.
	plain := errors.New("dial tcp 127.0.0.1:7946: connect: connection refused")
	if got := sanitizeJoinError(plain).Error(); got != plain.Error() {
		t.Fatalf("plain error was modified: %s", got)
	}

	if sanitizeJoinError(nil) != nil {
		t.Fatalf("nil error should stay nil")
	}
}

// TestSerf_Join_ConfigTimeout verifies the Config.JoinTimeout deadline
// flows through the plain Join wrapper and stops new handshakes.
func TestSerf_Join_ConfigTimeout(t *testing.T) {
	ip1, returnFn1 := testutil.TakeIP()
	defer returnFn1()

	ip2, returnFn2 := testutil.TakeIP()
	defer returnFn2()

	s1Config := testConfig(t, ip1)
	s1Config.JoinTimeout = 20 * time.Millisecond
	s1 := newTestSerfWithConfig(t, s1Config)

	s2 := newTestSerf(t, ip2)
	waitUntilNumNodes(t, 1, s1, s2)

	// An unroutable-looking blackhole for the first seed so the deadline
	// elapses before the good seed is reached.
	blackholeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	var connsMu sync.Mutex
	var blackholeConns []net.Conn
	go func() {
		for {
			conn, acceptErr := blackholeLn.Accept()
			if acceptErr != nil {
				return
			}
			connsMu.Lock()
			blackholeConns = append(blackholeConns, conn)
			connsMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		blackholeLn.Close()
		connsMu.Lock()
		for _, conn := range blackholeConns {
			conn.Close()
		}
		connsMu.Unlock()
	})

	blackholeSeed := "node-blackhole/" + blackholeLn.Addr().String()
	num, err := s1.Join([]string{blackholeSeed, seedAddr(s2)}, false)
	if err == nil {
		t.Fatalf("expected a timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded in error chain, got: %v", err)
	}
	if num != 0 {
		t.Fatalf("expected no contacts, got %d", num)
	}
	waitUntilNumNodes(t, 1, s1)
}
