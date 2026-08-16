package hs2

import (
	"context"
	"testing"

	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

// testService returns a service whose clock the test drives, so handles can
// be aged without sleeping.
func testService(t *testing.T) (*Service, *int64) {
	t.Helper()
	var now int64 = 1_000_000
	s := New(fakeBackend{running: true, ok: true, stdout: `[[1]]`})
	s.Now = func() int64 { return now }
	return s, &now
}

func openSession(t *testing.T, s *Service) *cliservice.TSessionHandle {
	t.Helper()
	resp, err := s.OpenSession(withWarehouse(context.Background(), "wh1"), &cliservice.TOpenSessionReq{})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if resp.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("OpenSession: %v", resp.Status)
	}
	return resp.SessionHandle
}

func execute(t *testing.T, s *Service, h *cliservice.TSessionHandle) *cliservice.TOperationHandle {
	t.Helper()
	resp, err := s.ExecuteStatement(context.Background(), &cliservice.TExecuteStatementReq{
		SessionHandle: h, Statement: "SELECT 1",
	})
	if err != nil {
		t.Fatalf("ExecuteStatement: %v", err)
	}
	if resp.Status.StatusCode != cliservice.TStatusCode_SUCCESS_STATUS {
		t.Fatalf("ExecuteStatement: %v", resp.Status)
	}
	return resp.OperationHandle
}

func (s *Service) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sess), len(s.ops)
}

// The common real leak: a client closes its session but never closes each
// operation. Every result table it produced used to stay pinned.
func TestClosingASessionReleasesItsOperations(t *testing.T) {
	s, _ := testService(t)
	h := openSession(t, s)
	for i := 0; i < 5; i++ {
		execute(t, s, h)
	}
	if _, ops := s.counts(); ops != 5 {
		t.Fatalf("ops before close = %d, want 5", ops)
	}
	if _, err := s.CloseSession(context.Background(), &cliservice.TCloseSessionReq{SessionHandle: h}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	sess, ops := s.counts()
	if sess != 0 || ops != 0 {
		t.Fatalf("after close sessions=%d ops=%d, want 0 and 0", sess, ops)
	}
}

// A session that closes must not take another session's results with it.
func TestClosingOneSessionLeavesAnotherIntact(t *testing.T) {
	s, _ := testService(t)
	a, b := openSession(t, s), openSession(t, s)
	execute(t, s, a)
	execute(t, s, b)
	if _, err := s.CloseSession(context.Background(), &cliservice.TCloseSessionReq{SessionHandle: a}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	sess, ops := s.counts()
	if sess != 1 || ops != 1 {
		t.Fatalf("sessions=%d ops=%d, want 1 and 1", sess, ops)
	}
}

// A client that vanishes never closes anything. Idle handles must age out.
func TestIdleHandlesAreReaped(t *testing.T) {
	s, now := testService(t)
	h := openSession(t, s)
	execute(t, s, h)
	if sess, ops := s.counts(); sess != 1 || ops != 1 {
		t.Fatalf("sessions=%d ops=%d, want 1 and 1", sess, ops)
	}

	*now += idleTTLSeconds + 1
	// Reaping is lazy, so it takes one more call to sweep.
	openSession(t, s)

	sess, ops := s.counts()
	if ops != 0 {
		t.Fatalf("ops = %d, want the abandoned result reaped", ops)
	}
	if sess != 1 {
		t.Fatalf("sessions = %d, want only the fresh one", sess)
	}
}

// A client still polling must not have its results reaped underneath it.
func TestActiveHandlesSurviveTheTTL(t *testing.T) {
	s, now := testService(t)
	h := openSession(t, s)
	opKey := handleKey(execute(t, s, h).OperationId)

	// Poll just under the TTL, repeatedly, well past it in total.
	for i := 0; i < 5; i++ {
		*now += idleTTLSeconds - 1
		s.mu.Lock()
		op := s.ops[opKey]
		s.touchOpLocked(opKey, op)
		sess := s.sess[handleKey(h.SessionId)]
		sess.seen = s.now()
		s.sess[handleKey(h.SessionId)] = sess
		s.reapLocked()
		s.mu.Unlock()
	}
	if sess, ops := s.counts(); sess != 1 || ops != 1 {
		t.Fatalf("sessions=%d ops=%d, want the polled handles kept alive", sess, ops)
	}
}

// A client that churns handles faster than the TTL still cannot grow the
// maps without bound.
func TestOperationCeilingHolds(t *testing.T) {
	s, _ := testService(t)
	h := openSession(t, s)
	for i := 0; i < maxOperations+50; i++ {
		execute(t, s, h)
	}
	if _, ops := s.counts(); ops > maxOperations {
		t.Fatalf("ops = %d, want <= %d", ops, maxOperations)
	}
}

func TestSessionCeilingHolds(t *testing.T) {
	s, _ := testService(t)
	for i := 0; i < maxSessions+50; i++ {
		openSession(t, s)
	}
	if sess, _ := s.counts(); sess > maxSessions {
		t.Fatalf("sessions = %d, want <= %d", sess, maxSessions)
	}
}

// A reaped operation reports the same "unknown operation" a closed one does,
// rather than surfacing some new failure mode to the client.
func TestReapedOperationReadsAsUnknown(t *testing.T) {
	s, now := testService(t)
	h := openSession(t, s)
	op := execute(t, s, h)

	*now += idleTTLSeconds + 1
	openSession(t, s) // reaping is lazy; this call sweeps

	resp, err := s.GetOperationStatus(context.Background(), &cliservice.TGetOperationStatusReq{
		OperationHandle: op,
	})
	if err != nil {
		t.Fatalf("GetOperationStatus: %v", err)
	}
	if resp.Status.StatusCode != cliservice.TStatusCode_ERROR_STATUS {
		t.Fatal("a reaped operation should not report success")
	}
	if got := resp.Status.GetErrorMessage(); got != "unknown operation" {
		t.Fatalf("error = %q, want %q", got, "unknown operation")
	}
}
