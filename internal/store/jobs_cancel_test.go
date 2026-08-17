package store

import (
	"sync"
	"testing"
)

func cancelStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The regression: a run cancelled while in flight was un-cancelled by the
// executing goroutine's closing publish, and the caller was told the run it
// had stopped finished normally.
func TestCancelSurvivesALaterPublish(t *testing.T) {
	s := cancelStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)

	run.LifeCycle = "RUNNING"
	s.Jobs.UpdateRun(run)

	if !s.Jobs.CancelRun(run.ID) {
		t.Fatal("CancelRun reported the run missing")
	}

	// The run was already in flight; it finishes and publishes success.
	run.LifeCycle = "TERMINATED"
	run.ResultState = "SUCCESS"
	run.Stdout = "work that happened after the cancel"
	s.Jobs.UpdateRun(run)

	got, ok := s.Jobs.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found")
	}
	if got.ResultState != ResultCanceled {
		t.Fatalf("ResultState = %q, want %q", got.ResultState, ResultCanceled)
	}
	if got.LifeCycle != "TERMINATED" {
		t.Fatalf("LifeCycle = %q, want TERMINATED", got.LifeCycle)
	}
	if got.Stdout != "" {
		t.Errorf("Stdout = %q, want the post-cancel output withheld", got.Stdout)
	}
}

// Cancelling twice is idempotent and stays cancelled.
func TestCancelIsIdempotent(t *testing.T) {
	s := cancelStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)

	if !s.Jobs.CancelRun(run.ID) || !s.Jobs.CancelRun(run.ID) {
		t.Fatal("CancelRun should report success both times")
	}
	got, _ := s.Jobs.GetRun(run.ID)
	if got.ResultState != ResultCanceled {
		t.Fatalf("ResultState = %q, want %q", got.ResultState, ResultCanceled)
	}
}

// Stickiness must not freeze runs that were never cancelled.
func TestUncancelledRunsStillUpdate(t *testing.T) {
	s := cancelStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)

	for _, state := range []string{"RUNNING", "TERMINATED"} {
		run.LifeCycle = state
		s.Jobs.UpdateRun(run)
		got, _ := s.Jobs.GetRun(run.ID)
		if got.LifeCycle != state {
			t.Fatalf("LifeCycle = %q, want %q", got.LifeCycle, state)
		}
	}
	run.ResultState = "SUCCESS"
	s.Jobs.UpdateRun(run)
	if got, _ := s.Jobs.GetRun(run.ID); got.ResultState != "SUCCESS" {
		t.Fatalf("ResultState = %q, want SUCCESS", got.ResultState)
	}
}

// A run that already finished normally is not retroactively cancellable:
// CancelRun reports success (the run is over) but must not rewrite it.
func TestCancelDoesNotRewriteAFinishedRun(t *testing.T) {
	s := cancelStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)
	run.LifeCycle = "TERMINATED"
	run.ResultState = "SUCCESS"
	s.Jobs.UpdateRun(run)

	if !s.Jobs.CancelRun(run.ID) {
		t.Fatal("CancelRun on a finished run should report success")
	}
	if got, _ := s.Jobs.GetRun(run.ID); got.ResultState != "SUCCESS" {
		t.Fatalf("ResultState = %q, want the finished run left alone", got.ResultState)
	}
}

// Cancelling one run must not freeze another.
func TestCancelOnlyAffectsItsOwnRun(t *testing.T) {
	s := cancelStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	a, b := s.Jobs.NewRun(job.ID), s.Jobs.NewRun(job.ID)

	s.Jobs.CancelRun(a.ID)
	b.LifeCycle = "TERMINATED"
	b.ResultState = "SUCCESS"
	s.Jobs.UpdateRun(b)

	gotA, _ := s.Jobs.GetRun(a.ID)
	gotB, _ := s.Jobs.GetRun(b.ID)
	if gotA.ResultState != ResultCanceled {
		t.Errorf("run A = %q, want %q", gotA.ResultState, ResultCanceled)
	}
	if gotB.ResultState != "SUCCESS" {
		t.Errorf("run B = %q, want SUCCESS", gotB.ResultState)
	}
}

// Cancel racing the executing goroutine must leave one of the two legal
// outcomes, and the store must survive the concurrency.
//
// The earlier version of this test asserted CANCELED unconditionally, on the
// reasoning that the cancel wins whichever order it lands. That was wrong:
// CancelRun returns early on an already-TERMINATED run, so a cancel arriving
// after the final publish is a no-op by design -- the rule
// TestCancelDoesNotRewriteAFinishedRun asserts directly. The two tests
// contradicted each other, and this one only passed because the writer's 200
// updates made the cancel win almost every time. CI scheduling found the
// other ordering.
//
// The deterministic guarantee -- a cancel landing mid-run is not overwritten
// -- is covered by TestCancelSurvivesALaterPublish.
func TestCancelConcurrentWithPublishLeavesALegalState(t *testing.T) {
	s := cancelStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		local := run.clone()
		for i := 0; i < 200; i++ {
			local.LifeCycle = "RUNNING"
			s.Jobs.UpdateRun(local)
		}
		local.LifeCycle = "TERMINATED"
		local.ResultState = "SUCCESS"
		s.Jobs.UpdateRun(local)
	}()
	go func() {
		defer wg.Done()
		s.Jobs.CancelRun(run.ID)
	}()
	wg.Wait()

	got, ok := s.Jobs.GetRun(run.ID)
	if !ok {
		t.Fatal("run vanished")
	}
	if got.LifeCycle != "TERMINATED" {
		t.Fatalf("LifeCycle = %q, want TERMINATED whichever order they landed", got.LifeCycle)
	}
	if got.ResultState != ResultCanceled && got.ResultState != "SUCCESS" {
		t.Fatalf("ResultState = %q, want %q (cancel landed first) or SUCCESS (publish landed first)",
			got.ResultState, ResultCanceled)
	}
}
