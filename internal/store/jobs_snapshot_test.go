package store

import (
	"sync"
	"testing"
)

func snapshotStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The run accessors must hand back snapshots. Returning the stored pointer is
// what let the jobs goroutine write fields while HTTP handlers read them.
func TestRunAccessorsReturnSnapshots(t *testing.T) {
	s := snapshotStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)
	run.Tasks = []TaskRun{{Key: "only", LifeCycle: "RUNNING"}}
	s.Jobs.UpdateRun(run)

	stored, ok := s.Jobs.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found")
	}
	if stored == run {
		t.Fatal("GetRun handed back the caller's own pointer")
	}

	// Mutating a snapshot must not reach into the store.
	stored.LifeCycle = "MUTATED"
	stored.Tasks[0].LifeCycle = "MUTATED"
	again, _ := s.Jobs.GetRun(run.ID)
	if again.LifeCycle == "MUTATED" {
		t.Error("mutating a snapshot changed the stored run")
	}
	if again.Tasks[0].LifeCycle == "MUTATED" {
		t.Error("Tasks shares its backing array with the stored run")
	}

	// The same must hold for the run handed to UpdateRun.
	run.LifeCycle = "MUTATED-AFTER-PUBLISH"
	after, _ := s.Jobs.GetRun(run.ID)
	if after.LifeCycle == "MUTATED-AFTER-PUBLISH" {
		t.Error("UpdateRun kept the caller's pointer instead of a snapshot")
	}

	// ListRuns too.
	for _, r := range s.Jobs.ListRuns(job.ID) {
		if r == run {
			t.Fatal("ListRuns handed back the caller's own pointer")
		}
		r.LifeCycle = "MUTATED"
	}
	final, _ := s.Jobs.GetRun(run.ID)
	if final.LifeCycle == "MUTATED" {
		t.Error("mutating a ListRuns element changed the stored run")
	}
}

// Writers and readers running together, the shape the jobs runner has in
// production. Under -race this fails if any accessor shares a pointer.
func TestConcurrentRunReadsAndWrites(t *testing.T) {
	s := snapshotStore(t)
	job := s.Jobs.Create("j", []Task{{Key: "only", NotebookPath: "/n"}})
	run := s.Jobs.NewRun(job.ID)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() { // the executing run, publishing as it progresses
		defer wg.Done()
		local := run
		for i := 0; i < 200; i++ {
			local.LifeCycle = "RUNNING"
			local.Stdout = "chunk"
			local.Tasks = []TaskRun{{Key: "only", LifeCycle: "RUNNING"}}
			s.Jobs.UpdateRun(local)
		}
		local.LifeCycle = "TERMINATED"
		local.ResultState = "SUCCESS"
		s.Jobs.UpdateRun(local)
		close(stop)
	}()

	for r := 0; r < 4; r++ { // handlers reporting on it
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if got, ok := s.Jobs.GetRun(run.ID); ok {
					_ = got.LifeCycle
					_ = got.Stdout
					for _, task := range got.Tasks {
						_ = task.LifeCycle
					}
				}
				for _, got := range s.Jobs.ListRuns(job.ID) {
					_ = got.ResultState
				}
			}
		}()
	}
	wg.Wait()

	final, ok := s.Jobs.GetRun(run.ID)
	if !ok || final.LifeCycle != "TERMINATED" || final.ResultState != "SUCCESS" {
		t.Fatalf("final run = %+v, want TERMINATED/SUCCESS", final)
	}
}
