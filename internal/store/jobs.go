package store

import (
	"sync"
)

// Job is a Jobs 2.2 definition.
type Job struct {
	ID    int64
	Name  string
	Tasks []Task
}

// Task is one node in a job DAG.
type Task struct {
	Key            string
	DependsOn      []string
	RunIf          string
	NotebookPath   string
	NotebookParams map[string]string
	PythonFile     string
	PythonParams   []string
	SQLFile        string
	SparkEnvVars   map[string]string
	SparkConf      map[string]string
}

// Run is a job execution record.
type Run struct {
	ID           int64
	JobID        int64
	LifeCycle    string
	ResultState  string
	StateMessage string
	ExecutedBy   string
	Tasks        []TaskRun
	Stdout       string
	Stderr       string
}

// TaskRun is one task's result inside a run.
type TaskRun struct {
	Key         string
	LifeCycle   string
	ResultState string
	Stdout      string
	Stderr      string
}

// Jobs holds job definitions and runs.
type Jobs struct {
	mu     sync.Mutex
	nextID int64
	jobs   map[int64]*Job
	runs   map[int64]*Run
}

func newJobs() *Jobs {
	return &Jobs{jobs: map[int64]*Job{}, runs: map[int64]*Run{}}
}

// Create inserts a job.
func (j *Jobs) Create(name string, tasks []Task) *Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.nextID++
	job := &Job{ID: j.nextID, Name: name, Tasks: tasks}
	j.jobs[job.ID] = job
	return job
}

// Get returns a job.
func (j *Jobs) Get(id int64) (*Job, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	job, ok := j.jobs[id]
	return job, ok
}

// List returns all jobs.
func (j *Jobs) List() []*Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*Job
	for _, job := range j.jobs {
		out = append(out, job)
	}
	return out
}

// Delete removes a job.
func (j *Jobs) Delete(id int64) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.jobs[id]; !ok {
		return false
	}
	delete(j.jobs, id)
	return true
}

// Reset replaces a job's tasks and name.
func (j *Jobs) Reset(id int64, name string, tasks []Task) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	job, ok := j.jobs[id]
	if !ok {
		return false
	}
	job.Name = name
	job.Tasks = tasks
	return true
}

// NewRun records a PENDING run.
// clone copies a run so no two goroutines ever share one. Tasks is copied
// too: sharing the backing array would leave the same race a level down.
func (r *Run) clone() *Run {
	if r == nil {
		return nil
	}
	out := *r
	if r.Tasks != nil {
		out.Tasks = append([]TaskRun(nil), r.Tasks...)
	}
	return &out
}

// A run is written by the goroutine executing it and read by every HTTP
// handler that reports on it. The mutex below only protects the map, so
// handing out the stored pointer put those writes and reads in a race.
// Every accessor therefore returns a snapshot, and UpdateRun publishes one.
func (j *Jobs) NewRun(jobID int64) *Run {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.nextID++
	run := &Run{ID: j.nextID, JobID: jobID, LifeCycle: "PENDING"}
	j.runs[run.ID] = run
	return run.clone()
}

// GetRun returns a run.
func (j *Jobs) GetRun(id int64) (*Run, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	run, ok := j.runs[id]
	return run.clone(), ok
}

// ResultCanceled is the terminal result a cancelled run keeps. It is
// load-bearing in two places now, so it is named rather than spelled twice.
const ResultCanceled = "CANCELED"

// UpdateRun stores a finished or in-flight run.
//
// A cancelled run is final. The emulator cannot interrupt a run already in
// flight, so executeRun keeps working and publishes its own result at the
// end; without this guard that closing SUCCESS silently undid the cancel and
// the caller was told the run it stopped had finished normally.
func (j *Jobs) UpdateRun(run *Run) {
	if run == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if cur, ok := j.runs[run.ID]; ok && cur.ResultState == ResultCanceled {
		return
	}
	j.runs[run.ID] = run.clone()
}

// ListRuns returns runs, optionally filtered by job.
func (j *Jobs) ListRuns(jobID int64) []*Run {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*Run
	for _, r := range j.runs {
		if jobID == 0 || r.JobID == jobID {
			out = append(out, r.clone())
		}
	}
	return out
}

// CancelRun marks a run canceled if it is still running.
func (j *Jobs) CancelRun(id int64) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	run, ok := j.runs[id]
	if !ok {
		return false
	}
	if run.LifeCycle == "TERMINATED" {
		return true
	}
	run.LifeCycle = "TERMINATED"
	run.ResultState = ResultCanceled
	return true
}
