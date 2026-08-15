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
func (j *Jobs) NewRun(jobID int64) *Run {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.nextID++
	run := &Run{ID: j.nextID, JobID: jobID, LifeCycle: "PENDING"}
	j.runs[run.ID] = run
	return run
}

// GetRun returns a run.
func (j *Jobs) GetRun(id int64) (*Run, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	run, ok := j.runs[id]
	return run, ok
}

// UpdateRun stores a finished or in-flight run.
func (j *Jobs) UpdateRun(run *Run) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.runs[run.ID] = run
}

// ListRuns returns runs, optionally filtered by job.
func (j *Jobs) ListRuns(jobID int64) []*Run {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*Run
	for _, r := range j.runs {
		if jobID == 0 || r.JobID == jobID {
			out = append(out, r)
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
	run.ResultState = "CANCELED"
	return true
}
