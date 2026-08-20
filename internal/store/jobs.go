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

// Dependency is one edge into a task.
//
// Outcome is the branch of an if/else condition this edge follows, "true" or
// "false", and empty for an ordinary edge. It is carried rather than dropped
// because it is the ONLY thing that distinguishes the two branches of a
// condition: both downstream tasks depend on the same task_key, and a
// dependency model that keeps just the key would run both arms of every
// if/else and report SUCCESS. `TaskDependency` in databricks-sdk 0.129.0 is
// exactly {task_key, outcome}.
type Dependency struct {
	Key     string
	Outcome string
}

// Condition is an if/else condition_task: `ConditionTask` in the SDK is
// exactly {op, left, right}.
type Condition struct {
	Op    string
	Left  string
	Right string
}

// Dbt is a dbt_task. `DbtTask` in databricks-sdk 0.129.0 is
// {commands, catalog, profiles_directory, project_directory, schema, source,
// warehouse_id}.
//
// Commands are dbt's own argv, e.g. ["dbt deps", "dbt run"]. WarehouseID names
// the SQL warehouse the models execute against, which is the same handle a
// client would target directly: dbt is a warehouse client, and running it as a
// job changes who invokes it, not what it connects to.
type Dbt struct {
	Commands          []string
	Catalog           string
	Schema            string
	ProjectDirectory  string
	ProfilesDirectory string
	WarehouseID       string
}

// Task is one node in a job DAG.
type Task struct {
	Key            string
	DependsOn      []Dependency
	RunIf          string
	NotebookPath   string
	NotebookParams map[string]string
	PythonFile     string
	PythonParams   []string
	SQLFile        string
	Condition      *Condition
	Dbt            *Dbt
	RunJob         *RunJob
	ForEach        *ForEach
	SparkEnvVars   map[string]string
	SparkConf      map[string]string
}

// RunJob is a run_job_task: this task's work is another job's run.
type RunJob struct {
	JobID  int64
	Params map[string]string
}

// ForEach is a for_each_task: run one nested task once per input.
//
// Inputs is the DECODED list. The API carries it as a JSON-encoded *string*
// (`"[\"a\",\"b\"]"`), which is easy to mistake for a list and is decoded at
// parse time so a malformed one is refused when the job is created rather than
// when it runs.
type ForEach struct {
	Inputs      []string
	Concurrency int
	Task        *Task
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
//
// ConditionOutcome is "true" or "false" for a condition_task and empty for
// every other kind. A condition that evaluates false still SUCCEEDS -- the
// task did its job -- so the outcome, not the result state, is what the
// branches are selected on. `RunConditionTask` in the SDK carries the same
// field beside the operands it evaluated.
type TaskRun struct {
	Key              string
	LifeCycle        string
	ResultState      string
	ConditionOutcome string
	// ChildRunID is the run a run_job_task started, so a client can fetch it.
	// Real Databricks reports it as run_job_output.run_id; without it the
	// caller sees a SUCCESS with no way to reach what actually ran.
	ChildRunID int64
	// Iterations are a for_each_task's per-input results, in input order.
	Iterations []TaskRun
	Stdout     string
	Stderr     string
	// What dbt wrote to target/, for a dbt_task. Present on a FAILED run too:
	// a failing `dbt test` is precisely when run_results.json is worth having,
	// because it names which test failed where the exit code does not.
	DbtArtifacts map[string]string
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
