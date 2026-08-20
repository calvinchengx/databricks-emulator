package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

var secretRef = regexp.MustCompile(`\{\{secrets/([^/]+)/([^}]+)\}\}`)

func (s *Server) jobsCreate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	job, err := s.parseJob(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	created := s.Store.Jobs.Create(job.Name, job.Tasks)
	writeJSON(w, http.StatusOK, map[string]any{"job_id": created.ID})
}

func (s *Server) jobsReset(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeBodyErr(w, err)
		return
	}
	id := int64From(raw["job_id"])
	newSettings := raw["new_settings"]
	if len(newSettings) == 0 {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "new_settings required")
		return
	}
	job, err := parseJobBytes(newSettings)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if !s.Store.Jobs.Reset(id, job.Name, job.Tasks) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "job not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) jobsDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		JobID int64 `json:"job_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if !s.Store.Jobs.Delete(body.JobID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "job not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) jobsGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := parseInt64(query(r, "job_id"))
	job, ok := s.Store.Jobs.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "job not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": job.ID, "settings": jobSettings(job)})
}

func (s *Server) jobsList(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	var jobs []map[string]any
	for _, job := range s.Store.Jobs.List() {
		jobs = append(jobs, map[string]any{"job_id": job.ID, "settings": jobSettings(job)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) jobsRunNow(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		JobID int64 `json:"job_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	job, ok := s.Store.Jobs.Get(body.JobID)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "job not found")
		return
	}
	run := s.Store.Jobs.NewRun(job.ID)
	// Read the id before handing the run to the goroutine: after this the
	// run belongs to executeRun alone.
	runID := run.ID
	go s.executeRun(job, run, 0)
	writeJSON(w, http.StatusOK, map[string]any{"run_id": runID, "number_in_job": 1})
}

func (s *Server) jobsRunsGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	run, ok := s.Store.Jobs.GetRun(parseInt64(query(r, "run_id")))
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "run not found")
		return
	}
	writeJSON(w, http.StatusOK, runJSON(run))
}

func (s *Server) jobsRunsList(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var runs []map[string]any
	for _, run := range s.Store.Jobs.ListRuns(parseInt64(query(r, "job_id"))) {
		runs = append(runs, runJSON(run))
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) jobsRunsOutput(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	run, ok := s.Store.Jobs.GetRun(parseInt64(query(r, "run_id")))
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "run not found")
		return
	}
	out := map[string]any{
		"metadata": runJSON(run),
		"logs":     run.Stdout,
		"error":    run.Stderr,
	}
	// run_job_output.run_id — the child a run_job_task started, so a caller can
	// fetch what actually ran.
	//
	// HERE, not on the task in runs/get, and that was measured rather than
	// assumed: the first attempt put it on the task, and the SDK's `RunTask`
	// has no such field at all — `jobs.RunOutput.run_job_output` is where
	// `databricks-sdk` looks. A field the client cannot read is not an API.
	for _, t := range run.Tasks {
		if t.ChildRunID != 0 {
			out["run_job_output"] = map[string]any{"run_id": t.ChildRunID}
			break
		}
	}
	// dbt_output, when the run had a dbt_task and it produced artefacts.
	//
	// ABSENT RATHER THAN EMPTY when there are none, so "this run produced no
	// artefacts" and "this was not a dbt run" stay distinguishable -- the same
	// reason a snapshot omits contract_failures instead of carrying [].
	//
	// A DELIBERATE DEVIATION, named here rather than discovered: real
	// Databricks returns `artifacts_link` and `artifacts_headers`, a URL the
	// caller then fetches. This returns the files INLINE. A link would need an
	// artefact store, an expiry and a second round trip to emulate something
	// whose only purpose locally is to hand back one JSON file. Callers written
	// against the real API will not find `artifacts_link` here; that is the
	// cost, and it is why this is written down.
	for _, tr := range run.Tasks {
		if len(tr.DbtArtifacts) == 0 {
			continue
		}
		out["dbt_output"] = map[string]any{"artifacts": tr.DbtArtifacts}
		break
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) jobsRunsCancel(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		RunID int64 `json:"run_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if !s.Store.Jobs.CancelRun(body.RunID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "run not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) parseJob(r *http.Request) (*store.Job, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return parseJobBytes(raw)
}

func parseJobBytes(raw json.RawMessage) (*store.Job, error) {
	var body struct {
		Name  string           `json:"name"`
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	var tasks []store.Task
	for _, t := range body.Tasks {
		task, err := parseTask(t)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 {
		return nil, fmt.Errorf("at least one task is required")
	}
	return &store.Job{Name: body.Name, Tasks: tasks}, nil
}

func parseTask(t map[string]any) (store.Task, error) {
	task := store.Task{Key: str(t["task_key"]), RunIf: str(t["run_if"])}
	if task.RunIf == "" {
		task.RunIf = "ALL_SUCCESS"
	}
	if deps, ok := t["depends_on"].([]any); ok {
		for _, d := range deps {
			m, ok := d.(map[string]any)
			if !ok {
				continue
			}
			dep := store.Dependency{Key: str(m["task_key"]), Outcome: str(m["outcome"])}
			switch dep.Outcome {
			case "", "true", "false":
			default:
				return task, fmt.Errorf("depends_on.outcome for task %q is %q; an if/else edge is %q or %q",
					task.Key, dep.Outcome, "true", "false")
			}
			task.DependsOn = append(task.DependsOn, dep)
		}
	}
	if libs, ok := t["libraries"]; ok && libs != nil {
		if arr, ok := libs.([]any); ok && len(arr) > 0 {
			return task, fmt.Errorf("libraries installs packages on a cluster whose lifecycle the emulator does not own")
		}
	}
	if _, ok := t["spark_jar_task"]; ok {
		return task, fmt.Errorf("spark_jar_task asks the emulator to EXECUTE a Java/Scala main class — refused")
	}
	if raw, ok := t["dbt_task"].(map[string]any); ok {
		d := &store.Dbt{
			Catalog:           str(raw["catalog"]),
			Schema:            str(raw["schema"]),
			ProjectDirectory:  str(raw["project_directory"]),
			ProfilesDirectory: str(raw["profiles_directory"]),
			WarehouseID:       str(raw["warehouse_id"]),
		}
		if cmds, ok := raw["commands"].([]any); ok {
			for _, c := range cmds {
				d.Commands = append(d.Commands, str(c))
			}
		}
		if len(d.Commands) == 0 {
			return task, fmt.Errorf("dbt_task.commands is required")
		}
		// A `source` of GIT would have this process clone at run time. Repos
		// already clones into the workspace store, so the honest shape is to
		// point project_directory at what that produced rather than grow a
		// second, hidden clone path here.
		if src := str(raw["source"]); src != "" && src != "WORKSPACE" {
			return task, fmt.Errorf("dbt_task.source %q is not implemented; use WORKSPACE "+
				"(clone with the Repos API first, then point project_directory at it)", src)
		}
		if d.ProjectDirectory == "" {
			return task, fmt.Errorf("dbt_task.project_directory is required")
		}
		// The warehouse is where the models actually execute. Without it this
		// process would have to pick one, and a dbt run silently against a
		// different warehouse than the caller named is the wrong answer
		// delivered confidently.
		if d.WarehouseID == "" {
			return task, fmt.Errorf("dbt_task.warehouse_id is required: it names the " +
				"warehouse the models execute against")
		}
		task.Dbt = d
	}
	if _, ok := t["pipeline_task"]; ok {
		return task, fmt.Errorf("pipeline_task (DLT) is refused by name")
	}
	// Named, not left to the catch-all below. These three reached the generic
	// "must be notebook_task, spark_python_task, or sql_task.file" error, which
	// never says the thing you asked for is the thing that is missing -- the
	// one failure mode this repo's refusals exist to avoid.
	if _, ok := t["python_wheel_task"]; ok {
		return task, fmt.Errorf("python_wheel_task installs a wheel on a cluster whose lifecycle the emulator does not own: refused")
	}
	if raw, ok := t["run_job_task"]; ok {
		m, _ := raw.(map[string]any)
		if m == nil {
			return task, fmt.Errorf("run_job_task must be an object with job_id")
		}
		rj := &store.RunJob{JobID: int64(num(m["job_id"])), Params: stringMap(m["job_parameters"])}
		if rj.JobID == 0 {
			return task, fmt.Errorf("run_job_task.job_id is required")
		}
		task.RunJob = rj
	}
	if raw, ok := t["for_each_task"]; ok {
		m, _ := raw.(map[string]any)
		if m == nil {
			return task, fmt.Errorf("for_each_task must be an object with inputs and task")
		}
		fe := &store.ForEach{Concurrency: int(num(m["concurrency"]))}
		// `inputs` is a JSON-encoded STRING in the API, not a list — decoded
		// here so a malformed one is refused at job-create time rather than
		// surfacing mid-run as an empty loop that reports SUCCESS.
		inputs, err := decodeForEachInputs(m["inputs"])
		if err != nil {
			return task, err
		}
		fe.Inputs = inputs
		nested, ok := m["task"].(map[string]any)
		if !ok {
			return task, fmt.Errorf("for_each_task.task is required: the task to run per input")
		}
		inner, err := parseTask(nested)
		if err != nil {
			return task, fmt.Errorf("for_each_task.task: %w", err)
		}
		// Nesting a loop inside a loop, or a job-run inside one, multiplies work
		// this process would have to bound. Refused by name rather than left to
		// surprise someone at run time.
		if inner.ForEach != nil {
			return task, fmt.Errorf("for_each_task.task may not itself be a for_each_task")
		}
		if inner.RunJob != nil {
			return task, fmt.Errorf("for_each_task.task may not be a run_job_task")
		}
		fe.Task = &inner
		task.ForEach = fe
	}
	if raw, ok := t["condition_task"].(map[string]any); ok {
		c := &store.Condition{Op: str(raw["op"]), Left: str(raw["left"]), Right: str(raw["right"])}
		if !validConditionOp(c.Op) {
			return task, fmt.Errorf("condition_task.op %q is not one of %s", c.Op, strings.Join(conditionOps, ", "))
		}
		task.Condition = c
	}
	if raw, ok := t["sql_task"].(map[string]any); ok {
		if raw["query"] != nil || raw["dashboard"] != nil || raw["alert"] != nil {
			return task, fmt.Errorf("sql_task.query/dashboard/alert are refused — only sql_task.file on the Spark SQL warehouse surface")
		}
		if f, ok := raw["file"].(map[string]any); ok {
			task.SQLFile = str(f["path"])
		}
		if task.SQLFile == "" {
			return task, fmt.Errorf("sql_task.file.path is required")
		}
	}
	if nb, ok := t["notebook_task"].(map[string]any); ok {
		task.NotebookPath = str(nb["notebook_path"])
		task.NotebookParams = stringMap(nb["base_parameters"])
	}
	if py, ok := t["spark_python_task"].(map[string]any); ok {
		task.PythonFile = str(py["python_file"])
		if params, ok := py["parameters"].([]any); ok {
			for _, p := range params {
				task.PythonParams = append(task.PythonParams, fmt.Sprint(p))
			}
		}
	}
	if nc, ok := t["new_cluster"].(map[string]any); ok {
		task.SparkEnvVars = stringMap(nc["spark_env_vars"])
		task.SparkConf = stringMap(nc["spark_conf"])
	}
	if task.NotebookPath == "" && task.PythonFile == "" && task.SQLFile == "" &&
		task.Condition == nil && task.Dbt == nil && task.RunJob == nil && task.ForEach == nil {
		return task, fmt.Errorf("task %q must be notebook_task, spark_python_task, "+
			"sql_task.file, condition_task, or dbt_task", task.Key)
	}
	return task, nil
}

// conditionOps is the ConditionTaskOp enum, verbatim from databricks-sdk
// 0.129.0 (the pin e2e/sdk drives this emulator with).
var conditionOps = []string{
	"EQUAL_TO", "NOT_EQUAL",
	"GREATER_THAN", "GREATER_THAN_OR_EQUAL",
	"LESS_THAN", "LESS_THAN_OR_EQUAL",
}

func validConditionOp(op string) bool {
	return slices.Contains(conditionOps, op)
}

// evalCondition returns "true" or "false" for an if/else task.
//
// THE TWO OPERATOR FAMILIES COMPARE DIFFERENTLY, and this is documented
// behaviour rather than an implementation detail worth smoothing over:
// `==` and `!=` compare as STRINGS, so `12.0 == 12` is false; `>`, `>=`, `<`
// and `<=` compare as NUMBERS, so `12.0 >= 12` is true. Making all six
// numeric-aware would be the friendlier rule and the wrong one -- a job whose
// equality branch passes here and fails on a real workspace is exactly the
// divergence this emulator exists to avoid.
func evalCondition(c store.Condition) string {
	if c.Op == "EQUAL_TO" || c.Op == "NOT_EQUAL" {
		equal := c.Left == c.Right
		if (c.Op == "EQUAL_TO") == equal {
			return "true"
		}
		return "false"
	}
	// Ordering operators are numeric. A non-numeric operand has no ordering,
	// so it is false rather than a string comparison wearing a numeric name.
	left, lerr := strconv.ParseFloat(strings.TrimSpace(c.Left), 64)
	right, rerr := strconv.ParseFloat(strings.TrimSpace(c.Right), 64)
	if lerr != nil || rerr != nil {
		return "false"
	}
	var ok bool
	switch c.Op {
	case "GREATER_THAN":
		ok = left > right
	case "GREATER_THAN_OR_EQUAL":
		ok = left >= right
	case "LESS_THAN":
		ok = left < right
	case "LESS_THAN_OR_EQUAL":
		ok = left <= right
	}
	if ok {
		return "true"
	}
	return "false"
}

// runCancelled reports whether the run has been cancelled since it started.
func (s *Server) runCancelled(id int64) bool {
	run, ok := s.Store.Jobs.GetRun(id)
	return ok && run.ResultState == store.ResultCanceled
}

func (s *Server) executeRun(job *store.Job, run *store.Run, depth int) {
	run.LifeCycle = "RUNNING"
	run.ExecutedBy = "the emulator's Spark engine, not a Databricks cluster"
	s.Store.Jobs.UpdateRun(run)
	if s.Spark == nil {
		run.LifeCycle = "TERMINATED"
		run.ResultState = "FAILED"
		run.StateMessage = "no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL"
		s.Store.Jobs.UpdateRun(run)
		return
	}

	results := map[string]store.TaskRun{}
	remaining := map[string]store.Task{}
	for _, t := range job.Tasks {
		remaining[t.Key] = t
	}
	for len(remaining) > 0 {
		// A cancelled run stops dispatching. Tasks already in flight finish
		// -- the emulator cannot interrupt the engine mid-statement -- but no
		// further wave starts, so cancelling actually stops consuming the
		// attach instead of only relabelling the result.
		//
		// Nothing is published on the way out: the store already holds the
		// cancelled state, and UpdateRun would refuse this write anyway.
		if s.runCancelled(run.ID) {
			return
		}
		var ready []store.Task
		for _, t := range remaining {
			if depsSatisfied(t, results) {
				ready = append(ready, t)
			}
		}
		if len(ready) == 0 {
			for _, t := range remaining {
				results[t.Key] = store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED", ResultState: "SKIPPED"}
			}
			break
		}
		var wg sync.WaitGroup
		// mu guards results for this wave. The loop below dispatches while
		// earlier tasks in the same wave are already running, so the parent
		// has to take the lock too: guarding only the goroutines protected
		// them from each other and not from the loop that spawned them.
		var mu sync.Mutex
		for _, t := range ready {
			t := t
			delete(remaining, t.Key)
			mu.Lock()
			skip := !shouldRun(t, results)
			if skip {
				results[t.Key] = store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED", ResultState: "SKIPPED"}
			}
			mu.Unlock()
			if skip {
				continue
			}
			// A condition is decided here, not on the engine. It is pure
			// comparison, so dispatching it to Spark would be inventing work --
			// and it must still be evaluated when no engine is attached at all.
			if t.Condition != nil {
				outcome := evalCondition(*t.Condition)
				mu.Lock()
				results[t.Key] = store.TaskRun{
					Key: t.Key, LifeCycle: "TERMINATED", ResultState: "SUCCESS",
					ConditionOutcome: outcome,
				}
				mu.Unlock()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				tr := s.runTask(t, depth)
				mu.Lock()
				results[t.Key] = tr
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	var tasks []store.TaskRun
	failed := false
	var stdout, stderr strings.Builder
	for _, t := range job.Tasks {
		tr := results[t.Key]
		tasks = append(tasks, tr)
		stdout.WriteString(tr.Stdout)
		stderr.WriteString(tr.Stderr)
		if tr.ResultState == "FAILED" {
			failed = true
		}
	}
	run.Tasks = tasks
	run.Stdout = stdout.String()
	run.Stderr = stderr.String()
	run.LifeCycle = "TERMINATED"
	if failed {
		run.ResultState = "FAILED"
	} else {
		run.ResultState = "SUCCESS"
	}
	s.Store.Jobs.UpdateRun(run)
}

func depsSatisfied(t store.Task, done map[string]store.TaskRun) bool {
	for _, d := range t.DependsOn {
		if _, ok := done[d.Key]; !ok {
			return false
		}
	}
	return true
}

func shouldRun(t store.Task, done map[string]store.TaskRun) bool {
	if len(t.DependsOn) == 0 {
		return true
	}
	// THE BRANCH NOT TAKEN IS SKIPPED, and this runs before run_if. An edge
	// that names an outcome only fires when the condition produced it, so the
	// false arm of an if/else does not run merely because the condition task
	// itself succeeded -- which it always does.
	for _, d := range t.DependsOn {
		if d.Outcome == "" {
			continue
		}
		if done[d.Key].ConditionOutcome != d.Outcome {
			return false
		}
	}
	success, failed, finished := 0, 0, 0
	for _, d := range t.DependsOn {
		tr := done[d.Key]
		finished++
		switch tr.ResultState {
		case "SUCCESS":
			success++
		case "FAILED":
			failed++
		}
	}
	switch t.RunIf {
	case "ALL_DONE":
		return true
	case "AT_LEAST_ONE_SUCCESS":
		return success >= 1
	case "ALL_FAILED":
		return failed == finished
	case "AT_LEAST_ONE_FAILED":
		return failed >= 1
	case "NONE_FAILED":
		return failed == 0
	default: // ALL_SUCCESS
		return success == finished
	}
}

// runDepth bounds run_job_task nesting. A job that runs itself — directly or
// through a cycle — would otherwise spawn runs until the process died, and the
// emulator would look hung rather than wrong. Real Databricks caps nesting;
// the number here is ours, and the refusal names it.
const maxRunJobDepth = 5

// runJobTask runs another job and reports ITS outcome as this task's.
//
// Synchronous on purpose: the parent task is not finished until the child is,
// which is what `depends_on` downstream of it has to mean. The child's run id
// travels back on the TaskRun so a client can fetch the child run — real
// Databricks reports it as run_job_output.run_id, and a SUCCESS with no way to
// reach what ran is the kind of green this repo treats as a defect.
func (s *Server) runJobTask(t store.Task, depth int) store.TaskRun {
	tr := store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED"}
	child, ok := s.Store.Jobs.Get(t.RunJob.JobID)
	if !ok {
		tr.ResultState = "FAILED"
		tr.Stderr = fmt.Sprintf("run_job_task.job_id %d does not exist", t.RunJob.JobID)
		return tr
	}
	if depth+1 > maxRunJobDepth {
		tr.ResultState = "FAILED"
		tr.Stderr = fmt.Sprintf("run_job_task nested deeper than %d: a job that runs itself, "+
			"directly or through a cycle, would not terminate", maxRunJobDepth)
		return tr
	}
	run := s.Store.Jobs.NewRun(child.ID)
	tr.ChildRunID = run.ID
	s.executeRun(child, run, depth+1)
	done, ok := s.Store.Jobs.GetRun(run.ID)
	if !ok {
		tr.ResultState = "FAILED"
		tr.Stderr = fmt.Sprintf("child run %d vanished", run.ID)
		return tr
	}
	tr.ResultState = done.ResultState
	tr.Stdout = done.Stdout
	tr.Stderr = done.Stderr
	return tr
}

// runForEachTask runs one nested task per input.
//
// `{{input}}` is substituted in the nested task's parameters, which is how the
// iteration learns which input it is. Substituted into a COPY per iteration:
// the parsed task is shared, and rewriting it in place would leave every
// iteration reading the last input — the same shape of bug as the shared
// sys.argv that databricks#56 witnessed.
func (s *Server) runForEachTask(t store.Task, depth int) store.TaskRun {
	tr := store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED", ResultState: "SUCCESS"}
	fe := t.ForEach
	limit := fe.Concurrency
	if limit <= 0 {
		limit = 1 // the API's default is serial
	}
	if limit > len(fe.Inputs) {
		limit = len(fe.Inputs)
	}
	results := make([]store.TaskRun, len(fe.Inputs))
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i, in := range fe.Inputs {
		i, in := i, in
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = s.runTask(substituteInput(*fe.Task, t.Key, in), depth)
		}()
	}
	wg.Wait()

	var stdout, stderr strings.Builder
	failed := 0
	for _, r := range results {
		stdout.WriteString(r.Stdout)
		stderr.WriteString(r.Stderr)
		if r.ResultState != "SUCCESS" {
			failed++
		}
	}
	tr.Iterations = results
	tr.Stdout = stdout.String()
	tr.Stderr = stderr.String()
	if failed > 0 {
		tr.ResultState = "FAILED"
		tr.Stderr = fmt.Sprintf("%d of %d iterations failed\n%s", failed, len(results), tr.Stderr)
	}
	return tr
}

// substituteInput returns a COPY of the nested task with `{{input}}` replaced
// by this iteration's value, and a per-iteration key so the results are
// distinguishable.
func substituteInput(t store.Task, parentKey, input string) store.Task {
	t.Key = fmt.Sprintf("%s_%s", parentKey, input)
	if len(t.PythonParams) > 0 {
		params := make([]string, len(t.PythonParams))
		for i, p := range t.PythonParams {
			params[i] = strings.ReplaceAll(p, "{{input}}", input)
		}
		t.PythonParams = params
	}
	if len(t.NotebookParams) > 0 {
		params := make(map[string]string, len(t.NotebookParams))
		for k, v := range t.NotebookParams {
			params[k] = strings.ReplaceAll(v, "{{input}}", input)
		}
		t.NotebookParams = params
	}
	return t
}
func (s *Server) runTask(t store.Task, depth int) store.TaskRun {
	tr := store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED"}
	env, err := s.resolveSecrets(t.SparkEnvVars)
	if err != nil {
		tr.ResultState = "FAILED"
		tr.Stderr = err.Error()
		return tr
	}
	conf, err := s.resolveSecrets(t.SparkConf)
	if err != nil {
		tr.ResultState = "FAILED"
		tr.Stderr = err.Error()
		return tr
	}
	// dbt loads no task code: its "code" is a project directory, and the
	// statement it produces is generated rather than read from a file.
	if t.Dbt != nil {
		return s.runDbtTask(t, env, conf)
	}
	if t.RunJob != nil {
		return s.runJobTask(t, depth)
	}
	if t.ForEach != nil {
		return s.runForEachTask(t, depth)
	}
	code, path, err := s.loadTaskCode(t)
	if err != nil {
		tr.ResultState = "FAILED"
		tr.Stderr = err.Error()
		return tr
	}
	var req spark.Request
	if t.SQLFile != "" {
		req = sparkSQLRequest(code, "job-"+t.Key)
		req.Env, req.Conf = env, conf
	} else {
		req = spark.Request{
			Session: "job-" + t.Key,
			Code:    pythonPreamble(t, path, env) + code,
			Kind:    "python",
			Env:     env,
			Conf:    conf,
		}
	}
	res, err := s.Spark.Run(req)
	if err != nil {
		tr.ResultState = "FAILED"
		tr.Stderr = err.Error()
		return tr
	}
	tr.Stdout = res.Stdout
	tr.Stderr = res.EValue
	if !res.OK {
		tr.ResultState = "FAILED"
		if tr.Stderr == "" {
			tr.Stderr = res.EName
		}
		return tr
	}
	tr.ResultState = "SUCCESS"
	return tr
}

// pythonPreamble delivers argv, notebook params, and resolved secrets the
// way a Databricks cluster would: the workspace process resolves
// {{secrets}} and the driver sees them in os.environ. The family's
// spark-agent drops req.Env, so baking the map into the code is the
// attach, not a lookalike.
func pythonPreamble(t store.Task, path string, env map[string]string) string {
	var b strings.Builder
	b.WriteString("import json, os, sys\n")
	if t.NotebookPath != "" {
		pj, _ := json.Marshal(t.NotebookParams)
		fmt.Fprintf(&b, "for __k, __v in json.loads(%q).items():\n    globals()[__k] = __v\n", string(pj))
	} else {
		argv := append([]string{path}, t.PythonParams...)
		aj, _ := json.Marshal(argv)
		fmt.Fprintf(&b, "sys.argv = json.loads(%q)\n", string(aj))
	}
	if len(env) > 0 {
		ej, _ := json.Marshal(env)
		fmt.Fprintf(&b, "os.environ.update(json.loads(%q))\n", string(ej))
	}
	return b.String()
}

func (s *Server) loadTaskCode(t store.Task) (code, path string, err error) {
	p := t.NotebookPath
	if p == "" {
		p = t.PythonFile
	}
	if p == "" {
		p = t.SQLFile
	}
	if strings.HasPrefix(p, "dbfs:") {
		b, err := s.Store.DBFS.Get(p)
		return string(b), p, err
	}
	b, _, err := s.Store.Workspace.Get(p)
	return string(b), p, err
}

func (s *Server) resolveSecrets(vars map[string]string) (map[string]string, error) {
	if len(vars) == 0 {
		return nil, nil
	}
	out := map[string]string{}
	for k, v := range vars {
		matches := secretRef.FindAllStringSubmatch(v, -1)
		for _, m := range matches {
			val, err := s.resolveSecretValue(m[1], m[2])
			if err != nil {
				return nil, fmt.Errorf("secret {{secrets/%s/%s}} : %w", m[1], m[2], err)
			}
			v = strings.ReplaceAll(v, m[0], val)
		}
		out[k] = v
	}
	return out, nil
}

func jobSettings(job *store.Job) map[string]any {
	var tasks []map[string]any
	for _, t := range job.Tasks {
		m := map[string]any{"task_key": t.Key, "run_if": t.RunIf}
		if len(t.DependsOn) > 0 {
			var deps []map[string]any
			for _, d := range t.DependsOn {
				dep := map[string]any{"task_key": d.Key}
				if d.Outcome != "" {
					dep["outcome"] = d.Outcome
				}
				deps = append(deps, dep)
			}
			m["depends_on"] = deps
		}
		if t.Condition != nil {
			m["condition_task"] = map[string]any{
				"op": t.Condition.Op, "left": t.Condition.Left, "right": t.Condition.Right,
			}
		}
		if t.NotebookPath != "" {
			m["notebook_task"] = map[string]any{"notebook_path": t.NotebookPath, "base_parameters": t.NotebookParams}
		}
		if t.PythonFile != "" {
			m["spark_python_task"] = map[string]any{"python_file": t.PythonFile, "parameters": t.PythonParams}
		}
		if t.SQLFile != "" {
			m["sql_task"] = map[string]any{"file": map[string]any{"path": t.SQLFile}}
		}
		if t.Dbt != nil {
			d := map[string]any{
				"commands":          t.Dbt.Commands,
				"project_directory": t.Dbt.ProjectDirectory,
				"warehouse_id":      t.Dbt.WarehouseID,
			}
			for k, v := range map[string]string{
				"catalog": t.Dbt.Catalog, "schema": t.Dbt.Schema,
				"profiles_directory": t.Dbt.ProfilesDirectory,
			} {
				if v != "" {
					d[k] = v
				}
			}
			m["dbt_task"] = d
		}
		tasks = append(tasks, m)
	}
	return map[string]any{"name": job.Name, "tasks": tasks}
}

func runJSON(run *store.Run) map[string]any {
	var tasks []map[string]any
	for _, t := range run.Tasks {
		m := map[string]any{
			"task_key": t.Key,
			"state":    map[string]any{"life_cycle_state": t.LifeCycle, "result_state": t.ResultState},
		}
		// The outcome is the only way a caller can tell which arm ran: the
		// condition task's own result_state is SUCCESS either way.
		if t.ConditionOutcome != "" {
			m["condition_task"] = map[string]any{"outcome": t.ConditionOutcome}
		}
		// Per-input outcome counts, the shape the API reports for a loop. The
		// task's own result_state says only whether ANY iteration failed.
		if t.Iterations != nil {
			succeeded := 0
			for _, it := range t.Iterations {
				if it.ResultState == "SUCCESS" {
					succeeded++
				}
			}
			m["for_each_task"] = map[string]any{"task_run_stats": map[string]any{
				"total_iterations":     len(t.Iterations),
				"succeeded_iterations": succeeded,
				"failed_iterations":    len(t.Iterations) - succeeded,
			}}
		}
		// Why a task failed, where the caller looks for it. Without this a
		// refusal — a nesting bound, a missing job_id — arrives as a bare
		// FAILED and the operator has to read the emulator's logs to learn
		// which of several possible causes it was.
		if t.ResultState == "FAILED" && t.Stderr != "" {
			m["state"].(map[string]any)["state_message"] = t.Stderr
		}
		tasks = append(tasks, m)
	}
	return map[string]any{
		"run_id":     run.ID,
		"job_id":     run.JobID,
		"state":      map[string]any{"life_cycle_state": run.LifeCycle, "result_state": run.ResultState, "state_message": run.StateMessage},
		"tasks":      tasks,
		"executedBy": run.ExecutedBy,
	}
}

// num reads a JSON number, which arrives as float64 through encoding/json.
// Returns 0 for anything else, so a caller that requires the value checks it.
func num(v any) float64 {
	f, _ := v.(float64)
	return f
}

// decodeForEachInputs reads for_each_task.inputs, which the Jobs API carries as
// a JSON-ENCODED STRING rather than a list — `"[\"a\",\"b\"]"`. Accepting a
// real list too, because an SDK that has already decoded it is not wrong and
// refusing that would be a distinction with no user.
//
// Elements are stringified rather than required to be strings: real inputs are
// commonly numbers, and `{{input}}` substitutes text either way.
func decodeForEachInputs(v any) ([]string, error) {
	var raw []any
	switch t := v.(type) {
	case string:
		if strings.TrimSpace(t) == "" {
			return nil, fmt.Errorf("for_each_task.inputs is required")
		}
		if err := json.Unmarshal([]byte(t), &raw); err != nil {
			return nil, fmt.Errorf("for_each_task.inputs is not a JSON array: %w", err)
		}
	case []any:
		raw = t
	case nil:
		return nil, fmt.Errorf("for_each_task.inputs is required")
	default:
		return nil, fmt.Errorf("for_each_task.inputs must be a JSON array (or its string form)")
	}
	if len(raw) == 0 {
		// A loop over nothing that reports SUCCESS is the silent no-op this
		// repo's refusals exist to prevent.
		return nil, fmt.Errorf("for_each_task.inputs is empty: a loop over no inputs runs nothing")
	}
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		out = append(out, fmt.Sprint(e))
	}
	return out, nil
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func stringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return nil
	}
	out := map[string]string{}
	for k, val := range m {
		out[k] = fmt.Sprint(val)
	}
	return out
}

func int64From(raw json.RawMessage) int64 {
	var n int64
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return parseInt64(s)
	}
	return 0
}
