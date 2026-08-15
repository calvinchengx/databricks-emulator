package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
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
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
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
	go s.executeRun(job, run)
	writeJSON(w, http.StatusOK, map[string]any{"run_id": run.ID, "number_in_job": 1})
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
	writeJSON(w, http.StatusOK, map[string]any{
		"metadata": runJSON(run),
		"logs":     run.Stdout,
		"error":    run.Stderr,
	})
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
			if m, ok := d.(map[string]any); ok {
				task.DependsOn = append(task.DependsOn, str(m["task_key"]))
			}
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
	if _, ok := t["dbt_task"]; ok {
		return task, fmt.Errorf("dbt_task is refused by name")
	}
	if _, ok := t["pipeline_task"]; ok {
		return task, fmt.Errorf("pipeline_task (DLT) is refused by name")
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
	if task.NotebookPath == "" && task.PythonFile == "" && task.SQLFile == "" {
		return task, fmt.Errorf("task %q must be notebook_task, spark_python_task, or sql_task.file", task.Key)
	}
	return task, nil
}

func (s *Server) executeRun(job *store.Job, run *store.Run) {
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
		var mu sync.Mutex
		for _, t := range ready {
			t := t
			delete(remaining, t.Key)
			if !shouldRun(t, results) {
				results[t.Key] = store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED", ResultState: "SKIPPED"}
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				tr := s.runTask(t)
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
		if _, ok := done[d]; !ok {
			return false
		}
	}
	return true
}

func shouldRun(t store.Task, done map[string]store.TaskRun) bool {
	if len(t.DependsOn) == 0 {
		return true
	}
	success, failed, finished := 0, 0, 0
	for _, d := range t.DependsOn {
		tr := done[d]
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

func (s *Server) runTask(t store.Task) store.TaskRun {
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
		var preamble string
		if t.NotebookPath != "" {
			pj, _ := json.Marshal(t.NotebookParams)
			preamble = fmt.Sprintf("import json\nfor __k, __v in json.loads(%q).items():\n    globals()[__k] = __v\n", string(pj))
		} else {
			argv := append([]string{path}, t.PythonParams...)
			aj, _ := json.Marshal(argv)
			preamble = fmt.Sprintf("import sys, json\nsys.argv = json.loads(%q)\n", string(aj))
		}
		req = spark.Request{
			Session: "job-" + t.Key,
			Code:    preamble + code,
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

func (s *Server) loadTaskCode(t store.Task) (code, path string, err error) {
	p := t.NotebookPath
	if p == "" {
		p = t.PythonFile
	}
	if p == "" {
		p = t.SQLFile
	}
	path = p
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
		if t.NotebookPath != "" {
			m["notebook_task"] = map[string]any{"notebook_path": t.NotebookPath, "base_parameters": t.NotebookParams}
		}
		if t.PythonFile != "" {
			m["spark_python_task"] = map[string]any{"python_file": t.PythonFile, "parameters": t.PythonParams}
		}
		if t.SQLFile != "" {
			m["sql_task"] = map[string]any{"file": map[string]any{"path": t.SQLFile}}
		}
		tasks = append(tasks, m)
	}
	return map[string]any{"name": job.Name, "tasks": tasks}
}

func runJSON(run *store.Run) map[string]any {
	var tasks []map[string]any
	for _, t := range run.Tasks {
		tasks = append(tasks, map[string]any{
			"task_key": t.Key,
			"state":    map[string]any{"life_cycle_state": t.LifeCycle, "result_state": t.ResultState},
		})
	}
	return map[string]any{
		"run_id":     run.ID,
		"job_id":     run.JobID,
		"state":      map[string]any{"life_cycle_state": run.LifeCycle, "result_state": run.ResultState, "state_message": run.StateMessage},
		"tasks":      tasks,
		"executedBy": run.ExecutedBy,
	}
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
