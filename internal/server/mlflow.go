package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) mlflow(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	path := strings.TrimPrefix(r.URL.Path, "/api/2.0/mlflow")
	key := r.Method + " " + strings.TrimSuffix(path, "/")
	switch key {
	case "POST /experiments/create":
		s.mlCreateExperiment(w, r)
	case "GET /experiments/get":
		s.mlGetExperiment(w, r)
	case "GET /experiments/get-by-name":
		s.mlGetExperimentByName(w, r)
	case "GET /experiments/list", "POST /experiments/search":
		s.mlSearchExperiments(w, r)
	case "POST /experiments/update":
		s.mlUpdateExperiment(w, r)
	case "POST /experiments/delete":
		s.mlDeleteExperiment(w, r)
	case "POST /experiments/restore":
		s.mlRestoreExperiment(w, r)
	case "POST /runs/create":
		s.mlCreateRun(w, r, p)
	case "GET /runs/get":
		s.mlGetRun(w, r)
	case "POST /runs/update":
		s.mlUpdateRun(w, r)
	case "POST /runs/delete":
		s.mlDeleteRun(w, r)
	case "POST /runs/restore":
		s.mlRestoreRun(w, r)
	case "POST /runs/search":
		s.mlSearchRuns(w, r)
	case "POST /runs/log-metric":
		s.mlLogMetric(w, r)
	case "POST /runs/log-parameter", "POST /runs/log-param":
		s.mlLogParam(w, r)
	case "POST /runs/log-batch":
		s.mlLogBatch(w, r)
	case "POST /runs/set-tag":
		s.mlSetTag(w, r)
	case "POST /runs/delete-tag":
		s.mlDeleteTag(w, r)
	case "GET /metrics/get-history":
		s.mlMetricHistory(w, r)
	case "POST /registered-models/create":
		s.mlCreateModel(w, r, p)
	case "GET /registered-models/get":
		s.mlGetModelOSS(w, r)
	case "GET /databricks/registered-models/get":
		s.mlGetModelDatabricks(w, r)
	case "GET /registered-models/list", "GET /registered-models/search":
		s.mlSearchModels(w, r)
	case "PATCH /registered-models/update":
		s.mlUpdateModel(w, r)
	case "POST /registered-models/rename":
		s.mlRenameModel(w, r)
	case "DELETE /registered-models/delete":
		s.mlDeleteModel(w, r)
	case "POST /registered-models/get-latest-versions":
		s.mlLatestVersions(w, r)
	case "POST /model-versions/create":
		s.mlCreateModelVersion(w, r, p)
	case "GET /model-versions/get":
		s.mlGetModelVersion(w, r)
	case "GET /model-versions/search":
		s.mlSearchModelVersions(w, r)
	case "PATCH /model-versions/update":
		s.mlUpdateModelVersion(w, r)
	case "POST /model-versions/transition-stage", "POST /databricks/model-versions/transition-stage":
		s.mlTransitionStage(w, r)
	case "GET /artifacts/list", "POST /runs/log-model":
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", store.ErrMLArtifact.Error())
	default:
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			"API endpoint "+r.URL.Path+" is not implemented in databricks-emulator")
	}
}

type mlTagBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func mlTags(in []mlTagBody) []store.MLTag {
	out := make([]store.MLTag, 0, len(in))
	for _, t := range in {
		out = append(out, store.MLTag{Key: t.Key, Value: t.Value})
	}
	return out
}

func experimentJSON(e *store.Experiment) map[string]any {
	tags := make([]map[string]string, 0, len(e.Tags))
	for _, t := range e.Tags {
		tags = append(tags, map[string]string{"key": t.Key, "value": t.Value})
	}
	return map[string]any{
		"experiment_id":     e.ID,
		"name":              e.Name,
		"artifact_location": e.ArtifactLocation,
		"lifecycle_stage":   e.Lifecycle,
		"creation_time":     e.CreationTime,
		"last_update_time":  e.LastUpdateTime,
		"tags":              tags,
	}
}

func mlRunJSON(r *store.MLRun) map[string]any {
	metrics := make([]map[string]any, 0)
	for _, m := range store.LatestMetrics(r.Metrics) {
		metrics = append(metrics, map[string]any{
			"key": m.Key, "value": m.Value, "timestamp": m.Timestamp, "step": m.Step,
		})
	}
	params := make([]map[string]string, 0, len(r.Params))
	for k, v := range r.Params {
		params = append(params, map[string]string{"key": k, "value": v})
	}
	tags := make([]map[string]string, 0, len(r.Tags))
	for k, v := range r.Tags {
		tags = append(tags, map[string]string{"key": k, "value": v})
	}
	info := map[string]any{
		"run_id":          r.ID,
		"run_uuid":        r.ID,
		"experiment_id":   r.ExperimentID,
		"user_id":         r.UserID,
		"status":          r.Status,
		"start_time":      r.StartTime,
		"artifact_uri":    r.ArtifactURI,
		"lifecycle_stage": r.Lifecycle,
		"run_name":        r.Name,
	}
	if r.EndTime > 0 {
		info["end_time"] = r.EndTime
	}
	return map[string]any{
		"info": info,
		"data": map[string]any{"metrics": metrics, "params": params, "tags": tags},
	}
}

func modelJSON(m *store.RegisteredModel, versions []*store.ModelVersion) map[string]any {
	var latest []map[string]any
	for _, v := range versions {
		if v.Stage == store.MLStageStaging || v.Stage == store.MLStageProduction {
			latest = append(latest, modelVersionJSON(v))
		}
	}
	tags := make([]map[string]string, 0, len(m.Tags))
	for k, v := range m.Tags {
		tags = append(tags, map[string]string{"key": k, "value": v})
	}
	out := map[string]any{
		"name":                   m.Name,
		"creation_timestamp":     m.CreatedAt,
		"last_updated_timestamp": m.UpdatedAt,
		"user_id":                m.UserID,
		"latest_versions":        latest,
		"tags":                   tags,
	}
	if m.Description != "" {
		out["description"] = m.Description
	}
	return out
}

func modelDatabricksJSON(m *store.RegisteredModel, versions []*store.ModelVersion) map[string]any {
	out := modelJSON(m, versions)
	out["id"] = m.Name
	return out
}

func modelVersionJSON(v *store.ModelVersion) map[string]any {
	tags := make([]map[string]string, 0, len(v.Tags))
	for k, val := range v.Tags {
		tags = append(tags, map[string]string{"key": k, "value": val})
	}
	out := map[string]any{
		"name":                   v.Name,
		"version":                v.Version,
		"creation_timestamp":     v.CreatedAt,
		"last_updated_timestamp": v.UpdatedAt,
		"current_stage":          v.Stage,
		"source":                 v.Source,
		"status":                 v.Status,
		"tags":                   tags,
	}
	if v.Description != "" {
		out["description"] = v.Description
	}
	if v.RunID != "" {
		out["run_id"] = v.RunID
	}
	if v.RunLink != "" {
		out["run_link"] = v.RunLink
	}
	if v.UserID != "" {
		out["user_id"] = v.UserID
	}
	return out
}

func writeMLErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrMLExists):
		writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", err.Error())
	case errors.Is(err, store.ErrMLNotFound):
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", err.Error())
	case errors.Is(err, store.ErrMLFilter), errors.Is(err, store.ErrMLArtifact):
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
	default:
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
	}
}

func (s *Server) mlCreateExperiment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name             string      `json:"name"`
		ArtifactLocation string      `json:"artifact_location"`
		Tags             []mlTagBody `json:"tags"`
		TraceLocation    any         `json:"trace_location"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if body.TraceLocation != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "experiment traces are not implemented")
		return
	}
	exp, err := s.Store.MLflow.CreateExperiment(body.Name, body.ArtifactLocation, mlTags(body.Tags), s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiment_id": exp.ID})
}

func (s *Server) mlGetExperiment(w http.ResponseWriter, r *http.Request) {
	exp, err := s.Store.MLflow.GetExperiment(query(r, "experiment_id"))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiment": experimentJSON(exp)})
}

func (s *Server) mlGetExperimentByName(w http.ResponseWriter, r *http.Request) {
	exp, err := s.Store.MLflow.GetExperimentByName(query(r, "experiment_name"))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiment": experimentJSON(exp)})
}

// maxResultsCap bounds a caller's max_results before it is narrowed to int.
// The value only truncates an already-built slice, so the store is in no
// danger from a large one — but int64 -> int is lossy on a 32-bit build,
// where max_results=2147483648 wraps negative and silently becomes the
// store's default instead of the ceiling the caller asked for. Clamping
// first makes the conversion provably exact on every platform.
const maxResultsCap = 50000

// parseMaxResults reads a max_results query parameter. Absent, unparseable
// or non-positive returns 0, which leaves the store's own default in place.
func parseMaxResults(s string) int {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	if n > maxResultsCap {
		return maxResultsCap
	}
	return int(n)
}

func (s *Server) mlSearchExperiments(w http.ResponseWriter, r *http.Request) {
	filter, view := query(r, "filter"), query(r, "view_type")
	maxResults := parseMaxResults(query(r, "max_results"))
	if r.Method == http.MethodPost {
		var body struct {
			Filter     string `json:"filter"`
			MaxResults int    `json:"max_results"`
			ViewType   string `json:"view_type"`
			PageToken  string `json:"page_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			writeBodyErr(w, err)
			return
		}
		if body.PageToken != "" {
			writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "page_token is not implemented")
			return
		}
		filter, view, maxResults = body.Filter, body.ViewType, body.MaxResults
	}
	exps, err := s.Store.MLflow.SearchExperiments(filter, view, maxResults)
	if err != nil {
		writeMLErr(w, err)
		return
	}
	rows := make([]map[string]any, 0, len(exps))
	for _, e := range exps {
		rows = append(rows, experimentJSON(e))
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiments": rows})
}

func (s *Server) mlUpdateExperiment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExperimentID string `json:"experiment_id"`
		NewName      string `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if _, err := s.Store.MLflow.UpdateExperiment(body.ExperimentID, body.NewName, s.Clock.Now()); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlDeleteExperiment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExperimentID string `json:"experiment_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if err := s.Store.MLflow.DeleteExperiment(body.ExperimentID, s.Clock.Now()); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlRestoreExperiment(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExperimentID string `json:"experiment_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if err := s.Store.MLflow.RestoreExperiment(body.ExperimentID, s.Clock.Now()); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlCreateRun(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		ExperimentID string      `json:"experiment_id"`
		RunName      string      `json:"run_name"`
		StartTime    int64       `json:"start_time"`
		UserID       string      `json:"user_id"`
		Tags         []mlTagBody `json:"tags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	user := body.UserID
	if user == "" {
		user = p.UserName
	}
	run, err := s.Store.MLflow.CreateRun(body.ExperimentID, body.RunName, user, body.StartTime, mlTags(body.Tags), s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": mlRunJSON(run)})
}

func (s *Server) mlGetRun(w http.ResponseWriter, r *http.Request) {
	id := query(r, "run_id")
	if id == "" {
		id = query(r, "run_uuid")
	}
	run, err := s.Store.MLflow.GetRun(id)
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": mlRunJSON(run)})
}

func (s *Server) mlUpdateRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID   string `json:"run_id"`
		RunUUID string `json:"run_uuid"`
		Status  string `json:"status"`
		EndTime int64  `json:"end_time"`
		RunName string `json:"run_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	id := body.RunID
	if id == "" {
		id = body.RunUUID
	}
	run, err := s.Store.MLflow.UpdateRun(id, body.Status, body.RunName, body.EndTime, s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run_info": mlRunJSON(run)["info"]})
}

func (s *Server) mlDeleteRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if err := s.Store.MLflow.DeleteRun(body.RunID); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlRestoreRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if err := s.Store.MLflow.RestoreRun(body.RunID); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlSearchRuns(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExperimentIDs []string `json:"experiment_ids"`
		Filter        string   `json:"filter"`
		MaxResults    int      `json:"max_results"`
		RunViewType   string   `json:"run_view_type"`
		PageToken     string   `json:"page_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeBodyErr(w, err)
		return
	}
	if body.PageToken != "" {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "page_token is not implemented")
		return
	}
	runs, err := s.Store.MLflow.SearchRuns(body.ExperimentIDs, body.Filter, body.RunViewType, body.MaxResults)
	if err != nil {
		writeMLErr(w, err)
		return
	}
	rows := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		rows = append(rows, mlRunJSON(run))
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": rows})
}

func (s *Server) mlLogMetric(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID     string  `json:"run_id"`
		RunUUID   string  `json:"run_uuid"`
		Key       string  `json:"key"`
		Value     float64 `json:"value"`
		Timestamp int64   `json:"timestamp"`
		Step      int64   `json:"step"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	id := body.RunID
	if id == "" {
		id = body.RunUUID
	}
	if err := s.Store.MLflow.LogMetric(id, body.Key, body.Value, body.Timestamp, body.Step); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlLogParam(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID   string `json:"run_id"`
		RunUUID string `json:"run_uuid"`
		Key     string `json:"key"`
		Value   string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	id := body.RunID
	if id == "" {
		id = body.RunUUID
	}
	if err := s.Store.MLflow.LogParam(id, body.Key, body.Value); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlLogBatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID   string `json:"run_id"`
		Metrics []struct {
			Key       string  `json:"key"`
			Value     float64 `json:"value"`
			Timestamp int64   `json:"timestamp"`
			Step      int64   `json:"step"`
		} `json:"metrics"`
		Params []mlTagBody `json:"params"`
		Tags   []mlTagBody `json:"tags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	var metrics []store.MLMetric
	for _, m := range body.Metrics {
		metrics = append(metrics, store.MLMetric{Key: m.Key, Value: m.Value, Timestamp: m.Timestamp, Step: m.Step})
	}
	var params []store.MLParam
	for _, p := range body.Params {
		params = append(params, store.MLParam{Key: p.Key, Value: p.Value})
	}
	if err := s.Store.MLflow.LogBatch(body.RunID, metrics, params, mlTags(body.Tags)); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlSetTag(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if err := s.Store.MLflow.SetTag(body.RunID, body.Key, body.Value); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlDeleteTag(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID string `json:"run_id"`
		Key   string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if err := s.Store.MLflow.DeleteTag(body.RunID, body.Key); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlMetricHistory(w http.ResponseWriter, r *http.Request) {
	hist, err := s.Store.MLflow.MetricHistory(query(r, "run_id"), query(r, "metric_key"))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	rows := make([]map[string]any, 0, len(hist))
	for _, m := range hist {
		rows = append(rows, map[string]any{
			"key": m.Key, "value": m.Value, "timestamp": m.Timestamp, "step": m.Step,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"metrics": rows})
}

func (s *Server) mlCreateModel(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		Name        string      `json:"name"`
		Description string      `json:"description"`
		Tags        []mlTagBody `json:"tags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	mod, err := s.Store.MLflow.CreateModel(body.Name, body.Description, p.UserName, mlTags(body.Tags), s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"registered_model": modelJSON(mod, nil)})
}

func (s *Server) mlGetModelOSS(w http.ResponseWriter, r *http.Request) {
	mod, err := s.Store.MLflow.GetModel(query(r, "name"))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"registered_model": modelJSON(mod, s.Store.MLflow.ListVersions(mod.Name))})
}

func (s *Server) mlGetModelDatabricks(w http.ResponseWriter, r *http.Request) {
	mod, err := s.Store.MLflow.GetModel(query(r, "name"))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"registered_model_databricks": modelDatabricksJSON(mod, s.Store.MLflow.ListVersions(mod.Name)),
	})
}

func (s *Server) mlSearchModels(w http.ResponseWriter, r *http.Request) {
	if query(r, "page_token") != "" {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "page_token is not implemented")
		return
	}
	mods, err := s.Store.MLflow.SearchModels(query(r, "filter"), parseMaxResults(query(r, "max_results")))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	rows := make([]map[string]any, 0, len(mods))
	for _, mod := range mods {
		rows = append(rows, modelJSON(mod, s.Store.MLflow.ListVersions(mod.Name)))
	}
	writeJSON(w, http.StatusOK, map[string]any{"registered_models": rows})
}

func (s *Server) mlUpdateModel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if _, err := s.Store.MLflow.UpdateModel(body.Name, body.Description, s.Clock.Now()); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlRenameModel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		NewName string `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	mod, err := s.Store.MLflow.RenameModel(body.Name, body.NewName, s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"registered_model": modelJSON(mod, s.Store.MLflow.ListVersions(mod.Name))})
}

func (s *Server) mlDeleteModel(w http.ResponseWriter, r *http.Request) {
	name := query(r, "name")
	if name == "" {
		var body struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		name = body.Name
	}
	if err := s.Store.MLflow.DeleteModel(name); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlLatestVersions(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name   string   `json:"name"`
		Stages []string `json:"stages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	vers, err := s.Store.MLflow.LatestVersions(body.Name, body.Stages)
	if err != nil {
		writeMLErr(w, err)
		return
	}
	rows := make([]map[string]any, 0, len(vers))
	for _, v := range vers {
		rows = append(rows, modelVersionJSON(v))
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_versions": rows})
}

func (s *Server) mlCreateModelVersion(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		Name        string      `json:"name"`
		Source      string      `json:"source"`
		RunID       string      `json:"run_id"`
		RunLink     string      `json:"run_link"`
		Description string      `json:"description"`
		Tags        []mlTagBody `json:"tags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	v, err := s.Store.MLflow.CreateModelVersion(body.Name, body.Source, body.RunID, body.RunLink, body.Description, p.UserName, mlTags(body.Tags), s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_version": modelVersionJSON(v)})
}

func (s *Server) mlGetModelVersion(w http.ResponseWriter, r *http.Request) {
	v, err := s.Store.MLflow.GetModelVersion(query(r, "name"), query(r, "version"))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_version": modelVersionJSON(v)})
}

func (s *Server) mlSearchModelVersions(w http.ResponseWriter, r *http.Request) {
	if query(r, "page_token") != "" {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "page_token is not implemented")
		return
	}
	vers, err := s.Store.MLflow.SearchModelVersions(query(r, "filter"), parseMaxResults(query(r, "max_results")))
	if err != nil {
		writeMLErr(w, err)
		return
	}
	rows := make([]map[string]any, 0, len(vers))
	for _, v := range vers {
		rows = append(rows, modelVersionJSON(v))
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_versions": rows})
}

func (s *Server) mlUpdateModelVersion(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	if _, err := s.Store.MLflow.UpdateModelVersion(body.Name, body.Version, body.Description, s.Clock.Now()); err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) mlTransitionStage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name                    string `json:"name"`
		Version                 string `json:"version"`
		Stage                   string `json:"stage"`
		ArchiveExistingVersions bool   `json:"archive_existing_versions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeBodyErr(w, err)
		return
	}
	v, err := s.Store.MLflow.TransitionStage(body.Name, body.Version, body.Stage, body.ArchiveExistingVersions, s.Clock.Now())
	if err != nil {
		writeMLErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_version_databricks": modelVersionJSON(v)})
}
