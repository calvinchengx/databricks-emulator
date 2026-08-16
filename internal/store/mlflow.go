package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Tracking lifecycle and run status match the MLflow REST contract.
const (
	MLActive  = "active"
	MLDeleted = "deleted"

	MLRunning   = "RUNNING"
	MLScheduled = "SCHEDULED"
	MLFinished  = "FINISHED"
	MLFailed    = "FAILED"
	MLKilled    = "KILLED"

	MLStageNone       = "None"
	MLStageStaging    = "Staging"
	MLStageProduction = "Production"
	MLStageArchived   = "Archived"

	MLVersionReady = "READY"

	MLViewActive  = "ACTIVE_ONLY"
	MLViewDeleted = "DELETED_ONLY"
	MLViewAll     = "ALL"
)

var (
	ErrMLExists   = errors.New("already exists")
	ErrMLNotFound = errors.New("not found")
	ErrMLInvalid  = errors.New("invalid")
	ErrMLFilter   = errors.New("filter not implemented")
	ErrMLArtifact = errors.New("artifact store is not attached; tracking metadata only")
)

// MLTag is an experiment, run, or model tag.
type MLTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// MLMetric is one logged metric point.
type MLMetric struct {
	Key       string  `json:"key"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Step      int64   `json:"step"`
}

// MLParam is a run parameter (string value, logged once).
type MLParam struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Experiment is an MLflow experiment.
type Experiment struct {
	ID               string
	Name             string
	ArtifactLocation string
	Lifecycle        string
	CreationTime     int64
	LastUpdateTime   int64
	Tags             []MLTag
}

// MLRun is one tracking run.
type MLRun struct {
	ID           string
	ExperimentID string
	Name         string
	UserID       string
	Status       string
	Lifecycle    string
	ArtifactURI  string
	StartTime    int64
	EndTime      int64
	Metrics      []MLMetric
	Params       map[string]string
	Tags         map[string]string
}

// RegisteredModel is a model-registry name.
type RegisteredModel struct {
	Name        string
	Description string
	UserID      string
	CreatedAt   int64
	UpdatedAt   int64
	Tags        map[string]string
}

// ModelVersion is one version under a registered model.
type ModelVersion struct {
	Name        string
	Version     string
	Source      string
	RunID       string
	RunLink     string
	Description string
	UserID      string
	Stage       string
	Status      string
	CreatedAt   int64
	UpdatedAt   int64
	Tags        map[string]string
}

type persistedMLflow struct {
	NextExp     int64                 `json:"next_exp"`
	Experiments []persistedExperiment `json:"experiments"`
	Runs        []persistedRun        `json:"runs"`
	Models      []persistedModel      `json:"models"`
	Versions    []persistedVersion    `json:"versions"`
}

type persistedExperiment struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	ArtifactLocation string  `json:"artifact_location"`
	Lifecycle        string  `json:"lifecycle"`
	CreationTime     int64   `json:"creation_time"`
	LastUpdateTime   int64   `json:"last_update_time"`
	Tags             []MLTag `json:"tags,omitempty"`
}

type persistedRun struct {
	ID           string            `json:"id"`
	ExperimentID string            `json:"experiment_id"`
	Name         string            `json:"name,omitempty"`
	UserID       string            `json:"user_id,omitempty"`
	Status       string            `json:"status"`
	Lifecycle    string            `json:"lifecycle"`
	ArtifactURI  string            `json:"artifact_uri"`
	StartTime    int64             `json:"start_time"`
	EndTime      int64             `json:"end_time,omitempty"`
	Metrics      []MLMetric        `json:"metrics,omitempty"`
	Params       map[string]string `json:"params,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

type persistedModel struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	UserID      string            `json:"user_id,omitempty"`
	CreatedAt   int64             `json:"created_at"`
	UpdatedAt   int64             `json:"updated_at"`
	Tags        map[string]string `json:"tags,omitempty"`
}

type persistedVersion struct {
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Source      string            `json:"source"`
	RunID       string            `json:"run_id,omitempty"`
	RunLink     string            `json:"run_link,omitempty"`
	Description string            `json:"description,omitempty"`
	UserID      string            `json:"user_id,omitempty"`
	Stage       string            `json:"stage"`
	Status      string            `json:"status"`
	CreatedAt   int64             `json:"created_at"`
	UpdatedAt   int64             `json:"updated_at"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// MLflow is a file-backed tracking store + model registry under data/mlflow/.
type MLflow struct {
	mu          sync.Mutex
	dir         string
	nextExp     int64
	experiments map[string]*Experiment
	runs        map[string]*MLRun
	models      map[string]*RegisteredModel
	versions    map[string][]*ModelVersion
}

func openMLflow(dataDir string) (*MLflow, error) {
	dir := filepath.Join(dataDir, "mlflow")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m := &MLflow{
		dir:         dir,
		experiments: map[string]*Experiment{},
		runs:        map[string]*MLRun{},
		models:      map[string]*RegisteredModel{},
		versions:    map[string][]*ModelVersion{},
	}
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	var dump persistedMLflow
	if err := json.Unmarshal(b, &dump); err != nil {
		return nil, fmt.Errorf("mlflow store: %w", err)
	}
	m.nextExp = dump.NextExp
	for _, row := range dump.Experiments {
		m.experiments[row.ID] = &Experiment{
			ID: row.ID, Name: row.Name, ArtifactLocation: row.ArtifactLocation,
			Lifecycle: row.Lifecycle, CreationTime: row.CreationTime,
			LastUpdateTime: row.LastUpdateTime, Tags: append([]MLTag{}, row.Tags...),
		}
	}
	for _, row := range dump.Runs {
		params := row.Params
		if params == nil {
			params = map[string]string{}
		}
		tags := row.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		m.runs[row.ID] = &MLRun{
			ID: row.ID, ExperimentID: row.ExperimentID, Name: row.Name,
			UserID: row.UserID, Status: row.Status, Lifecycle: row.Lifecycle,
			ArtifactURI: row.ArtifactURI, StartTime: row.StartTime, EndTime: row.EndTime,
			Metrics: append([]MLMetric{}, row.Metrics...), Params: params, Tags: tags,
		}
	}
	for _, row := range dump.Models {
		tags := row.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		m.models[row.Name] = &RegisteredModel{
			Name: row.Name, Description: row.Description, UserID: row.UserID,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Tags: tags,
		}
	}
	for _, row := range dump.Versions {
		tags := row.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		m.versions[row.Name] = append(m.versions[row.Name], &ModelVersion{
			Name: row.Name, Version: row.Version, Source: row.Source,
			RunID: row.RunID, RunLink: row.RunLink, Description: row.Description,
			UserID: row.UserID, Stage: row.Stage, Status: row.Status,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, Tags: tags,
		})
	}
	return m, nil
}

func (m *MLflow) persistLocked() error {
	dump := persistedMLflow{NextExp: m.nextExp}
	for _, exp := range m.experiments {
		dump.Experiments = append(dump.Experiments, persistedExperiment{
			ID: exp.ID, Name: exp.Name, ArtifactLocation: exp.ArtifactLocation,
			Lifecycle: exp.Lifecycle, CreationTime: exp.CreationTime,
			LastUpdateTime: exp.LastUpdateTime, Tags: append([]MLTag{}, exp.Tags...),
		})
	}
	for _, run := range m.runs {
		dump.Runs = append(dump.Runs, persistedRun{
			ID: run.ID, ExperimentID: run.ExperimentID, Name: run.Name,
			UserID: run.UserID, Status: run.Status, Lifecycle: run.Lifecycle,
			ArtifactURI: run.ArtifactURI, StartTime: run.StartTime, EndTime: run.EndTime,
			Metrics: append([]MLMetric{}, run.Metrics...),
			Params:  copyMap(run.Params), Tags: copyMap(run.Tags),
		})
	}
	for _, mod := range m.models {
		dump.Models = append(dump.Models, persistedModel{
			Name: mod.Name, Description: mod.Description, UserID: mod.UserID,
			CreatedAt: mod.CreatedAt, UpdatedAt: mod.UpdatedAt, Tags: copyMap(mod.Tags),
		})
	}
	for _, vers := range m.versions {
		for _, v := range vers {
			dump.Versions = append(dump.Versions, persistedVersion{
				Name: v.Name, Version: v.Version, Source: v.Source,
				RunID: v.RunID, RunLink: v.RunLink, Description: v.Description,
				UserID: v.UserID, Stage: v.Stage, Status: v.Status,
				CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt, Tags: copyMap(v.Tags),
			})
		}
	}
	b, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(m.dir, "state.json.tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(m.dir, "state.json"))
}

func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func ms(now int64) int64 { return now * 1000 }

var readRand = rand.Read

func newRunID() (string, error) {
	var b [16]byte
	if _, err := readRand(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func cloneExperiment(e *Experiment) *Experiment {
	if e == nil {
		return nil
	}
	cp := *e
	cp.Tags = append([]MLTag{}, e.Tags...)
	return &cp
}

func cloneRun(r *MLRun) *MLRun {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Metrics = append([]MLMetric{}, r.Metrics...)
	cp.Params = copyMap(r.Params)
	if cp.Params == nil {
		cp.Params = map[string]string{}
	}
	cp.Tags = copyMap(r.Tags)
	if cp.Tags == nil {
		cp.Tags = map[string]string{}
	}
	return &cp
}

func cloneModel(mod *RegisteredModel) *RegisteredModel {
	if mod == nil {
		return nil
	}
	cp := *mod
	cp.Tags = copyMap(mod.Tags)
	if cp.Tags == nil {
		cp.Tags = map[string]string{}
	}
	return &cp
}

func cloneVersion(v *ModelVersion) *ModelVersion {
	if v == nil {
		return nil
	}
	cp := *v
	cp.Tags = copyMap(v.Tags)
	if cp.Tags == nil {
		cp.Tags = map[string]string{}
	}
	return &cp
}

func matchView(lifecycle, view string) bool {
	switch strings.ToUpper(strings.TrimSpace(view)) {
	case "", MLViewActive:
		return lifecycle == MLActive
	case MLViewDeleted:
		return lifecycle == MLDeleted
	case MLViewAll:
		return true
	default:
		return lifecycle == MLActive
	}
}

func parseNameEquals(filter string) (string, bool) {
	f := strings.TrimSpace(filter)
	if f == "" {
		return "", true
	}
	for _, prefix := range []string{"name = ", "name=", "name == "} {
		if strings.HasPrefix(strings.ToLower(f), strings.ToLower(prefix)) {
			raw := strings.TrimSpace(f[len(prefix):])
			raw = strings.Trim(raw, `"'`)
			return raw, true
		}
	}
	return "", false
}

// CreateExperiment adds an active experiment. Deleted names may be reused.
func (m *MLflow) CreateExperiment(name, artifact string, tags []MLTag, now int64) (*Experiment, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, exp := range m.experiments {
		if exp.Name == name && exp.Lifecycle == MLActive {
			return nil, fmt.Errorf("%w: experiment %q", ErrMLExists, name)
		}
	}
	m.nextExp++
	id := strconv.FormatInt(m.nextExp, 10)
	if artifact == "" {
		artifact = "dbfs:/databricks/mlflow-tracking/" + id
	}
	t := ms(now)
	exp := &Experiment{
		ID: id, Name: name, ArtifactLocation: artifact, Lifecycle: MLActive,
		CreationTime: t, LastUpdateTime: t, Tags: append([]MLTag{}, tags...),
	}
	m.experiments[id] = exp
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneExperiment(exp), nil
}

// GetExperiment returns an experiment, including deleted ones.
func (m *MLflow) GetExperiment(id string) (*Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	exp, ok := m.experiments[id]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %s", ErrMLNotFound, id)
	}
	return cloneExperiment(exp), nil
}

// GetExperimentByName prefers an active experiment of that name.
func (m *MLflow) GetExperimentByName(name string) (*Experiment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var deleted *Experiment
	for _, exp := range m.experiments {
		if exp.Name != name {
			continue
		}
		if exp.Lifecycle == MLActive {
			return cloneExperiment(exp), nil
		}
		deleted = exp
	}
	if deleted != nil {
		return cloneExperiment(deleted), nil
	}
	return nil, fmt.Errorf("%w: experiment %q", ErrMLNotFound, name)
}

// SearchExperiments lists experiments. Only name= filters are implemented.
func (m *MLflow) SearchExperiments(filter, view string, maxResults int) ([]*Experiment, error) {
	name, ok := parseNameEquals(filter)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMLFilter, filter)
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Experiment
	for _, exp := range m.experiments {
		if !matchView(exp.Lifecycle, view) {
			continue
		}
		if name != "" && exp.Name != name {
			continue
		}
		out = append(out, cloneExperiment(exp))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if len(out) > maxResults {
		out = out[:maxResults]
	}
	return out, nil
}

// UpdateExperiment renames an active experiment.
func (m *MLflow) UpdateExperiment(id, newName string, now int64) (*Experiment, error) {
	newName = strings.TrimSpace(newName)
	if newName == "" {
		return nil, fmt.Errorf("%w: new_name is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	exp, ok := m.experiments[id]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %s", ErrMLNotFound, id)
	}
	for _, other := range m.experiments {
		if other.ID != id && other.Name == newName && other.Lifecycle == MLActive {
			return nil, fmt.Errorf("%w: experiment %q", ErrMLExists, newName)
		}
	}
	exp.Name = newName
	exp.LastUpdateTime = ms(now)
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneExperiment(exp), nil
}

// DeleteExperiment marks an experiment deleted.
func (m *MLflow) DeleteExperiment(id string, now int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exp, ok := m.experiments[id]
	if !ok {
		return fmt.Errorf("%w: experiment %s", ErrMLNotFound, id)
	}
	exp.Lifecycle = MLDeleted
	exp.LastUpdateTime = ms(now)
	return m.persistLocked()
}

// RestoreExperiment undeletes an experiment.
func (m *MLflow) RestoreExperiment(id string, now int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exp, ok := m.experiments[id]
	if !ok {
		return fmt.Errorf("%w: experiment %s", ErrMLNotFound, id)
	}
	for _, other := range m.experiments {
		if other.ID != id && other.Name == exp.Name && other.Lifecycle == MLActive {
			return fmt.Errorf("%w: experiment %q", ErrMLExists, exp.Name)
		}
	}
	exp.Lifecycle = MLActive
	exp.LastUpdateTime = ms(now)
	return m.persistLocked()
}

// CreateRun starts a RUNNING run in an active experiment.
func (m *MLflow) CreateRun(experimentID, name, userID string, startTime int64, tags []MLTag, now int64) (*MLRun, error) {
	if experimentID == "" {
		return nil, fmt.Errorf("%w: experiment_id is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	exp, ok := m.experiments[experimentID]
	if !ok {
		return nil, fmt.Errorf("%w: experiment %s", ErrMLNotFound, experimentID)
	}
	if exp.Lifecycle != MLActive {
		return nil, fmt.Errorf("%w: experiment %s is deleted", ErrMLInvalid, experimentID)
	}
	id, err := newRunID()
	if err != nil {
		return nil, err
	}
	if startTime == 0 {
		startTime = ms(now)
	}
	tagMap := map[string]string{}
	for _, t := range tags {
		tagMap[t.Key] = t.Value
		if t.Key == "mlflow.runName" && name == "" {
			name = t.Value
		}
	}
	if name != "" {
		tagMap["mlflow.runName"] = name
	}
	run := &MLRun{
		ID: id, ExperimentID: experimentID, Name: name, UserID: userID,
		Status: MLRunning, Lifecycle: MLActive,
		ArtifactURI: strings.TrimRight(exp.ArtifactLocation, "/") + "/" + id + "/artifacts",
		StartTime:   startTime, Params: map[string]string{}, Tags: tagMap,
	}
	m.runs[id] = run
	exp.LastUpdateTime = ms(now)
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneRun(run), nil
}

func (m *MLflow) runLocked(id string) (*MLRun, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: run_id is required", ErrMLInvalid)
	}
	run, ok := m.runs[id]
	if !ok {
		return nil, fmt.Errorf("%w: run %s", ErrMLNotFound, id)
	}
	return run, nil
}

// GetRun returns a run.
func (m *MLflow) GetRun(id string) (*MLRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(id)
	if err != nil {
		return nil, err
	}
	return cloneRun(run), nil
}

// UpdateRun sets status, end time, and/or name.
func (m *MLflow) UpdateRun(id, status, name string, endTime, now int64) (*MLRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(id)
	if err != nil {
		return nil, err
	}
	if status != "" {
		switch status {
		case MLRunning, MLScheduled, MLFinished, MLFailed, MLKilled:
			run.Status = status
		default:
			return nil, fmt.Errorf("%w: status %s", ErrMLInvalid, status)
		}
	}
	if endTime > 0 {
		run.EndTime = endTime
	} else if status == MLFinished || status == MLFailed || status == MLKilled {
		run.EndTime = ms(now)
	}
	if name != "" {
		run.Name = name
		run.Tags["mlflow.runName"] = name
	}
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneRun(run), nil
}

// DeleteRun marks a run deleted.
func (m *MLflow) DeleteRun(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(id)
	if err != nil {
		return err
	}
	run.Lifecycle = MLDeleted
	return m.persistLocked()
}

// RestoreRun undeletes a run.
func (m *MLflow) RestoreRun(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(id)
	if err != nil {
		return err
	}
	run.Lifecycle = MLActive
	return m.persistLocked()
}

// LogMetric appends a metric point.
func (m *MLflow) LogMetric(runID, key string, value float64, timestamp, step int64) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%w: metric key is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(runID)
	if err != nil {
		return err
	}
	run.Metrics = append(run.Metrics, MLMetric{Key: key, Value: value, Timestamp: timestamp, Step: step})
	return m.persistLocked()
}

// LogParam stores a parameter. A different value for the same key is refused.
func (m *MLflow) LogParam(runID, key, value string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%w: param key is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(runID)
	if err != nil {
		return err
	}
	if prev, ok := run.Params[key]; ok && prev != value {
		return fmt.Errorf("%w: param %q already logged", ErrMLExists, key)
	}
	run.Params[key] = value
	return m.persistLocked()
}

// SetTag sets a run tag.
func (m *MLflow) SetTag(runID, key, value string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%w: tag key is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(runID)
	if err != nil {
		return err
	}
	run.Tags[key] = value
	if key == "mlflow.runName" {
		run.Name = value
	}
	return m.persistLocked()
}

// DeleteTag removes a run tag.
func (m *MLflow) DeleteTag(runID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(runID)
	if err != nil {
		return err
	}
	if _, ok := run.Tags[key]; !ok {
		return fmt.Errorf("%w: tag %q", ErrMLNotFound, key)
	}
	delete(run.Tags, key)
	return m.persistLocked()
}

// LogBatch writes metrics, params, and tags in one persist.
func (m *MLflow) LogBatch(runID string, metrics []MLMetric, params []MLParam, tags []MLTag) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(runID)
	if err != nil {
		return err
	}
	for _, p := range params {
		if strings.TrimSpace(p.Key) == "" {
			return fmt.Errorf("%w: param key is required", ErrMLInvalid)
		}
		if prev, ok := run.Params[p.Key]; ok && prev != p.Value {
			return fmt.Errorf("%w: param %q already logged", ErrMLExists, p.Key)
		}
		run.Params[p.Key] = p.Value
	}
	for _, t := range tags {
		if strings.TrimSpace(t.Key) == "" {
			return fmt.Errorf("%w: tag key is required", ErrMLInvalid)
		}
		run.Tags[t.Key] = t.Value
		if t.Key == "mlflow.runName" {
			run.Name = t.Value
		}
	}
	for _, met := range metrics {
		if strings.TrimSpace(met.Key) == "" {
			return fmt.Errorf("%w: metric key is required", ErrMLInvalid)
		}
		run.Metrics = append(run.Metrics, met)
	}
	return m.persistLocked()
}

// MetricHistory returns every point for a key, oldest first.
func (m *MLflow) MetricHistory(runID, key string) ([]MLMetric, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, err := m.runLocked(runID)
	if err != nil {
		return nil, err
	}
	var out []MLMetric
	for _, met := range run.Metrics {
		if met.Key == key {
			out = append(out, met)
		}
	}
	return out, nil
}

// LatestMetrics returns one point per key: latest timestamp, then max value.
func LatestMetrics(metrics []MLMetric) []MLMetric {
	best := map[string]MLMetric{}
	for _, met := range metrics {
		cur, ok := best[met.Key]
		if !ok || met.Timestamp > cur.Timestamp || (met.Timestamp == cur.Timestamp && met.Value > cur.Value) {
			best[met.Key] = met
		}
	}
	keys := make([]string, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]MLMetric, 0, len(keys))
	for _, k := range keys {
		out = append(out, best[k])
	}
	return out
}

// SearchRuns lists runs in the given experiments. Filters are refused.
func (m *MLflow) SearchRuns(experimentIDs []string, filter, view string, maxResults int) ([]*MLRun, error) {
	if strings.TrimSpace(filter) != "" {
		return nil, fmt.Errorf("%w: %s", ErrMLFilter, filter)
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	wanted := map[string]bool{}
	for _, id := range experimentIDs {
		if id != "" {
			wanted[id] = true
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*MLRun
	for _, run := range m.runs {
		if len(wanted) > 0 && !wanted[run.ExperimentID] {
			continue
		}
		if !matchView(run.Lifecycle, view) {
			continue
		}
		out = append(out, cloneRun(run))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartTime > out[j].StartTime })
	if len(out) > maxResults {
		out = out[:maxResults]
	}
	return out, nil
}

// CreateModel registers a model name.
func (m *MLflow) CreateModel(name, description, userID string, tags []MLTag, now int64) (*RegisteredModel, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.models[name]; ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLExists, name)
	}
	t := ms(now)
	tagMap := map[string]string{}
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}
	mod := &RegisteredModel{
		Name: name, Description: description, UserID: userID,
		CreatedAt: t, UpdatedAt: t, Tags: tagMap,
	}
	m.models[name] = mod
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneModel(mod), nil
}

// GetModel returns a registered model.
func (m *MLflow) GetModel(name string) (*RegisteredModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mod, ok := m.models[name]
	if !ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	return cloneModel(mod), nil
}

// SearchModels lists registered models. Only name= filters are implemented.
func (m *MLflow) SearchModels(filter string, maxResults int) ([]*RegisteredModel, error) {
	name, ok := parseNameEquals(filter)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMLFilter, filter)
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*RegisteredModel
	for _, mod := range m.models {
		if name != "" && mod.Name != name {
			continue
		}
		out = append(out, cloneModel(mod))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) > maxResults {
		out = out[:maxResults]
	}
	return out, nil
}

// UpdateModel sets a registered model's description.
func (m *MLflow) UpdateModel(name, description string, now int64) (*RegisteredModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mod, ok := m.models[name]
	if !ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	mod.Description = description
	mod.UpdatedAt = ms(now)
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneModel(mod), nil
}

// RenameModel changes a registered model's name and its versions.
func (m *MLflow) RenameModel(name, newName string, now int64) (*RegisteredModel, error) {
	newName = strings.TrimSpace(newName)
	if newName == "" {
		return nil, fmt.Errorf("%w: new_name is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mod, ok := m.models[name]
	if !ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	if _, ok := m.models[newName]; ok && newName != name {
		return nil, fmt.Errorf("%w: model %q", ErrMLExists, newName)
	}
	delete(m.models, name)
	mod.Name = newName
	mod.UpdatedAt = ms(now)
	m.models[newName] = mod
	vers := m.versions[name]
	delete(m.versions, name)
	for _, v := range vers {
		v.Name = newName
		v.UpdatedAt = ms(now)
	}
	m.versions[newName] = vers
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneModel(mod), nil
}

// DeleteModel removes a registered model and its versions.
func (m *MLflow) DeleteModel(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.models[name]; !ok {
		return fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	delete(m.models, name)
	delete(m.versions, name)
	return m.persistLocked()
}

// CreateModelVersion appends a READY version at stage None.
func (m *MLflow) CreateModelVersion(name, source, runID, runLink, description, userID string, tags []MLTag, now int64) (*ModelVersion, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: name is required", ErrMLInvalid)
	}
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("%w: source is required", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mod, ok := m.models[name]
	if !ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	if runID != "" {
		if _, ok := m.runs[runID]; !ok {
			return nil, fmt.Errorf("%w: run %s", ErrMLNotFound, runID)
		}
	}
	n := len(m.versions[name]) + 1
	t := ms(now)
	tagMap := map[string]string{}
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}
	v := &ModelVersion{
		Name: name, Version: strconv.Itoa(n), Source: source,
		RunID: runID, RunLink: runLink, Description: description, UserID: userID,
		Stage: MLStageNone, Status: MLVersionReady, CreatedAt: t, UpdatedAt: t, Tags: tagMap,
	}
	m.versions[name] = append(m.versions[name], v)
	mod.UpdatedAt = t
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneVersion(v), nil
}

func (m *MLflow) versionLocked(name, version string) (*ModelVersion, error) {
	if _, ok := m.models[name]; !ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	for _, v := range m.versions[name] {
		if v.Version == version {
			return v, nil
		}
	}
	return nil, fmt.Errorf("%w: model %q version %s", ErrMLNotFound, name, version)
}

// GetModelVersion returns one version.
func (m *MLflow) GetModelVersion(name, version string) (*ModelVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, err := m.versionLocked(name, version)
	if err != nil {
		return nil, err
	}
	return cloneVersion(v), nil
}

// SearchModelVersions lists versions. Only name= filters are implemented.
func (m *MLflow) SearchModelVersions(filter string, maxResults int) ([]*ModelVersion, error) {
	name, ok := parseNameEquals(filter)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMLFilter, filter)
	}
	if maxResults <= 0 {
		maxResults = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*ModelVersion
	for modelName, vers := range m.versions {
		if name != "" && modelName != name {
			continue
		}
		for _, v := range vers {
			out = append(out, cloneVersion(v))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Version > out[j].Version
	})
	if len(out) > maxResults {
		out = out[:maxResults]
	}
	return out, nil
}

// UpdateModelVersion sets a version description.
func (m *MLflow) UpdateModelVersion(name, version, description string, now int64) (*ModelVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, err := m.versionLocked(name, version)
	if err != nil {
		return nil, err
	}
	v.Description = description
	v.UpdatedAt = ms(now)
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneVersion(v), nil
}

// TransitionStage moves a version. archiveExisting archives others in the target stage.
func (m *MLflow) TransitionStage(name, version, stage string, archiveExisting bool, now int64) (*ModelVersion, error) {
	switch stage {
	case MLStageNone, MLStageStaging, MLStageProduction, MLStageArchived:
	default:
		return nil, fmt.Errorf("%w: stage %s", ErrMLInvalid, stage)
	}
	if archiveExisting && stage != MLStageStaging && stage != MLStageProduction {
		return nil, fmt.Errorf("%w: archive_existing_versions only for Staging or Production", ErrMLInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, err := m.versionLocked(name, version)
	if err != nil {
		return nil, err
	}
	t := ms(now)
	if archiveExisting {
		for _, other := range m.versions[name] {
			if other.Version != version && other.Stage == stage {
				other.Stage = MLStageArchived
				other.UpdatedAt = t
			}
		}
	}
	v.Stage = stage
	v.UpdatedAt = t
	if mod, ok := m.models[name]; ok {
		mod.UpdatedAt = t
	}
	if err := m.persistLocked(); err != nil {
		return nil, err
	}
	return cloneVersion(v), nil
}

// LatestVersions returns the newest version in each requested stage.
func (m *MLflow) LatestVersions(name string, stages []string) ([]*ModelVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.models[name]; !ok {
		return nil, fmt.Errorf("%w: model %q", ErrMLNotFound, name)
	}
	wanted := map[string]bool{}
	for _, s := range stages {
		if s != "" {
			wanted[s] = true
		}
	}
	best := map[string]*ModelVersion{}
	for _, v := range m.versions[name] {
		if len(wanted) > 0 && !wanted[v.Stage] {
			continue
		}
		cur, ok := best[v.Stage]
		if !ok || versionNum(v.Version) > versionNum(cur.Version) {
			best[v.Stage] = v
		}
	}
	var out []*ModelVersion
	for _, v := range best {
		out = append(out, cloneVersion(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stage < out[j].Stage })
	return out, nil
}

func versionNum(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// ListVersions returns every version of a model, newest first.
func (m *MLflow) ListVersions(name string) []*ModelVersion {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*ModelVersion
	for _, v := range m.versions[name] {
		out = append(out, cloneVersion(v))
	}
	sort.Slice(out, func(i, j int) bool { return versionNum(out[i].Version) > versionNum(out[j].Version) })
	return out
}
