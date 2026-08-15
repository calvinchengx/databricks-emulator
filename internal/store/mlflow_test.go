package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMLflowExperimentRunAndRegistryPersist(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	exp, err := s.MLflow.CreateExperiment("/Users/admin/e2e", "", []MLTag{{Key: "team", Value: "ml"}}, 10)
	if err != nil || exp.ID != "1" || exp.Lifecycle != MLActive {
		t.Fatalf("create %+v %v", exp, err)
	}
	if _, err := s.MLflow.CreateExperiment("/Users/admin/e2e", "", nil, 11); !errors.Is(err, ErrMLExists) {
		t.Fatalf("dup %v", err)
	}
	run, err := s.MLflow.CreateRun(exp.ID, "trial", "admin", 0, nil, 12)
	if err != nil || run.Status != MLRunning || run.ArtifactURI == "" {
		t.Fatalf("run %+v %v", run, err)
	}
	if err := s.MLflow.LogParam(run.ID, "lr", "0.01"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.LogParam(run.ID, "lr", "0.02"); !errors.Is(err, ErrMLExists) {
		t.Fatalf("param overwrite %v", err)
	}
	if err := s.MLflow.LogMetric(run.ID, "acc", 0.8, 1000, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.LogMetric(run.ID, "acc", 0.91, 2000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.UpdateRun(run.ID, MLFinished, "", 0, 13); err != nil {
		t.Fatal(err)
	}
	mod, err := s.MLflow.CreateModel("e2e-model", "demo", "admin", nil, 14)
	if err != nil || mod.Name != "e2e-model" {
		t.Fatalf("model %+v %v", mod, err)
	}
	ver, err := s.MLflow.CreateModelVersion("e2e-model", "dbfs:/models/e2e", run.ID, "", "", "admin", nil, 15)
	if err != nil || ver.Version != "1" || ver.Stage != MLStageNone {
		t.Fatalf("version %+v %v", ver, err)
	}
	staged, err := s.MLflow.TransitionStage("e2e-model", "1", MLStageStaging, false, 16)
	if err != nil || staged.Stage != MLStageStaging {
		t.Fatalf("stage %+v %v", staged, err)
	}

	s2, err := Open(dir, 20)
	if err != nil {
		t.Fatal(err)
	}
	gotExp, err := s2.MLflow.GetExperimentByName("/Users/admin/e2e")
	if err != nil || gotExp.ID != "1" || gotExp.Tags[0].Value != "ml" {
		t.Fatalf("reload exp %+v %v", gotExp, err)
	}
	gotRun, err := s2.MLflow.GetRun(run.ID)
	if err != nil || gotRun.Status != MLFinished || gotRun.Params["lr"] != "0.01" {
		t.Fatalf("reload run %+v %v", gotRun, err)
	}
	latest := LatestMetrics(gotRun.Metrics)
	if len(latest) != 1 || latest[0].Value != 0.91 || latest[0].Step != 1 {
		t.Fatalf("latest %+v", latest)
	}
	hist, err := s2.MLflow.MetricHistory(run.ID, "acc")
	if err != nil || len(hist) != 2 {
		t.Fatalf("history %+v %v", hist, err)
	}
	gotVer, err := s2.MLflow.GetModelVersion("e2e-model", "1")
	if err != nil || gotVer.Stage != MLStageStaging || gotVer.RunID != run.ID {
		t.Fatalf("reload ver %+v %v", gotVer, err)
	}
}

func TestMLflowDeleteReuseNameAndArchiveExisting(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	exp, err := s.MLflow.CreateExperiment("reuse", "", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteExperiment(exp.ID, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateRun(exp.ID, "", "", 0, nil, 3); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("run on deleted %v", err)
	}
	again, err := s.MLflow.CreateExperiment("reuse", "", nil, 4)
	if err != nil || again.ID == exp.ID {
		t.Fatalf("reuse %+v %v", again, err)
	}
	byName, err := s.MLflow.GetExperimentByName("reuse")
	if err != nil || byName.ID != again.ID {
		t.Fatalf("by name prefers active %+v %v", byName, err)
	}
	listed, err := s.MLflow.SearchExperiments("", MLViewActive, 10)
	if err != nil || len(listed) != 1 || listed[0].ID != again.ID {
		t.Fatalf("search active %+v %v", listed, err)
	}
	if _, err := s.MLflow.SearchExperiments("metrics.acc > 0", "", 10); !errors.Is(err, ErrMLFilter) {
		t.Fatalf("filter %v", err)
	}

	if _, err := s.MLflow.CreateModel("m", "", "", nil, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "dbfs:/a", "", "", "", "", nil, 6); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "dbfs:/b", "", "", "", "", nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.TransitionStage("m", "1", MLStageProduction, false, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.TransitionStage("m", "2", MLStageProduction, true, 9); err != nil {
		t.Fatal(err)
	}
	v1, _ := s.MLflow.GetModelVersion("m", "1")
	v2, _ := s.MLflow.GetModelVersion("m", "2")
	if v1.Stage != MLStageArchived || v2.Stage != MLStageProduction {
		t.Fatalf("archive existing v1=%s v2=%s", v1.Stage, v2.Stage)
	}
	latest, err := s.MLflow.LatestVersions("m", []string{MLStageProduction})
	if err != nil || len(latest) != 1 || latest[0].Version != "2" {
		t.Fatalf("latest %+v %v", latest, err)
	}
}

func TestMLflowMissingAndBatch(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.GetExperiment("9"); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("missing exp %v", err)
	}
	if _, err := s.MLflow.CreateRun("9", "", "", 0, nil, 1); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("run missing exp %v", err)
	}
	exp, err := s.MLflow.CreateExperiment("batch", "", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.MLflow.CreateRun(exp.ID, "", "admin", 0, []MLTag{{Key: "mlflow.runName", Value: "from-tag"}}, 2)
	if err != nil || run.Name != "from-tag" {
		t.Fatalf("named %+v %v", run, err)
	}
	if err := s.MLflow.LogBatch(run.ID,
		[]MLMetric{{Key: "loss", Value: 1.2, Timestamp: 1, Step: 0}},
		[]MLParam{{Key: "opt", Value: "adam"}},
		[]MLTag{{Key: "note", Value: "ok"}},
	); err != nil {
		t.Fatal(err)
	}
	got, err := s.MLflow.GetRun(run.ID)
	if err != nil || got.Params["opt"] != "adam" || got.Tags["note"] != "ok" || len(got.Metrics) != 1 {
		t.Fatalf("batch %+v %v", got, err)
	}
	if err := s.MLflow.SetTag(run.ID, "note", "edited"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteTag(run.ID, "note"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteRun(run.ID); err != nil {
		t.Fatal(err)
	}
	active, err := s.MLflow.SearchRuns([]string{exp.ID}, "", MLViewActive, 10)
	if err != nil || len(active) != 0 {
		t.Fatalf("deleted still listed %+v %v", active, err)
	}
	if err := s.MLflow.RestoreRun(run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("missing", "dbfs:/x", "", "", "", "", nil, 3); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("version missing model %v", err)
	}
	if err := s.MLflow.RestoreExperiment(exp.ID, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("upd", "old", "", nil, 5); err != nil {
		t.Fatal(err)
	}
	mod, err := s.MLflow.UpdateModel("upd", "new", 6)
	if err != nil || mod.Description != "new" {
		t.Fatalf("update model %+v %v", mod, err)
	}
	if _, err := s.MLflow.CreateModelVersion("upd", "dbfs:/u", "", "http://run", "v", "", []MLTag{{Key: "t", Value: "1"}}, 7); err != nil {
		t.Fatal(err)
	}
	ver, err := s.MLflow.UpdateModelVersion("upd", "1", "edited", 8)
	if err != nil || ver.Description != "edited" {
		t.Fatalf("update version %+v %v", ver, err)
	}
	found, err := s.MLflow.SearchModelVersions("name='upd'", 10)
	if err != nil || len(found) != 1 {
		t.Fatalf("search versions %+v %v", found, err)
	}
	if _, err := s.MLflow.SearchModelVersions("version > 1", 10); !errors.Is(err, ErrMLFilter) {
		t.Fatalf("version filter %v", err)
	}
	deleted, err := s.MLflow.SearchExperiments("", MLViewDeleted, 10)
	if err != nil {
		t.Fatal(err)
	}
	_ = deleted
	all, err := s.MLflow.SearchExperiments("name = 'batch'", MLViewAll, 1)
	if err != nil || len(all) != 1 {
		t.Fatalf("name filter %+v %v", all, err)
	}
}

func TestMLflowEveryStoreBranch(t *testing.T) {
	if cloneExperiment(nil) != nil || cloneRun(nil) != nil || cloneModel(nil) != nil || cloneVersion(nil) != nil {
		t.Fatal("nil clones")
	}
	if copyMap(nil) != nil {
		t.Fatal("nil copy")
	}
	if versionNum("2") != 2 {
		t.Fatal("versionNum")
	}
	if matchView(MLActive, "BOGUS") != true || matchView(MLDeleted, "BOGUS") {
		t.Fatal("default view")
	}

	asFile := t.TempDir() + "/notdir"
	if err := os.WriteFile(asFile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openMLflow(asFile); err == nil {
		t.Fatal("mkdir on file")
	}

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "mlflow", "state.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, 1); err == nil {
		t.Fatal("state.json is a directory")
	}

	bad := t.TempDir()
	if err := os.MkdirAll(filepath.Join(bad, "mlflow"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "mlflow", "state.json"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(bad, 1); err == nil {
		t.Fatal("corrupt json")
	}

	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateExperiment("", "", nil, 1); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty name %v", err)
	}
	exp, err := s.MLflow.CreateExperiment("a", "dbfs:/custom/", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateRun("", "", "", 0, nil, 2); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty exp %v", err)
	}
	if _, err := s.MLflow.GetRun(""); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty run %v", err)
	}
	run, err := s.MLflow.CreateRun(exp.ID, "", "u", 1234, []MLTag{{Key: "other", Value: "x"}}, 2)
	if err != nil || run.StartTime != 1234 || run.Name != "" {
		t.Fatalf("explicit start %+v %v", run, err)
	}
	if err := s.MLflow.LogParam(run.ID, "", "x"); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty param %v", err)
	}
	if err := s.MLflow.LogParam(run.ID, "lr", "0.01"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.LogParam(run.ID, "lr", "0.01"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.LogMetric(run.ID, "", 1, 1, 0); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty metric %v", err)
	}
	if err := s.MLflow.SetTag(run.ID, "", "x"); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty tag %v", err)
	}
	if err := s.MLflow.SetTag(run.ID, "mlflow.runName", "named"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteTag(run.ID, "missing"); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("missing tag %v", err)
	}
	if err := s.MLflow.LogBatch(run.ID, nil, []MLParam{{Key: "", Value: "x"}}, nil); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("batch param %v", err)
	}
	if err := s.MLflow.LogBatch(run.ID, nil, []MLParam{{Key: "lr", Value: "9"}}, nil); !errors.Is(err, ErrMLExists) {
		t.Fatalf("batch conflict %v", err)
	}
	if err := s.MLflow.LogBatch(run.ID, nil, nil, []MLTag{{Key: "", Value: "x"}}); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("batch tag %v", err)
	}
	if err := s.MLflow.LogBatch(run.ID, []MLMetric{{Key: "", Value: 1}}, nil, nil); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("batch metric %v", err)
	}
	if err := s.MLflow.LogBatch(run.ID, nil, nil, []MLTag{{Key: "mlflow.runName", Value: "batched"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.UpdateRun(run.ID, "NOPE", "", 0, 3); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("bad status %v", err)
	}
	if _, err := s.MLflow.UpdateRun(run.ID, MLScheduled, "renamed", 999, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.UpdateRun(run.ID, MLFailed, "", 0, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.UpdateRun(run.ID, MLKilled, "", 0, 5); err != nil {
		t.Fatal(err)
	}

	if _, err := s.MLflow.UpdateExperiment(exp.ID, "", 6); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty rename %v", err)
	}
	if _, err := s.MLflow.CreateExperiment("b", "", nil, 6); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.UpdateExperiment(exp.ID, "b", 7); !errors.Is(err, ErrMLExists) {
		t.Fatalf("rename clash %v", err)
	}
	if _, err := s.MLflow.UpdateExperiment("missing", "z", 7); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("rename missing %v", err)
	}
	if err := s.MLflow.DeleteExperiment(exp.ID, 8); err != nil {
		t.Fatal(err)
	}
	again, err := s.MLflow.CreateExperiment("a", "", nil, 9)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.RestoreExperiment(exp.ID, 10); !errors.Is(err, ErrMLExists) {
		t.Fatalf("restore clash %v", err)
	}
	if err := s.MLflow.RestoreExperiment("missing", 10); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("restore missing %v", err)
	}
	byDel, err := s.MLflow.GetExperimentByName("gone-name")
	if err == nil || byDel != nil {
		t.Fatalf("missing by name %v", err)
	}
	if err := s.MLflow.DeleteExperiment(again.ID, 11); err != nil {
		t.Fatal(err)
	}
	dead, err := s.MLflow.GetExperimentByName("a")
	if err != nil || dead.Lifecycle != MLDeleted {
		t.Fatalf("deleted by name %+v %v", dead, err)
	}
	listed, err := s.MLflow.SearchExperiments("", "BOGUS", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = listed
	many, err := s.MLflow.SearchExperiments("", MLViewAll, 1)
	if err != nil || len(many) != 1 {
		t.Fatalf("truncate %+v %v", many, err)
	}
	runs, err := s.MLflow.SearchRuns(nil, "", MLViewAll, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("search all runs %+v %v", runs, err)
	}

	if _, err := s.MLflow.CreateModel("", "", "", nil, 12); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty model %v", err)
	}
	if _, err := s.MLflow.CreateModel("m", "d", "u", []MLTag{{Key: "k", Value: "v"}}, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("m", "", "", nil, 13); !errors.Is(err, ErrMLExists) {
		t.Fatalf("dup model %v", err)
	}
	if _, err := s.MLflow.UpdateModel("missing", "x", 13); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("update missing %v", err)
	}
	if _, err := s.MLflow.RenameModel("m", "", 13); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty new name %v", err)
	}
	if _, err := s.MLflow.CreateModel("n", "", "", nil, 13); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.RenameModel("m", "n", 14); !errors.Is(err, ErrMLExists) {
		t.Fatalf("rename clash %v", err)
	}
	if _, err := s.MLflow.RenameModel("missing", "z", 14); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("rename missing %v", err)
	}
	if _, err := s.MLflow.CreateModelVersion("", "dbfs:/x", "", "", "", "", nil, 14); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty ver name %v", err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "", "", "", "", "", nil, 14); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("empty source %v", err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "dbfs:/x", "nope", "", "", "", nil, 14); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("ver missing run %v", err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "dbfs:/x", run.ID, "http://r", "d", "u", []MLTag{{Key: "t", Value: "1"}}, 15); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.GetModelVersion("m", "9"); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("missing ver %v", err)
	}
	if _, err := s.MLflow.UpdateModelVersion("m", "9", "x", 16); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("update missing ver %v", err)
	}
	if _, err := s.MLflow.TransitionStage("m", "1", "Nope", false, 16); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("bad stage %v", err)
	}
	if _, err := s.MLflow.TransitionStage("m", "1", MLStageNone, true, 16); !errors.Is(err, ErrMLInvalid) {
		t.Fatalf("archive none %v", err)
	}
	if _, err := s.MLflow.LatestVersions("missing", nil); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("latest missing %v", err)
	}
	allVers, err := s.MLflow.LatestVersions("m", nil)
	if err != nil || len(allVers) != 1 {
		t.Fatalf("latest all %+v %v", allVers, err)
	}
	trimmed, err := s.MLflow.SearchModels("name == 'm'", 1)
	if err != nil || len(trimmed) != 1 {
		t.Fatalf("search models %+v %v", trimmed, err)
	}
	if _, err := s.MLflow.SearchModels("tags.x='y'", 10); !errors.Is(err, ErrMLFilter) {
		t.Fatalf("model filter %v", err)
	}
	vers, err := s.MLflow.SearchModelVersions("", 1)
	if err != nil || len(vers) != 1 {
		t.Fatalf("search vers truncate %+v %v", vers, err)
	}
	renamed, err := s.MLflow.RenameModel("m", "m2", 17)
	if err != nil || renamed.Name != "m2" {
		t.Fatalf("rename %+v %v", renamed, err)
	}
	if len(s.MLflow.ListVersions("m2")) != 1 {
		t.Fatal("versions followed rename")
	}

	old := readRand
	readRand = func([]byte) (int, error) { return 0, errors.New("rand") }
	live, err := s.MLflow.CreateExperiment("live", "", nil, 18)
	if err != nil {
		readRand = old
		t.Fatal(err)
	}
	_, randErr := s.MLflow.CreateRun(live.ID, "", "", 0, nil, 19)
	readRand = old
	if randErr == nil {
		t.Fatal("rand fail")
	}

	s.MLflow.dir = filepath.Join(t.TempDir(), "missing-dir")
	if _, err := s.MLflow.CreateExperiment("persist-fail", "", nil, 20); err == nil {
		t.Fatal("persist write")
	}
	s2, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.MLflow.CreateExperiment("x", "", nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(s2.MLflow.dir, "state.json"), 0o755); err == nil {
		// may fail if file exists; replace file with dir
	}
	_ = os.Remove(filepath.Join(s2.MLflow.dir, "state.json"))
	if err := os.Mkdir(filepath.Join(s2.MLflow.dir, "state.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.MLflow.CreateExperiment("y", "", nil, 2); err == nil {
		t.Fatal("persist rename onto dir")
	}

	reloadDir := t.TempDir()
	s3, err := Open(reloadDir, 1)
	if err != nil {
		t.Fatal(err)
	}
	e3, err := s3.MLflow.CreateExperiment("reload", "", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	r3, err := s3.MLflow.CreateRun(e3.ID, "n", "u", 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3.MLflow.CreateModel("rm", "", "", nil, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.MLflow.CreateModelVersion("rm", "dbfs:/z", r3.ID, "", "", "", nil, 4); err != nil {
		t.Fatal(err)
	}
	raw := `{
  "next_exp": 1,
  "experiments": [{"id":"1","name":"reload","artifact_location":"dbfs:/x","lifecycle":"active","creation_time":1,"last_update_time":1}],
  "runs": [{"id":"` + r3.ID + `","experiment_id":"1","status":"RUNNING","lifecycle":"active","artifact_uri":"dbfs:/x","start_time":1}],
  "models": [{"name":"rm","created_at":1,"updated_at":1}],
  "versions": [{"name":"rm","version":"1","source":"dbfs:/z","stage":"None","status":"READY","created_at":1,"updated_at":1}]
}`
	if err := os.WriteFile(filepath.Join(reloadDir, "mlflow", "state.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	s4, err := Open(reloadDir, 5)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s4.MLflow.GetRun(r3.ID)
	if err != nil || got.Params == nil || got.Tags == nil {
		t.Fatalf("nil maps %+v %v", got, err)
	}
	mod, err := s4.MLflow.GetModel("rm")
	if err != nil || mod.Tags == nil {
		t.Fatalf("model tags %+v %v", mod, err)
	}
	mv, err := s4.MLflow.GetModelVersion("rm", "1")
	if err != nil || mv.Tags == nil {
		t.Fatalf("ver tags %+v %v", mv, err)
	}
}

func TestMLflowPersistErrorsAndNilClones(t *testing.T) {
	if cloneRun(&MLRun{ID: "x"}) == nil || cloneModel(&RegisteredModel{Name: "m"}) == nil || cloneVersion(&ModelVersion{Name: "m", Version: "1"}) == nil {
		t.Fatal("nil-map clones")
	}
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	exp, err := s.MLflow.CreateExperiment("e", "", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.MLflow.CreateRun(exp.ID, "n", "", 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.LogParam(run.ID, "p", "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.SetTag(run.ID, "t", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("m", "", "", nil, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "dbfs:/x", "", "", "", "", nil, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteModel("m"); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteModel("m"); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("delete missing %v", err)
	}
	if _, err := s.MLflow.GetExperiment("missing"); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("get missing %v", err)
	}
	if err := s.MLflow.DeleteExperiment("missing", 1); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("delete missing exp %v", err)
	}
	if _, err := s.MLflow.SearchRuns([]string{exp.ID}, "metrics.x > 0", "", 10); !errors.Is(err, ErrMLFilter) {
		t.Fatalf("run filter %v", err)
	}
	if _, err := s.MLflow.SearchRuns([]string{exp.ID}, "", MLViewDeleted, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.MetricHistory("missing", "x"); !errors.Is(err, ErrMLNotFound) {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("m", "", "", nil, 5); err != nil {
		t.Fatal(err)
	}
	s.MLflow.dir = filepath.Join(t.TempDir(), "gone")
	_, _ = s.MLflow.UpdateExperiment(exp.ID, "e2", 6)
	_ = s.MLflow.DeleteExperiment(exp.ID, 7)
	_ = s.MLflow.RestoreExperiment(exp.ID, 8)
	_, _ = s.MLflow.CreateRun(exp.ID, "", "", 0, nil, 9)
	_, _ = s.MLflow.UpdateRun(run.ID, MLRunning, "", 0, 10)
	_ = s.MLflow.DeleteRun(run.ID)
	_ = s.MLflow.RestoreRun(run.ID)
	_ = s.MLflow.LogMetric(run.ID, "k", 1, 1, 0)
	_ = s.MLflow.LogParam(run.ID, "q", "2")
	_ = s.MLflow.SetTag(run.ID, "u", "2")
	_ = s.MLflow.DeleteTag(run.ID, "t")
	_ = s.MLflow.LogBatch(run.ID, []MLMetric{{Key: "a", Value: 1}}, nil, nil)
	_, _ = s.MLflow.CreateModelVersion("m", "dbfs:/y", "", "", "", "", nil, 11)
	_, _ = s.MLflow.UpdateModelVersion("m", "1", "d", 12)
	_, _ = s.MLflow.TransitionStage("m", "1", MLStageStaging, false, 13)
	_, _ = s.MLflow.CreateModel("m2", "", "", nil, 14)
	_, _ = s.MLflow.UpdateModel("m", "d", 15)
	_, _ = s.MLflow.RenameModel("m", "m3", 16)
	_ = s.MLflow.DeleteModel("m")
	_, _ = s.MLflow.SearchRuns([]string{exp.ID}, "", "", 0)
}

func TestMLflowSearchTruncationAndGet(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.MLflow.CreateExperiment("alpha", "", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.MLflow.CreateExperiment("beta", "", nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.MLflow.GetExperiment(a.ID)
	if err != nil || got.Name != "alpha" {
		t.Fatalf("get %+v %v", got, err)
	}
	if _, err := s.MLflow.SearchExperiments("name='alpha'", "", 10); err != nil {
		t.Fatal(err)
	}
	r1, err := s.MLflow.CreateRun(a.ID, "r1", "", 100, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateRun(a.ID, "r2", "", 200, nil, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateRun(b.ID, "r3", "", 300, nil, 5); err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.DeleteRun(r1.ID); err != nil {
		t.Fatal(err)
	}
	runs, err := s.MLflow.SearchRuns([]string{a.ID, ""}, "", MLViewActive, 1)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs %+v %v", runs, err)
	}
	if _, err := s.MLflow.SearchRuns([]string{"nope"}, "", "", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("alpha-m", "", "", nil, 6); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("beta-m", "", "", nil, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("alpha-m", "dbfs:/1", "", "", "", "", nil, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("alpha-m", "dbfs:/2", "", "", "", "", nil, 9); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("beta-m", "dbfs:/3", "", "", "", "", nil, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.TransitionStage("alpha-m", "1", MLStageStaging, false, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.TransitionStage("alpha-m", "2", MLStageProduction, false, 12); err != nil {
		t.Fatal(err)
	}
	mods, err := s.MLflow.SearchModels("", 1)
	if err != nil || len(mods) != 1 {
		t.Fatalf("models %+v %v", mods, err)
	}
	if _, err := s.MLflow.SearchModels("name='alpha-m'", 10); err != nil {
		t.Fatal(err)
	}
	vers, err := s.MLflow.SearchModelVersions("", 0)
	if err != nil || len(vers) < 2 {
		t.Fatalf("all vers %+v %v", vers, err)
	}
	vers, err = s.MLflow.SearchModelVersions("name='alpha-m'", 1)
	if err != nil || len(vers) != 1 {
		t.Fatalf("named vers %+v %v", vers, err)
	}
	if len(s.MLflow.ListVersions("alpha-m")) != 2 {
		t.Fatal("list versions")
	}
	latest, err := s.MLflow.LatestVersions("alpha-m", []string{MLStageStaging, MLStageProduction, ""})
	if err != nil || len(latest) != 2 {
		t.Fatalf("latest %+v %v", latest, err)
	}
	trimmedRuns, err := s.MLflow.SearchRuns(nil, "", MLViewActive, 1)
	if err != nil || len(trimmedRuns) != 1 {
		t.Fatalf("truncate runs %+v %v", trimmedRuns, err)
	}
	if _, err := s.MLflow.SearchModels("", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.GetModel("missing"); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("get model %v", err)
	}
	if _, err := s.MLflow.TransitionStage("missing", "1", MLStageStaging, false, 13); !errors.Is(err, ErrMLNotFound) {
		t.Fatalf("transition missing %v", err)
	}
}

func TestMLflowPersistWriteFails(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	exp, err := s.MLflow.CreateExperiment("e", "", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.MLflow.CreateRun(exp.ID, "n", "", 0, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MLflow.SetTag(run.ID, "t", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModel("m", "", "", nil, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MLflow.CreateModelVersion("m", "dbfs:/x", "", "", "", "", nil, 4); err != nil {
		t.Fatal(err)
	}
	s.MLflow.dir = filepath.Join(t.TempDir(), "gone")
	if _, err := s.MLflow.UpdateExperiment(exp.ID, "e2", 5); err == nil {
		t.Fatal("update exp persist")
	}
	if _, err := s.MLflow.UpdateRun(run.ID, MLRunning, "", 0, 6); err == nil {
		t.Fatal("update run persist")
	}
	if err := s.MLflow.DeleteRun(run.ID); err == nil {
		t.Fatal("delete run persist")
	}
	if err := s.MLflow.RestoreRun(run.ID); err == nil {
		t.Fatal("restore run persist")
	}
	if err := s.MLflow.LogMetric(run.ID, "k", 1, 1, 0); err == nil {
		t.Fatal("metric persist")
	}
	if err := s.MLflow.LogParam(run.ID, "q", "2"); err == nil {
		t.Fatal("param persist")
	}
	if err := s.MLflow.SetTag(run.ID, "u", "2"); err == nil {
		t.Fatal("tag persist")
	}
	if err := s.MLflow.DeleteTag(run.ID, "t"); err == nil {
		t.Fatal("delete tag persist")
	}
	if err := s.MLflow.LogBatch(run.ID, []MLMetric{{Key: "a", Value: 1}}, nil, nil); err == nil {
		t.Fatal("batch persist")
	}
	if _, err := s.MLflow.CreateModel("m2", "", "", nil, 7); err == nil {
		t.Fatal("create model persist")
	}
	if _, err := s.MLflow.SearchModels("name='m'", 0); err != nil {
		t.Fatal(err)
	}
}
