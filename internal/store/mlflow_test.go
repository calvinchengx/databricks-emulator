package store

import (
	"errors"
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
