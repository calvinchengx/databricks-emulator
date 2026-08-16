package server

import (
	"strings"
	"testing"
)

func TestMLflowTrackingStoreAndRegistry(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT

	if st := h.json("POST", "/api/2.0/mlflow/experiments/create", "", map[string]any{"name": "x"}, nil); st != 401 {
		t.Fatalf("unauth %d", st)
	}

	var created map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/experiments/create", pat, map[string]any{
		"name": "/Users/admin/e2e-ml", "tags": []map[string]string{{"key": "team", "value": "ml"}},
	}, &created); st != 200 {
		t.Fatalf("create exp %d %+v", st, created)
	}
	expID := str(created["experiment_id"])
	if expID == "" {
		t.Fatal("no experiment_id")
	}
	var dup map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/experiments/create", pat, map[string]any{
		"name": "/Users/admin/e2e-ml",
	}, &dup); st != 409 || str(dup["error_code"]) != "RESOURCE_ALREADY_EXISTS" {
		t.Fatalf("dup %d %+v", st, dup)
	}

	var byName map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/experiments/get-by-name?experiment_name=/Users/admin/e2e-ml", pat, nil, &byName); st != 200 {
		t.Fatalf("by name %d", st)
	}
	if byName["experiment"].(map[string]any)["experiment_id"] != expID {
		t.Fatalf("by name %+v", byName)
	}

	var runCreated map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/runs/create", pat, map[string]any{
		"experiment_id": expID, "run_name": "trial",
	}, &runCreated); st != 200 {
		t.Fatalf("create run %d %+v", st, runCreated)
	}
	run := runCreated["run"].(map[string]any)
	info := run["info"].(map[string]any)
	runID := str(info["run_id"])
	if runID == "" || str(info["status"]) != "RUNNING" {
		t.Fatalf("run info %+v", info)
	}

	if st := h.json("POST", "/api/2.0/mlflow/runs/log-parameter", pat, map[string]any{
		"run_id": runID, "key": "lr", "value": "0.01",
	}, nil); st != 200 {
		t.Fatalf("param %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/log-metric", pat, map[string]any{
		"run_id": runID, "key": "acc", "value": 0.91, "timestamp": 2000, "step": 1,
	}, nil); st != 200 {
		t.Fatalf("metric %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/update", pat, map[string]any{
		"run_id": runID, "status": "FINISHED",
	}, nil); st != 200 {
		t.Fatalf("update %d", st)
	}

	var gotRun map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/runs/get?run_id="+runID, pat, nil, &gotRun); st != 200 {
		t.Fatalf("get run %d", st)
	}
	data := gotRun["run"].(map[string]any)["data"].(map[string]any)
	params := data["params"].([]any)
	if len(params) != 1 || params[0].(map[string]any)["value"] != "0.01" {
		t.Fatalf("params %+v", params)
	}
	metrics := data["metrics"].([]any)
	if len(metrics) != 1 || metrics[0].(map[string]any)["value"].(float64) != 0.91 {
		t.Fatalf("metrics %+v", metrics)
	}

	var searched map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/runs/search", pat, map[string]any{
		"experiment_ids": []string{expID},
	}, &searched); st != 200 {
		t.Fatalf("search runs %d", st)
	}
	if len(searched["runs"].([]any)) != 1 {
		t.Fatalf("search runs %+v", searched)
	}

	var model map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/registered-models/create", pat, map[string]any{
		"name": "e2e-model",
	}, &model); st != 200 {
		t.Fatalf("create model %d %+v", st, model)
	}
	var ver map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/model-versions/create", pat, map[string]any{
		"name": "e2e-model", "source": "dbfs:/models/e2e", "run_id": runID,
	}, &ver); st != 200 {
		t.Fatalf("create version %d %+v", st, ver)
	}
	if str(ver["model_version"].(map[string]any)["version"]) != "1" {
		t.Fatalf("version %+v", ver)
	}
	var staged map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/databricks/model-versions/transition-stage", pat, map[string]any{
		"name": "e2e-model", "version": "1", "stage": "Staging", "archive_existing_versions": false,
	}, &staged); st != 200 {
		t.Fatalf("transition %d %+v", st, staged)
	}
	if str(staged["model_version_databricks"].(map[string]any)["current_stage"]) != "Staging" {
		t.Fatalf("staged %+v", staged)
	}

	var dbx map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/databricks/registered-models/get?name=e2e-model", pat, nil, &dbx); st != 200 {
		t.Fatalf("get model %d", st)
	}
	if str(dbx["registered_model_databricks"].(map[string]any)["name"]) != "e2e-model" {
		t.Fatalf("dbx model %+v", dbx)
	}

	var artifacts map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/artifacts/list?run_id="+runID, pat, nil, &artifacts); st != 501 || !strings.Contains(str(artifacts["message"]), "artifact") {
		t.Fatalf("artifacts %d %+v", st, artifacts)
	}
	var missing map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/runs/get?run_id=nope", pat, nil, &missing); st != 404 {
		t.Fatalf("missing run %d %+v", st, missing)
	}
	var filtered map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/experiments/search", pat, map[string]any{
		"filter": "metrics.acc > 0",
	}, &filtered); st != 501 {
		t.Fatalf("filter %d %+v", st, filtered)
	}
	var unknown map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/logged-models/get?model_id=x", pat, nil, &unknown); st != 501 {
		t.Fatalf("logged model %d %+v", st, unknown)
	}

	if st := h.json("POST", "/api/2.0/mlflow/experiments/update", pat, map[string]any{
		"experiment_id": expID, "new_name": "/Users/admin/renamed",
	}, nil); st != 200 {
		t.Fatalf("rename exp %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/log-batch", pat, map[string]any{
		"run_id":  runID,
		"metrics": []map[string]any{{"key": "loss", "value": 0.1, "timestamp": 3, "step": 2}},
		"params":  []map[string]string{{"key": "opt", "value": "adam"}},
		"tags":    []map[string]string{{"key": "note", "value": "ok"}},
	}, nil); st != 200 {
		t.Fatalf("batch %d", st)
	}
	var hist map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/metrics/get-history?run_id="+runID+"&metric_key=acc", pat, nil, &hist); st != 200 {
		t.Fatalf("history %d", st)
	}
	if len(hist["metrics"].([]any)) != 1 {
		t.Fatalf("history %+v", hist)
	}
	if st := h.json("POST", "/api/2.0/mlflow/registered-models/rename", pat, map[string]any{
		"name": "e2e-model", "new_name": "e2e-renamed",
	}, nil); st != 200 {
		t.Fatalf("rename model %d", st)
	}
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/registered-models/list", pat, nil, &listed); st != 200 {
		t.Fatalf("list models %d", st)
	}
	if len(listed["registered_models"].([]any)) != 1 {
		t.Fatalf("list %+v", listed)
	}
	var latest map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/registered-models/get-latest-versions", pat, map[string]any{
		"name": "e2e-renamed", "stages": []string{"Staging"},
	}, &latest); st != 200 {
		t.Fatalf("latest %d %+v", st, latest)
	}
	if st := h.json("DELETE", "/api/2.0/mlflow/registered-models/delete?name=e2e-renamed", pat, nil, nil); st != 200 {
		t.Fatalf("delete model %d", st)
	}
}

func TestMLflowRemainingRoutesAndRefusals(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT

	var created map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/experiments/create", pat, map[string]any{
		"name": "more", "trace_location": map[string]any{"type": "mlflow"},
	}, &created); st != 501 {
		t.Fatalf("trace %d %+v", st, created)
	}
	if st := h.json("POST", "/api/2.0/mlflow/experiments/create", pat, map[string]any{"name": "more"}, &created); st != 200 {
		t.Fatalf("create %d %+v", st, created)
	}
	expID := str(created["experiment_id"])
	var got map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/experiments/get?experiment_id="+expID, pat, nil, &got); st != 200 {
		t.Fatalf("get exp %d", st)
	}
	if st := h.json("GET", "/api/2.0/mlflow/experiments/list", pat, nil, &got); st != 200 {
		t.Fatalf("list exp %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/experiments/delete", pat, map[string]any{"experiment_id": expID}, nil); st != 200 {
		t.Fatalf("delete exp %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/experiments/restore", pat, map[string]any{"experiment_id": expID}, nil); st != 200 {
		t.Fatalf("restore exp %d", st)
	}

	var runCreated map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/runs/create", pat, map[string]any{"experiment_id": expID}, &runCreated); st != 200 {
		t.Fatalf("run %d %+v", st, runCreated)
	}
	runID := str(runCreated["run"].(map[string]any)["info"].(map[string]any)["run_id"])
	if st := h.json("POST", "/api/2.0/mlflow/runs/set-tag", pat, map[string]any{
		"run_id": runID, "key": "k", "value": "v",
	}, nil); st != 200 {
		t.Fatalf("set-tag %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/delete-tag", pat, map[string]any{
		"run_id": runID, "key": "k",
	}, nil); st != 200 {
		t.Fatalf("delete-tag %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/delete", pat, map[string]any{"run_id": runID}, nil); st != 200 {
		t.Fatalf("delete run %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/restore", pat, map[string]any{"run_id": runID}, nil); st != 200 {
		t.Fatalf("restore run %d", st)
	}

	if st := h.json("POST", "/api/2.0/mlflow/registered-models/create", pat, map[string]any{
		"name": "more-model", "description": "d",
	}, nil); st != 200 {
		t.Fatalf("model %d", st)
	}
	if st := h.json("GET", "/api/2.0/mlflow/registered-models/get?name=more-model", pat, nil, &got); st != 200 {
		t.Fatalf("oss get %d", st)
	}
	if st := h.json("PATCH", "/api/2.0/mlflow/registered-models/update", pat, map[string]any{
		"name": "more-model", "description": "edited",
	}, nil); st != 200 {
		t.Fatalf("update model %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/model-versions/create", pat, map[string]any{
		"name": "more-model", "source": "dbfs:/m", "run_link": "http://x",
	}, nil); st != 200 {
		t.Fatalf("version %d", st)
	}
	if st := h.json("GET", "/api/2.0/mlflow/model-versions/get?name=more-model&version=1", pat, nil, &got); st != 200 {
		t.Fatalf("get version %d", st)
	}
	if st := h.json("GET", "/api/2.0/mlflow/model-versions/search?filter=name='more-model'", pat, nil, &got); st != 200 {
		t.Fatalf("search versions %d %+v", st, got)
	}
	if st := h.json("PATCH", "/api/2.0/mlflow/model-versions/update", pat, map[string]any{
		"name": "more-model", "version": "1", "description": "v1",
	}, nil); st != 200 {
		t.Fatalf("update version %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/log-model", pat, map[string]any{"run_id": runID}, &got); st != 501 {
		t.Fatalf("log-model %d %+v", st, got)
	}
	if st := h.json("GET", "/api/2.0/mlflow/experiments/get?experiment_id=missing", pat, nil, &got); st != 404 {
		t.Fatalf("missing exp %d", st)
	}

	for _, tc := range []struct {
		method, path string
		body         any
		want         int
	}{
		{"POST", "/api/2.0/mlflow/experiments/search", map[string]any{"page_token": "n"}, 501},
		{"POST", "/api/2.0/mlflow/runs/search", map[string]any{"page_token": "n"}, 501},
		{"POST", "/api/2.0/mlflow/runs/search", map[string]any{"filter": "metrics.acc > 0"}, 501},
		{"GET", "/api/2.0/mlflow/registered-models/search?page_token=n", nil, 501},
		{"GET", "/api/2.0/mlflow/model-versions/search?page_token=n", nil, 501},
		{"GET", "/api/2.0/mlflow/registered-models/search?filter=tags.x='y'", nil, 501},
		{"POST", "/api/2.0/mlflow/experiments/create", map[string]any{}, 400},
		{"POST", "/api/2.0/mlflow/runs/create", map[string]any{}, 400},
		{"POST", "/api/2.0/mlflow/registered-models/create", map[string]any{}, 400},
		{"GET", "/api/2.0/mlflow/experiments/get-by-name?experiment_name=nope", nil, 404},
		{"GET", "/api/2.0/mlflow/databricks/registered-models/get?name=nope", nil, 404},
		{"GET", "/api/2.0/mlflow/registered-models/get?name=nope", nil, 404},
		{"GET", "/api/2.0/mlflow/model-versions/get?name=nope&version=1", nil, 404},
		{"POST", "/api/2.0/mlflow/runs/log-parameter", map[string]any{"run_id": "nope", "key": "a", "value": "b"}, 404},
		{"POST", "/api/2.0/mlflow/runs/log-metric", map[string]any{"run_uuid": "nope", "key": "a", "value": 1, "timestamp": 1}, 404},
		{"POST", "/api/2.0/mlflow/databricks/model-versions/transition-stage", map[string]any{"name": "more-model", "version": "1", "stage": "Nope"}, 400},
		{"POST", "/api/2.0/mlflow/model-versions/transition-stage", map[string]any{"name": "more-model", "version": "1", "stage": "Archived", "archive_existing_versions": true}, 400},
		{"PATCH", "/api/2.0/mlflow/registered-models/update", map[string]any{"name": "nope"}, 404},
		{"POST", "/api/2.0/mlflow/registered-models/rename", map[string]any{"name": "nope", "new_name": "x"}, 404},
		{"DELETE", "/api/2.0/mlflow/registered-models/delete?name=nope", nil, 404},
		{"POST", "/api/2.0/mlflow/registered-models/get-latest-versions", map[string]any{"name": "nope"}, 404},
		{"POST", "/api/2.0/mlflow/runs/set-tag", map[string]any{"run_id": "nope", "key": "a", "value": "b"}, 404},
		{"POST", "/api/2.0/mlflow/runs/delete-tag", map[string]any{"run_id": "nope", "key": "a"}, 404},
		{"POST", "/api/2.0/mlflow/runs/log-batch", map[string]any{"run_id": "nope"}, 404},
		{"GET", "/api/2.0/mlflow/metrics/get-history?run_id=nope&metric_key=acc", nil, 404},
		{"POST", "/api/2.0/mlflow/experiments/update", map[string]any{"experiment_id": "nope", "new_name": "x"}, 404},
		{"POST", "/api/2.0/mlflow/experiments/delete", map[string]any{"experiment_id": "nope"}, 404},
		{"POST", "/api/2.0/mlflow/experiments/restore", map[string]any{"experiment_id": "nope"}, 404},
		{"POST", "/api/2.0/mlflow/runs/update", map[string]any{"run_id": "nope", "status": "FINISHED"}, 404},
		{"POST", "/api/2.0/mlflow/runs/delete", map[string]any{"run_id": "nope"}, 404},
		{"POST", "/api/2.0/mlflow/runs/restore", map[string]any{"run_id": "nope"}, 404},
		{"PATCH", "/api/2.0/mlflow/model-versions/update", map[string]any{"name": "nope", "version": "1"}, 404},
		{"GET", "/api/2.0/mlflow/runs/get?run_uuid=nope", nil, 404},
		{"POST", "/api/2.0/mlflow/experiments/create", "{", 400},
		{"POST", "/api/2.0/mlflow/experiments/search", "{", 400},
		{"POST", "/api/2.0/mlflow/experiments/update", "{", 400},
		{"POST", "/api/2.0/mlflow/experiments/delete", "{", 400},
		{"POST", "/api/2.0/mlflow/experiments/restore", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/create", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/update", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/delete", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/restore", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/search", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/log-metric", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/log-parameter", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/log-batch", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/set-tag", "{", 400},
		{"POST", "/api/2.0/mlflow/runs/delete-tag", "{", 400},
		{"POST", "/api/2.0/mlflow/registered-models/create", "{", 400},
		{"PATCH", "/api/2.0/mlflow/registered-models/update", "{", 400},
		{"POST", "/api/2.0/mlflow/registered-models/rename", "{", 400},
		{"POST", "/api/2.0/mlflow/registered-models/get-latest-versions", "{", 400},
		{"POST", "/api/2.0/mlflow/model-versions/create", "{", 400},
		{"PATCH", "/api/2.0/mlflow/model-versions/update", "{", 400},
		{"POST", "/api/2.0/mlflow/databricks/model-versions/transition-stage", "{", 400},
	} {
		var body map[string]any
		st := h.json(tc.method, tc.path, pat, tc.body, &body)
		if st != tc.want {
			t.Fatalf("%s %s: %d want %d %+v", tc.method, tc.path, st, tc.want, body)
		}
	}
}

func TestMLflowOptionalFieldsAndDecode(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT

	var created map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/experiments/create", pat, map[string]any{
		"name": "fields", "artifact_location": "dbfs:/custom",
	}, &created); st != 200 {
		t.Fatalf("artifact loc %d %+v", st, created)
	}
	expID := str(created["experiment_id"])
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/experiments/list?filter=name='fields'&view_type=ACTIVE_ONLY&max_results=10", pat, nil, &listed); st != 200 {
		t.Fatalf("list query %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/experiments/search", pat, nil, &listed); st != 200 {
		t.Fatalf("search eof %d", st)
	}
	var runCreated map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/runs/create", pat, map[string]any{
		"experiment_id": expID, "user_id": "alice", "start_time": 1,
	}, &runCreated); st != 200 {
		t.Fatalf("run user %d %+v", st, runCreated)
	}
	runID := str(runCreated["run"].(map[string]any)["info"].(map[string]any)["run_id"])
	if st := h.json("POST", "/api/2.0/mlflow/runs/update", pat, map[string]any{
		"run_uuid": runID, "run_name": "n", "end_time": 9, "status": "RUNNING",
	}, nil); st != 200 {
		t.Fatalf("update uuid %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/log-parameter", pat, map[string]any{
		"run_uuid": runID, "key": "p", "value": "1",
	}, nil); st != 200 {
		t.Fatalf("param uuid %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/log-metric", pat, map[string]any{
		"run_uuid": runID, "key": "m", "value": 1, "timestamp": 1,
	}, nil); st != 200 {
		t.Fatalf("metric uuid %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/runs/search", pat, nil, nil); st != 200 {
		t.Fatalf("search runs eof %d", st)
	}
	if st := h.json("POST", "/api/2.0/mlflow/registered-models/create", pat, map[string]any{
		"name": "desc-model", "description": "hello", "tags": []map[string]string{{"key": "t", "value": "1"}},
	}, nil); st != 200 {
		t.Fatalf("desc model %d", st)
	}
	var ver map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/model-versions/create", pat, map[string]any{
		"name": "desc-model", "source": "dbfs:/m", "run_id": runID, "run_link": "http://r",
		"description": "v", "tags": []map[string]string{{"key": "t", "value": "1"}},
	}, &ver); st != 200 {
		t.Fatalf("full version %d %+v", st, ver)
	}
	if st := h.json("POST", "/api/2.0/mlflow/model-versions/transition-stage", pat, map[string]any{
		"name": "desc-model", "version": "1", "stage": "Production",
	}, nil); st != 200 {
		t.Fatalf("oss transition %d", st)
	}
	var dbx map[string]any
	if st := h.json("GET", "/api/2.0/mlflow/databricks/registered-models/get?name=desc-model", pat, nil, &dbx); st != 200 {
		t.Fatalf("latest prod %d", st)
	}
	if st := h.json("DELETE", "/api/2.0/mlflow/registered-models/delete", pat, map[string]any{"name": "desc-model"}, nil); st != 200 {
		t.Fatalf("delete body %d", st)
	}
	var miss map[string]any
	if st := h.json("POST", "/api/2.0/mlflow/model-versions/create", pat, map[string]any{
		"name": "nope", "source": "dbfs:/x",
	}, &miss); st != 404 {
		t.Fatalf("version missing model %d %+v", st, miss)
	}
	if st := h.json("GET", "/api/2.0/mlflow/model-versions/search", pat, nil, &miss); st != 200 {
		t.Fatalf("search versions all %d", st)
	}
	if st := h.json("GET", "/api/2.0/mlflow/model-versions/search?filter=stage='x'", pat, nil, &miss); st != 501 {
		t.Fatalf("search versions filter %d %+v", st, miss)
	}
}
