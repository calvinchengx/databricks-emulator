package server

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/config"
)

func TestNewWiresSparkAgentAndDefaultOrigin(t *testing.T) {
	s, err := New(&config.Config{
		DataDir:       t.TempDir(),
		DisableTLS:    true,
		Addr:          ":8447",
		SparkAgentURL: "http://sail:8080",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Spark == nil {
		t.Fatal("SparkAgentURL did not attach an agent")
	}
	if s.Origin != "http://localhost:8447" {
		t.Fatalf("origin = %q", s.Origin)
	}
}

func TestWorkspaceErrorPaths(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	content := base64.StdEncoding.EncodeToString([]byte("print(1)\n"))
	h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/once.py", "content": content,
	}, nil)
	if st := h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/once.py", "content": content,
	}, nil); st != 409 {
		t.Fatalf("overwrite-required %d", st)
	}
	if st := h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/bad.py", "content": "!!!not-b64!!!",
	}, nil); st != 400 {
		t.Fatalf("bad b64 %d", st)
	}
	h.json("POST", "/api/2.0/workspace/mkdirs", pat, map[string]any{"path": "/Shared/dir"}, nil)
	if st := h.json("GET", "/api/2.0/workspace/export?path=/Shared/dir", pat, nil, nil); st != 400 {
		t.Fatalf("export dir %d", st)
	}
	if st := h.json("GET", "/api/2.0/workspace/get-status?path=/missing", pat, nil, nil); st != 404 {
		t.Fatalf("status missing %d", st)
	}
	if st := h.json("GET", "/api/2.0/workspace/list?path=/missing", pat, nil, nil); st != 404 {
		t.Fatalf("list missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/workspace/delete", pat, map[string]any{"path": "/missing"}, nil); st != 404 {
		t.Fatalf("delete missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/workspace/mkdirs", pat, map[string]any{"path": "/../etc"}, nil); st != 400 {
		t.Fatalf("mkdir traversal %d", st)
	}
	got := h.do("GET", "/api/2.0/workspace-files/Shared/dir", pat, nil)
	got.Body.Close()
	if got.StatusCode != 400 {
		t.Fatalf("files get dir %d", got.StatusCode)
	}
	trav := h.do("POST", "/api/2.0/workspace-files/import-file/%2e%2e/etc/passwd", pat, []byte("x"))
	trav.Body.Close()
	if trav.StatusCode != 400 {
		t.Fatalf("files import traversal %d", trav.StatusCode)
	}
}

func TestDBFSAndFilesErrorPaths(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/dbfs/put", pat, map[string]any{"path": "/a.txt", "contents": "!!!"}, nil); st != 400 {
		t.Fatalf("put b64 %d", st)
	}
	if st := h.json("POST", "/api/2.0/dbfs/put", pat, "{", nil); st != 400 {
		t.Fatalf("put json %d", st)
	}
	if st := h.json("GET", "/api/2.0/dbfs/read?path=/missing", pat, nil, nil); st != 404 {
		t.Fatalf("read missing %d", st)
	}
	if st := h.json("GET", "/api/2.0/dbfs/get-status?path=/missing", pat, nil, nil); st != 404 {
		t.Fatalf("status missing %d", st)
	}
	if st := h.json("GET", "/api/2.0/dbfs/list?path=/missing", pat, nil, nil); st != 404 {
		t.Fatalf("list missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/dbfs/mkdirs", pat, map[string]any{"path": "/../etc"}, nil); st != 400 {
		t.Fatalf("mkdirs traversal %d", st)
	}
	if st := h.json("POST", "/api/2.0/dbfs/delete", pat, map[string]any{"path": "/missing"}, nil); st != 404 {
		t.Fatalf("delete missing %d", st)
	}
	if st := h.json("POST", "/api/2.0/dbfs/move", pat, map[string]any{"source_path": "/missing", "destination_path": "/x"}, nil); st != 404 {
		t.Fatalf("move missing %d", st)
	}
	h.json("POST", "/api/2.0/dbfs/put", pat, map[string]any{
		"path": "/exists.bin", "contents": base64.StdEncoding.EncodeToString([]byte("x")),
	}, nil)
	if st := h.json("POST", "/api/2.0/dbfs/create", pat, map[string]any{"path": "/exists.bin"}, nil); st != 400 && st != 409 {
		// overwrite defaults false; existing file must fail by name
		if st == 200 {
			t.Fatal("create without overwrite succeeded on an existing file")
		}
	}
	if st := h.json("POST", "/api/2.0/dbfs/add-block", pat, map[string]any{"handle": 1, "data": "!!!"}, nil); st != 400 {
		t.Fatalf("add-block b64 %d", st)
	}
	if st := h.json("POST", "/api/2.0/dbfs/close", pat, map[string]any{"handle": 99}, nil); st != 400 {
		t.Fatalf("close missing %d", st)
	}

	miss := h.do("GET", "/api/2.0/fs/files/nope.bin", pat, nil)
	miss.Body.Close()
	if miss.StatusCode != 404 {
		t.Fatalf("fs get missing %d", miss.StatusCode)
	}
	head := h.do("HEAD", "/api/2.0/fs/files/nope.bin", pat, nil)
	head.Body.Close()
	if head.StatusCode != 404 {
		t.Fatalf("fs head missing %d", head.StatusCode)
	}
	h.json("POST", "/api/2.0/dbfs/mkdirs", pat, map[string]any{"path": "/adir"}, nil)
	dirHead := h.do("HEAD", "/api/2.0/fs/files/adir", pat, nil)
	dirHead.Body.Close()
	if dirHead.StatusCode != 400 {
		t.Fatalf("fs head dir %d", dirHead.StatusCode)
	}
	trav := h.do("PUT", "/api/2.0/fs/files/%2e%2e/etc/passwd", pat, []byte("x"))
	trav.Body.Close()
	if trav.StatusCode != 400 {
		t.Fatalf("fs put traversal %d", trav.StatusCode)
	}
	del := h.do("DELETE", "/api/2.0/fs/files/nope.bin", pat, nil)
	del.Body.Close()
	if del.StatusCode != 404 {
		t.Fatalf("fs delete missing %d", del.StatusCode)
	}
	md := h.do("PUT", "/api/2.0/fs/directories/%2e%2e/etc", pat, nil)
	md.Body.Close()
	if md.StatusCode != 400 {
		t.Fatalf("fs mkdir traversal %d", md.StatusCode)
	}
	list := h.do("GET", "/api/2.0/fs/directories/missing", pat, nil)
	list.Body.Close()
	if list.StatusCode != 404 {
		t.Fatalf("fs list missing %d", list.StatusCode)
	}
}

func TestJobsMissingResourcesAndResetErrors(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("GET", "/api/2.2/jobs/get?job_id=99", pat, nil, nil); st != 404 {
		t.Fatalf("get %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/delete", pat, map[string]any{"job_id": 99}, nil); st != 404 {
		t.Fatalf("delete %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": 99}, nil); st != 404 {
		t.Fatalf("run-now %d", st)
	}
	if st := h.json("GET", "/api/2.2/jobs/runs/get?run_id=99", pat, nil, nil); st != 404 {
		t.Fatalf("runs/get %d", st)
	}
	if st := h.json("GET", "/api/2.2/jobs/runs/get-output?run_id=99", pat, nil, nil); st != 404 {
		t.Fatalf("get-output %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/runs/cancel", pat, map[string]any{"run_id": 99}, nil); st != 404 {
		t.Fatalf("cancel %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/reset", pat, map[string]any{"job_id": 1}, nil); st != 400 {
		t.Fatalf("reset no settings %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/reset", pat, "{", nil); st != 400 {
		t.Fatalf("reset json %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/reset", pat, map[string]any{
		"job_id": 99, "new_settings": map[string]any{"name": "x", "tasks": []any{
			map[string]any{"task_key": "t", "spark_python_task": map[string]any{"python_file": "/x.py"}},
		}},
	}, nil); st != 404 {
		t.Fatalf("reset missing %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/create", pat, "{", nil); st != 400 {
		t.Fatalf("create json %d", st)
	}
	if st := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "empty", "tasks": []any{map[string]any{"task_key": "t"}},
	}, nil); st != 400 {
		t.Fatalf("create no task type %d", st)
	}
}

func TestSecretsMissingResources(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/secrets/scopes/delete", pat, map[string]any{"scope": "nope"}, nil); st != 404 {
		t.Fatalf("delete scope %d", st)
	}
	if st := h.json("POST", "/api/2.0/secrets/delete", pat, map[string]any{"scope": "nope", "key": "k"}, nil); st != 404 {
		t.Fatalf("delete key %d", st)
	}
	if st := h.json("GET", "/api/2.0/secrets/list?scope=nope", pat, nil, nil); st != 404 {
		t.Fatalf("list keys %d", st)
	}
}

func TestHelpers(t *testing.T) {
	if itoa(0) != "0" || itoa(-12) != "-12" || itoa(7) != "7" {
		t.Fatalf("itoa %q %q %q", itoa(0), itoa(-12), itoa(7))
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"a":1}`))
	var dest map[string]any
	if err := decodeJSON(req, &dest); err != nil || dest["a"] != float64(1) {
		t.Fatalf("decodeJSON %v %+v", err, dest)
	}
	raw, err := decodeB64(base64.RawStdEncoding.EncodeToString([]byte("hi")))
	if err != nil || string(raw) != "hi" {
		t.Fatalf("raw b64 %q %v", raw, err)
	}
}
