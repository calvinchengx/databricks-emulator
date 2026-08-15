package server

import (
	"encoding/base64"
	"io"
	"testing"
)

func TestWorkspaceImportExportAndRefuseFormats(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	content := base64.StdEncoding.EncodeToString([]byte("print(1)\n"))
	if st := h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/etl.py", "format": "SOURCE", "language": "PYTHON", "content": content, "overwrite": true,
	}, nil); st != 200 {
		t.Fatalf("import %d", st)
	}
	var exp map[string]any
	if st := h.json("GET", "/api/2.0/workspace/export?path=/Shared/etl.py", pat, nil, &exp); st != 200 {
		t.Fatalf("export %d", st)
	}
	got, _ := base64.StdEncoding.DecodeString(exp["content"].(string))
	if string(got) != "print(1)\n" {
		t.Fatalf("bytes = %q", got)
	}
	if st := h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/etl.py", "format": "JUPYTER", "language": "PYTHON", "content": content,
	}, nil); st != 501 {
		t.Fatalf("jupyter %d", st)
	}
	if st := h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/x.scala", "format": "SOURCE", "language": "SCALA", "content": content,
	}, nil); st != 501 {
		t.Fatalf("scala %d", st)
	}
	if st := h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/../etc/passwd", "format": "SOURCE", "language": "PYTHON", "content": content, "overwrite": true,
	}, nil); st != 400 {
		t.Fatalf("traversal %d", st)
	}
}

func TestWorkspaceFilesRawBytesAnd404(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	whl := []byte{0x50, 0x4b, 0x03, 0x04, 0x00, 0xff} // not valid zip; bytes must round-trip
	resp := h.do("POST", "/api/2.0/workspace-files/import-file/libs/pkg.whl", pat, whl)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("import-file %d", resp.StatusCode)
	}
	got := h.do("GET", "/api/2.0/workspace-files/libs/pkg.whl", pat, nil)
	defer got.Body.Close()
	if got.StatusCode != 200 {
		t.Fatalf("get %d", got.StatusCode)
	}
	b, _ := io.ReadAll(got.Body)
	if string(b) != string(whl) {
		t.Fatalf("round-trip %x", b)
	}
	miss := h.do("GET", "/api/2.0/workspace-files/missing.lock", pat, nil)
	miss.Body.Close()
	if miss.StatusCode != 404 {
		t.Fatalf("missing %d", miss.StatusCode)
	}
}

func TestWorkspaceListMkdirDelete(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	if st := h.json("POST", "/api/2.0/workspace/mkdirs", pat, map[string]any{"path": "/Shared/n"}, nil); st != 200 {
		t.Fatalf("mkdirs %d", st)
	}
	content := base64.StdEncoding.EncodeToString([]byte("x"))
	h.json("POST", "/api/2.0/workspace/import", pat, map[string]any{
		"path": "/Shared/n/a.py", "content": content, "overwrite": true,
	}, nil)
	var listed map[string]any
	if st := h.json("GET", "/api/2.0/workspace/list?path=/Shared/n", pat, nil, &listed); st != 200 {
		t.Fatalf("list %d", st)
	}
	var status map[string]any
	h.json("GET", "/api/2.0/workspace/get-status?path=/Shared/n/a.py", pat, nil, &status)
	if status["object_type"] != "NOTEBOOK" {
		t.Fatalf("status %+v", status)
	}
	if st := h.json("POST", "/api/2.0/workspace/delete", pat, map[string]any{"path": "/Shared/n", "recursive": true}, nil); st != 200 {
		t.Fatalf("delete %d", st)
	}
	if st := h.json("GET", "/api/2.0/workspace/export?path=/Shared/n/a.py", pat, nil, nil); st != 404 {
		t.Fatalf("deleted still there %d", st)
	}
}
