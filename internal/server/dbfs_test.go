package server

import (
	"encoding/base64"
	"io"
	"testing"
)

func TestDBFSPutGetListMoveDeleteAndTraversal(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	payload := base64.StdEncoding.EncodeToString([]byte("hello-dbfs"))
	if st := h.json("POST", "/api/2.0/dbfs/put", pat, map[string]any{"path": "/tmp/a.txt", "contents": payload}, nil); st != 200 {
		t.Fatalf("put %d", st)
	}
	var read map[string]any
	if st := h.json("GET", "/api/2.0/dbfs/read?path=/tmp/a.txt&offset=0&length=100", pat, nil, &read); st != 200 {
		t.Fatalf("read %d", st)
	}
	got, _ := base64.StdEncoding.DecodeString(read["data"].(string))
	if string(got) != "hello-dbfs" {
		t.Fatalf("got %q", got)
	}
	if st := h.json("GET", "/api/2.0/dbfs/read?path=/tmp/a.txt&offset=0&length=2000000", pat, nil, nil); st != 400 {
		t.Fatalf("oversize length %d", st)
	}
	var stt map[string]any
	h.json("GET", "/api/2.0/dbfs/get-status?path=/tmp/a.txt", pat, nil, &stt)
	if stt["is_dir"] != false {
		t.Fatalf("status %+v", stt)
	}
	h.json("POST", "/api/2.0/dbfs/mkdirs", pat, map[string]any{"path": "/tmp/dir"}, nil)
	h.json("POST", "/api/2.0/dbfs/move", pat, map[string]any{"source_path": "/tmp/a.txt", "destination_path": "/tmp/dir/b.txt"}, nil)
	var listed map[string]any
	h.json("GET", "/api/2.0/dbfs/list?path=/tmp/dir", pat, nil, &listed)
	if st := h.json("POST", "/api/2.0/dbfs/put", pat, map[string]any{"path": "/../etc/passwd", "contents": payload}, nil); st != 400 {
		t.Fatalf("traversal %d", st)
	}
	h.json("POST", "/api/2.0/dbfs/delete", pat, map[string]any{"path": "/tmp/dir", "recursive": true}, nil)
}

func TestDBFSChunkedUpload(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	if st := h.json("POST", "/api/2.0/dbfs/create", pat, map[string]any{"path": "/chunk.bin", "overwrite": true}, &created); st != 200 {
		t.Fatalf("create %d", st)
	}
	handle := created["handle"]
	block := base64.StdEncoding.EncodeToString([]byte("AB"))
	h.json("POST", "/api/2.0/dbfs/add-block", pat, map[string]any{"handle": handle, "data": block}, nil)
	h.json("POST", "/api/2.0/dbfs/add-block", pat, map[string]any{"handle": handle, "data": base64.StdEncoding.EncodeToString([]byte("CD"))}, nil)
	h.json("POST", "/api/2.0/dbfs/close", pat, map[string]any{"handle": handle}, nil)
	b, err := h.srv.Store.DBFS.Get("/chunk.bin")
	if err != nil || string(b) != "ABCD" {
		t.Fatalf("chunked %q %v", b, err)
	}
	if st := h.json("POST", "/api/2.0/dbfs/add-block", pat, map[string]any{"handle": 99, "data": block}, nil); st != 400 {
		t.Fatalf("bad handle %d", st)
	}
}

func TestFilesAPIRoundTrip(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	put := h.do("PUT", "/api/2.0/fs/files/data/x.bin", pat, []byte{1, 2, 3})
	put.Body.Close()
	if put.StatusCode != 204 {
		t.Fatalf("put %d", put.StatusCode)
	}
	get := h.do("GET", "/api/2.0/fs/files/data/x.bin", pat, nil)
	defer get.Body.Close()
	b, _ := io.ReadAll(get.Body)
	if string(b) != string([]byte{1, 2, 3}) {
		t.Fatalf("get %x", b)
	}
	head := h.do("HEAD", "/api/2.0/fs/files/data/x.bin", pat, nil)
	head.Body.Close()
	if head.StatusCode != 200 {
		t.Fatalf("head %d", head.StatusCode)
	}
	mkdir := h.do("PUT", "/api/2.0/fs/directories/data/sub", pat, nil)
	mkdir.Body.Close()
	var listed map[string]any
	h.json("GET", "/api/2.0/fs/directories/data", pat, nil, &listed)
	del := h.do("DELETE", "/api/2.0/fs/files/data/x.bin", pat, nil)
	del.Body.Close()
	if del.StatusCode != 204 {
		t.Fatalf("delete %d", del.StatusCode)
	}
}
