package server

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) dbfsPut(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path     string `json:"path"`
		Contents string `json:"contents"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	raw, err := decodeB64(body.Contents)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "contents must be base64")
		return
	}
	if err := s.Store.DBFS.Put(body.Path, raw); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) dbfsRead(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := query(r, "path")
	offset := parseInt64(query(r, "offset"))
	length := parseInt64(query(r, "length"))
	if length <= 0 {
		length = store.MaxDBFSRead
	}
	if length > store.MaxDBFSRead {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "length exceeds 1MB")
		return
	}
	b, err := s.Store.DBFS.ReadAt(p, offset, length)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": encodeB64(b), "bytes_read": len(b)})
}

func (s *Server) dbfsStatus(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := query(r, "path")
	size, isDir, err := s.Store.DBFS.Stat(p)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": p, "is_dir": isDir, "file_size": size})
}

func (s *Server) dbfsList(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	ents, err := s.Store.DBFS.List(query(r, "path"))
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	var files []map[string]any
	for _, e := range ents {
		files = append(files, map[string]any{"path": e.Path, "is_dir": e.IsDir, "file_size": e.Size})
	}
	writeJSON(w, http.StatusOK, map[string]any{"files": files})
}

func (s *Server) dbfsMkdirs(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path string `json:"path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.DBFS.Mkdir(body.Path); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) dbfsDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.DBFS.Delete(body.Path, body.Recursive); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) dbfsMove(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		SourcePath      string `json:"source_path"`
		DestinationPath string `json:"destination_path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.DBFS.Move(body.SourcePath, body.DestinationPath); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) dbfsCreate(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path      string `json:"path"`
		Overwrite bool   `json:"overwrite"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	h, err := s.Store.DBFS.CreateHandle(body.Path, body.Overwrite)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"handle": h})
}

func (s *Server) dbfsAddBlock(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Handle int64  `json:"handle"`
		Data   string `json:"data"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	raw, err := decodeB64(body.Data)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "data must be base64")
		return
	}
	if err := s.Store.DBFS.AddBlock(body.Handle, raw); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) dbfsClose(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Handle int64 `json:"handle"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.DBFS.CloseHandle(body.Handle); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) fsPut(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/fs/files/")
	if err := s.Store.DBFS.Put(p, readAll(r.Body)); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fsGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/fs/files/")
	b, err := s.Store.DBFS.Get(p)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		writeWorkspaceErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(b)
}

func (s *Server) fsHead(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/fs/files/")
	size, isDir, err := s.Store.DBFS.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		writeWorkspaceErr(w, err)
		return
	}
	if isDir {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "not a file")
		return
	}
	w.Header().Set("Content-Length", itoa(size))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) fsDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/fs/files/")
	if err := s.Store.DBFS.Delete(p, false); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fsMkdir(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/fs/directories/")
	if err := s.Store.DBFS.Mkdir(p); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fsListDir(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/fs/directories/")
	ents, err := s.Store.DBFS.List(p)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	var contents []map[string]any
	for _, e := range ents {
		kind := "FILE"
		if e.IsDir {
			kind = "DIRECTORY"
		}
		contents = append(contents, map[string]any{"path": e.Path, "name": e.Path, "file_size": e.Size, "type": kind})
	}
	writeJSON(w, http.StatusOK, map[string]any{"contents": contents})
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
