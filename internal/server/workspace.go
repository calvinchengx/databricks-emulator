package server

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) workspaceImport(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path      string `json:"path"`
		Format    string `json:"format"`
		Language  string `json:"language"`
		Content   string `json:"content"`
		Overwrite bool   `json:"overwrite"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	format := strings.ToUpper(body.Format)
	if format == "" {
		format = "SOURCE"
	}
	lang := strings.ToUpper(body.Language)
	if lang == "" {
		lang = "PYTHON"
	}
	if format != "SOURCE" || lang != "PYTHON" {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			"classic /workspace/import only accepts format=SOURCE language=PYTHON")
		return
	}
	raw, err := decodeB64(body.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "content must be base64")
		return
	}
	if !body.Overwrite {
		if _, _, err := s.Store.Workspace.Get(body.Path); err == nil {
			writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", body.Path)
			return
		}
	}
	if err := s.Store.Workspace.Put(body.Path, raw, store.ObjectNotebook, lang); err != nil {
		if err == store.ErrTraversal || strings.Contains(err.Error(), "traversal") {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) workspaceExport(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := query(r, "path")
	b, obj, err := s.Store.Workspace.Get(p)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	if obj.ObjectType == store.ObjectDir {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot export a directory")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"content":  encodeB64(b),
		"path":     obj.Path,
		"language": obj.Language,
	})
}

func (s *Server) workspaceStatus(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := query(r, "path")
	_, obj, err := s.Store.Workspace.Get(p)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, obj)
}

func (s *Server) workspaceList(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := query(r, "path")
	if p == "" {
		p = "/"
	}
	objs, err := s.Store.Workspace.List(p)
	if err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": objs})
}

func (s *Server) workspaceMkdirs(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path string `json:"path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.Workspace.Mkdir(body.Path); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) workspaceDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Store.Workspace.Delete(body.Path, body.Recursive); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) workspaceFilesImport(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/workspace-files/import-file/")
	if err := s.Store.Workspace.Put(p, readAll(r.Body), store.ObjectFile, ""); err != nil {
		writeWorkspaceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) workspaceFilesGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	p := pathFromURL(r.URL, "/api/2.0/workspace-files/")
	b, obj, err := s.Store.Workspace.Get(p)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		writeWorkspaceErr(w, err)
		return
	}
	if obj.ObjectType == store.ObjectDir {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "not a file")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

func writeWorkspaceErr(w http.ResponseWriter, err error) {
	if os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", err.Error())
		return
	}
	if err == store.ErrTraversal || strings.Contains(err.Error(), "traversal") {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
}

func encodeB64(b []byte) string {
	return b64(string(b))
}
