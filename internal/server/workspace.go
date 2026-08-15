package server

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func (s *Server) workspaceImport(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	req, err := parseWorkspaceImport(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	storePath, objectType, lang, err := classifyImport(req.path, req.format, req.language, req.raw)
	if err != nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", err.Error())
		return
	}
	if !req.overwrite {
		if _, _, err := s.Store.Workspace.Get(storePath); err == nil {
			writeError(w, http.StatusConflict, "RESOURCE_ALREADY_EXISTS", storePath)
			return
		}
	}
	if err := s.Store.Workspace.Put(storePath, req.raw, objectType, lang); err != nil {
		if err == store.ErrTraversal || strings.Contains(err.Error(), "traversal") {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

type workspaceImportReq struct {
	path      string
	format    string
	language  string
	overwrite bool
	raw       []byte
}

func parseWorkspaceImport(r *http.Request) (workspaceImportReq, error) {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			return workspaceImportReq{}, err
		}
		out := workspaceImportReq{
			path:      r.FormValue("path"),
			format:    r.FormValue("format"),
			language:  r.FormValue("language"),
			overwrite: truthy(r.FormValue("overwrite")),
		}
		if f, _, err := r.FormFile("content"); err == nil {
			defer f.Close()
			out.raw, err = io.ReadAll(f)
			if err != nil {
				return workspaceImportReq{}, err
			}
			return out, nil
		}
		if s := r.FormValue("content"); s != "" {
			raw, err := decodeB64(s)
			if err != nil {
				return workspaceImportReq{}, err
			}
			out.raw = raw
			return out, nil
		}
		return workspaceImportReq{}, errNoContent
	}
	var body struct {
		Path      string `json:"path"`
		Format    string `json:"format"`
		Language  string `json:"language"`
		Content   string `json:"content"`
		Overwrite bool   `json:"overwrite"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return workspaceImportReq{}, err
	}
	raw, err := decodeB64(body.Content)
	if err != nil {
		return workspaceImportReq{}, err
	}
	return workspaceImportReq{
		path:      body.Path,
		format:    body.Format,
		language:  body.Language,
		overwrite: body.Overwrite,
		raw:       raw,
	}, nil
}

var errNoContent = io.ErrUnexpectedEOF

func truthy(s string) bool {
	switch strings.ToLower(s) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func classifyImport(p, format, language string, raw []byte) (storePath, objectType, lang string, err error) {
	format = strings.ToUpper(strings.TrimSpace(format))
	if format == "" {
		format = "SOURCE"
	}
	lang = strings.ToUpper(strings.TrimSpace(language))
	if format == "AUTO" {
		if isNotebookSource(raw) {
			lang = langFromExt(p)
			if lang != "PYTHON" {
				return "", "", "", errClassicPythonOnly
			}
			return stripNotebookExt(p), store.ObjectNotebook, lang, nil
		}
		return p, store.ObjectFile, "", nil
	}
	if lang == "" {
		lang = "PYTHON"
	}
	if format != "SOURCE" || lang != "PYTHON" {
		return "", "", "", errClassicPythonOnly
	}
	return p, store.ObjectNotebook, lang, nil
}

var errClassicPythonOnly = errString("classic /workspace/import only accepts format=SOURCE language=PYTHON, or AUTO for a file / Python notebook")

type errString string

func (e errString) Error() string { return string(e) }

func isNotebookSource(raw []byte) bool {
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		return strings.TrimSpace(line) == "# Databricks notebook source"
	}
	return false
}

func langFromExt(p string) string {
	switch strings.ToLower(path.Ext(p)) {
	case ".py":
		return "PYTHON"
	case ".sql":
		return "SQL"
	case ".scala":
		return "SCALA"
	case ".r":
		return "R"
	}
	return ""
}

func stripNotebookExt(p string) string {
	ext := path.Ext(p)
	if ext == "" {
		return p
	}
	return strings.TrimSuffix(p, ext)
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
	if truthy(query(r, "direct_download")) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
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
