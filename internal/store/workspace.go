package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ObjectType is a workspace listing discriminator.
const (
	ObjectNotebook = "NOTEBOOK"
	ObjectFile     = "FILE"
	ObjectDir      = "DIRECTORY"
)

// WorkspaceObject is listing metadata.
type WorkspaceObject struct {
	Path       string `json:"path"`
	ObjectType string `json:"object_type"`
	Language   string `json:"language,omitempty"`
}

type workspaceMeta struct {
	Language   string `json:"language,omitempty"`
	ObjectType string `json:"object_type"`
}

// Workspace is a file-backed workspace store under data/workspace.
type Workspace struct {
	mu   sync.Mutex
	root string
}

func openWorkspace(dataDir string) (*Workspace, error) {
	root := filepath.Join(dataDir, "workspace")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Workspace{root: root}, nil
}

func (w *Workspace) disk(p string) (string, error) {
	rel, err := CleanRel(p)
	if err != nil {
		return "", err
	}
	return filepath.Join(w.root, filepath.FromSlash(rel)), nil
}

func (w *Workspace) metaPath(disk string) string { return disk + ".wsmeta.json" }

// Put writes bytes at path. objectType is NOTEBOOK or FILE.
func (w *Workspace) Put(p string, data []byte, objectType, language string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	disk, err := w.disk(p)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(disk), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(disk, data, 0o644); err != nil {
		return err
	}
	meta := workspaceMeta{Language: language, ObjectType: objectType}
	b, _ := json.Marshal(meta)
	return os.WriteFile(w.metaPath(disk), b, 0o644)
}

// Get reads bytes and metadata. missing is (nil, "", os.ErrNotExist).
func (w *Workspace) Get(p string) ([]byte, WorkspaceObject, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	disk, err := w.disk(p)
	if err != nil {
		return nil, WorkspaceObject{}, err
	}
	st, err := os.Stat(disk)
	if err != nil {
		return nil, WorkspaceObject{}, err
	}
	if st.IsDir() {
		wp, _ := WorkspacePath(p)
		return nil, WorkspaceObject{Path: wp, ObjectType: ObjectDir}, nil
	}
	b, err := os.ReadFile(disk)
	if err != nil {
		return nil, WorkspaceObject{}, err
	}
	obj := WorkspaceObject{ObjectType: ObjectFile}
	obj.Path, _ = WorkspacePath(p)
	if mb, err := os.ReadFile(w.metaPath(disk)); err == nil {
		var m workspaceMeta
		if json.Unmarshal(mb, &m) == nil {
			if m.ObjectType != "" {
				obj.ObjectType = m.ObjectType
			}
			obj.Language = m.Language
		}
	}
	return b, obj, nil
}

// Mkdir creates a directory tree.
func (w *Workspace) Mkdir(p string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	disk, err := w.disk(p)
	if err != nil {
		return err
	}
	return os.MkdirAll(disk, 0o755)
}

// Delete removes a file or a directory tree.
func (w *Workspace) Delete(p string, recursive bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	disk, err := w.disk(p)
	if err != nil {
		return err
	}
	st, err := os.Stat(disk)
	if err != nil {
		return err
	}
	if st.IsDir() && !recursive {
		return fmt.Errorf("directory delete requires recursive=true")
	}
	_ = os.Remove(w.metaPath(disk))
	if st.IsDir() {
		return os.RemoveAll(disk)
	}
	return os.Remove(disk)
}

// List returns direct children of p.
func (w *Workspace) List(p string) ([]WorkspaceObject, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	disk, err := w.disk(p)
	if err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(disk)
	if err != nil {
		return nil, err
	}
	parent, _ := WorkspacePath(p)
	var out []WorkspaceObject
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".wsmeta.json") {
			continue
		}
		child := parent
		if child == "/" {
			child = "/" + e.Name()
		} else {
			child = parent + "/" + e.Name()
		}
		obj := WorkspaceObject{Path: child, ObjectType: ObjectFile}
		if e.IsDir() {
			obj.ObjectType = ObjectDir
		} else if mb, err := os.ReadFile(w.metaPath(filepath.Join(disk, e.Name()))); err == nil {
			var m workspaceMeta
			if json.Unmarshal(mb, &m) == nil {
				if m.ObjectType != "" {
					obj.ObjectType = m.ObjectType
				}
				obj.Language = m.Language
			}
		}
		out = append(out, obj)
	}
	return out, nil
}
