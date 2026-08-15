package store

import (
	"fmt"
	"path"
	"strings"
)

// ErrTraversal is a path that would escape its root.
var ErrTraversal = fmt.Errorf("path traversal refused")

// CleanRel returns a slash-separated path with no leading slash and no
// ".." segments. Empty after clean is allowed (the root).
func CleanRel(p string) (string, error) {
	p = strings.TrimSpace(p)
	p = strings.ReplaceAll(p, "\\", "/")
	if strings.Contains(p, "..") {
		return "", fmt.Errorf("%w: %q", ErrTraversal, p)
	}
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", nil
	}
	cleaned := path.Clean("/" + p)
	if cleaned == "/" {
		return "", nil
	}
	rel := strings.TrimPrefix(cleaned, "/")
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("%w: %q", ErrTraversal, p)
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: %q", ErrTraversal, p)
		}
	}
	return rel, nil
}

// WorkspacePath normalises a workspace object path to begin with /.
func WorkspacePath(p string) (string, error) {
	rel, err := CleanRel(p)
	if err != nil {
		return "", err
	}
	if rel == "" {
		return "/", nil
	}
	return "/" + rel, nil
}

// DBFSPath strips a dbfs: prefix and returns a rooted path.
func DBFSPath(p string) (string, error) {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "dbfs:")
	rel, err := CleanRel(p)
	if err != nil {
		return "", err
	}
	if rel == "" {
		return "/", nil
	}
	return "/" + rel, nil
}
