package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// MaxDBFSRead is the Databricks dbfs/read length cap (1 MiB). Allocating
// from an unbounded query parameter is a denial of service.
const MaxDBFSRead int64 = 1 << 20

// DBFS is a file-backed store under data/dbfs. The Files API shares this root.
type DBFS struct {
	mu      sync.Mutex
	root    string
	handles map[int64]*os.File
	next    int64
}

func openDBFS(dataDir string) (*DBFS, error) {
	root := filepath.Join(dataDir, "dbfs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &DBFS{root: root, handles: map[int64]*os.File{}}, nil
}

func (d *DBFS) disk(p string) (string, error) {
	rooted, err := DBFSPath(p)
	if err != nil {
		return "", err
	}
	rel, err := CleanRel(rooted)
	if err != nil {
		return "", err
	}
	return filepath.Join(d.root, filepath.FromSlash(rel)), nil
}

// Put writes an entire file.
func (d *DBFS) Put(p string, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(disk), 0o755); err != nil {
		return err
	}
	return os.WriteFile(disk, data, 0o644)
}

// Get reads an entire file.
func (d *DBFS) Get(p string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(disk)
}

// Stat returns size and whether the path is a directory.
func (d *DBFS) Stat(p string) (size int64, isDir bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return 0, false, err
	}
	st, err := os.Stat(disk)
	if err != nil {
		return 0, false, err
	}
	return st.Size(), st.IsDir(), nil
}

// Mkdir creates a directory tree.
func (d *DBFS) Mkdir(p string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return err
	}
	return os.MkdirAll(disk, 0o755)
}

// Delete removes a file or directory.
func (d *DBFS) Delete(p string, recursive bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
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
	if st.IsDir() {
		return os.RemoveAll(disk)
	}
	return os.Remove(disk)
}

// Move renames a path.
func (d *DBFS) Move(src, dst string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	from, err := d.disk(src)
	if err != nil {
		return err
	}
	to, err := d.disk(dst)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return err
	}
	return os.Rename(from, to)
}

// List returns names (not full paths) of children plus a dir flag.
func (d *DBFS) List(p string) ([]DBFSEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(disk)
	if err != nil {
		return nil, err
	}
	parent, _ := DBFSPath(p)
	var out []DBFSEntry
	for _, e := range ents {
		info, err := e.Info()
		if err != nil {
			continue
		}
		child := parent
		if child == "/" {
			child = "/" + e.Name()
		} else {
			child = parent + "/" + e.Name()
		}
		out = append(out, DBFSEntry{Path: child, IsDir: e.IsDir(), Size: info.Size()})
	}
	return out, nil
}

// DBFSEntry is a list/status row.
type DBFSEntry struct {
	Path  string
	IsDir bool
	Size  int64
}

// CreateHandle starts a chunked upload.
func (d *DBFS) CreateHandle(p string, overwrite bool) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return 0, err
	}
	if !overwrite {
		if _, err := os.Stat(disk); err == nil {
			return 0, fmt.Errorf("file exists")
		}
	}
	if err := os.MkdirAll(filepath.Dir(disk), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(disk)
	if err != nil {
		return 0, err
	}
	d.next++
	id := d.next
	d.handles[id] = f
	return id, nil
}

// AddBlock appends to an open handle.
func (d *DBFS) AddBlock(handle int64, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, ok := d.handles[handle]
	if !ok {
		return fmt.Errorf("unknown handle")
	}
	_, err := f.Write(data)
	return err
}

// CloseHandle finalizes a chunked upload.
func (d *DBFS) CloseHandle(handle int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, ok := d.handles[handle]
	if !ok {
		return fmt.Errorf("unknown handle")
	}
	delete(d.handles, handle)
	return f.Close()
}

// ReadAt reads a slice of a file (DBFS read).
func (d *DBFS) ReadAt(p string, offset, length int64) ([]byte, error) {
	if length <= 0 || length > MaxDBFSRead {
		return nil, fmt.Errorf("length must be between 1 and %d", MaxDBFSRead)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	disk, err := d.disk(p)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(disk)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(f, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return buf[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
