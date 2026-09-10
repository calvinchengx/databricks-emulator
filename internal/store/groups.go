package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// Group is a SCIM v2 group. Members are held as SCIM "value" references, which
// is what the wire format carries; nothing here resolves them to users, because
// a membership this process cannot enforce would be a claim without a witness.
type Group struct {
	ID          string
	DisplayName string
	Members     []string
	CreatedAt   int64
}

type persistedGroup struct {
	ID          string   `json:"id"`
	DisplayName string   `json:"display_name"`
	Members     []string `json:"members,omitempty"`
	CreatedAt   int64    `json:"created_at"`
}

type persistedGroups struct {
	NextID int64            `json:"next_id"`
	Groups []persistedGroup `json:"groups"`
}

// Groups is the SCIM group directory. State survives the process: a group
// created by one client is readable by the next, which is the only thing that
// distinguishes a directory from an endpoint that answers.
type Groups struct {
	mu     sync.Mutex
	dir    string
	nextID int64
	all    map[string]*Group
}

func openGroups(dataDir string) (*Groups, error) {
	dir := filepath.Join(dataDir, "groups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	g := &Groups{dir: dir, all: map[string]*Group{}}
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return g, nil
		}
		return nil, err
	}
	var dump persistedGroups
	if err := json.Unmarshal(b, &dump); err != nil {
		return nil, fmt.Errorf("groups store: %w", err)
	}
	g.nextID = dump.NextID
	for _, row := range dump.Groups {
		g.all[row.ID] = &Group{
			ID: row.ID, DisplayName: row.DisplayName,
			Members: row.Members, CreatedAt: row.CreatedAt,
		}
	}
	return g, nil
}

func (g *Groups) persistLocked() error {
	dump := persistedGroups{NextID: g.nextID}
	for _, grp := range g.all {
		dump.Groups = append(dump.Groups, persistedGroup{
			ID: grp.ID, DisplayName: grp.DisplayName,
			Members: grp.Members, CreatedAt: grp.CreatedAt,
		})
	}
	sort.Slice(dump.Groups, func(i, j int) bool { return dump.Groups[i].ID < dump.Groups[j].ID })
	b, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(g.dir, "state.json"), b, 0o644)
}

// ErrGroupExists is returned when displayName is already taken. Real SCIM
// refuses a duplicate with 409, so allowing one here would be a lie the SDK
// would believe.
var ErrGroupExists = fmt.Errorf("group already exists")

// ErrGroupNotFound is returned for an unknown id.
var ErrGroupNotFound = fmt.Errorf("group not found")

func (g *Groups) Create(displayName string, members []string, now int64) (*Group, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, grp := range g.all {
		if grp.DisplayName == displayName {
			return nil, ErrGroupExists
		}
	}
	g.nextID++
	grp := &Group{
		ID:          strconv.FormatInt(g.nextID, 10),
		DisplayName: displayName,
		Members:     members,
		CreatedAt:   now,
	}
	g.all[grp.ID] = grp
	if err := g.persistLocked(); err != nil {
		delete(g.all, grp.ID)
		return nil, err
	}
	return grp, nil
}

func (g *Groups) Get(id string) (*Group, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	grp, ok := g.all[id]
	if !ok {
		return nil, ErrGroupNotFound
	}
	return grp, nil
}

// List returns groups by id, ascending, so paging and diffs are stable.
func (g *Groups) List() []*Group {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*Group, 0, len(g.all))
	for _, grp := range g.all {
		out = append(out, grp)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := strconv.ParseInt(out[i].ID, 10, 64)
		b, _ := strconv.ParseInt(out[j].ID, 10, 64)
		return a < b
	})
	return out
}

func (g *Groups) Delete(id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.all[id]; !ok {
		return ErrGroupNotFound
	}
	delete(g.all, id)
	return g.persistLocked()
}
