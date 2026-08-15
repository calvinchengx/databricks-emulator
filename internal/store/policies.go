package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Policy is a cluster policy. Definition is Databricks policy JSON; every
// attribute in it is one this process actually enforces.
type Policy struct {
	ID          string
	Name        string
	Definition  string
	Description string
	FamilyID    string
	Creator     string
	CreatedAt   int64
}

// ClusterAttrs are the create/get fields a policy can constrain.
type ClusterAttrs struct {
	SparkVersion string
	NodeTypeID   string
	NumWorkers   int
	Autoscale    bool
	Libraries    bool
}

var knownPolicyAttrs = map[string]bool{
	"spark_version": true,
	"node_type_id":  true,
	"num_workers":   true,
	"autoscale":     true,
	"libraries":     true,
}

// EmulatorSessionFamily is the one policy family: a session handle, not a VM.
const EmulatorSessionFamilyID = "emulator-session"

// EmulatorSessionFamilyDefinition locks the two fields this process actually
// runs with. Autoscale and libraries are already refused on create.
const EmulatorSessionFamilyDefinition = `{"spark_version":{"type":"fixed","value":"emulator-spark"},"node_type_id":{"type":"fixed","value":"emulator.session"},"num_workers":{"type":"range","minValue":0,"maxValue":0},"autoscale":{"type":"forbidden"},"libraries":{"type":"forbidden"}}`

type persistedPolicies struct {
	NextID   int64             `json:"next_id"`
	Policies []persistedPolicy `json:"policies"`
}

type persistedPolicy struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Definition  string `json:"definition"`
	Description string `json:"description,omitempty"`
	FamilyID    string `json:"family_id,omitempty"`
	Creator     string `json:"creator,omitempty"`
	CreatedAt   int64  `json:"created_at,omitempty"`
}

// Policies is file-backed cluster policy metadata under data/policies/.
type Policies struct {
	mu     sync.Mutex
	dir    string
	nextID int64
	all    map[string]*Policy
}

func openPolicies(dataDir string) (*Policies, error) {
	dir := filepath.Join(dataDir, "policies")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &Policies{dir: dir, all: map[string]*Policy{}}
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return p, nil
		}
		return nil, err
	}
	var dump persistedPolicies
	if err := json.Unmarshal(b, &dump); err != nil {
		return nil, fmt.Errorf("policies store: %w", err)
	}
	p.nextID = dump.NextID
	for _, row := range dump.Policies {
		p.all[row.ID] = &Policy{
			ID: row.ID, Name: row.Name, Definition: row.Definition,
			Description: row.Description, FamilyID: row.FamilyID,
			Creator: row.Creator, CreatedAt: row.CreatedAt,
		}
	}
	return p, nil
}

func (p *Policies) persistLocked() error {
	dump := persistedPolicies{NextID: p.nextID}
	for _, pol := range p.all {
		dump.Policies = append(dump.Policies, persistedPolicy{
			ID: pol.ID, Name: pol.Name, Definition: pol.Definition,
			Description: pol.Description, FamilyID: pol.FamilyID,
			Creator: pol.Creator, CreatedAt: pol.CreatedAt,
		})
	}
	b, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(p.dir, "state.json.tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(p.dir, "state.json"))
}

// CreatePolicy stores a definition that ValidateDefinition has already accepted.
func (p *Policies) CreatePolicy(name, definition, description, familyID, creator string, now int64) (*Policy, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("name is required")
	}
	if err := ValidateDefinition(definition); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextID++
	pol := &Policy{
		ID: fmt.Sprintf("policy-%d", p.nextID), Name: name, Definition: definition,
		Description: description, FamilyID: familyID, Creator: creator, CreatedAt: now,
	}
	p.all[pol.ID] = pol
	if err := p.persistLocked(); err != nil {
		return nil, err
	}
	return clonePolicy(pol), nil
}

func (p *Policies) Get(id string) (*Policy, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pol, ok := p.all[id]
	if !ok {
		return nil, false
	}
	return clonePolicy(pol), true
}

func (p *Policies) List() []*Policy {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Policy, 0, len(p.all))
	for _, pol := range p.all {
		out = append(out, clonePolicy(pol))
	}
	return out
}

func (p *Policies) Edit(id, name, definition, description string) (*Policy, error) {
	if definition != "" {
		if err := ValidateDefinition(definition); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	pol, ok := p.all[id]
	if !ok {
		return nil, fmt.Errorf("policy not found")
	}
	if name != "" {
		pol.Name = name
	}
	if definition != "" {
		pol.Definition = definition
	}
	if description != "" {
		pol.Description = description
	}
	if err := p.persistLocked(); err != nil {
		return nil, err
	}
	return clonePolicy(pol), nil
}

func (p *Policies) Delete(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.all[id]; !ok {
		return false
	}
	delete(p.all, id)
	_ = p.persistLocked()
	return true
}

func clonePolicy(p *Policy) *Policy {
	cp := *p
	return &cp
}

// ValidateDefinition refuses attributes and types this process cannot enforce.
func ValidateDefinition(definition string) error {
	attrs, err := parseDefinition(definition)
	if err != nil {
		return err
	}
	if len(attrs) == 0 {
		return fmt.Errorf("definition is required")
	}
	for name, spec := range attrs {
		if !knownPolicyAttrs[name] {
			return fmt.Errorf("policy attribute %s is not enforced on a session handle; not stored-and-ignored", name)
		}
		typ, _ := spec["type"].(string)
		switch typ {
		case "fixed", "forbidden", "unlimited", "range", "allowlist":
		default:
			return fmt.Errorf("policy type %q on %s is not implemented", typ, name)
		}
	}
	return nil
}

func parseDefinition(definition string) (map[string]map[string]any, error) {
	definition = strings.TrimSpace(definition)
	if definition == "" {
		return nil, fmt.Errorf("definition is required")
	}
	var attrs map[string]map[string]any
	if err := json.Unmarshal([]byte(definition), &attrs); err != nil {
		return nil, fmt.Errorf("definition: %w", err)
	}
	return attrs, nil
}

// EvaluatePolicy returns attribute → message for each violation. Empty means compliant.
func EvaluatePolicy(definition string, a ClusterAttrs) (map[string]string, error) {
	attrs, err := parseDefinition(definition)
	if err != nil {
		return nil, err
	}
	violations := map[string]string{}
	for name, spec := range attrs {
		typ, _ := spec["type"].(string)
		got := attrValue(a, name)
		switch typ {
		case "unlimited":
			continue
		case "forbidden":
			if present(got) {
				violations[name] = name + " is forbidden by policy"
			}
		case "fixed":
			want := fmt.Sprint(spec["value"])
			if fmt.Sprint(got) != want {
				violations[name] = fmt.Sprintf("%s must be %s", name, want)
			}
		case "range":
			n, ok := asFloat(got)
			if !ok {
				violations[name] = name + " must be a number"
				continue
			}
			if minV, ok := asFloat(spec["minValue"]); ok && n < minV {
				violations[name] = fmt.Sprintf("%s must be >= %v", name, spec["minValue"])
			}
			if maxV, ok := asFloat(spec["maxValue"]); ok && n > maxV {
				violations[name] = fmt.Sprintf("%s must be <= %v", name, spec["maxValue"])
			}
		case "allowlist":
			ok := false
			switch values := spec["values"].(type) {
			case []any:
				for _, v := range values {
					if fmt.Sprint(v) == fmt.Sprint(got) {
						ok = true
						break
					}
				}
			}
			if !ok {
				violations[name] = name + " is not in the policy allowlist"
			}
		}
	}
	return violations, nil
}

func attrValue(a ClusterAttrs, name string) any {
	switch name {
	case "spark_version":
		return a.SparkVersion
	case "node_type_id":
		return a.NodeTypeID
	case "num_workers":
		return a.NumWorkers
	case "autoscale":
		if a.Autoscale {
			return true
		}
		return nil
	case "libraries":
		if a.Libraries {
			return true
		}
		return nil
	}
	return nil
}

func present(v any) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	s := fmt.Sprint(v)
	return s != "" && s != "0" && s != "<nil>"
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}

// ApplyFixedDefaults fills omitted spark_version / node_type_id from fixed rules.
func ApplyFixedDefaults(definition string, a ClusterAttrs) ClusterAttrs {
	attrs, err := parseDefinition(definition)
	if err != nil {
		return a
	}
	if a.SparkVersion == "" {
		if spec, ok := attrs["spark_version"]; ok && fmt.Sprint(spec["type"]) == "fixed" {
			a.SparkVersion = fmt.Sprint(spec["value"])
		}
	}
	if a.NodeTypeID == "" {
		if spec, ok := attrs["node_type_id"]; ok && fmt.Sprint(spec["type"]) == "fixed" {
			a.NodeTypeID = fmt.Sprint(spec["value"])
		}
	}
	return a
}
