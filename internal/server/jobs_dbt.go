package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/hs2"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

// dbt_task, terminated here and executed by the attached agent.
//
// WHAT REAL DATABRICKS DOES, and what this has to match: a dbt_task runs the
// dbt CLI on compute, against the SQL warehouse named by `warehouse_id`. dbt
// itself is an ordinary warehouse client -- the same one `e2e/dbt` already
// drives from a host -- so running it as a job changes WHO invokes it, not what
// it connects to. That is why this needs no new engine: the project and a
// generated profile travel to the agent, and the models execute on the
// warehouse over the wire a client would use anyway.
//
// The project travels INLINE rather than over a shared volume. The workspace
// store lives on this process's disk and the agent is a separate container, so
// a path that is meaningful here is meaningless there. Carrying the bytes makes
// the task work with no volume wiring at all, which is also what makes it
// runnable on a stack a consumer assembled themselves.

// maxDbtProjectBytes bounds what one task may carry to the agent. A dbt project
// is SQL and YAML; anything approaching this is a data directory that has been
// pointed at by mistake, and failing here names the cause where the agent
// would only report a body it could not read.
const maxDbtProjectBytes = 8 << 20

// dbtProjectFiles walks the workspace store under dir and returns
// relative-path -> bytes.
// The agent has one channel back to us -- stdout -- so the artefacts ride in
// it between markers unlikely to occur in dbt's own output. They are lifted
// out again before the logs are stored, so a caller reading `logs` sees what
// dbt printed and nothing else.
const (
	dbtArtifactsOpen  = "<<<DBT-ARTIFACTS>>>"
	dbtArtifactsClose = "<<<END-DBT-ARTIFACTS>>>"
)

// splitDbtArtifacts separates the artefact payload from dbt's own output.
//
// Returns the cleaned stdout and the artefacts as raw JSON. A run whose
// payload is missing or malformed yields no artefacts and UNCHANGED stdout:
// losing an artefact must not also lose the log that would explain why.
func splitDbtArtifacts(stdout string) (string, map[string]string, string) {
	start := strings.Index(stdout, dbtArtifactsOpen)
	if start < 0 {
		return stdout, nil, ""
	}
	rest := stdout[start+len(dbtArtifactsOpen):]
	end := strings.Index(rest, dbtArtifactsClose)
	if end < 0 {
		return stdout, nil, ""
	}
	var env struct {
		Artifacts map[string]string `json:"artifacts"`
		Failure   string            `json:"failure"`
	}
	if err := json.Unmarshal([]byte(rest[:end]), &env); err != nil {
		return stdout, nil, ""
	}
	cleaned := stdout[:start] + rest[end+len(dbtArtifactsClose):]
	return strings.TrimLeft(cleaned, "\n"), env.Artifacts, env.Failure
}

func (s *Server) dbtProjectFiles(dir string) (map[string][]byte, error) {
	root := strings.TrimRight(dir, "/")
	if root == "" {
		return nil, fmt.Errorf("dbt_task.project_directory is required")
	}
	out := map[string][]byte{}
	total := 0
	var walk func(string) error
	walk = func(p string) error {
		entries, err := s.Store.Workspace.List(p)
		if err != nil {
			return fmt.Errorf("project_directory %q: %w", root, err)
		}
		for _, e := range entries {
			if e.ObjectType == store.ObjectDir {
				if err := walk(e.Path); err != nil {
					return err
				}
				continue
			}
			data, _, err := s.Store.Workspace.Get(e.Path)
			if err != nil {
				return fmt.Errorf("read %q: %w", e.Path, err)
			}
			total += len(data)
			if total > maxDbtProjectBytes {
				return fmt.Errorf("project_directory %q exceeds %d bytes; a dbt project is "+
					"SQL and YAML, so this is likely a data directory", root, maxDbtProjectBytes)
			}
			rel := strings.TrimPrefix(e.Path, root+"/")
			out[rel] = data
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("project_directory %q holds no files", root)
	}
	if _, ok := out["dbt_project.yml"]; !ok {
		// Named here rather than left to dbt, whose own error for this is
		// about a directory it cannot find and does not say the emulator
		// handed it the wrong one.
		return nil, fmt.Errorf("project_directory %q has no dbt_project.yml at its root", root)
	}
	return out, nil
}

// defaultDbtProfile is the profile name used when a project declares none.
// dbt itself requires `profile:`, so this is the name for a project dbt would
// have rejected anyway -- it keeps the generator total rather than making an
// absent key a second error path.
const defaultDbtProfile = "databricks_emulator"

// projectProfileName returns the profile name a dbt project asks for.
//
// dbt resolves `profile:` from dbt_project.yml against profiles.yml, so a
// generated profile filed under any OTHER key sends dbt looking for one that
// was never written. This generated `databricks_emulator` unconditionally,
// which meant a real project could only run here by RENAMING ITS OWN PROFILE
// -- an edit that makes the project emulator-only, on the one surface whose
// entire purpose is rehearsing what will later run against Databricks. The
// project that found this is contoso-data-product's gold, which names its
// profile `contoso_gold` because that is what it is called everywhere else.
//
// Scanned, not parsed. This is one top-level scalar, and taking a YAML
// dependency to read it would be a larger change than the defect. Only a key
// at column zero counts, so a `profile:` nested inside some other block is
// never mistaken for the project's own.
func projectProfileName(b []byte) string {
	for _, line := range strings.Split(string(b), "\n") {
		rest, ok := strings.CutPrefix(line, "profile:")
		if !ok {
			continue
		}
		if i := strings.Index(rest, "#"); i >= 0 {
			rest = rest[:i]
		}
		if v := strings.Trim(strings.TrimSpace(rest), "\"'"); v != "" {
			return v
		}
	}
	return defaultDbtProfile
}

// dbtCode is the Python the agent runs: materialise the project, write a
// profile pointing back at this emulator's warehouse, then invoke dbt.
//
// dbt's own CLI is used through `dbtRunner`, not a shell. The agent executes
// Python statements and is not a shell, so shelling out would be inventing a
// capability it does not have; `dbtRunner().invoke` is dbt's supported
// programmatic entry point and takes the same argv the CLI does.
func dbtCode(d *store.Dbt, files map[string][]byte, host, token string) string {
	type file struct {
		Path string `json:"path"`
		B64  string `json:"b64"`
	}
	var payload []file
	for p, b := range files {
		payload = append(payload, file{Path: p, B64: base64.StdEncoding.EncodeToString(b)})
	}
	// Sorted so the same project produces the same code: an agent that caches
	// on the statement text, or a human diffing two runs, should not see churn
	// from Go's map order.
	sort.Slice(payload, func(i, j int) bool { return payload[i].Path < payload[j].Path })
	filesJSON, _ := json.Marshal(payload)
	argvJSON, _ := json.Marshal(dbtArgv(d.Commands))
	profile, _ := json.Marshal(map[string]any{
		projectProfileName(files["dbt_project.yml"]): map[string]any{
			"target": "emulator",
			"outputs": map[string]any{
				"emulator": map[string]any{
					"type":            "databricks",
					"host":            host,
					"http_path":       hs2.WarehousePath(d.WarehouseID),
					"token":           token,
					"catalog":         d.Catalog,
					"schema":          d.Schema,
					"threads":         1,
					"connect_retries": 0,
					// Without these the connector builds its own URL from
					// `host`, forcing https and :443, and hangs there rather
					// than failing -- the run sits in RUNNING with dbt stopped
					// at "Opening a new connection, currently in state init".
					// _connection_uri names the exact endpoint instead, which
					// is what lets a plain-HTTP emulator be reached at all.
					// Cloud Fetch is off because this warehouse refuses it;
					// leaving it on makes the connector negotiate a path that
					// is documented as unsupported here.
					"connection_parameters": map[string]any{
						"_connection_uri":  strings.TrimRight(host, "/") + hs2.WarehousePath(d.WarehouseID),
						"use_cloud_fetch":  false,
						"enable_telemetry": false,
					},
				},
			},
		},
	})
	return fmt.Sprintf(`
import base64, json, os, pathlib, sys, tempfile
_files = json.loads(%q)
_argv = json.loads(%q)
_profile = json.loads(%q)
_root = pathlib.Path(tempfile.mkdtemp(prefix="dbt-task-"))
for _f in _files:
    _p = _root / _f["path"]
    _p.parent.mkdir(parents=True, exist_ok=True)
    _p.write_bytes(base64.b64decode(_f["b64"]))
_prof = _root / "profiles"
_prof.mkdir(parents=True, exist_ok=True)
try:
    import yaml
    (_prof / "profiles.yml").write_text(yaml.safe_dump(_profile))
except ImportError:
    # dbt ships PyYAML, so this only fires when dbt itself is absent, and the
    # import below reports that in the words the operator needs.
    (_prof / "profiles.yml").write_text(json.dumps(_profile))
os.environ["DBT_PROFILES_DIR"] = str(_prof)
os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
try:
    from dbt.cli.main import dbtRunner
except ImportError as exc:
    # RuntimeError, not SystemExit, for the same reason the dbt failure below
    # is a field: an agent that closes the connection on SystemExit turns this
    # explanation into 'Post /statements: EOF', which tells the operator
    # nothing about the missing package.
    raise RuntimeError(
        "dbt_task needs dbt-databricks on the statement agent and it is not "
        "installed there (%%s). The agent image carries it from "
        "emulator-spark-agent onward; an older agent cannot run dbt." %% exc
    )
_runner = dbtRunner()
_failure = None
for _cmd in _argv:
    _res = _runner.invoke(_cmd + ["--project-dir", str(_root), "--profiles-dir", str(_prof)])
    if not _res.success:
        _failure = "dbt %%s failed: %%s" %% (" ".join(_cmd), _res.exception)
        break
# THE FAILURE TRAVELS AS DATA, in the same envelope as the artefacts, and
# NOTHING here raises. A failing dbt test is exactly when a caller needs
# run_results.json -- it names WHICH test failed and by how many rows, where
# an exit code says only that something did.
#
# This used to print the artefacts and then exit with the failure,
# relying on the ordering to get them out first. That is not enough, because
# it assumes every agent turns an exception into a REPLY. One does not: the
# fabric statement agent closes the connection on SystemExit without
# responding at all, so the emulator saw 'Post /statements: EOF' and the
# artefacts -- printed, correct, already out of the process -- were lost with
# the response that would have carried them. The caller then learned that the
# TRANSPORT failed, on a run where dbt had simply reported failing tests.
#
# So the failure is a field, not a control-flow event. An agent cannot drop
# what it does not have to interpret, and the evidence and the verdict can no
# longer be separated by anything between here and the caller. A genuine
# Python fault still raises and is still reported the ordinary way.
#
# run_results.json alone. manifest.json is large, changes on every parse, and
# no caller has asked for it -- shipping it through stdout would cost every
# run for a file nobody reads.
_arts = {}
_rr = _root / "target" / "run_results.json"
if _rr.exists():
    _arts["run_results.json"] = _rr.read_text(encoding="utf-8")
print("%s" + json.dumps({"artifacts": _arts, "failure": _failure}) + "%s")
if not _failure:
    print("dbt ok:", " | ".join(" ".join(c) for c in _argv))
`, string(filesJSON), string(argvJSON), string(profile), dbtArtifactsOpen, dbtArtifactsClose)
}

// dbtArgv turns Databricks' command strings into dbt argv.
//
// Databricks documents these as whole commands, "dbt run", "dbt test --select
// x". The leading "dbt" is the CLI's own name and is dropped: dbtRunner takes
// the arguments AFTER it, and passing it through makes dbt look for a command
// called "dbt".
func dbtArgv(commands []string) [][]string {
	var out [][]string
	for _, c := range commands {
		fields := strings.Fields(c)
		if len(fields) > 0 && fields[0] == "dbt" {
			fields = fields[1:]
		}
		if len(fields) == 0 {
			continue
		}
		out = append(out, fields)
	}
	return out
}

// runDbtTask materialises the project, generates the statement, and maps the
// agent's answer the way every other task kind does.
func (s *Server) runDbtTask(t store.Task, env, conf map[string]string) store.TaskRun {
	tr := store.TaskRun{Key: t.Key, LifeCycle: "TERMINATED"}
	files, err := s.dbtProjectFiles(t.Dbt.ProjectDirectory)
	if err != nil {
		tr.ResultState = "FAILED"
		tr.Stderr = err.Error()
		return tr
	}
	// The warehouse must exist before the run, not be discovered as a
	// connection refusal inside dbt: dbt's own error for a bad http_path is a
	// transport failure that names neither the warehouse nor this task.
	if _, ok := s.Store.SQL.GetWarehouse(t.Dbt.WarehouseID); !ok {
		tr.ResultState = "FAILED"
		tr.Stderr = fmt.Sprintf("dbt_task.warehouse_id %q does not exist", t.Dbt.WarehouseID)
		return tr
	}
	res, err := s.Spark.Run(spark.Request{
		Session: "job-" + t.Key,
		Code:    dbtCode(t.Dbt, files, s.AgentOrigin, s.Store.AdminPAT),
		Kind:    "python",
		Env:     env,
		Conf:    conf,
	})
	if err != nil {
		tr.ResultState = "FAILED"
		tr.Stderr = err.Error()
		return tr
	}
	cleaned, arts, failure := splitDbtArtifacts(res.Stdout)
	tr.Stdout = cleaned
	tr.DbtArtifacts = arts
	tr.Stderr = res.EValue
	// dbt reported the failure itself, so the artefacts came back WITH it and
	// the run fails carrying its own evidence.
	if failure != "" {
		tr.ResultState = "FAILED"
		tr.Stderr = failure
		return tr
	}
	if !res.OK {
		tr.ResultState = "FAILED"
		if tr.Stderr == "" {
			tr.Stderr = res.EName
		}
		return tr
	}
	tr.ResultState = "SUCCESS"
	return tr
}
