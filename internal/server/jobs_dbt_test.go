package server

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/clock"
	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/hs2"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

// profileHTTPPath pulls http_path out of the profile the generated statement
// carries. It decodes the embedded literal the way the agent will -- Go quoted
// string, then JSON -- rather than grepping the source, so a change in how the
// profile is embedded surfaces here instead of quietly matching nothing.
func profileHTTPPath(t *testing.T, code string) string {
	t.Helper()
	const marker = "_profile = json.loads("
	i := strings.Index(code, marker)
	if i < 0 {
		t.Fatal("generated code carries no profile")
	}
	rest := code[i+len(marker):]
	j := strings.Index(rest, ")\n")
	if j < 0 {
		t.Fatal("the profile literal is not terminated")
	}
	raw, err := strconv.Unquote(rest[:j])
	if err != nil {
		t.Fatalf("profile literal is not a Go quoted string: %v", err)
	}
	var prof map[string]any
	if err := json.Unmarshal([]byte(raw), &prof); err != nil {
		t.Fatalf("profile is not JSON: %v", err)
	}
	if len(prof) != 1 {
		t.Fatalf("a generated profile holds exactly one profile, got %d: %s", len(prof), raw)
	}
	node := any(prof)
	// The profile's NAME is the project's to choose, so it is discovered here
	// rather than asserted -- TestDbtProfileIsKeyedByTheProjectsOwnName is
	// where the name itself is checked.
	var name string
	for k := range prof {
		name = k
	}
	for _, key := range []string{name, "outputs", "emulator", "http_path"} {
		m, ok := node.(map[string]any)
		if !ok {
			t.Fatalf("profile is not the shape dbt reads: %q is not under a map: %s", key, raw)
		}
		node, ok = m[key]
		if !ok {
			t.Fatalf("profile has no %q: %s", key, raw)
		}
	}
	out, ok := node.(string)
	if !ok {
		t.Fatalf("profile http_path is not a string: %s", raw)
	}
	return out
}

// Databricks documents dbt_task.commands as whole command lines, "dbt run".
// dbtRunner takes the arguments AFTER the CLI's own name, so the leading "dbt"
// is dropped. Passing it through makes dbt look for a subcommand called "dbt"
// and fail on a project that is perfectly fine.
func TestDbtArgvDropsTheCLIName(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want [][]string
	}{
		{[]string{"dbt run"}, [][]string{{"run"}}},
		{[]string{"dbt test --select gold"}, [][]string{{"test", "--select", "gold"}}},
		// Databricks' UI writes them with the prefix; the API accepts either.
		{[]string{"run"}, [][]string{{"run"}}},
		{[]string{"dbt deps", "dbt run"}, [][]string{{"deps"}, {"run"}}},
		// Empty and whitespace-only entries carry no command and are dropped
		// rather than becoming an empty invocation dbt reports obscurely.
		{[]string{"dbt", "   ", ""}, nil},
	} {
		if got := dbtArgv(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("dbtArgv(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// The generated statement must carry the project, a profile aimed at THIS
// emulator's warehouse, and dbt's argv. Asserted on the code rather than on a
// live agent, because the agent is what this repo attaches rather than owns.
func TestDbtCodeCarriesProjectProfileAndArgv(t *testing.T) {
	d := &store.Dbt{
		Commands:         []string{"dbt run"},
		ProjectDirectory: "/gold",
		WarehouseID:      "wh-1",
		Catalog:          "main",
		Schema:           "gold",
	}
	files := map[string][]byte{
		"dbt_project.yml":   []byte("name: gold\n"),
		"models/fct.sql":    []byte("select 1"),
		"models/schema.yml": []byte("version: 2\n"),
	}
	code := dbtCode(d, files, "http://127.0.0.1:8447", "dapi-secret", nil)

	// The project travels inline: every file, base64'd, so no shared volume is
	// needed between this process and the agent container.
	for name := range files {
		if !strings.Contains(code, name) {
			t.Errorf("generated code does not carry %q", name)
		}
	}
	// The profile points at the emulator itself, on the named warehouse.
	for _, want := range []string{
		"http://127.0.0.1:8447",
		hs2.WarehousePath("wh-1"),
		"dapi-secret",
		`\"catalog\":\"main\"`,
		`\"schema\":\"gold\"`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("generated code is missing %s", want)
		}
	}
	if !strings.Contains(code, "dbtRunner") {
		t.Error("generated code does not invoke dbt")
	}
	// The assertion that was missing, and the reason this test passed for
	// months against a profile that could never connect: containing SOME path
	// is not the property. The property is that the emulator ROUTES the path
	// the profile names, so the path is parsed back with the same function the
	// Thrift handler uses to resolve an incoming request.
	httpPath := profileHTTPPath(t, code)
	id, ok := hs2.WarehouseID(httpPath)
	if !ok {
		t.Fatalf("the profile names http_path %q, which no route serves: "+
			"hs2.WarehouseID rejects it, so dbt would fail as a transport error", httpPath)
	}
	if id != "wh-1" {
		t.Fatalf("http_path %q resolves to warehouse %q, not the one the task named", httpPath, id)
	}
	// A missing dbt on the agent has to say so. Without this the operator sees
	// a bare ImportError and no indication that the agent image is the thing
	// to change.
	if !strings.Contains(code, "dbt-databricks on the statement agent") {
		t.Error("generated code does not name the agent when dbt is absent")
	}
}

// The same project must produce the same statement. Go map order is random, so
// building the payload straight from the map makes two identical runs differ.
func TestDbtCodeIsStableAcrossRuns(t *testing.T) {
	d := &store.Dbt{Commands: []string{"dbt run"}, ProjectDirectory: "/g", WarehouseID: "w"}
	files := map[string][]byte{}
	for _, n := range []string{"dbt_project.yml", "a.sql", "b.sql", "c.sql", "d.sql", "e.sql"} {
		files[n] = []byte(n)
	}
	first := dbtCode(d, files, "http://h", "t", nil)
	for i := 0; i < 20; i++ {
		if got := dbtCode(d, files, "http://h", "t", nil); got != first {
			t.Fatal("the same project produced two different statements; the file " +
				"payload is being built in map order")
		}
	}
}

// A dbt_task runs against the warehouse it names. An unknown warehouse fails
// here, naming it, rather than inside dbt as a transport error that mentions
// neither the warehouse nor the task.
func TestDbtTaskFailsNamingAnUnknownWarehouse(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	seedDbtProject(t, h)

	var created map[string]any
	if code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "gold",
		"tasks": []map[string]any{{"task_key": "g", "dbt_task": map[string]any{
			"commands": []any{"dbt run"}, "project_directory": "/gold",
			"warehouse_id": "nope",
		}}},
	}, &created); code != 200 {
		t.Fatalf("create = %d", code)
	}
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	got := h.waitRun(int64(run["run_id"].(float64)))

	if st := got["state"].(map[string]any)["result_state"]; st != "FAILED" {
		t.Fatalf("result_state = %v, want FAILED", st)
	}
	var out map[string]any
	h.json("GET", "/api/2.2/jobs/runs/get-output?run_id="+itoa(int64(run["run_id"].(float64))), pat, nil, &out)
	if !strings.Contains(strings.ToLower(str(out["error"])), "nope") {
		t.Errorf("the failure does not name the warehouse: %v", out["error"])
	}
}

// The whole path, through the REST the SDK drives: create a dbt_task, run it,
// and assert the statement the AGENT received is a dbt invocation over this
// emulator's own warehouse.
func TestDbtTaskReachesTheAgentAsADbtInvocation(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	seedDbtProject(t, h)
	wh := h.srv.Store.SQL.CreateWarehouse("gold-wh", "SMALL")

	var seen spark.Request
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		seen = req
		return spark.Result{OK: true, Stdout: "dbt ok: run"}, nil
	}

	var created map[string]any
	if code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "gold",
		"tasks": []map[string]any{{"task_key": "g", "dbt_task": map[string]any{
			"commands": []any{"dbt deps", "dbt run"}, "project_directory": "/gold",
			"warehouse_id": wh.ID, "catalog": "main", "schema": "gold",
		}}},
	}, &created); code != 200 {
		t.Fatalf("create = %d", code)
	}
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	got := h.waitRun(int64(run["run_id"].(float64)))

	if st := got["state"].(map[string]any)["result_state"]; st != "SUCCESS" {
		t.Fatalf("result_state = %v, want SUCCESS", st)
	}
	// It goes as python, not a shell: the agent executes statements and is not
	// a shell, so a shell-out would be inventing a capability it lacks.
	if seen.Kind != "python" {
		t.Errorf("agent Kind = %q, want python", seen.Kind)
	}
	if !strings.Contains(seen.Code, "dbtRunner") {
		t.Error("the agent did not receive a dbt invocation")
	}
	// Routable, not merely present: the path the agent was handed is resolved
	// with the same function the Thrift handler uses on the way back in.
	if id, ok := hs2.WarehouseID(profileHTTPPath(t, seen.Code)); !ok || id != wh.ID {
		t.Errorf("the profile's http_path resolves to %q (ok=%v), not the warehouse %q "+
			"the task named; dbt would fail as a transport error", id, ok, wh.ID)
	}
	// Both commands, in order.
	var argv [][]string
	for _, line := range strings.Split(seen.Code, "\n") {
		if strings.HasPrefix(line, "_argv = json.loads(") {
			raw := line[len("_argv = json.loads(") : len(line)-1]
			var unq string
			if err := json.Unmarshal([]byte(raw), &unq); err == nil {
				_ = json.Unmarshal([]byte(unq), &argv)
			}
		}
	}
	if !reflect.DeepEqual(argv, [][]string{{"deps"}, {"run"}}) {
		t.Errorf("argv = %v, want [[deps] [run]]", argv)
	}
}

// A project directory that is not a dbt project is refused before anything is
// sent to the agent, naming what is missing.
func TestDbtTaskRefusesADirectoryWithNoProjectFile(t *testing.T) {
	h := newHarness(t)
	_ = h.srv.Store.Workspace.Mkdir("/notgold")
	_ = h.srv.Store.Workspace.Put("/notgold/x.sql", []byte("select 1"), "FILE", "SQL")

	_, err := h.srv.dbtProjectFiles("/notgold")
	if err == nil {
		t.Fatal("a directory with no dbt_project.yml was accepted")
	}
	if !strings.Contains(err.Error(), "dbt_project.yml") {
		t.Errorf("the refusal does not name dbt_project.yml: %v", err)
	}
}

func seedDbtProject(t *testing.T, h *harness) {
	t.Helper()
	_ = h.srv.Store.Workspace.Mkdir("/gold")
	_ = h.srv.Store.Workspace.Mkdir("/gold/models")
	_ = h.srv.Store.Workspace.Put("/gold/dbt_project.yml", []byte("name: gold\nversion: '1'\n"), "FILE", "PYTHON")
	_ = h.srv.Store.Workspace.Put("/gold/models/fct.sql", []byte("select 1 as n"), "FILE", "SQL")
}

// Every way a dbt_task can be malformed is refused at CREATE, each naming the
// field. A task accepted here and failed at run time costs a whole dispatch to
// learn something the request already showed.
func TestDbtTaskValidationRefusesByField(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	for _, tc := range []struct {
		name string
		dbt  map[string]any
		want string
	}{
		{"no commands", map[string]any{
			"project_directory": "/gold", "warehouse_id": "w"}, "commands"},
		{"no project", map[string]any{
			"commands": []any{"dbt run"}, "warehouse_id": "w"}, "project_directory"},
		{"no warehouse", map[string]any{
			"commands": []any{"dbt run"}, "project_directory": "/gold"}, "warehouse_id"},
		{"git source", map[string]any{
			"commands": []any{"dbt run"}, "project_directory": "/gold",
			"warehouse_id": "w", "source": "GIT"}, "source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out map[string]any
			code := h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
				"name":  "x",
				"tasks": []map[string]any{{"task_key": "g", "dbt_task": tc.dbt}},
			}, &out)
			if code == 200 {
				t.Fatalf("%s was accepted", tc.name)
			}
			if !strings.Contains(fmt.Sprint(out), tc.want) {
				t.Errorf("the refusal does not name %s: %v", tc.want, out)
			}
		})
	}
}

// A project directory that does not exist fails naming it, rather than
// reaching the agent with an empty project.
func TestDbtProjectDirectoryMustExist(t *testing.T) {
	h := newHarness(t)
	if _, err := h.srv.dbtProjectFiles("/nope"); err == nil {
		t.Fatal("a missing project_directory was accepted")
	}
}

// Nested model directories are walked, not just the project root: a dbt
// project keeps its models one level down by convention, so a non-recursive
// read would ship a project with no models and dbt would build nothing.
func TestDbtProjectWalksNestedDirectories(t *testing.T) {
	h := newHarness(t)
	seedDbtProject(t, h)
	_ = h.srv.Store.Workspace.Mkdir("/gold/models/marts")
	_ = h.srv.Store.Workspace.Put("/gold/models/marts/deep.sql", []byte("select 2"), "FILE", "SQL")

	files, err := h.srv.dbtProjectFiles("/gold")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := files["models/marts/deep.sql"]; !ok {
		t.Fatalf("a nested model was not collected: %v", keysOf(files))
	}
	// Paths are relative to the project root, since that is what dbt is handed.
	for p := range files {
		if strings.HasPrefix(p, "/") {
			t.Errorf("path %q is absolute; dbt is given a project root", p)
		}
	}
}

// The agent refusing the statement is reported as a failed task carrying what
// the agent said, not as a success with empty output.
func TestDbtTaskReportsAnAgentFailure(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	seedDbtProject(t, h)
	wh := h.srv.Store.SQL.CreateWarehouse("w", "SMALL")
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{OK: false, EName: "SystemExit", EValue: "dbt run failed: boom"}, nil
	}
	var created map[string]any
	h.json("POST", "/api/2.2/jobs/create", pat, map[string]any{
		"name": "gold",
		"tasks": []map[string]any{{"task_key": "g", "dbt_task": map[string]any{
			"commands": []any{"dbt run"}, "project_directory": "/gold", "warehouse_id": wh.ID,
		}}},
	}, &created)
	var run map[string]any
	h.json("POST", "/api/2.2/jobs/run-now", pat, map[string]any{"job_id": created["job_id"]}, &run)
	got := h.waitRun(int64(run["run_id"].(float64)))
	if st := got["state"].(map[string]any)["result_state"]; st != "FAILED" {
		t.Fatalf("result_state = %v, want FAILED", st)
	}
	var out map[string]any
	h.json("GET", "/api/2.2/jobs/runs/get-output?run_id="+itoa(int64(run["run_id"].(float64))), pat, nil, &out)
	if !strings.Contains(str(out["error"]), "boom") {
		t.Errorf("the agent's message was lost: %v", out["error"])
	}
}

func keysOf(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// The size bound fires before anything is sent. A project_directory pointed at
// a data directory by mistake would otherwise be base64'd into one statement,
// and the agent's error would be about a body it could not read rather than
// about the path that was wrong.
func TestDbtProjectSizeIsBounded(t *testing.T) {
	h := newHarness(t)
	_ = h.srv.Store.Workspace.Mkdir("/big")
	_ = h.srv.Store.Workspace.Put("/big/dbt_project.yml", []byte("name: big\n"), "FILE", "PYTHON")
	chunk := make([]byte, 1<<20)
	for i := 0; i < 9; i++ {
		_ = h.srv.Store.Workspace.Put(fmt.Sprintf("/big/f%d.bin", i), chunk, "FILE", "PYTHON")
	}
	_, err := h.srv.dbtProjectFiles("/big")
	if err == nil {
		t.Fatal("an oversized project_directory was accepted")
	}
	if !strings.Contains(err.Error(), "data directory") {
		t.Errorf("the refusal does not explain the likely cause: %v", err)
	}
}

// A transport failure reaching the agent is a failed task carrying the
// transport's own words, not a success.
func TestDbtTaskReportsATransportFailure(t *testing.T) {
	h := newHarness(t)
	seedDbtProject(t, h)
	wh := h.srv.Store.SQL.CreateWarehouse("w", "SMALL")
	h.exec.Hook = func(req spark.Request) (spark.Result, error) {
		return spark.Result{}, fmt.Errorf("spark agent: connection refused")
	}
	tr := h.srv.runDbtTask(store.Task{Key: "g", Dbt: &store.Dbt{
		Commands: []string{"dbt run"}, ProjectDirectory: "/gold", WarehouseID: wh.ID,
	}}, nil)
	if tr.ResultState != "FAILED" {
		t.Fatalf("ResultState = %q, want FAILED", tr.ResultState)
	}
	if !strings.Contains(tr.Stderr, "connection refused") {
		t.Errorf("the transport error was lost: %q", tr.Stderr)
	}
}

// The artefact is worth most on the run that FAILED, so the payload must not
// be attached to success only.
func TestDbtArtifactsSurviveTheMarkersAndTheFailure(t *testing.T) {
	rr := `{"args":{"which":"test"},"results":[{"unique_id":"test.p.c.1","status":"fail"}]}`
	stdout := "12:00 Running with dbt=1.9\n" +
		dbtArtifactsOpen + `{"artifacts":{"run_results.json":` + strconvQuote(rr) + `},"failure":"dbt test failed"}` + dbtArtifactsClose +
		"\nsomething after\n"

	cleaned, arts, failure := splitDbtArtifacts(stdout)
	if failure != "dbt test failed" {
		t.Fatalf("the verdict travelled separately from its evidence: %q", failure)
	}
	if got := arts["run_results.json"]; got != rr {
		t.Fatalf("run_results.json round-tripped wrong:\n got %q\nwant %q", got, rr)
	}
	// The log a caller reads must not carry the payload: it is megabytes of
	// JSON in a field meant for what dbt printed.
	if strings.Contains(cleaned, dbtArtifactsOpen) || strings.Contains(cleaned, "unique_id") {
		t.Fatalf("payload leaked into logs: %q", cleaned)
	}
	for _, want := range []string{"Running with dbt=1.9", "something after"} {
		if !strings.Contains(cleaned, want) {
			t.Fatalf("real output lost from logs: %q", cleaned)
		}
	}
}

// Losing an artefact must not also lose the log that would explain why.
func TestDbtArtifactsMalformedPayloadKeepsTheLog(t *testing.T) {
	for _, tc := range []struct {
		name, stdout string
	}{
		{"no markers", "plain dbt output"},
		{"unterminated", "before " + dbtArtifactsOpen + `{"a":"b"}`},
		{"not json", "before " + dbtArtifactsOpen + `{oops` + dbtArtifactsClose + " after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cleaned, arts, _ := splitDbtArtifacts(tc.stdout)
			if arts != nil {
				t.Fatalf("artefacts from a broken payload: %#v", arts)
			}
			if cleaned != tc.stdout {
				t.Fatalf("stdout altered when the payload could not be read:\n got %q\nwant %q",
					cleaned, tc.stdout)
			}
		})
	}
}

// A dbt failure must leave the agent as DATA, never as an exception.
//
// The artefacts used to be printed and then followed by
// `raise SystemExit(_failure)`, on the assumption that every agent turns an
// exception into a reply. The fabric statement agent closes the connection
// instead, so the emulator got `Post /statements: EOF` and the artefacts --
// already printed -- died with the response. Ordering cannot fix that; not
// raising can.
func TestAFailingDbtRunReportsThroughTheEnvelopeNotAnException(t *testing.T) {
	code := dbtCode(&store.Dbt{Commands: []string{"dbt test"}}, map[string][]byte{
		"dbt_project.yml": []byte("name: p\n"),
	}, "http://host", "pat", nil)
	if strings.Contains(code, "raise SystemExit(_failure)") {
		t.Fatal("the dbt failure is still raised; an agent that does not answer " +
			"exceptions loses the artefacts and reports a transport error instead")
	}
	if !strings.Contains(code, `json.dumps({"artifacts": _arts, "failure": _failure})`) {
		t.Fatal("the envelope no longer carries the failure beside the artefacts")
	}
	// The success line must stay conditional, so "dbt ok" cannot be printed by
	// a run that failed.
	if !strings.Contains(code, "if not _failure:") {
		t.Fatal("the success line is unconditional, so a failing run still prints dbt ok")
	}
	if !strings.Contains(code, "run_results.json") {
		t.Fatal("generated code does not read run_results.json")
	}
}

// strconvQuote is strconv.Quote under a local name, so the test reads as
// "this JSON, embedded as a JSON string" rather than as an import detail.
func strconvQuote(s string) string { return strconv.Quote(s) }

// A dbt project names the profile it wants in dbt_project.yml, and dbt looks
// that name up in profiles.yml. dbt_task GENERATES the profile, so if it files
// it under a name of its own choosing, the only projects that can run here are
// the ones edited to say that name -- which is an edit that makes a project
// emulator-only. The name travels from the project.
func TestDbtProfileIsKeyedByTheProjectsOwnName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		project string
		want    string
	}{
		{"plain", "name: p\nprofile: contoso_gold\n", "contoso_gold"},
		{"quoted", "profile: \"contoso_gold\"\n", "contoso_gold"},
		{"single quoted", "profile: 'contoso_gold'\n", "contoso_gold"},
		{"trailing comment", "profile: contoso_gold # what it is called\n", "contoso_gold"},
		{"crlf", "profile: contoso_gold\r\n", "contoso_gold"},
		// dbt requires `profile:`, so a project without one is already broken.
		// Falling back keeps the generator total instead of adding an error
		// path for a project dbt would reject on its own.
		{"absent", "name: p\n", defaultDbtProfile},
		// Only column zero is the project's own key. An indented `profile:`
		// belongs to whatever block encloses it.
		{"nested only", "name: p\nmodels:\n  profile: not_mine\n", defaultDbtProfile},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := map[string][]byte{
				"dbt_project.yml": []byte(tc.project),
				"models/m.sql":    []byte("select 1"),
			}
			code := dbtCode(&store.Dbt{Commands: []string{"dbt run"}, WarehouseID: "w"}, files, "http://h", "t", nil)
			// Read the profile the way the agent will, and assert on the name
			// it is filed under -- not on the source text, which would match a
			// name that never reaches dbt.
			const marker = "_profile = json.loads("
			i := strings.Index(code, marker)
			if i < 0 {
				t.Fatal("generated code carries no profile")
			}
			rest := code[i+len(marker):]
			raw, err := strconv.Unquote(rest[:strings.Index(rest, ")\n")])
			if err != nil {
				t.Fatalf("profile literal is not a Go quoted string: %v", err)
			}
			var prof map[string]any
			if err := json.Unmarshal([]byte(raw), &prof); err != nil {
				t.Fatalf("profile is not JSON: %v", err)
			}
			if _, ok := prof[tc.want]; !ok {
				t.Fatalf("dbt will look up profile %q and it is not there; the profile is %s", tc.want, raw)
			}
			if len(prof) != 1 {
				t.Fatalf("profile carries names dbt did not ask for: %s", raw)
			}
		})
	}
}

// The profile handed to the agent must name the emulator AS THE AGENT SEES IT.
//
// The agent is another container. `-public-url` is what CLIENTS are told, and
// in a compose deployment that is a host-published address like
// http://127.0.0.1:18470 -- which, resolved inside the agent, is the agent
// itself. dbt then cannot reach the warehouse at all, and because the failure
// used to leave as a SystemExit the caller saw a transport EOF rather than a
// connection error naming the host.
//
// This is the same mistake dbtProjectFiles already avoids for the project
// files, one field over: a path meaningful in this process is meaningless in
// the agent.
func TestDbtProfilePointsAtTheAgentFacingOrigin(t *testing.T) {
	exec := &spark.Scripted{}
	cfg := &config.Config{
		Addr:       ":0",
		DataDir:    t.TempDir(),
		DisableTLS: true,
		PublicURL:  "http://127.0.0.1:18470", // what the client on the host uses
		AgentURL:   "http://databricks:8447", // what the agent must use
	}
	s, err := New(cfg, clock.New(), exec)
	if err != nil {
		t.Fatal(err)
	}
	if s.Origin != "http://127.0.0.1:18470" {
		t.Fatalf("client-facing Origin = %q", s.Origin)
	}
	if s.AgentOrigin != "http://databricks:8447" {
		t.Fatalf("AgentOrigin = %q, want the agent-facing URL", s.AgentOrigin)
	}
	// Through runDbtTask, not by handing dbtCode the value directly: passing
	// s.AgentOrigin into dbtCode myself would assert that dbtCode uses its
	// argument, which was never in doubt. The defect was at the CALL SITE.
	_ = s.Store.Workspace.Mkdir("/gold")
	_ = s.Store.Workspace.Put("/gold/dbt_project.yml", []byte("profile: p\n"), "FILE", "PYTHON")
	wh := s.Store.SQL.CreateWarehouse("w", "SMALL")
	var code string
	exec.Hook = func(req spark.Request) (spark.Result, error) {
		code = req.Code
		return spark.Result{OK: true, Stdout: "dbt ok"}, nil
	}
	s.runDbtTask(store.Task{Key: "g", Dbt: &store.Dbt{
		Commands: []string{"dbt run"}, ProjectDirectory: "/gold", WarehouseID: wh.ID,
	}}, nil)
	if code == "" {
		t.Fatal("the task never reached the agent, so this asserts nothing")
	}
	if strings.Contains(code, "127.0.0.1:18470") {
		t.Fatal("the profile carries the CLIENT's address; inside the agent that " +
			"resolves to the agent itself and dbt never reaches the warehouse")
	}
	if !strings.Contains(code, "http://databricks:8447") {
		t.Fatalf("the profile does not name the agent-facing origin:\n%s", code[:400])
	}
}

// Where one name reaches this process from everywhere, the two ARE the same,
// and every deployment that never heard of -agent-url must keep working.
func TestAgentOriginDefaultsToTheAdvertisedOrigin(t *testing.T) {
	s, err := New(&config.Config{
		Addr: ":0", DataDir: t.TempDir(), DisableTLS: true,
		PublicURL: "http://host.docker.internal:8447",
	}, clock.New(), &spark.Scripted{})
	if err != nil {
		t.Fatal(err)
	}
	if s.AgentOrigin != s.Origin {
		t.Fatalf("AgentOrigin = %q, want it to fall back to Origin %q", s.AgentOrigin, s.Origin)
	}
}

// A task's spark_env_vars must reach the DBT PROCESS, not merely the request.
//
// The agent accepts an `env` field and, on the fabric image, never applies it:
// a statement asking os.environ back answers None. Relying on it made
// spark_env_vars look supported while doing nothing, and dbt failed with "Env
// var required but not provided" for a variable the task had plainly set. A
// project reads its sources through env_var(), so this decides whether a task
// can name the data it reads.
//
// Asserted on the GENERATED CODE, because that is what survives an agent that
// ignores the field.
func TestSparkEnvVarsReachTheDbtProcess(t *testing.T) {
	exec := &spark.Scripted{}
	s, err := New(&config.Config{
		Addr: ":0", DataDir: t.TempDir(), DisableTLS: true, PublicURL: "http://dbx.test",
	}, clock.New(), exec)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Store.Workspace.Mkdir("/gold")
	_ = s.Store.Workspace.Put("/gold/dbt_project.yml", []byte("profile: p\n"), "FILE", "PYTHON")
	wh := s.Store.SQL.CreateWarehouse("w", "SMALL")

	var code string
	exec.Hook = func(req spark.Request) (spark.Result, error) {
		code = req.Code
		return spark.Result{OK: true, Stdout: "dbt ok"}, nil
	}
	s.runDbtTask(store.Task{
		Key:          "g",
		SparkEnvVars: map[string]string{"LAKEHOUSE_ID": "contoso"},
		Dbt: &store.Dbt{
			Commands: []string{"dbt run"}, ProjectDirectory: "/gold", WarehouseID: wh.ID,
		},
	}, map[string]string{"LAKEHOUSE_ID": "contoso"})

	if !strings.Contains(code, "os.environ.update(") {
		t.Fatal("the generated code never sets the environment, so it depends on " +
			"an agent field that one agent silently drops")
	}
	// Decoded, not grepped: both strings also appear in the profile and the
	// project payload, so a substring match here would pass on a statement
	// that set no environment at all.
	if got := deliveredEnv(t, code)["LAKEHOUSE_ID"]; got != "contoso" {
		t.Fatalf("the task's env did not travel into the code: LAKEHOUSE_ID = %q", got)
	}
	// Before dbt is imported, or the parse that reads env_var() has already run.
	if strings.Index(code, "os.environ.update(") > strings.Index(code, "from dbt.cli.main") {
		t.Fatal("the environment is set after dbt is imported, which is after " +
			"the project parse that reads env_var()")
	}
}
