package spark

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewAgentEmptyIsNil(t *testing.T) {
	if NewAgent("") != nil {
		t.Fatal("empty URL should be nil")
	}
}

func TestAgentRunMapsStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/statements" {
			t.Fatalf("path %s", r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["code"] != "print(1)" {
			t.Fatalf("code = %v", body["code"])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok",
			"data":   map[string]string{"text/plain": "1"},
		})
	}))
	defer srv.Close()
	a := NewAgent(srv.URL)
	res, err := a.Run(Request{Session: "s", Code: "print(1)", Kind: "python"})
	if err != nil || !res.OK || res.Stdout != "1" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestAgentRunErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()
	if _, err := NewAgent(srv.URL).Run(Request{Code: "x"}); err == nil {
		t.Fatal("expected error")
	}
	var nilAgent *Agent
	if _, err := nilAgent.Run(Request{}); err == nil {
		t.Fatal("nil agent should fail")
	}
}

func TestScriptedRecords(t *testing.T) {
	s := &Scripted{}
	res, err := s.Run(Request{Code: "print(hi)"})
	if err != nil || !res.OK || len(s.Calls) != 1 {
		t.Fatalf("scripted: %+v %v", res, err)
	}
}

// The posted body carries exactly the keys the agent reads.
//
// It used to carry `env` and `spark_conf` as well. The agent's /statements
// handler reads `session`, `code`, `kind` and its identity fields and nothing
// else -- `sparkConfig` is read only by /environment -- so both were discarded
// silently. A task's environment reaches it through the code the emulator
// generates instead (internal/server, pythonPreamble), and sending a second,
// inert copy made the field look supported and gave a test somewhere else to
// pass against.
func TestPostedBodyCarriesNoInertFields(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"status":"ok","data":{"text/plain":"1"}}`))
	}))
	defer srv.Close()

	if _, err := NewAgent(srv.URL).Run(Request{Session: "s", Code: "print(1)", Kind: "python"}); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"session": true, "code": true, "kind": true}
	for k := range got {
		if !want[k] {
			t.Errorf("the body carries %q, which the agent does not read", k)
		}
	}
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("the body is missing %q", k)
		}
	}
}
