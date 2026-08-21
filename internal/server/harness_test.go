package server

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/calvinchengx/databricks-emulator/internal/clock"
	"github.com/calvinchengx/databricks-emulator/internal/config"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
)

type harness struct {
	t      *testing.T
	srv    *Server
	http   *httptest.Server
	exec   *spark.Scripted
	client *http.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	exec := &spark.Scripted{}
	cfg := &config.Config{
		Addr:       ":0",
		DataDir:    t.TempDir(),
		DisableTLS: true,
		PublicURL:  "http://dbx.test",
	}
	clk := clock.New()
	s, err := New(cfg, clk, exec)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(s.Handler())
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = p
	ts.Start()
	t.Cleanup(ts.Close)
	return &harness{t: t, srv: s, http: ts, exec: exec, client: ts.Client()}
}

// h2cURL serves h on prior-knowledge HTTP/2 only — Sail's Spark Connect shape.
func h2cURL(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: p}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}

func (h *harness) do(method, path, token string, body any) *http.Response {
	h.t.Helper()
	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case []byte:
			rdr = bytes.NewReader(b)
		case string:
			rdr = bytes.NewReader([]byte(b))
		default:
			raw, _ := json.Marshal(b)
			rdr = bytes.NewReader(raw)
		}
	}
	req, err := http.NewRequest(method, h.http.URL+path, rdr)
	if err != nil {
		h.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		if _, ok := body.([]byte); ok {
			req.Header.Set("Content-Type", "application/octet-stream")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

func (h *harness) multipart(path, token string, fields map[string]string, fileField string, fileBytes []byte) *http.Response {
	h.t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			h.t.Fatal(err)
		}
	}
	if fileField != "" {
		part, err := mw.CreateFormFile(fileField, "upload")
		if err != nil {
			h.t.Fatal(err)
		}
		if _, err := part.Write(fileBytes); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := mw.Close(); err != nil {
		h.t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, h.http.URL+path, &buf)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func (h *harness) json(method, path, token string, body any, dest any) int {
	h.t.Helper()
	resp := h.do(method, path, token, body)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if dest != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, dest); err != nil {
			h.t.Fatalf("decode %s: %v body=%s", path, err, raw)
		}
	}
	return resp.StatusCode
}

func (h *harness) waitRun(runID int64) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var out map[string]any
		st := h.json("GET", "/api/2.2/jobs/runs/get?run_id="+itoa(runID), h.srv.Store.AdminPAT, nil, &out)
		if st != 200 {
			h.t.Fatalf("runs/get %d", st)
		}
		state, _ := out["state"].(map[string]any)
		if str(state["life_cycle_state"]) == "TERMINATED" {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatal("run did not terminate")
	return nil
}

// deliveredEnv decodes what a generated statement will actually put in
// os.environ, by reading the `os.environ.update(json.loads("..."))` the
// preamble emits and undoing both quotings.
//
// Tests used to assert on the request's `Env` field instead. That field was
// never read by the agent, so those assertions proved the emulator had
// RESOLVED a secret and said nothing about whether the task ever saw it -- a
// green suite was compatible with the value never arriving. The field is gone
// now (internal/spark), and this reads the only path that delivers, so a test
// cannot pass on the strength of the other one.
func deliveredEnv(t *testing.T, code string) map[string]string {
	t.Helper()
	const marker = "os.environ.update(json.loads("
	i := strings.Index(code, marker)
	if i < 0 {
		return nil
	}
	lit, err := strconv.QuotedPrefix(code[i+len(marker):])
	if err != nil {
		t.Fatalf("os.environ.update argument is not a quoted literal: %v", err)
	}
	raw, err := strconv.Unquote(lit)
	if err != nil {
		t.Fatalf("unquote os.environ.update argument: %v", err)
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("os.environ.update argument is not a JSON object: %v (%q)", err, raw)
	}
	return out
}
