package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestConnectTransportIsH2COnly(t *testing.T) {
	tr, ok := connectTransport().(*http.Transport)
	if !ok || tr.Protocols == nil {
		t.Fatal("expected *http.Transport with Protocols")
	}
	if tr.Protocols.HTTP1() || !tr.Protocols.UnencryptedHTTP2() {
		t.Fatal("Sail is h2c; HTTP/1 on the transport disables unencrypted HTTP/2")
	}
}

func TestSparkConnectProxiesAfterPATAndClusterID(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{"cluster_name": "dev"}, &created)
	id := str(created["cluster_id"])

	var sawPath, sawAuth, sawCluster string
	h.srv.Cfg.SparkConnectGRPCURL = h2cURL(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPath = r.URL.Path
		sawAuth = r.Header.Get("Authorization")
		sawCluster = r.Header.Get("x-databricks-cluster-id")
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("plan-ok"))
	}))
	h.srv.Cfg.SparkAgentURL = "http://127.0.0.1:9" // HTTP agent must not be the Connect backend

	req, err := http.NewRequest(http.MethodPost, h.http.URL+"/spark.connect.SparkConnectService/AnalyzePlan", strings.NewReader("plan"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("x-databricks-cluster-id", id)
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "plan-ok" {
		t.Fatalf("proxy %d %s", resp.StatusCode, body)
	}
	if sawPath != "/spark.connect.SparkConnectService/AnalyzePlan" {
		t.Fatalf("path %s", sawPath)
	}
	if sawAuth != "" {
		t.Fatalf("authorization leaked to the engine: %q", sawAuth)
	}
	if sawCluster != id {
		t.Fatalf("cluster header %s", sawCluster)
	}
}

func TestSparkConnectProxiesH2CClient(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{"cluster_name": "dev"}, &created)
	id := str(created["cluster_id"])

	var sawPath string
	h.srv.Cfg.SparkConnectGRPCURL = h2cURL(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPath = r.URL.Path
		w.Header().Set("Content-Type", "application/grpc")
		_, _ = w.Write([]byte("h2c-ok"))
	}))

	tr := &http.Transport{}
	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	tr.Protocols = p
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodPost, h.http.URL+"/spark.connect.SparkConnectService/ExecutePlan", strings.NewReader("plan"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("x-databricks-cluster-id", id)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.ProtoMajor != 2 {
		t.Fatalf("client proto %s", resp.Proto)
	}
	if resp.StatusCode != 200 || string(body) != "h2c-ok" {
		t.Fatalf("h2c proxy %d %s", resp.StatusCode, body)
	}
	if sawPath != "/spark.connect.SparkConnectService/ExecutePlan" {
		t.Fatalf("path %s", sawPath)
	}
}

func TestSparkConnectRefusesWithoutEngineURLOrCluster(t *testing.T) {
	h := newHarness(t)
	pat := h.srv.Store.AdminPAT
	var created map[string]any
	h.json("POST", "/api/2.0/clusters/create", pat, map[string]any{"cluster_name": "dev"}, &created)
	id := str(created["cluster_id"])

	post := func(path, token, cluster, ct string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, h.http.URL+path, strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if cluster != "" {
			req.Header.Set("x-databricks-cluster-id", cluster)
		}
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		resp, err := h.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}

	if st, _ := post("/spark.connect.SparkConnectService/ExecutePlan", "", id, "application/grpc"); st != 401 {
		t.Fatalf("no pat %d", st)
	}
	if st, body := post("/spark.connect.SparkConnectService/ExecutePlan", pat, "", "application/grpc"); st != 400 {
		t.Fatalf("no cluster %d %+v", st, body)
	}
	if st, _ := post("/spark.connect.SparkConnectService/ExecutePlan", pat, "missing", "application/grpc"); st != 404 {
		t.Fatalf("unknown cluster %d", st)
	}
	h.srv.Store.Clusters.SetState(id, "TERMINATED", "stopped")
	if st, body := post("/spark.connect.SparkConnectService/ExecutePlan", pat, id, "application/grpc"); st != 400 {
		t.Fatalf("stopped %d %+v", st, body)
	}
	h.srv.Store.Clusters.SetState(id, "RUNNING", clusterSessionMsg)
	h.srv.Cfg.SparkAgentURL = "http://127.0.0.1:8099"
	st, body := post("/spark.connect.SparkConnectService/ExecutePlan", pat, id, "application/grpc")
	if st != 501 {
		t.Fatalf("http agent is not a gRPC url %d %+v", st, body)
	}
	if !strings.Contains(str(body["message"]), "DATABRICKS_SPARK_CONNECT_GRPC_URL") {
		t.Fatalf("501 must name the gRPC var: %+v", body)
	}
	h.srv.Cfg.SparkConnectGRPCURL = "http://["
	if st, _ := post("/spark.connect.SparkConnectService/ExecutePlan", pat, id, "application/grpc"); st != 502 {
		t.Fatalf("bad url %d", st)
	}

	// gRPC content-type on a non-prefixed path still routes through Connect.
	h.srv.Cfg.SparkConnectGRPCURL = "http://127.0.0.1:1"
	st, body = post("/custom-grpc", pat, id, "application/grpc")
	if st != 502 {
		t.Fatalf("dead backend %d %+v", st, body)
	}
}
