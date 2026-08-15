package server

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
)

const sparkConnectPrefix = "/spark.connect."

func isSparkConnectRequest(r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, sparkConnectPrefix) {
		return true
	}
	ct := strings.ToLower(r.Header.Get("Content-Type"))
	return strings.Contains(ct, "application/grpc")
}

func (s *Server) sparkConnect(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	clusterID := r.Header.Get("x-databricks-cluster-id")
	if clusterID == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"x-databricks-cluster-id is required")
		return
	}
	cl, ok := s.Store.Clusters.Get(clusterID)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "cluster not found")
		return
	}
	if cl.State != "RUNNING" {
		writeError(w, http.StatusBadRequest, "INVALID_STATE",
			"cluster is not RUNNING — Databricks Connect needs a started session handle")
		return
	}
	backend := strings.TrimRight(s.Cfg.SparkConnectGRPCURL, "/")
	if backend == "" {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			"Spark Connect gRPC is reverse-proxied to DATABRICKS_SPARK_CONNECT_GRPC_URL; no gRPC engine URL is configured")
		return
	}
	u, err := url.Parse(backend)
	if err != nil {
		writeError(w, http.StatusBadGateway, "ABORTED", err.Error())
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.FlushInterval = -1
	proxy.Transport = connectTransport()
	proxy.ErrorHandler = func(rw http.ResponseWriter, _ *http.Request, err error) {
		writeError(rw, http.StatusBadGateway, "ABORTED", "spark connect: "+err.Error())
	}
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = u.Scheme
		req.URL.Host = u.Host
		req.Host = u.Host
		req.Header.Del("Authorization")
	}
	proxy.ServeHTTP(w, r)
}

// connectTransport speaks h2c to Sail. Go only uses unencrypted HTTP/2 when
// HTTP/1 is off; leaving both on silently stays HTTP/1 and Sail RSTs.
func connectTransport() http.RoundTripper {
	tr := &http.Transport{}
	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	tr.Protocols = p
	return tr
}
