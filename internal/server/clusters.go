package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/calvinchengx/databricks-emulator/internal/auth"
	"github.com/calvinchengx/databricks-emulator/internal/spark"
	"github.com/calvinchengx/databricks-emulator/internal/store"
)

var errNoSparkEngine = fmt.Errorf("no Spark engine is attached — set DATABRICKS_SPARK_CONNECT_URL")

const (
	emulatorSparkVersion = "emulator-spark"
	emulatorNodeType     = "emulator.session"
	clusterSessionMsg    = "session handle onto the emulator's Spark engine, not a VM"
)

func (s *Server) clustersCreate(w http.ResponseWriter, r *http.Request, p *auth.Principal) {
	var body struct {
		ClusterName  string `json:"cluster_name"`
		SparkVersion string `json:"spark_version"`
		NodeTypeID   string `json:"node_type_id"`
		NumWorkers   int    `json:"num_workers"`
		Autoscale    any    `json:"autoscale"`
		Libraries    any    `json:"libraries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if body.ClusterName == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cluster_name is required")
		return
	}
	if body.Autoscale != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"autoscale is refused: clusters are a session handle, not a VM pool")
		return
	}
	if body.Libraries != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"libraries installs packages on a cluster whose lifecycle the emulator does not own")
		return
	}
	if body.SparkVersion == "" {
		body.SparkVersion = emulatorSparkVersion
	}
	if body.NodeTypeID == "" {
		body.NodeTypeID = emulatorNodeType
	}
	cl := s.Store.Clusters.Create(body.ClusterName, body.SparkVersion, body.NodeTypeID, body.NumWorkers, p.UserName)
	if err := s.startClusterSession(cl); err != nil {
		s.Store.Clusters.Delete(cl.ID)
		writeError(w, http.StatusBadRequest, "INVALID_STATE", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cluster_id": cl.ID})
}

func (s *Server) clustersGet(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	id := query(r, "cluster_id")
	cl, ok := s.Store.Clusters.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "cluster not found")
		return
	}
	writeJSON(w, http.StatusOK, clusterJSON(cl))
}

func (s *Server) clustersList(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	list := []map[string]any{}
	for _, cl := range s.Store.Clusters.List() {
		list = append(list, clusterJSON(cl))
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusters": list})
}

func (s *Server) clustersStart(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		ClusterID string `json:"cluster_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	cl, ok := s.Store.Clusters.Get(body.ClusterID)
	if !ok {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "cluster not found")
		return
	}
	if err := s.startClusterSession(cl); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_STATE", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) clustersDelete(w http.ResponseWriter, r *http.Request, _ *auth.Principal) {
	var body struct {
		ClusterID string `json:"cluster_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	if !s.Store.Clusters.Delete(body.ClusterID) {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "cluster not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) clustersSparkVersions(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeJSON(w, http.StatusOK, map[string]any{
		"versions": []map[string]any{{
			"key":  emulatorSparkVersion,
			"name": "the emulator's Spark engine (not a DBR)",
		}},
	})
}

func (s *Server) clustersNodeTypes(w http.ResponseWriter, _ *http.Request, _ *auth.Principal) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_types": []map[string]any{{
			"node_type_id":     emulatorNodeType,
			"memory_mb":        0,
			"num_cores":        0,
			"description":      "session handle onto the attached Spark engine, not a VM",
			"instance_type_id": emulatorNodeType,
			"is_deprecated":    false,
		}},
	})
}

func (s *Server) startClusterSession(cl *store.Cluster) error {
	if s.Spark == nil {
		return errNoSparkEngine
	}
	res, err := s.Spark.Run(spark.Request{
		Session: "cluster-" + cl.ID,
		Kind:    "python",
		Code:    "print(1)\n",
	})
	if err != nil {
		s.Store.Clusters.SetState(cl.ID, "ERROR", err.Error())
		return err
	}
	if !res.OK {
		msg := res.EValue
		if msg == "" {
			msg = res.EName
		}
		if msg == "" {
			msg = "spark session failed"
		}
		s.Store.Clusters.SetState(cl.ID, "ERROR", msg)
		return fmt.Errorf("%s", msg)
	}
	s.Store.Clusters.SetState(cl.ID, "RUNNING", clusterSessionMsg)
	cl.State = "RUNNING"
	cl.StateMessage = clusterSessionMsg
	return nil
}

func clusterJSON(cl *store.Cluster) map[string]any {
	return map[string]any{
		"cluster_id":        cl.ID,
		"cluster_name":      cl.Name,
		"spark_version":     cl.SparkVersion,
		"node_type_id":      cl.NodeTypeID,
		"num_workers":       cl.NumWorkers,
		"state":             cl.State,
		"state_message":     cl.StateMessage,
		"creator_user_name": cl.Creator,
		"cluster_source":    "API",
		"executedBy":        "the emulator's Spark engine, not a Databricks cluster VM",
	}
}
