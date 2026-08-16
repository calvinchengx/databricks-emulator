package hs2

import (
	"io"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
	cliservice "github.com/calvinchengx/databricks-emulator/internal/hs2/cliservice"
)

// ServeHTTP answers one TBinaryProtocol RPC on a warehouse path.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request, warehouseID string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	in := thrift.NewTMemoryBuffer()
	if _, err := in.Write(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	out := thrift.NewTMemoryBuffer()
	factory := thrift.NewTBinaryProtocolFactoryConf(nil)
	proc := cliservice.NewTCLIServiceProcessor(s)
	ctx := withWarehouse(r.Context(), warehouseID)
	if _, err := proc.Process(ctx, factory.GetProtocol(in), factory.GetProtocol(out)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-thrift")
	_, _ = w.Write(out.Bytes())
}
