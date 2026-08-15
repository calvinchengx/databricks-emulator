package server

import (
	"encoding/json"
	"net/http"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error_code": code,
		"message":    message,
	})
}

func write401(w http.ResponseWriter, authorization string, message string) {
	w.Header().Set("WWW-Authenticate", `Bearer authorization="`+authorization+`"`)
	writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", message)
}
