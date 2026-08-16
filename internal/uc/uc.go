// Package uc is the outbound client to a Unity Catalog OSS sidecar.
// Databricks' /api/2.1/unity-catalog REST is the same wire UC OSS speaks,
// so this is a reverse proxy after our PAT/OIDC check — not a second catalog.
package uc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxBody = 8 << 20

// Client forwards Unity Catalog REST to one configured sidecar.
type Client struct {
	base string
	http *http.Client
}

// New builds a client. Empty base means no sidecar is attached. client
// carries the TLS trust for the sidecar — see internal/tlsclient.
func New(base string, client *http.Client) *Client {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{base: base, http: client}
}

// Attached is true when a sidecar URL was configured.
func (c *Client) Attached() bool {
	return c != nil && c.base != ""
}

// Proxy copies the request onto the sidecar and writes the response.
// Authorization is stripped: the caller already passed our PAT/OIDC check,
// and UC OSS is a local sidecar, not a Databricks workspace.
func (c *Client) Proxy(w http.ResponseWriter, r *http.Request) error {
	if !c.Attached() {
		return fmt.Errorf("no Unity Catalog sidecar is attached")
	}
	u, err := url.Parse(c.base + r.URL.Path)
	if err != nil {
		return err
	}
	u.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, u.String(), r.Body)
	if err != nil {
		return err
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("unity catalog sidecar: %w", err)
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if strings.EqualFold(k, "Transfer-Encoding") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, io.LimitReader(resp.Body, maxBody))
	return err
}

// JSON calls the sidecar directly (warehouse shim, not the inbound proxy).
func (c *Client) JSON(method, path string, body any) (int, []byte, error) {
	if !c.Attached() {
		return 0, nil, fmt.Errorf("no Unity Catalog sidecar is attached")
	}
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("unity catalog sidecar: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	return resp.StatusCode, raw, err
}

// AlreadyThere is a create that UC OSS reports as present (409, or 400 with exists).
func AlreadyThere(status int, body []byte) bool {
	if status == http.StatusOK || status == http.StatusCreated || status == http.StatusConflict {
		return true
	}
	if status != http.StatusBadRequest {
		return false
	}
	s := strings.ToLower(string(body))
	return strings.Contains(s, "already") || strings.Contains(s, "exists")
}
