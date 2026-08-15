// Package uc is the outbound client to a Unity Catalog OSS sidecar.
// Databricks' /api/2.1/unity-catalog REST is the same wire UC OSS speaks,
// so this is a reverse proxy after our PAT/OIDC check — not a second catalog.
package uc

import (
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
