package main

import "testing"

func TestRunVersion(t *testing.T) {
	if err := run([]string{"version"}); err != nil {
		t.Fatal(err)
	}
}

func TestHealthcheckBadAddr(t *testing.T) {
	if err := healthcheck("not-an-addr"); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunBadFlag(t *testing.T) {
	if err := run([]string{"-bogus"}); err == nil {
		t.Fatal("unknown flag accepted")
	}
}

func TestRunUnlistenableAddr(t *testing.T) {
	t.Setenv("DATABRICKS_DATA_DIR", t.TempDir())
	if err := run([]string{"-disable-tls", "-addr", "999.999.999.999:1"}); err == nil {
		t.Fatal("unlistenable addr accepted")
	}
}
