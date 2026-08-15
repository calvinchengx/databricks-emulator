package server

import (
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/store"
)

func TestRunIfMatrix(t *testing.T) {
	success := map[string]store.TaskRun{"d": {ResultState: "SUCCESS"}}
	failed := map[string]store.TaskRun{"d": {ResultState: "FAILED"}}
	mixed := map[string]store.TaskRun{"d": {ResultState: "SUCCESS"}, "e": {ResultState: "FAILED"}}

	task := func(runIf string, deps ...string) store.Task {
		return store.Task{RunIf: runIf, DependsOn: deps}
	}

	if !shouldRun(task("ALL_SUCCESS", "d"), success) {
		t.Fatal("ALL_SUCCESS on success")
	}
	if shouldRun(task("ALL_SUCCESS", "d"), failed) {
		t.Fatal("ALL_SUCCESS on fail")
	}
	if !shouldRun(task("ALL_DONE", "d"), failed) {
		t.Fatal("ALL_DONE")
	}
	if !shouldRun(task("AT_LEAST_ONE_SUCCESS", "d", "e"), mixed) {
		t.Fatal("AT_LEAST_ONE_SUCCESS")
	}
	if !shouldRun(task("AT_LEAST_ONE_FAILED", "d", "e"), mixed) {
		t.Fatal("AT_LEAST_ONE_FAILED")
	}
	if !shouldRun(task("ALL_FAILED", "d"), failed) {
		t.Fatal("ALL_FAILED")
	}
	if shouldRun(task("ALL_FAILED", "d", "e"), mixed) {
		t.Fatal("ALL_FAILED mixed")
	}
	if !shouldRun(task("NONE_FAILED", "d"), success) {
		t.Fatal("NONE_FAILED")
	}
	if shouldRun(task("NONE_FAILED", "d", "e"), mixed) {
		t.Fatal("NONE_FAILED mixed")
	}
	if !shouldRun(task("ALL_SUCCESS"), nil) {
		t.Fatal("no deps")
	}
	if !depsSatisfied(task("ALL_SUCCESS"), nil) {
		t.Fatal("no deps satisfied")
	}
	if depsSatisfied(task("ALL_SUCCESS", "missing"), map[string]store.TaskRun{}) {
		t.Fatal("unmet dep")
	}
}
