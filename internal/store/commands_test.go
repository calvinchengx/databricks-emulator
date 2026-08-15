package store

import "testing"

func TestCommandsContextAndCancel(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := s.Commands.CreateContext("cluster-1", "python")
	if ctx.ID == "" || ctx.Status != "Pending" {
		t.Fatalf("create %+v", ctx)
	}
	if !s.Commands.SetContextStatus(ctx.ID, "Running") {
		t.Fatal("set status")
	}
	got, ok := s.Commands.GetContext(ctx.ID)
	if !ok || got.Status != "Running" {
		t.Fatalf("get %+v", got)
	}
	cmd := s.Commands.CreateCommand("cluster-1", ctx.ID, "python", "print(1)")
	if !s.Commands.FinishCommand(cmd.ID, "Finished", "text", "1", "", "") {
		t.Fatal("finish")
	}
	out, ok := s.Commands.GetCommand(cmd.ID)
	if !ok || out.Data != "1" {
		t.Fatalf("cmd %+v", out)
	}
	if !s.Commands.CancelCommand(cmd.ID) {
		t.Fatal("cancel finished")
	}
	run := s.Commands.CreateCommand("cluster-1", ctx.ID, "python", "print(2)")
	if !s.Commands.CancelCommand(run.ID) {
		t.Fatal("cancel running")
	}
	cancelled, _ := s.Commands.GetCommand(run.ID)
	if cancelled.Status != "Cancelled" {
		t.Fatalf("cancelled %+v", cancelled)
	}
	if !s.Commands.DestroyContext(ctx.ID) {
		t.Fatal("destroy")
	}
	if _, ok := s.Commands.GetContext(ctx.ID); ok {
		t.Fatal("destroyed context remains")
	}
	if _, ok := s.Commands.GetCommand(cmd.ID); ok {
		t.Fatal("destroyed command remains")
	}
	if s.Commands.SetContextStatus("missing", "Running") {
		t.Fatal("set missing")
	}
	if s.Commands.DestroyContext("missing") {
		t.Fatal("destroy missing")
	}
	if s.Commands.FinishCommand("missing", "Error", "error", "", "x", "") {
		t.Fatal("finish missing")
	}
	if s.Commands.CancelCommand("missing") {
		t.Fatal("cancel missing")
	}
	if _, ok := s.Commands.GetContext("missing"); ok {
		t.Fatal("get missing context")
	}
	if _, ok := s.Commands.GetCommand("missing"); ok {
		t.Fatal("get missing command")
	}
}
