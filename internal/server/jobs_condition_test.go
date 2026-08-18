package server

import (
	"testing"

	"github.com/calvinchengx/databricks-emulator/internal/store"
)

// The two operator families compare differently, and Databricks documents the
// difference with these exact examples: "The == and != operators perform string
// comparison of their operands. For example, 12.0 == 12 evaluates to false" and
// "The >, >=, <, and <= operators perform numeric comparisons. For example,
// 12.0 >= 12 evaluates to true."
//
// Both examples are cases below. They are the ones a friendlier implementation
// gets wrong: making equality numeric-aware is the obvious "fix" and would put a
// job on the wrong branch here that takes the right one on a real workspace.
func TestConditionOperatorFamiliesCompareDifferently(t *testing.T) {
	for _, tc := range []struct {
		op, left, right, want string
	}{
		// Databricks' own two examples, verbatim.
		{"EQUAL_TO", "12.0", "12", "false"},
		{"GREATER_THAN_OR_EQUAL", "12.0", "12", "true"},

		// Equality is string comparison throughout.
		{"EQUAL_TO", "12", "12", "true"},
		{"EQUAL_TO", "abc", "abc", "true"},
		{"EQUAL_TO", "abc", "abd", "false"},
		{"NOT_EQUAL", "12.0", "12", "true"},
		{"NOT_EQUAL", "12", "12", "false"},

		// Ordering is numeric, so it does NOT order strings lexically:
		// "9" > "10" as text, but 9 > 10 is false.
		{"GREATER_THAN", "9", "10", "false"},
		{"LESS_THAN", "9", "10", "true"},
		{"GREATER_THAN", "10", "9", "true"},
		{"LESS_THAN_OR_EQUAL", "12", "12", "true"},
		{"GREATER_THAN", "12", "12", "false"},

		// An operand with no ordering is false rather than a string
		// comparison wearing a numeric operator's name.
		{"GREATER_THAN", "abc", "12", "false"},
		{"LESS_THAN", "abc", "12", "false"},
	} {
		got := evalCondition(store.Condition{Op: tc.op, Left: tc.left, Right: tc.right})
		if got != tc.want {
			t.Errorf("%s(%q, %q) = %q, want %q", tc.op, tc.left, tc.right, got, tc.want)
		}
	}
}

// A condition SUCCEEDS whichever way it evaluates, so result_state cannot be
// what selects the branch. Only the outcome can, which is why the dependency
// carries it.
func TestOnlyTheNamedBranchRuns(t *testing.T) {
	for _, outcome := range []string{"true", "false"} {
		done := map[string]store.TaskRun{
			"cond": {Key: "cond", ResultState: "SUCCESS", ConditionOutcome: outcome},
		}
		yes := store.Task{Key: "yes", DependsOn: []store.Dependency{{Key: "cond", Outcome: "true"}}}
		no := store.Task{Key: "no", DependsOn: []store.Dependency{{Key: "cond", Outcome: "false"}}}

		if got := shouldRun(yes, done); got != (outcome == "true") {
			t.Errorf("outcome %q: true-arm ran = %v", outcome, got)
		}
		if got := shouldRun(no, done); got != (outcome == "false") {
			t.Errorf("outcome %q: false-arm ran = %v", outcome, got)
		}
	}
}

// The mutation this test exists to kill: drop `Outcome` from the dependency
// model, or ignore it in shouldRun, and BOTH arms run while the job still
// reports SUCCESS. Nothing about the run's own status would look wrong.
func TestIgnoringTheOutcomeWouldRunBothArms(t *testing.T) {
	done := map[string]store.TaskRun{
		"cond": {Key: "cond", ResultState: "SUCCESS", ConditionOutcome: "true"},
	}
	falseArm := store.Task{Key: "no", DependsOn: []store.Dependency{{Key: "cond", Outcome: "false"}}}
	if shouldRun(falseArm, done) {
		t.Fatal("the false arm ran on a true condition — the outcome is being ignored, " +
			"which is the silent-wrong-answer this branch model exists to prevent")
	}
	// An edge with no outcome is unaffected: an ordinary task downstream of a
	// condition still runs on the condition's own SUCCESS.
	plain := store.Task{Key: "after", DependsOn: []store.Dependency{{Key: "cond"}}}
	if !shouldRun(plain, done) {
		t.Fatal("an ordinary edge stopped following a condition task")
	}
}

// run_if still applies to the arm that was selected. Outcome gates first,
// then the ordinary rule decides.
func TestOutcomeGatesBeforeRunIf(t *testing.T) {
	done := map[string]store.TaskRun{
		"cond": {Key: "cond", ResultState: "SUCCESS", ConditionOutcome: "true"},
		"work": {Key: "work", ResultState: "FAILED"},
	}
	// Selected arm, but its other dependency failed: ALL_SUCCESS says no.
	t1 := store.Task{Key: "t", RunIf: "ALL_SUCCESS", DependsOn: []store.Dependency{
		{Key: "cond", Outcome: "true"}, {Key: "work"},
	}}
	if shouldRun(t1, done) {
		t.Fatal("ALL_SUCCESS ignored a failed sibling on the selected branch")
	}
	// Same edges under ALL_DONE: the branch was selected, so it runs.
	t2 := t1
	t2.RunIf = "ALL_DONE"
	if !shouldRun(t2, done) {
		t.Fatal("ALL_DONE did not run on the selected branch")
	}
	// Unselected arm under ALL_DONE: still skipped. run_if must not be able
	// to resurrect the branch the condition did not choose.
	t3 := store.Task{Key: "t3", RunIf: "ALL_DONE", DependsOn: []store.Dependency{
		{Key: "cond", Outcome: "false"},
	}}
	if shouldRun(t3, done) {
		t.Fatal("ALL_DONE ran the branch the condition did not choose")
	}
}
