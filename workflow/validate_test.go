package workflow

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestLoadAndValidate_OK(t *testing.T) {
	d, err := loadAndValidate(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a"},
		{Name: "b", Dependencies: []string{"a"}},
		{Name: "c", Dependencies: []string{"a"}},
		{Name: "d", Dependencies: []string{"b", "c"}},
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a", "b", "c", "d"}
	if strings.Join(d.order, ",") != strings.Join(want, ",") {
		t.Fatalf("topo order = %v, want %v (sorted for determinism)", d.order, want)
	}
}

func TestLoadAndValidate_DuplicateTask(t *testing.T) {
	_, err := loadAndValidate(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a"}, {Name: "a"},
	}})
	var ve *ValidationError
	if !errors.As(err, &ve) || !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask wrapped, got %v", err)
	}
	if ve.TaskName != "a" {
		t.Fatalf("want task a, got %q", ve.TaskName)
	}
}

func TestLoadAndValidate_MissingDependency(t *testing.T) {
	_, err := loadAndValidate(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a", Dependencies: []string{"ghost"}},
	}})
	var ve *ValidationError
	if !errors.As(err, &ve) || !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("want ErrMissingDependency, got %v", err)
	}
	if ve.TaskName != "a" {
		t.Fatalf("want task a, got %q", ve.TaskName)
	}
}

func TestLoadAndValidate_Cycle(t *testing.T) {
	_, err := loadAndValidate(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a", Dependencies: []string{"c"}},
		{Name: "b", Dependencies: []string{"a"}},
		{Name: "c", Dependencies: []string{"b"}},
	}})
	var ve *ValidationError
	if !errors.As(err, &ve) || !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("want ErrCycleDetected, got %v", err)
	}
}

// TestLoggerHelpers 确保三种错误在日志层面也可区分。
func TestValidationErrors_Distinct(t *testing.T) {
	if errors.Is(ErrDuplicateTask, ErrMissingDependency) {
		t.Fatal("error kinds must be distinct")
	}
	var buf bytes.Buffer
	lg := NewLogger(&buf)
	lg.LogDecision(Decision{RunID: "r", TaskName: "x", Kind: DecisionBlocked, Detail: "wait"})
	if !strings.Contains(buf.String(), "BLOCKED") {
		t.Fatalf("log missing decision kind: %s", buf.String())
	}
}
