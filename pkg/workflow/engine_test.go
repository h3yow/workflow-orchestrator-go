package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 将调度决策与尝试结果写入测试日志，保证可解释、可复现可审查。
type testLogger struct {
	t   *testing.T
	mu  sync.Mutex
	buf bytes.Buffer
}

func newTestLogger(t *testing.T) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	l.buf.WriteString(time.Now().Format("15:04:05.000000") + " " + msg + "\n")
	l.t.Log(msg)
}

func (l *testLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// assertState 校验任务实例终态与执行（开始）次数。
func assertState(t *testing.T, r *Run, taskID string, want TaskState, wantAttempts int) {
	t.Helper()
	ti := r.Tasks[taskID]
	if ti == nil {
		t.Fatalf("task %q missing in run", taskID)
	}
	if ti.State != want {
		t.Errorf("task %q state = %s, want %s (reason=%q; attempts=%d)",
			taskID, ti.State, want, ti.ResultReason, ti.AttemptCount)
	}
	if ti.AttemptCount != wantAttempts {
		t.Errorf("task %q attempt count = %d, want %d", taskID, ti.AttemptCount, wantAttempts)
	}
}

func TestLoadValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		def  WorkflowDef
		want error
	}{
		{
			name: "empty workflow",
			def:  WorkflowDef{Name: "empty"},
			want: ErrEmptyWorkflow,
		},
		{
			name: "duplicate task",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "A"}, {ID: "A"},
			}},
			want: ErrDuplicateTask,
		},
		{
			name: "missing dependency",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "A", Deps: []string{"X"}},
			}},
			want: ErrDependencyNotFound,
		},
		{
			name: "self cycle",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "A", Deps: []string{"A"}},
			}},
			want: ErrCyclicDependency,
		},
		{
			name: "two node cycle",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "A", Deps: []string{"B"}},
				{ID: "B", Deps: []string{"A"}},
			}},
			want: ErrCyclicDependency,
		},
		{
			name: "three node cycle",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "A", Deps: []string{"C"}},
				{ID: "B", Deps: []string{"A"}},
				{ID: "C", Deps: []string{"B"}},
			}},
			want: ErrCyclicDependency,
		},
		{
			name: "empty task id",
			def:  WorkflowDef{Tasks: []TaskDef{{ID: ""}}},
			want: ErrInvalidTask,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(tc.def)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Load() error = %v, want errors.Is %v", err, tc.want)
			}
		})
	}
}

func TestValidDAGLoadsInTopologicalOrder(t *testing.T) {
	wf, err := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "A"},
		{ID: "B", Deps: []string{"A"}},
		{ID: "C", Deps: []string{"A"}},
		{ID: "D", Deps: []string{"B", "C"}},
	}})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := make([]string, 0, 4)
	for _, td := range wf.OrderedTasks() {
		got = append(got, td.ID)
	}
	// A 必须在 B、C 之前；B、C 按定义顺序；D 最后。
	want := []string{"A", "B", "C", "D"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("topological order = %v, want %v", got, want)
	}
}

// TestDependencyBlockingAndSkip 覆盖依赖阻塞、上游失败向下游跳过传播、
// 以及相互独立链路不受失败链路影响。
func TestDependencyBlockingAndSkip(t *testing.T) {
	wf, err := Load(WorkflowDef{
		Name: "blocking",
		Tasks: []TaskDef{
			{ID: "A", Retries: RetryPolicy{MaxAttempts: 1}},
			{ID: "B", Deps: []string{"A"}},
			{ID: "C", Deps: []string{"B"}},
			{ID: "X"}, // 独立链路
			{ID: "Y", Deps: []string{"X"}},
		},
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	var bCalls, cCalls int32
	reg := TaskRegistry{
		"A": func(ctx context.Context, runID string, attempt int) error {
			return errors.New("A always fails")
		},
		"B": func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(&bCalls, 1)
			return nil
		},
		"C": func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(&cCalls, 1)
			return nil
		},
		"X": func(ctx context.Context, runID string, attempt int) error { return nil },
		"Y": func(ctx context.Context, runID string, attempt int) error { return nil },
	}

	lg := newTestLogger(t)
	eng := NewEngine(wf, NewMemoryStore(), reg, lg, Config{EngineID: "e1", RecoveryPoll: 2 * time.Millisecond})
	run, err := eng.Trigger(context.Background(), "r1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}

	assertState(t, run, "A", TaskFailed, 1)
	assertState(t, run, "B", TaskSkipped, 0)
	assertState(t, run, "C", TaskSkipped, 0)
	assertState(t, run, "X", TaskSucceeded, 1)
	assertState(t, run, "Y", TaskSucceeded, 1)
	if run.State != RunFailed {
		t.Fatalf("run state = %s, want FAILED", run.State)
	}
	if atomic.LoadInt32(&bCalls) != 0 || atomic.LoadInt32(&cCalls) != 0 {
		t.Fatalf("skipped tasks were executed: B calls=%d C calls=%d", bCalls, cCalls)
	}

	// 日志中必须能看到每个跳过/失败决策的依据。
	logs := lg.String()
	for _, want := range []string{
		"task=B PENDING->SKIPPED",
		"task=C PENDING->SKIPPED",
		"did not succeed",
		"RUNNING->FAILED terminal",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("decision log missing %q\n--- logs ---\n%s", want, logs)
		}
	}
}
