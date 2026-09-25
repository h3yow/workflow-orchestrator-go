package workflow

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// flakyExec 记录每个任务的尝试次数，便于断言执行次数。
type flakyExec struct {
	mu       sync.Mutex
	attempts map[string]int
	failN    map[string]int  // 前 N 次失败（键为 runID/task 或 task）
	failRuns map[string]bool // 仅这些 runID 下的任务失败
	hang     map[string]chan struct{}
	slow     map[string]time.Duration
}

func newFlakyExec() *flakyExec {
	return &flakyExec{
		attempts: map[string]int{},
		failN:    map[string]int{},
		failRuns: map[string]bool{},
		hang:     map[string]chan struct{}{},
		slow:     map[string]time.Duration{},
	}
}

func (f *flakyExec) Execute(name string) (ExecuteFunc, bool) {
	return func(ctx context.Context, runID, taskName string, attemptNo int) AttemptResult {
		f.mu.Lock()
		f.attempts[name]++
		n := f.attempts[name]
		failUntil := f.failN[name]
		if f.failRuns[runID] {
			failUntil = 99
		}
		slow := f.slow[name]
		wait := f.hang[name]
		f.mu.Unlock()

		if slow > 0 {
			select {
			case <-time.After(slow):
			case <-ctx.Done():
				return AttemptResult{Err: ctx.Err()}
			}
		}
		if wait != nil {
			select {
			case <-wait:
			case <-ctx.Done():
				return AttemptResult{Err: ctx.Err()}
			}
		}
		if n <= failUntil {
			return AttemptResult{Err: fmt.Errorf("%s fails on attempt %d", name, n)}
		}
		return AttemptResult{}
	}, true
}

func (f *flakyExec) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[name]
}

// TestDependentBlockingAndSkip 验证依赖阻塞、失败后下游跳过，以及独立链路不受影响。
func TestDependentBlockingAndSkip(t *testing.T) {
	var buf bytes.Buffer
	lg := NewLogger(&buf)
	store := NewMemoryStore()
	exec := newFlakyExec()
	exec.failN["bad"] = 99 // 始终失败，MaxAttempts=2 后永久失败

	eng := NewEngine(store, exec, lg)
	t.Cleanup(func() { _ = eng.Close() })

	// 图：root -> bad -> child(必跳过)；同时 root -> good(独立链路，必须成功)
	err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "root", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "bad", Retry: RetryPolicy{MaxAttempts: 2}},
		{Name: "child", Dependencies: []string{"bad"}, Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "good", Dependencies: []string{"root"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if _, _, err := eng.Trigger(ctx, "wf", "run-1"); err != nil {
		t.Fatal(err)
	}
	status, err := eng.RunAndWait(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if status != RunFailed {
		t.Fatalf("run status = %s, want FAILED", status)
	}

	snap, _ := store.GetRun(ctx, "run-1")
	cases := map[string]TaskStatus{
		"root":  StatusSucceeded,
		"bad":   StatusPermanentlyFailed,
		"child": StatusSkipped,
		"good":  StatusSucceeded,
	}
	for name, want := range cases {
		if got := snap.Tasks[name].Status; got != want {
			t.Errorf("task %s status = %s, want %s", name, got, want)
		}
	}
	if c := exec.count("child"); c != 0 {
		t.Errorf("skipped child executed %d times, want 0 (上游失败不得静默执行)", c)
	}
	if c := exec.count("bad"); c != 2 {
		t.Errorf("bad attempts = %d, want 2", c)
	}
	if c := exec.count("good"); c != 1 {
		t.Errorf("good attempts = %d, want 1 (独立链路必须继续推进)", c)
	}
	if !bytes.Contains(buf.Bytes(), []byte(DecisionBlocked)) ||
		!bytes.Contains(buf.Bytes(), []byte(DecisionSkip)) {
		t.Fatalf("日志缺少 BLOCKED/SKIP 决策:\n%s", buf.String())
	}
	t.Logf("调度决策日志:\n%s", buf.String())
}

// TestRetrySuccessThenNoDuplicate 验证失败重试后成功，且执行次数恰好等于重试+1。
func TestRetrySuccessThenNoDuplicate(t *testing.T) {
	var buf bytes.Buffer
	store := NewMemoryStore()
	exec := newFlakyExec()
	exec.failN["t"] = 2 // 前两次失败，第三次成功
	eng := NewEngine(store, exec, NewLogger(&buf))
	t.Cleanup(func() { _ = eng.Close() })

	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.Trigger(context.Background(), "wf", "r"); err != nil {
		t.Fatal(err)
	}
	status, err := eng.RunAndWait(context.Background(), "r")
	if err != nil {
		t.Fatal(err)
	}
	if status != RunSucceeded {
		t.Fatalf("status=%s", status)
	}
	if c := exec.count("t"); c != 3 {
		t.Fatalf("attempts=%d want 3，重试成功后不得重复执行", c)
	}
	if !bytes.Contains(buf.Bytes(), []byte(DecisionRetryScheduled)) ||
		!bytes.Contains(buf.Bytes(), []byte(DecisionAttemptSucceed)) {
		t.Fatalf("日志缺少重试/成功依据:\n%s", buf.String())
	}
	t.Logf("重试调度日志:\n%s", buf.String())
}

// TestRetriesExhaustedDeterministic 验证达到上限后确定终态。
func TestRetriesExhaustedDeterministic(t *testing.T) {
	store := NewMemoryStore()
	exec := newFlakyExec()
	exec.failN["t"] = 99
	eng := NewEngine(store, exec, NewLogger(&bytes.Buffer{}))
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.Trigger(context.Background(), "wf", "r"); err != nil {
		t.Fatal(err)
	}
	status, _ := eng.RunAndWait(context.Background(), "r")
	if status != RunFailed {
		t.Fatalf("status=%s want FAILED", status)
	}
	if c := exec.count("t"); c != 3 {
		t.Fatalf("attempts=%d want exactly 3", c)
	}
	snap, _ := store.GetRun(context.Background(), "r")
	if snap.Tasks["t"].Status != StatusPermanentlyFailed {
		t.Fatalf("task status=%s", snap.Tasks["t"].Status)
	}
}
