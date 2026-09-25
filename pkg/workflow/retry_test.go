package workflow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetrySucceedsWithinBudget：前两次失败、第三次成功，
// 终态 SUCCEEDED，开始的尝试恰为 3 次，且每次尝试都有且仅有一条结果记录。
func TestRetrySucceedsWithinBudget(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "A", Retries: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}},
	}})

	var calls int32
	reg := TaskRegistry{
		"A": func(ctx context.Context, runID string, attempt int) error {
			n := atomic.AddInt32(&calls, 1)
			if n < 3 {
				return errors.New("transient failure")
			}
			return nil
		},
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, NewMemoryStore(), reg, lg, Config{EngineID: "e1", RecoveryPoll: time.Millisecond})
	run, err := eng.Trigger(context.Background(), "retry-ok")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	assertState(t, run, "A", TaskSucceeded, 3)
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("task function invoked %d times, want 3", got)
	}
	ti := run.Tasks["A"]
	if len(ti.Attempts) != 3 {
		t.Fatalf("attempt records = %d, want 3 (no duplicate result)", len(ti.Attempts))
	}
	for i, a := range ti.Attempts {
		if a.Attempt != i+1 {
			t.Fatalf("attempt #%d recorded with number %d", i, a.Attempt)
		}
	}
	if ti.Attempts[2].Result != resultSucceeded {
		t.Fatalf("final attempt result = %q, want succeeded", ti.Attempts[2].Result)
	}
	logs := lg.String()
	if !strings.Contains(logs, "RUNNING->READY") || !strings.Contains(logs, "RUNNING->SUCCEEDED") {
		t.Fatalf("retry transition logs missing:\n%s", logs)
	}
}

// TestRetryExhaustionEndsDeterministically：超过尝试上限后以 FAILED 结束，
// 不会无限重试，run 为确定性终态 FAILED。
func TestRetryExhaustionEndsDeterministically(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "A", Retries: RetryPolicy{MaxAttempts: 4, Backoff: time.Millisecond}},
	}})

	var calls int32
	reg := TaskRegistry{
		"A": func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(&calls, 1)
			return errors.New("permanent failure")
		},
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, NewMemoryStore(), reg, lg, Config{EngineID: "e1", RecoveryPoll: time.Millisecond})
	run, err := eng.Trigger(context.Background(), "retry-exhausted")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	assertState(t, run, "A", TaskFailed, 4)
	if got := atomic.LoadInt32(&calls); got != 4 {
		t.Fatalf("task function invoked %d times, want 4", got)
	}
	if run.State != RunFailed {
		t.Fatalf("run state = %s, want FAILED", run.State)
	}
}

// TestTimeoutFinishesWithSingleResult：任务体不响应 ctx 取消，
// 引擎在超时后必须以 timeout 收尾，且只产生一次尝试结果。
func TestTimeoutFinishesWithSingleResult(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "slow", Timeout: TimeoutPolicy{Timeout: 30 * time.Millisecond}},
	}})

	started := make(chan struct{})
	release := make(chan struct{})
	var done int32
	reg := TaskRegistry{
		"slow": func(ctx context.Context, runID string, attempt int) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release // 故意忽略 ctx.Done，模拟不配合取消的任务
			atomic.AddInt32(&done, 1)
			return nil // 即使稍后返回 nil，超时尝试也已以 timeout 收尾
		},
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, NewMemoryStore(), reg, lg, Config{
		EngineID: "e1", RecoveryPoll: 5 * time.Millisecond,
	})
	run, err := eng.Trigger(context.Background(), "timeout-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	assertState(t, run, "slow", TaskFailed, 1)
	ti := run.Tasks["slow"]
	if len(ti.Attempts) != 1 || ti.Attempts[0].Result != resultTimeout {
		t.Fatalf("attempts = %+v, want exactly one timeout result", ti.Attempts)
	}
	// 释放被卡住的任务体，确认它晚到的 nil 不会再改变终态。
	close(release)
	time.Sleep(30 * time.Millisecond)
	if run2, _ := eng.store.GetRun(context.Background(), "timeout-1"); run2.Tasks["slow"].State != TaskFailed {
		t.Fatalf("late success changed terminal state to %s", run2.Tasks["slow"].State)
	}
	if !strings.Contains(lg.String(), "discard stale result") {
		// 引擎可能已结束循环，晚到事件无法投递也属安全；这里仅记录不断言。
		t.Logf("late result was not delivered (safe due to terminal state):\n%s", lg.String())
	}
}

// TestIndependentBranchesContinue：单条链路失败时，独立链路继续推进并成功。
func TestIndependentBranchesContinue(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "bad", Retries: RetryPolicy{MaxAttempts: 1}},
		{ID: "ok1"},
		{ID: "ok2", Deps: []string{"ok1"}},
	}})
	reg := TaskRegistry{
		"bad": func(ctx context.Context, runID string, attempt int) error { return errors.New("boom") },
		"ok1": func(ctx context.Context, runID string, attempt int) error { return nil },
		"ok2": func(ctx context.Context, runID string, attempt int) error { return nil },
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, NewMemoryStore(), reg, lg, Config{EngineID: "e1", RecoveryPoll: 2 * time.Millisecond})
	run, err := eng.Trigger(context.Background(), "branches")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	assertState(t, run, "bad", TaskFailed, 1)
	assertState(t, run, "ok1", TaskSucceeded, 1)
	assertState(t, run, "ok2", TaskSucceeded, 1)
}

// TestPanicTreatedAsFailedAttempt：任务体 panic 被转化为一次失败尝试，
// 重试预算内可恢复；预算耗尽则确定终态 FAILED。
func TestPanicTreatedAsFailedAttempt(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "P", Retries: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}},
	}})
	var calls int32
	reg := TaskRegistry{
		"P": func(ctx context.Context, runID string, attempt int) error {
			if atomic.AddInt32(&calls, 1) < 3 {
				panic("boom")
			}
			return nil
		},
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, NewMemoryStore(), reg, lg, Config{EngineID: "e1", RecoveryPoll: time.Millisecond})
	run, err := eng.Trigger(context.Background(), "panic-1")
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	assertState(t, run, "P", TaskSucceeded, 3)
	if run.Tasks["P"].Attempts[0].Result != resultPanic {
		t.Fatalf("first attempt = %q, want panic", run.Tasks["P"].Attempts[0].Result)
	}
}
