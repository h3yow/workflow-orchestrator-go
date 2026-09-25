package workflow

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestTimeoutClosesAttemptOnce 验证：
//   - 单次尝试超时后以 TIMED_OUT 收尾并占用重试额度；
//   - 超时计时器先返回时，执行函数即使稍后“才返回成功”，结果也被丢弃，
//     不会对同一尝试产生重复执行结果；
//   - 下一次快速尝试成功后，总执行次数恰为 2。
func TestTimeoutClosesAttemptOnce(t *testing.T) {
	var buf bytes.Buffer
	store := NewMemoryStore()

	var startedFirst int32
	release := make(chan struct{})
	exec := MapExecutor{
		"t": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			if attemptNo == 1 {
				atomic.AddInt32(&startedFirst, 1)
				// 模拟执行体不响应 ctx：超时之后仍停留一会儿再返回成功。
				select {
				case <-release:
				case <-time.After(200 * time.Millisecond):
				}
				return AttemptResult{} // 迟到的“成功”
			}
			return AttemptResult{} // 第二次尝试立即成功
		},
	}

	eng := NewEngine(store, exec, NewLogger(&buf))
	t.Cleanup(func() {
		close(release)
		_ = eng.Close()
	})
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Timeout: 30 * time.Millisecond,
			Retry: RetryPolicy{MaxAttempts: 2, Backoff: 5 * time.Millisecond}},
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
		t.Fatalf("status=%s want SUCCEEDED（第二次尝试成功）", status)
	}
	snap, _ := store.GetRun(context.Background(), "r")
	task := snap.Tasks["t"]
	if task.Status != StatusSucceeded {
		t.Fatalf("final status=%s", task.Status)
	}
	if task.Attempt != 2 {
		t.Fatalf("attempt=%d want 2，超时必须占用额度且迟到结果不得重复结算", task.Attempt)
	}
	if !bytes.Contains(buf.Bytes(), []byte(DecisionAttemptTimeout)) {
		t.Fatalf("日志缺少 ATTEMPT_TIMEOUT:\n%s", buf.String())
	}
	if atomic.LoadInt32(&startedFirst) != 1 {
		t.Fatalf("first attempt started %d times, want 1", startedFirst)
	}
	t.Logf("超时决策日志:\n%s", buf.String())
}

// TestTimeoutExhaustsBudget 验证每次超时都占额度，末次超时后为确定终态。
func TestTimeoutExhaustsBudget(t *testing.T) {
	store := NewMemoryStore()
	exec := MapExecutor{
		"t": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			<-time.After(200 * time.Millisecond) // 总是超时
			return AttemptResult{}
		},
	}
	eng := NewEngine(store, exec, NewLogger(&bytes.Buffer{}))
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Timeout: 20 * time.Millisecond,
			Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}},
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
	snap, _ := store.GetRun(context.Background(), "r")
	if snap.Tasks["t"].Status != StatusPermanentlyFailed {
		t.Fatalf("task=%s want PERMANENTLY_FAILED", snap.Tasks["t"].Status)
	}
}
