package workflow

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentTriggerUniqueOwnership 多个 goroutine 并发触发同一 runID：
// 只有一个真正创建运行（existed=false 恰好一次），任务执行次数恰好一次。
func TestConcurrentTriggerUniqueOwnership(t *testing.T) {
	var buf bytes.Buffer
	store := NewMemoryStore()
	var executions int64
	exec := MapExecutor{
		"a": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			atomic.AddInt64(&executions, 1)
			time.Sleep(10 * time.Millisecond)
			return AttemptResult{}
		},
		"b": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			atomic.AddInt64(&executions, 1)
			return AttemptResult{}
		},
	}
	eng := NewEngine(store, exec, NewLogger(&buf))
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "b", Dependencies: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}}); err != nil {
		t.Fatal(err)
	}

	const n = 20
	var wg sync.WaitGroup
	var created int64
	var existedCount int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, existed, err := eng.Trigger(context.Background(), "wf", "same-run")
			if err != nil {
				t.Errorf("trigger: %v", err)
				return
			}
			if existed {
				atomic.AddInt64(&existedCount, 1)
			} else {
				atomic.AddInt64(&created, 1)
			}
		}()
	}
	wg.Wait()

	if created != 1 {
		t.Fatalf("created=%d, 并发触发下必须恰好创建一次", created)
	}
	if existedCount != n-1 {
		t.Fatalf("existed=%d want %d", existedCount, n-1)
	}

	status, err := eng.RunAndWait(context.Background(), "same-run")
	if err != nil {
		t.Fatal(err)
	}
	if status != RunSucceeded {
		t.Fatalf("status=%s", status)
	}
	if got := atomic.LoadInt64(&executions); got != 2 {
		t.Fatalf("executions=%d want 2，每个任务实例的归属必须唯一", got)
	}

	// 状态文件/存储中同一 runID 只有一份，任务键唯一。
	snap, _ := store.GetRun(context.Background(), "same-run")
	if len(snap.Tasks) != 2 {
		t.Fatalf("tasks=%d want 2", len(snap.Tasks))
	}
	runs, _ := store.ListRuns(context.Background(), "")
	if len(runs) != 1 {
		t.Fatalf("runs=%d want 1", len(runs))
	}
	t.Logf("并发触发日志（节选）:\n%s", buf.String())
}

// TestIndependentRunsIsolated 不同 runID 并发触发互不影响：
// 一条链路永久失败，另一条仍然成功推进。
func TestIndependentRunsIsolated(t *testing.T) {
	store := NewMemoryStore()
	exec := newFlakyExec()
	exec.failRuns["bad-run"] = true
	eng := NewEngine(store, exec, NewLogger(&bytes.Buffer{}))
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}},
	}}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	statuses := make(map[string]RunStatus)
	var mu sync.Mutex
	wg.Add(2)
	go func() {
		defer wg.Done()
		s, _ := triggerWait(eng, "bad-run")
		mu.Lock()
		statuses["bad-run"] = s
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		s, _ := triggerWait(eng, "good-run")
		mu.Lock()
		statuses["good-run"] = s
		mu.Unlock()
	}()
	wg.Wait()

	if statuses["bad-run"] != RunFailed {
		t.Fatalf("bad-run=%s", statuses["bad-run"])
	}
	if statuses["good-run"] != RunSucceeded {
		t.Fatalf("good-run=%s，单条链路失败不得影响独立链路", statuses["good-run"])
	}
}

func triggerWait(eng *Engine, runID string) (RunStatus, error) {
	if _, _, err := eng.Trigger(context.Background(), "wf", runID); err != nil {
		return "", err
	}
	return eng.RunAndWait(context.Background(), runID)
}
