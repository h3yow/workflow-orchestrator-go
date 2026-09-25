package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestFileStoreRoundTrip：FileStore 的创建幂等、读取缺失可判定、快照可往返。
func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	r := &Run{
		ID: "f1", State: RunRunning, WorkflowName: "wf",
		Tasks: map[string]*TaskInstance{
			"A": {TaskID: "A", State: TaskRunning, OwnerID: "e", AttemptCount: 2,
				LeaseUntil: now.Add(time.Second), AttemptGeneration: 3, UpdatedAt: now},
		},
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := store.CreateRun(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	got2, err := store.CreateRun(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Tasks["A"].AttemptGeneration != 3 || got2.Tasks["A"].OwnerID != "e" {
		t.Fatalf("CreateRun not idempotent: %+v", got2.Tasks["A"])
	}
	if err := store.SaveRun(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.GetRun(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Tasks["A"].State != TaskRunning || loaded.Tasks["A"].AttemptCount != 2 {
		t.Fatalf("round trip mismatch: %+v", loaded.Tasks["A"])
	}
	if _, err := store.GetRun(context.Background(), "missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("GetRun missing = %v, want ErrRunNotFound", err)
	}
	// 返回的副本与存储隔离。
	loaded.Tasks["A"].State = TaskSucceeded
	again, _ := store.GetRun(context.Background(), "f1")
	if again.Tasks["A"].State != TaskRunning {
		t.Fatal("mutating returned run leaked into store")
	}
}

// TestRestartRecoveryFromFile：模拟真实进程中断与重启。
// 第一个引擎运行到 B 执行中时被取消（RUNNING 已持久化、无成功结果）；
// 第二个引擎用同一目录的 FileStore 恢复：A 不重复执行，B 不被判成功而重试。
func TestRestartRecoveryFromFile(t *testing.T) {
	dir := t.TempDir()

	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "A"},
		{ID: "B", Deps: []string{"A"}, Retries: RetryPolicy{MaxAttempts: 3}},
		{ID: "C", Deps: []string{"B"}},
	}})

	var aCalls, bCalls int32
	reg := func() TaskRegistry {
		return TaskRegistry{
			"A": func(ctx context.Context, runID string, attempt int) error {
				atomic.AddInt32(&aCalls, 1)
				return nil
			},
			"B": func(ctx context.Context, runID string, attempt int) error {
				n := atomic.AddInt32(&bCalls, 1)
				if n == 1 {
					<-ctx.Done() // 第一次尝试随进程取消而中断
					return ctx.Err()
				}
				return nil
			},
			"C": func(ctx context.Context, runID string, attempt int) error { return nil },
		}
	}

	// 短租约让取消后留下的 RUNNING 很快过期，恢复侧无需久等。
	lease := 80 * time.Millisecond
	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := NewEngine(wf, store1, reg(), newTestLogger(t), Config{
		EngineID: "proc-1", LeaseDuration: lease, RecoveryPoll: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		_, _ = eng1.Trigger(ctx, "restart-1")
		close(doneCh)
	}()
	// 等待 B 至少开始一次，再“杀掉进程”。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&bCalls) >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if atomic.LoadInt32(&bCalls) < 1 {
		t.Fatal("B never started before cancellation")
	}
	cancel()
	<-doneCh

	// 磁盘上必须留下 RUNNING 的 B（中断任务不得被写成成功）。
	store2, _ := NewFileStore(dir)
	mid, err := store2.GetRun(context.Background(), "restart-1")
	if err != nil {
		t.Fatal(err)
	}
	if mid.Tasks["B"].State != TaskRunning {
		t.Fatalf("interrupted B persisted as %s, want RUNNING", mid.Tasks["B"].State)
	}
	if mid.Tasks["A"].State != TaskSucceeded {
		t.Fatalf("A persisted as %s, want SUCCEEDED", mid.Tasks["A"].State)
	}

	// “重启”：全新引擎从同一文件状态恢复。
	eng2 := NewEngine(wf, store2, reg(), newTestLogger(t), Config{
		EngineID: "proc-2", LeaseDuration: 5 * time.Second, RecoveryPoll: 5 * time.Millisecond,
	})
	run, err := eng2.Recover(context.Background(), "restart-1")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertState(t, run, "A", TaskSucceeded, 1)
	// 中断的尝试不计入 AttemptCount（不产生结果、不耗预算），故 B 只记 1 次完成尝试。
	assertState(t, run, "B", TaskSucceeded, 1)
	assertState(t, run, "C", TaskSucceeded, 1)
	if len(run.Tasks["B"].Attempts) != 1 || run.Tasks["B"].Attempts[0].Result != resultSucceeded {
		t.Fatalf("B completed attempts = %+v, want exactly one succeeded record", run.Tasks["B"].Attempts)
	}
	if run.State != RunSucceeded {
		t.Fatalf("run state = %s, want SUCCEEDED", run.State)
	}
	if got := atomic.LoadInt32(&aCalls); got != 1 {
		t.Fatalf("A executed %d times across restart, want exactly 1", got)
	}
	if got := atomic.LoadInt32(&bCalls); got != 2 {
		t.Fatalf("B executed %d times across restart, want 2 (1 interrupted + 1 retry)", got)
	}

	// 文件内容确认终态已持久化。
	final, _ := store2.GetRun(context.Background(), "restart-1")
	if final.State != RunSucceeded {
		t.Fatalf("persisted final state = %s, want SUCCEEDED", final.State)
	}
}
