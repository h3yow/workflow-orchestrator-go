package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func diamondDef(retries int) *Workflow {
	wf, err := Load(WorkflowDef{
		Name: "diamond",
		Tasks: []TaskDef{
			{ID: "A", Retries: RetryPolicy{MaxAttempts: retries, Backoff: time.Millisecond}},
			{ID: "B", Deps: []string{"A"}},
			{ID: "C", Deps: []string{"A"}},
			{ID: "D", Deps: []string{"B", "C"}},
		},
	})
	if err != nil {
		panic(err)
	}
	return wf
}

func countingRegistry(failAUntil int32, calls map[string]*int32) TaskRegistry {
	reg := TaskRegistry{}
	for _, id := range []string{"A", "B", "C", "D"} {
		id := id
		c := calls[id]
		reg[id] = func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(c, 1)
			if id == "A" && failAUntil > 0 {
				if n := atomic.LoadInt32(calls["A"]); n <= failAUntil {
					return errors.New("transient")
				}
			}
			return nil
		}
	}
	return reg
}

// TestConcurrentTriggerSameRun：对同一 runID 并发触发多次，
// 任务实例归属必须唯一——每个任务只执行一次，所有调用看到同一终态。
func TestConcurrentTriggerSameRun(t *testing.T) {
	const n = 16
	wf := diamondDef(3)
	store := NewMemoryStore()
	calls := map[string]*int32{}
	for _, id := range []string{"A", "B", "C", "D"} {
		var v int32
		calls[id] = &v
	}
	lg := newTestLogger(t) // 用于结尾断言决策日志存在

	var wg sync.WaitGroup
	results := make([]*Run, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 每个触发 goroutine 使用独立 logger（各自持有 t，符合 t.Log 的并发约束）。
			il := newTestLogger(t)
			// 多个引擎实例并发触发同一 run；共享 store 模拟同一持久化后端。
			eng := NewEngine(wf, store, countingRegistry(2, calls), il,
				Config{EngineID: engineIDs[i], RecoveryPoll: 2 * time.Millisecond})
			<-start
			r, err := eng.Trigger(context.Background(), "same-run")
			results[i], errs[i] = r, err
			// 汇总该 goroutine 的决策日志到共享断言缓冲。
			lg.mu.Lock()
			lg.buf.WriteString(il.buf.String())
			lg.mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("trigger %d: %v", i, err)
		}
		if results[i].State != RunSucceeded {
			t.Fatalf("trigger %d state = %s, want SUCCEEDED", i, results[i].State)
		}
		for _, id := range []string{"A", "B", "C", "D"} {
			wantAttempts := 1
			if id == "A" {
				wantAttempts = 3 // A 前两次失败、第三次成功
			}
			assertState(t, results[i], id, TaskSucceeded, wantAttempts)
		}
	}
	// A 失败两次后第三次成功：A 执行 3 次，其余各 1 次。没有重复执行结果。
	if got := atomic.LoadInt32(calls["A"]); got != 3 {
		t.Fatalf("A invoked %d times, want 3", got)
	}
	for _, id := range []string{"B", "C", "D"} {
		if got := atomic.LoadInt32(calls[id]); got != 1 {
			t.Fatalf("%s invoked %d times, want exactly 1 (unique ownership)", id, got)
		}
	}
	logs := lg.String()
	if !strings.Contains(logs, "READY->RUNNING") {
		t.Fatalf("scheduling decisions missing in log:\n%s", logs)
	}
}

var engineIDs = []string{
	"eng-0", "eng-1", "eng-2", "eng-3", "eng-4", "eng-5", "eng-6", "eng-7",
	"eng-8", "eng-9", "eng-10", "eng-11", "eng-12", "eng-13", "eng-14", "eng-15",
}

// TestConcurrentTriggerDifferentRuns：不同 runID 互不干扰，各自独立推进。
func TestConcurrentTriggerDifferentRuns(t *testing.T) {
	wf := diamondDef(1)
	store := NewMemoryStore()
	var wg sync.WaitGroup
	const n = 8
	start := make(chan struct{})
	states := make([]RunState, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var calls [4]int32
			callsMap := map[string]*int32{"A": &calls[0], "B": &calls[1], "C": &calls[2], "D": &calls[3]}
			// 偶数 run 的 A 成功，奇数 run 的 A 失败。
			reg := countingRegistry(0, callsMap)
			if i%2 == 1 {
				reg["A"] = func(ctx context.Context, runID string, attempt int) error {
					atomic.AddInt32(callsMap["A"], 1)
					return errors.New("odd run fails")
				}
			}
			eng := NewEngine(wf, store, reg, newTestLogger(t),
				Config{EngineID: engineIDs[i], RecoveryPoll: 2 * time.Millisecond})
			<-start
			r, err := eng.Trigger(context.Background(), runIDFor(i))
			if err != nil {
				t.Errorf("run %d: %v", i, err)
				return
			}
			states[i] = r.State
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 0; i < n; i++ {
		want := RunSucceeded
		if i%2 == 1 {
			want = RunFailed
		}
		if states[i] != want {
			t.Errorf("run %d state = %s, want %s", i, states[i], want)
		}
	}
}

func runIDFor(i int) string { return "run-" + itoa(i) }

// TestRecoverInterruptedTaskRetried：模拟进程崩溃——
// A 已 SUCCEEDED、B 中断在 RUNNING（租约已过期）。恢复后：
// A 不得再次执行；B 不被判定为成功，回收后重试成功。
func TestRecoverInterruptedTaskRetried(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "A"},
		{ID: "B", Deps: []string{"A"}, Retries: RetryPolicy{MaxAttempts: 3}},
	}})
	store := NewMemoryStore()
	now := time.Now()
	crashed := &Run{
		ID:           "resume-1",
		WorkflowName: "recover",
		State:        RunRunning,
		Tasks: map[string]*TaskInstance{
			"A": {TaskID: "A", State: TaskSucceeded, AttemptCount: 1, UpdatedAt: now},
			"B": {
				TaskID: "B", State: TaskRunning, OwnerID: "dead-engine", AttemptCount: 1,
				LeaseUntil: now.Add(-time.Second), UpdatedAt: now.Add(-2 * time.Second),
				AttemptGeneration: 1,
			},
		},
		CreatedAt: now, UpdatedAt: now.Add(-2 * time.Second),
	}
	if _, err := store.CreateRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}

	var aCalls, bCalls int32
	reg := TaskRegistry{
		"A": func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(&aCalls, 1) // 恢复后不应被调用
			return nil
		},
		"B": func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(&bCalls, 1)
			return nil
		},
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, store, reg, lg, Config{
		EngineID: "new-engine", LeaseDuration: 30 * time.Millisecond, RecoveryPoll: 5 * time.Millisecond,
	})
	run, err := eng.Recover(context.Background(), "resume-1")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertState(t, run, "A", TaskSucceeded, 1)
	assertState(t, run, "B", TaskSucceeded, 2)
	if run.State != RunSucceeded {
		t.Fatalf("run state = %s, want SUCCEEDED", run.State)
	}
	if atomic.LoadInt32(&aCalls) != 0 {
		t.Fatalf("already-succeeded task A was re-executed %d times after recovery", aCalls)
	}
	if atomic.LoadInt32(&bCalls) != 1 {
		t.Fatalf("interrupted task B retried %d times, want 1", bCalls)
	}
	if !strings.Contains(lg.String(), "interrupted attempt is not counted as success") {
		t.Fatalf("recovery decision log missing:\n%s", lg.String())
	}
}

// TestRecoverInterruptedTaskExhausted：中断任务已用尽尝试预算时，
// 恢复后必须判 FAILED（而不是成功），下游被跳过。
func TestRecoverInterruptedTaskExhausted(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "B", Retries: RetryPolicy{MaxAttempts: 1}},
		{ID: "C", Deps: []string{"B"}},
	}})
	store := NewMemoryStore()
	now := time.Now()
	crashed := &Run{
		ID: "resume-2", State: RunRunning, WorkflowName: "recover",
		Tasks: map[string]*TaskInstance{
			"B": {
				TaskID: "B", State: TaskRunning, OwnerID: "dead-engine", AttemptCount: 1,
				LeaseUntil: now.Add(-time.Second), AttemptGeneration: 1, UpdatedAt: now.Add(-time.Second),
			},
			"C": {TaskID: "C", State: TaskPending, UpdatedAt: now},
		},
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := store.CreateRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}
	var bCalls int32
	reg := TaskRegistry{
		"B": func(ctx context.Context, runID string, attempt int) error {
			atomic.AddInt32(&bCalls, 1)
			return nil
		},
		"C": func(ctx context.Context, runID string, attempt int) error { return nil },
	}
	eng := NewEngine(wf, store, reg, newTestLogger(t), Config{
		EngineID: "new-engine", LeaseDuration: 30 * time.Millisecond, RecoveryPoll: 5 * time.Millisecond,
	})
	run, err := eng.Recover(context.Background(), "resume-2")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertState(t, run, "B", TaskFailed, 1)
	assertState(t, run, "C", TaskSkipped, 0)
	if atomic.LoadInt32(&bCalls) != 0 {
		t.Fatalf("exhausted interrupted task B was re-executed %d times", bCalls)
	}
}

// TestRecoverMissingRun：恢复不存在的 run 返回可判定的 ErrRunNotFound。
func TestRecoverMissingRun(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{{ID: "A"}}})
	eng := NewEngine(wf, NewMemoryStore(), TaskRegistry{"A": func(context.Context, string, int) error { return nil }},
		newTestLogger(t), Config{})
	if _, err := eng.Recover(context.Background(), "nope"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Recover missing run err = %v, want ErrRunNotFound", err)
	}
}

// TestStaleResultAfterReapIgnored：同引擎回收中断尝试后，
// 旧尝试晚到的结果必须因代次不一致而被丢弃，新尝试结果正常落库。
func TestStaleResultAfterReapIgnored(t *testing.T) {
	wf, _ := Load(WorkflowDef{Tasks: []TaskDef{
		{ID: "A", Retries: RetryPolicy{MaxAttempts: 3}},
	}})
	store := NewMemoryStore()
	now := time.Now()
	crashed := &Run{
		ID: "resume-3", State: RunRunning, WorkflowName: "recover",
		Tasks: map[string]*TaskInstance{
			"A": {
				TaskID: "A", State: TaskRunning, OwnerID: "eng", AttemptCount: 1,
				LeaseUntil: now.Add(-time.Second), AttemptGeneration: 1, UpdatedAt: now,
			},
		},
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := store.CreateRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}
	reg := TaskRegistry{
		"A": func(ctx context.Context, runID string, attempt int) error { return nil },
	}
	lg := newTestLogger(t)
	eng := NewEngine(wf, store, reg, lg, Config{
		EngineID: "eng", LeaseDuration: 30 * time.Millisecond, RecoveryPoll: 5 * time.Millisecond,
	})
	run, err := eng.Recover(context.Background(), "resume-3")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertState(t, run, "A", TaskSucceeded, 2)
}
