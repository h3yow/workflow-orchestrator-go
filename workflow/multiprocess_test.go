package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newProc 在同一状态目录上创建一个“进程”（独立的 FileStore + Engine），
// 模拟多 worker 进程共用同一份本地状态的部署方式。
// flock 作用于独立的打开文件描述，因此这些实例之间的互斥与真实跨进程一致。
func newProc(t *testing.T, dir string, exec Executor, ttl time.Duration) *Engine {
	t.Helper()
	return newProcWithLogger(t, dir, exec, ttl, NewLogger(io.Discard))
}

// newProcWithLogger 与 newProc 相同，但允许注入决策日志器。
func newProcWithLogger(t *testing.T, dir string, exec Executor, ttl time.Duration, logger Logger) *Engine {
	t.Helper()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 {
		ttl = 400 * time.Millisecond
	}
	eng := NewEngine(st, exec, logger,
		WithLeaseTTL(ttl), WithPollInterval(15*time.Millisecond))
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func mustRegister(t *testing.T, eng *Engine, def WorkflowDef) {
	t.Helper()
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
}

// TestMultiProcessConcurrentTrigger 多个“进程”同时对同一工作流发起同一 runID：
// 恰好一个进程创建运行；每个任务全程只被执行一次；所有进程等到同一个终态，
// 且各自读到的结果视图逐字节一致。
func TestMultiProcessConcurrentTrigger(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	const n = 6
	var execCount int64

	def := WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "b", Dependencies: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}}
	exec := MapExecutor{
		"a": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			atomic.AddInt64(&execCount, 1)
			time.Sleep(30 * time.Millisecond) // 拉开租约竞争窗口
			return AttemptResult{}
		},
		"b": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			atomic.AddInt64(&execCount, 1)
			return AttemptResult{}
		},
	}

	engines := make([]*Engine, n)
	for i := range engines {
		engines[i] = newProc(t, dir, exec, 0)
		mustRegister(t, engines[i], def)
	}

	var created, existed int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(eng *Engine) {
			defer wg.Done()
			_, ex, err := eng.Trigger(context.Background(), "wf", "shared-run")
			if err != nil {
				t.Errorf("trigger: %v", err)
				return
			}
			if ex {
				atomic.AddInt64(&existed, 1)
			} else {
				atomic.AddInt64(&created, 1)
			}
		}(engines[i])
	}
	wg.Wait()
	if created != 1 || existed != n-1 {
		t.Fatalf("created=%d existed=%d，并发触发必须恰好创建一次", created, existed)
	}

	// 所有进程（无论是否持有租约）都等到同一个终态。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	statuses := make([]RunStatus, n)
	for i := range engines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := engines[i].RunAndWait(ctx, "shared-run")
			if err != nil {
				t.Errorf("RunAndWait: %v", err)
				return
			}
			statuses[i] = s
		}(i)
	}
	wg.Wait()
	for i, s := range statuses {
		if s != RunSucceeded {
			t.Fatalf("engine %d status=%s want SUCCEEDED", i, s)
		}
	}
	if got := atomic.LoadInt64(&execCount); got != 2 {
		t.Fatalf("executions=%d want 2：同一任务绝不能被多个进程执行两遍", got)
	}

	// 每个进程读到的结果视图必须完全一致（可重复、可跨进程共享）。
	var want string
	for i, eng := range engines {
		snap, err := eng.Result(context.Background(), "shared-run")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(snap)
		if i == 0 {
			want = string(b)
			continue
		}
		if string(b) != want {
			t.Fatalf("进程 %d 读到的结果视图与进程 0 不一致:\n%s\nvs\n%s", i, b, want)
		}
	}
}

// TestLeaseExpiryTakeover 持有者掉线（租约到期未续）后，
// 另一个进程在合理时间内接管并把运行跑完；接管不重复执行已成功的任务。
func TestLeaseExpiryTakeover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")

	// 构造一份“死进程”遗留的状态：任务 a 已成功，任务 t 在飞（RUNNING），
	// 租约属于 dead-owner 且 300ms 后到期。
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := timeNow()
	rs := &RunState{
		WorkflowName: "wf", RunID: "orphan", Status: RunRunning,
		Tasks: map[string]*TaskState{
			"a": {WorkflowName: "wf", RunID: "orphan", TaskName: "a",
				Status: StatusSucceeded, Attempt: 1, UpdatedAt: now},
			"t": {WorkflowName: "wf", RunID: "orphan", TaskName: "t",
				Status: StatusRunning, Attempt: 1, Owner: "dead-owner", UpdatedAt: now},
		},
		CreatedAt: now, UpdatedAt: now,
		Lease:     &LeaseInfo{Owner: "dead-owner", Token: 1, Until: now.Add(300 * time.Millisecond)},
		LastToken: 1,
	}
	if _, err := st.CreateRunIfNotExists(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	var aExec, tExec int64
	exec := MapExecutor{
		"a": func(context.Context, string, string, int) AttemptResult {
			atomic.AddInt64(&aExec, 1)
			return AttemptResult{}
		},
		"t": func(context.Context, string, string, int) AttemptResult {
			atomic.AddInt64(&tExec, 1)
			return AttemptResult{}
		},
	}
	eng := newProc(t, dir, exec, 200*time.Millisecond)
	mustRegister(t, eng, WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "t", Dependencies: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 3}},
	}})

	if _, _, err := eng.Trigger(context.Background(), "wf", "orphan"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := eng.RunAndWait(ctx, "orphan")
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if status != RunSucceeded {
		t.Fatalf("status=%s want SUCCEEDED", status)
	}
	// 接管必须等租约到期（约 300ms），但应在合理时间内完成。
	if elapsed < 250*time.Millisecond {
		t.Fatalf("接管发生在租约到期前（%s），违反归属唯一", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("接管耗时 %s，超出合理时间", elapsed)
	}
	if got := atomic.LoadInt64(&aExec); got != 0 {
		t.Fatalf("已成功的任务 a 被重新执行 %d 次", got)
	}
	if got := atomic.LoadInt64(&tExec); got != 1 {
		t.Fatalf("任务 t 被接管后应恰好执行一次，实际 %d", got)
	}

	snap, err := eng.Result(context.Background(), "orphan")
	if err != nil {
		t.Fatal(err)
	}
	tsk := snap.Tasks["t"]
	if tsk.Status != StatusSucceeded || tsk.Attempt != 2 {
		t.Fatalf("t status=%s attempt=%d，中断尝试占第 1 次，接管后应为第 2 次成功",
			tsk.Status, tsk.Attempt)
	}
	if len(tsk.Attempts) != 2 || tsk.Attempts[0].Outcome != OutcomeInterrupted ||
		tsk.Attempts[1].Outcome != OutcomeSucceeded {
		b, _ := json.Marshal(tsk.Attempts)
		t.Fatalf("尝试记录应为 [INTERRUPTED, SUCCEEDED]，实际 %s", b)
	}
	if tsk.Attempts[1].Owner != eng.owner {
		t.Fatalf("接管后的尝试应归属新进程，owner=%s want %s", tsk.Attempts[1].Owner, eng.owner)
	}
}

// TestFencedWritesRejected 验证 fencing token：租约易主后，
// 旧持有者的写入/续期/释放全部失效，新持有者的状态不会被覆盖。
func TestFencedWritesRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T) Store
	}{
		{"memory", func(t *testing.T) Store { return NewMemoryStore() }},
		{"file", func(t *testing.T) Store {
			st, err := NewFileStore(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := tc.open(t)
			ctx := context.Background()
			now := timeNow()
			rs := &RunState{
				WorkflowName: "wf", RunID: "r", Status: RunRunning,
				Tasks: map[string]*TaskState{
					"t": {WorkflowName: "wf", RunID: "r", TaskName: "t",
						Status: StatusReady, UpdatedAt: now},
				},
				CreatedAt: now, UpdatedAt: now,
			}
			if _, err := st.CreateRunIfNotExists(ctx, rs); err != nil {
				t.Fatal(err)
			}

			// A 获取租约。
			ok, snapA, err := st.AcquireRunLease(ctx, "r", "A", 200*time.Millisecond)
			if err != nil || !ok {
				t.Fatalf("A acquire ok=%v err=%v", ok, err)
			}
			tokenA := snapA.Lease.Token

			// A 存活期间 B 拿不到。
			if ok, _, err := st.AcquireRunLease(ctx, "r", "B", 200*time.Millisecond); err != nil || ok {
				t.Fatalf("B 在 A 存活期间不应获取租约 ok=%v err=%v", ok, err)
			}
			// A 的写入与续期有效；错误 token 无效。
			task := &TaskState{WorkflowName: "wf", RunID: "r", TaskName: "t",
				Status: StatusRunning, Attempt: 1, Owner: "A"}
			if ok, err := st.PutTask(ctx, "r", task, tokenA); !ok || err != nil {
				t.Fatalf("A 持约写入应成功 ok=%v err=%v", ok, err)
			}
			if ok, err := st.PutTask(ctx, "r", task, tokenA+99); ok || err != nil {
				t.Fatalf("错误 token 写入应被拒绝 ok=%v err=%v", ok, err)
			}
			if ok, err := st.RenewRunLease(ctx, "r", "A", tokenA, 200*time.Millisecond); !ok || err != nil {
				t.Fatalf("A 续期应成功 ok=%v err=%v", ok, err)
			}
			if ok, _ := st.RenewRunLease(ctx, "r", "A", tokenA+99, 200*time.Millisecond); ok {
				t.Fatal("错误 token 续期应被拒绝")
			}

			// A 掉线（不再续期），租约到期后 B 接管。
			time.Sleep(250 * time.Millisecond)
			ok, snapB, err := st.AcquireRunLease(ctx, "r", "B", time.Second)
			if err != nil || !ok {
				t.Fatalf("B 接管 ok=%v err=%v", ok, err)
			}
			tokenB := snapB.Lease.Token
			if tokenB <= tokenA {
				t.Fatalf("fencing token 必须递增：A=%d B=%d", tokenA, tokenB)
			}

			// A 的迟到写入/续期/释放全部失效。
			stale := &TaskState{WorkflowName: "wf", RunID: "r", TaskName: "t",
				Status: StatusSucceeded, Attempt: 1, Owner: "A"}
			if ok, err := st.PutTask(ctx, "r", stale, tokenA); ok || err != nil {
				t.Fatalf("掉线持有者 A 的迟到写入必须被拒绝 ok=%v err=%v", ok, err)
			}
			if ok, _ := st.RenewRunLease(ctx, "r", "A", tokenA, time.Second); ok {
				t.Fatal("掉线持有者 A 的续期必须被拒绝")
			}
			if err := st.ReleaseRunLease(ctx, "r", "A", tokenA); err != nil {
				t.Fatal(err)
			}
			cur, err := st.GetRun(ctx, "r")
			if err != nil {
				t.Fatal(err)
			}
			if cur.Lease == nil || cur.Lease.Owner != "B" {
				t.Fatal("A 的迟到释放不得影响 B 的租约")
			}
			if cur.Tasks["t"].Status != StatusRunning {
				t.Fatalf("A 的迟到写入不得覆盖状态，t=%s", cur.Tasks["t"].Status)
			}
			// B 的写入有效。
			if ok, err := st.PutTask(ctx, "r", stale, tokenB); !ok || err != nil {
				t.Fatalf("B 持约写入应成功 ok=%v err=%v", ok, err)
			}
		})
	}
}

// TestResultViewCompleteAndConsistent 运行结束后，从“另一个进程”读取结果视图：
// 终态、每任务状态、尝试总次数、每次尝试的结算类别与依据都完整且多次读取一致；
// 运行期间的并发读取也只能看到完整快照，不会读到半截状态。
func TestResultViewCompleteAndConsistent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	var flakyCalls int64
	exec := MapExecutor{
		"flaky": func(context.Context, string, string, int) AttemptResult {
			if atomic.AddInt64(&flakyCalls, 1) < 3 {
				return AttemptResult{Err: fmt.Errorf("boom")}
			}
			return AttemptResult{}
		},
		"slow": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			time.Sleep(200 * time.Millisecond) // 不响应超时取消，迟到结果应被丢弃
			return AttemptResult{}
		},
		"downstream": func(context.Context, string, string, int) AttemptResult {
			return AttemptResult{}
		},
	}
	def := WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "flaky", Retry: RetryPolicy{MaxAttempts: 3, Backoff: 5 * time.Millisecond}},
		{Name: "slow", Timeout: 30 * time.Millisecond, Retry: RetryPolicy{MaxAttempts: 2, Backoff: 5 * time.Millisecond}},
		{Name: "downstream", Dependencies: []string{"slow"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}}
	eng := newProc(t, dir, exec, 0)
	mustRegister(t, eng, def)
	if _, _, err := eng.Trigger(context.Background(), "wf", "view-run"); err != nil {
		t.Fatal(err)
	}

	// 运行期间：另一个“进程”并发读取，只能看到完整快照。
	reader := newProc(t, dir, exec, 0)
	stopRead := make(chan struct{})
	var readErr atomic.Value
	go func() {
		maxAttempt := map[string]int{}
		for {
			select {
			case <-stopRead:
				return
			default:
			}
			snap, err := reader.Result(context.Background(), "view-run")
			if err != nil {
				readErr.Store(err)
				return
			}
			if len(snap.Tasks) != 3 {
				readErr.Store(fmt.Errorf("读到半截状态：tasks=%d", len(snap.Tasks)))
				return
			}
			for name, ts := range snap.Tasks {
				if ts.Attempt < maxAttempt[name] {
					readErr.Store(fmt.Errorf("任务 %s 尝试次数回退：%d < %d", name, ts.Attempt, maxAttempt[name]))
					return
				}
				maxAttempt[name] = ts.Attempt
				settled := 0
				for _, rec := range ts.Attempts {
					if rec.Outcome != OutcomeSkipped { // 跳过记录不占尝试序号
						settled++
					}
				}
				if settled > ts.Attempt {
					readErr.Store(fmt.Errorf("任务 %s 结算记录数 %d 超过已发起尝试 %d",
						name, settled, ts.Attempt))
					return
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := eng.RunAndWait(ctx, "view-run")
	close(stopRead)
	if err != nil {
		t.Fatal(err)
	}
	if e := readErr.Load(); e != nil {
		t.Fatalf("并发读取不一致：%v", e)
	}
	if status != RunFailed {
		t.Fatalf("status=%s want FAILED（slow 永久失败）", status)
	}

	// 从全新“进程”读取结果视图。
	checker := newProc(t, dir, exec, 0)
	snap, err := checker.Result(context.Background(), "view-run")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != RunFailed || !snap.Terminal() {
		t.Fatalf("run status=%s want FAILED", snap.Status)
	}
	if got := snap.TotalAttempts(); got != 5 {
		t.Fatalf("TotalAttempts=%d want 5（flaky 3 + slow 2）", got)
	}

	flaky := snap.Tasks["flaky"]
	if flaky.Status != StatusSucceeded || flaky.Attempt != 3 {
		t.Fatalf("flaky status=%s attempt=%d", flaky.Status, flaky.Attempt)
	}
	wantOutcomes := []AttemptOutcome{OutcomeFailed, OutcomeFailed, OutcomeSucceeded}
	if len(flaky.Attempts) != 3 {
		t.Fatalf("flaky 记录数=%d want 3", len(flaky.Attempts))
	}
	for i, rec := range flaky.Attempts {
		if rec.Outcome != wantOutcomes[i] || rec.Attempt != i+1 || rec.Reason == "" {
			b, _ := json.Marshal(flaky.Attempts)
			t.Fatalf("flaky 第 %d 条记录不符：%s", i, b)
		}
	}

	slow := snap.Tasks["slow"]
	if slow.Status != StatusPermanentlyFailed || slow.Attempt != 2 {
		t.Fatalf("slow status=%s attempt=%d", slow.Status, slow.Attempt)
	}
	for i, rec := range slow.Attempts {
		if rec.Outcome != OutcomeTimedOut || rec.Attempt != i+1 {
			b, _ := json.Marshal(slow.Attempts)
			t.Fatalf("slow 记录应全为 TIMED_OUT：%s", b)
		}
	}

	down := snap.Tasks["downstream"]
	if down.Status != StatusSkipped || down.Attempt != 0 {
		t.Fatalf("downstream status=%s attempt=%d", down.Status, down.Attempt)
	}
	if len(down.Attempts) != 1 || down.Attempts[0].Outcome != OutcomeSkipped ||
		down.Attempts[0].Reason == "" {
		b, _ := json.Marshal(down.Attempts)
		t.Fatalf("downstream 应有一条 SKIPPED 记录：%s", b)
	}

	// 多次读取、跨进程读取结果一致。
	again, err := eng.Result(context.Background(), "view-run")
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.Marshal(snap)
	b2, _ := json.Marshal(again)
	if string(b1) != string(b2) {
		t.Fatalf("两次读取结果视图不一致:\n%s\nvs\n%s", b1, b2)
	}
}

// TestMultiProcessRunsIsolated 某个运行被其他进程卡住时，
// 不影响本进程推进其他运行。
func TestMultiProcessRunsIsolated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	release := make(chan struct{})
	execStuck := MapExecutor{
		"block": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			select {
			case <-release:
				return AttemptResult{}
			case <-ctx.Done():
				<-release
				return AttemptResult{}
			}
		},
	}
	execFast := MapExecutor{
		"block": execStuck["block"],
		"quick": func(context.Context, string, string, int) AttemptResult { return AttemptResult{} },
	}
	engA := newProc(t, dir, execStuck, 0)
	mustRegister(t, engA, WorkflowDef{Name: "wf-stuck", Tasks: []TaskDef{
		{Name: "block", Retry: RetryPolicy{MaxAttempts: 1}},
	}})
	engB := newProc(t, dir, execFast, 0)
	mustRegister(t, engB, WorkflowDef{Name: "wf-fast", Tasks: []TaskDef{
		{Name: "quick", Retry: RetryPolicy{MaxAttempts: 1}},
	}})

	if _, _, err := engA.Trigger(context.Background(), "wf-stuck", "stuck-run"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engB.Trigger(context.Background(), "wf-fast", "fast-run"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// stuck-run 被 engA 持有且任务阻塞期间，fast-run 必须能独立完成。
	if s, err := engB.RunAndWait(ctx, "fast-run"); err != nil || s != RunSucceeded {
		t.Fatalf("fast-run status=%s err=%v，卡住的运行不得拖住其他运行", s, err)
	}
	snap, _ := engB.Result(context.Background(), "stuck-run")
	if snap.Status != RunRunning {
		t.Fatalf("stuck-run 应仍在运行，实际 %s", snap.Status)
	}

	close(release)
	if s, err := engA.RunAndWait(ctx, "stuck-run"); err != nil || s != RunSucceeded {
		t.Fatalf("stuck-run status=%s err=%v", s, err)
	}
}
