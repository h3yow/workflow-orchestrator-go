package workflow

import (
	"bytes"
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecoveryAfterCrash 模拟“进程中断重启”：
//  1. 使用 FileStore；任务 a 瞬间成功；任务 hang 阻塞不返回；任务 later 依赖 hang；
//  2. 第一个引擎运行期间 Close（等同进程中断），hang 的尝试停在 RUNNING（无结果）；
//  3. 第二个引擎基于落盘状态恢复：a 已成功，绝不能再执行；
//     hang 的中断尝试不得判定为成功，重新调度后放行完成；later 随后成功。
func TestRecoveryAfterCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	var aExecutions int32

	releaseHang := make(chan struct{})

	makeExec := func() Executor {
		return MapExecutor{
			"a": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
				atomic.AddInt32(&aExecutions, 1)
				return AttemptResult{}
			},
			"hang": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
				// 第一个引擎生命周期内：不响应 ctx 也不结束（模拟执行体卡在外部系统）。
				select {
				case <-releaseHang:
					return AttemptResult{}
				case <-ctx.Done():
					// 中断时即使 ctx 取消，也不立刻返回，模拟强杀进程后“结果未知”。
					<-releaseHang
					return AttemptResult{}
				}
			},
			"later": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
				return AttemptResult{}
			},
		}
	}

	def := WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "hang", Retry: RetryPolicy{MaxAttempts: 3}},
		{Name: "later", Dependencies: []string{"hang"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}}

	// --- 第一个“进程” ---
	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := NewEngine(store1, makeExec(), NewLogger(&bytes.Buffer{}))
	if err := eng1.Register(def); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng1.Trigger(context.Background(), "wf", "resume-run"); err != nil {
		t.Fatal(err)
	}

	// 等到 a 已成功、hang 已进入 RUNNING。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap, _ := store1.GetRun(context.Background(), "resume-run")
		if snap.Tasks["a"].Status == StatusSucceeded && snap.Tasks["hang"].Status == StatusRunning {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	snap, _ := store1.GetRun(context.Background(), "resume-run")
	if snap.Tasks["a"].Status != StatusSucceeded || snap.Tasks["hang"].Status != StatusRunning {
		t.Fatalf("中断前状态不符: a=%s hang=%s", snap.Tasks["a"].Status, snap.Tasks["hang"].Status)
	}
	if snap.Tasks["hang"].Attempt != 1 {
		t.Fatalf("hang attempt before crash = %d want 1", snap.Tasks["hang"].Attempt)
	}

	// 模拟进程被中断：直接关闭（Close 取消在飞 ctx，但卡住的执行体不会写结果）。
	_ = eng1.Close()
	_ = store1.Close()

	// 落盘状态中 hang 必须仍是中断态 RUNNING，不能是成功。
	store1b, _ := NewFileStore(dir)
	onDisk, _ := store1b.GetRun(context.Background(), "resume-run")
	if onDisk.Tasks["hang"].Status != StatusRunning {
		t.Fatalf("落盘 hang=%s，中断中的任务不得被判定为成功", onDisk.Tasks["hang"].Status)
	}
	if onDisk.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("落盘 a=%s want SUCCEEDED", onDisk.Tasks["a"].Status)
	}
	_ = store1b.Close()

	// --- 第二个“进程”：从既有状态恢复 ---
	store2, _ := NewFileStore(dir)
	var log2 bytes.Buffer
	eng2 := NewEngine(store2, makeExec(), NewLogger(&log2))
	if err := eng2.Register(def); err != nil {
		t.Fatal(err)
	}
	if _, existed, err := eng2.Trigger(context.Background(), "wf", "resume-run"); err != nil {
		t.Fatal(err)
	} else if !existed {
		t.Fatal("恢复时 Trigger 必须命中既有运行（existed=true）")
	}

	// 放行 hang，随后恢复循环应重新发起尝试。
	close(releaseHang)
	status, err := eng2.RunAndWait(context.Background(), "resume-run")
	if err != nil {
		t.Fatal(err)
	}
	if status != RunSucceeded {
		t.Fatalf("恢复后 status=%s want SUCCEEDED", status)
	}
	if got := atomic.LoadInt32(&aExecutions); got != 1 {
		t.Fatalf("已成功的任务 a 被执行了 %d 次，恢复后不得再次执行", got)
	}
	final, _ := store2.GetRun(context.Background(), "resume-run")
	if final.Tasks["hang"].Status != StatusSucceeded {
		t.Fatalf("hang=%s want SUCCEEDED", final.Tasks["hang"].Status)
	}
	if final.Tasks["hang"].Attempt < 2 {
		t.Fatalf("hang attempt=%d，中断尝试占第 1 次，恢复后应发起第 2 次", final.Tasks["hang"].Attempt)
	}
	if !bytes.Contains(log2.Bytes(), []byte(DecisionRecoverRunning)) {
		t.Fatalf("恢复日志缺少 RECOVER_RUNNING 决策:\n%s", log2.String())
	}
	t.Logf("恢复决策日志:\n%s", log2.String())
	_ = eng2.Close()
}

// TestRecoveryExhaustedDuringCrash 崩溃发生在“末次失败已落盘、永久失败终态未落盘”
// 的窗口时，恢复后必须收敛到确定终态，不得额外补跑。
func TestRecoveryExhaustedDuringCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	st, err := initialRunOnDisk(dir, "wf", "r", []string{"t"},
		func(name string) *TaskState {
			return &TaskState{
				WorkflowName: "wf", RunID: "r", TaskName: name,
				Status: StatusFailed, Attempt: 2, // MaxAttempts=2，已耗尽
				LastError: "boom",
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	var called int32
	eng := NewEngine(st, MapExecutor{"t": func(context.Context, string, string, int) AttemptResult {
		atomic.AddInt32(&called, 1)
		return AttemptResult{}
	}}, NewLogger(&bytes.Buffer{}))
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 2}},
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
	if atomic.LoadInt32(&called) != 0 {
		t.Fatalf("额度已在崩溃前耗尽，恢复后不得补跑，called=%d", called)
	}
	snap, _ := st.GetRun(context.Background(), "r")
	if snap.Tasks["t"].Status != StatusPermanentlyFailed {
		t.Fatalf("t=%s want PERMANENTLY_FAILED", snap.Tasks["t"].Status)
	}
	_ = eng.Close()
}
