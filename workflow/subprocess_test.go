package workflow

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// helperWorkflow 是 helper 子进程与父进程共用的一份工作流定义：
// a 快速成功（向 exec.log 追加一行，用于跨进程统计真实执行次数），
// block 依赖 a，在 helper 中永久阻塞，在父进程中立即成功。
func helperWorkflow() WorkflowDef {
	return WorkflowDef{Name: "wf-kill", Tasks: []TaskDef{
		{Name: "a", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "block", Dependencies: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 3}},
	}}
}

// appendExecLog 模拟任务的真实副作用：向共享日志追加一行。
// 该文件即“跨进程执行计数器”，用于验证成功任务不会被重复执行。
func appendExecLog(dir, line string) {
	f, err := os.OpenFile(filepath.Join(dir, "exec.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	fmt.Fprintln(f, line)
	_ = f.Close()
}

// TestHelperProcess 不是普通测试：它是被 TestTakeoverAfterSIGKILL 以
// 子进程方式启动的“worker 进程”。通过环境变量启用，正常 go test 时直接返回。
// 子进程触发运行后阻塞在 block 任务里，等待父进程 SIGKILL。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("WF_HELPER") != "1" {
		return
	}
	dir := os.Getenv("WF_DIR")
	runID := os.Getenv("WF_RUN")
	st, err := NewFileStore(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	eng := NewEngine(st, MapExecutor{
		"a": func(context.Context, string, string, int) AttemptResult {
			appendExecLog(dir, "a")
			return AttemptResult{}
		},
		"block": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			// 告知父进程“第 1 次尝试已在飞”，随后永久阻塞（不响应取消），
			// 模拟真实世界里卡在外部调用上的任务。
			fmt.Println("HELPER_BLOCK_STARTED")
			select {}
		},
	}, NewLogger(io.Discard), WithLeaseTTL(400*time.Millisecond), WithPollInterval(15*time.Millisecond))
	if err := eng.Register(helperWorkflow()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if _, _, err := eng.Trigger(context.Background(), "wf-kill", runID); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	select {} // 等待被 SIGKILL
}

// TestTakeoverAfterSIGKILL 端到端验证：一个真实进程在任务在飞时被 kill -9，
// 另一个进程在租约到期后接管并把运行跑完；
// 已成功任务不重复执行，中断尝试记为 INTERRUPTED 且占用尝试额度。
func TestTakeoverAfterSIGKILL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	runID := "kill-run"

	// --- 启动并强杀第一个 worker 进程 ---
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	cmd.Env = append(os.Environ(), "WF_HELPER=1", "WF_DIR="+dir, "WF_RUN="+runID)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "HELPER_BLOCK_STARTED" {
				close(started)
				return
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("helper 进程迟迟未进入 block 任务")
	}
	if err := cmd.Process.Kill(); err != nil { // SIGKILL，不给任何清理机会
		t.Fatal(err)
	}
	_ = cmd.Wait()

	// 强杀瞬间：磁盘上 block 必须是 RUNNING（结果未知），绝不能是成功。
	dead, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := dead.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Tasks["block"].Status != StatusRunning {
		t.Fatalf("强杀后 block=%s，中断中的任务不得被判定为成功", snap.Tasks["block"].Status)
	}
	if snap.Lease == nil {
		t.Fatal("强杀后租约应仍归死进程持有（未释放）")
	}
	_ = dead.Close()

	// --- 第二个进程接管 ---
	eng := newProc(t, dir, MapExecutor{
		"a": func(context.Context, string, string, int) AttemptResult {
			appendExecLog(dir, "a")
			return AttemptResult{}
		},
		"block": func(context.Context, string, string, int) AttemptResult {
			appendExecLog(dir, "block")
			return AttemptResult{}
		},
	}, 400*time.Millisecond)
	mustRegister(t, eng, helperWorkflow())

	if _, existed, err := eng.Trigger(context.Background(), "wf-kill", runID); err != nil {
		t.Fatal(err)
	} else if !existed {
		t.Fatal("接管时 Trigger 必须命中既有运行（existed=true）")
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status, err := eng.RunAndWait(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if status != RunSucceeded {
		t.Fatalf("接管后 status=%s want SUCCEEDED", status)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("接管耗时 %s，超出合理时间", elapsed)
	}

	// 已成功任务 a 全程只被执行一次（跨进程副作用计数）。
	logBytes, err := os.ReadFile(filepath.Join(dir, "exec.log"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(string(logBytes))
	countA, countBlock := 0, 0
	for _, ln := range lines {
		switch ln {
		case "a":
			countA++
		case "block":
			countBlock++
		}
	}
	if countA != 1 {
		t.Fatalf("任务 a 被执行 %d 次，已成功任务跨进程不得重复执行", countA)
	}
	if countBlock != 1 {
		t.Fatalf("任务 block 被接管后应恰好执行一次，实际 %d", countBlock)
	}

	// 结果视图：中断尝试记 INTERRUPTED（占第 1 次额度），接管尝试成功。
	final, err := eng.Result(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	blk := final.Tasks["block"]
	if blk.Status != StatusSucceeded || blk.Attempt != 2 {
		t.Fatalf("block status=%s attempt=%d want SUCCEEDED/2", blk.Status, blk.Attempt)
	}
	if len(blk.Attempts) != 2 || blk.Attempts[0].Outcome != OutcomeInterrupted ||
		blk.Attempts[1].Outcome != OutcomeSucceeded {
		b, _ := json.Marshal(blk.Attempts)
		t.Fatalf("block 尝试记录应为 [INTERRUPTED, SUCCEEDED]，实际 %s", b)
	}
	if final.TotalAttempts() != 3 { // a 1 次 + block 2 次
		t.Fatalf("TotalAttempts=%d want 3", final.TotalAttempts())
	}
}
