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
	"syscall"
	"testing"
	"time"
)

// helperWorkflow 是 helper 子进程与父进程共用的一份工作流定义：
// a 快速成功（向 exec.log 追加一行，用于跨进程统计真实执行次数），
// block 依赖 a，在 kill 模式中永久阻塞，在 stall 模式中“先报告在飞、
// 等待外部放行后再成功”。
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

// waitForFileGate 轮询等待 <dir>/<name> 哨兵文件出现：
// 用于在 SIGSTOP 场景下由父进程“放行”已暂停进程的任务
// （被暂停的进程无法响应 channel/管道信号，只能靠文件系统 + SIGCONT）。
func waitForFileGate(dir, name string) {
	gate := filepath.Join(dir, name)
	for {
		if _, err := os.Stat(gate); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestHelperProcess 不是普通测试：它是被真实多进程测试以子进程方式启动的
// “worker 进程”。通过环境变量启用，正常 go test 时直接返回。
//
//   - WF_MODE=kill（默认）：block 永久阻塞，等待父进程 SIGKILL；
//   - WF_MODE=stall：block 报告在飞后等待 release 哨兵文件，
//     期间父进程会发送 SIGSTOP 真暂停本进程（心跳也被内核冻结），
//     SIGCONT + 哨兵文件放行后 block 返回成功。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("WF_HELPER") != "1" {
		return
	}
	dir := os.Getenv("WF_DIR")
	runID := os.Getenv("WF_RUN")
	mode := os.Getenv("WF_MODE")
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
			// 告知父进程“第 1 次尝试已在飞”。
			fmt.Println("HELPER_BLOCK_STARTED")
			if mode == "stall" {
				// 等待父进程在 SIGCONT 后落下 release 哨兵，然后正常成功。
				waitForFileGate(dir, "release")
				appendExecLog(dir, "block-helper")
				fmt.Println("HELPER_BLOCK_DONE")
				return AttemptResult{}
			}
			// kill 模式：永久阻塞（不响应取消），
			// 模拟真实世界里卡在外部调用上的任务，等待被 SIGKILL。
			select {}
		},
	}, NewLogger(io.Discard), WithLeaseTTL(300*time.Millisecond), WithPollInterval(15*time.Millisecond))
	if err := eng.Register(helperWorkflow()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if _, _, err := eng.Trigger(context.Background(), "wf-kill", runID); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if mode == "stall" {
		// 等运行跑到终态后退出，便于父进程确认“由原进程自己跑完”。
		for {
			snap, err := eng.Result(context.Background(), runID)
			if err == nil && snap.Status != RunRunning {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	select {} // kill 模式：等待被 SIGKILL
}

// startHelper 以子进程方式启动一个 worker，mode 为 "kill" 或 "stall"。
func startHelper(t *testing.T, dir, runID, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	cmd.Env = append(os.Environ(),
		"WF_HELPER=1", "WF_DIR="+dir, "WF_RUN="+runID, "WF_MODE="+mode)
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
			line := strings.TrimSpace(sc.Text())
			if line == "HELPER_BLOCK_STARTED" {
				select {
				case <-started:
				default:
					close(started)
				}
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("helper 进程迟迟未进入 block 任务")
	}
	return cmd
}

// execLogCounts 读取跨进程副作用计数文件，返回各标记的出现次数。
func execLogCounts(t *testing.T, dir string) map[string]int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "exec.log"))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, ln := range strings.Fields(string(b)) {
		counts[ln]++
	}
	return counts
}

// TestTakeoverAfterSIGKILL 端到端验证：一个真实进程在任务在飞时被 kill -9，
// 另一个进程在租约到期后接管并把运行跑完；
// 已成功任务不重复执行，中断尝试记为 INTERRUPTED 且占用尝试额度。
// 多轮重复（独立目录）以排除偶发时序问题。
func TestTakeoverAfterSIGKILL(t *testing.T) {
	const rounds = 3
	for round := 0; round < rounds; round++ {
		t.Run("", func(t *testing.T) {
			runTakeoverAfterSIGKILL(t, round)
		})
	}
}

func runTakeoverAfterSIGKILL(t *testing.T, round int) {
	dir := filepath.Join(t.TempDir(), "state")
	runID := "kill-run"

	// --- 启动并强杀第一个 worker 进程 ---
	cmd := startHelper(t, dir, runID, "kill")
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil { // kill -9，不给清理机会
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
	}, 300*time.Millisecond)
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
	elapsed := time.Since(start)
	if status != RunSucceeded {
		t.Fatalf("接管后 status=%s want SUCCEEDED", status)
	}
	// 心跳 TTL=300ms：接管既要等 TTL 到期，也要确认存活锁已随进程死亡释放。
	if elapsed < 250*time.Millisecond {
		t.Fatalf("接管发生在租约到期前（%s），违反归属唯一", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("接管耗时 %s，强杀后应在合理时间内被接手", elapsed)
	}

	// 副作用计数：已成功任务 a 全程 1 次；block 接管后恰好 1 次。
	counts := execLogCounts(t, dir)
	if counts["a"] != 1 {
		t.Fatalf("任务 a 被执行 %d 次，已成功任务跨进程不得重复执行", counts["a"])
	}
	if counts["block"] != 1 {
		t.Fatalf("任务 block 被接管后应恰好执行一次，实际 %d", counts["block"])
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
	t.Logf("round=%d | 覆盖任务 a,block：kill -9 后等待 %s 接管，a 执行 %d 次（不重跑）、"+
		"block 执行 %d 次（[INTERRUPTED,SUCCEEDED]），终态=%s",
		round, elapsed, counts["a"], counts["block"], final.Status)
}

// TestPausedProcessNotTakenOver 端到端验证最关键的区分能力：
// 一个真实 worker 进程在任务在飞时被 SIGSTOP 整体暂停——连心跳 goroutine
// 都被内核冻结（租约心跳远超 TTL），但进程并没有退出，存活 flock 仍由内核持有。
// 另一个进程在此期间反复尝试接管，必须拿不到租约、绝不能重跑任务；
// 待 SIGCONT 恢复并放行任务后，由原进程自己把唯一一次尝试跑完。
// 多轮重复（独立目录），-race 下须稳定通过。
func TestPausedProcessNotTakenOver(t *testing.T) {
	const rounds = 3
	for round := 0; round < rounds; round++ {
		t.Run("", func(t *testing.T) {
			runPausedProcessNotTakenOver(t, round)
		})
	}
}

func runPausedProcessNotTakenOver(t *testing.T, round int) {
	dir := filepath.Join(t.TempDir(), "state")
	runID := "stall-run"

	// 1) 启动 helper（stall 模式），等 block 任务在飞。
	cmd := startHelper(t, dir, runID, "stall")
	defer func() {
		// 兜底清理：恢复并等待退出，避免悬挂子进程。
		_ = cmd.Process.Signal(syscall.SIGCONT)
		_ = cmd.Wait()
	}()

	// 2) SIGSTOP 整体暂停：心跳冻结，TTL=300ms，静默 1.5s ≫ TTL。
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)

	// 3) 接管候选进程在暂停窗口内反复触发/轮询。
	eng := newProc(t, dir, MapExecutor{
		"a": func(context.Context, string, string, int) AttemptResult {
			t.Error("已成功任务 a 在原进程暂停期间不得被重跑")
			appendExecLog(dir, "a-takeover")
			return AttemptResult{}
		},
		"block": func(context.Context, string, string, int) AttemptResult {
			t.Error("原进程仍存活（SIGSTOP），block 绝不能被别的进程重跑")
			appendExecLog(dir, "block-takeover")
			return AttemptResult{}
		},
	}, 300*time.Millisecond)
	mustRegister(t, eng, helperWorkflow())

	if _, existed, err := eng.Trigger(context.Background(), "wf-kill", runID); err != nil {
		t.Fatal(err)
	} else if !existed {
		t.Fatal("命中既有运行时 Trigger 必须返回 existed=true")
	}

	// 在暂停期间持续轮询：运行必须仍是 RUNNING、租约仍属原进程、
	// block 必须仍是 RUNNING、任何接管方副作用都不得出现。
	probeDeadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(probeDeadline) {
		snap, err := eng.Result(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status != RunRunning {
			t.Fatalf("原进程暂停期间运行被推进到终态 %s，被错误接管", snap.Status)
		}
		if snap.Lease == nil {
			t.Fatal("暂停期间租约不应消失")
		}
		if blk := snap.Tasks["block"]; blk.Status != StatusRunning {
			t.Fatalf("暂停期间 block 状态被改动：%s", blk.Status)
		}
		counts := execLogCounts(t, dir)
		if counts["block-takeover"] != 0 || counts["block-helper"] != 0 {
			t.Fatalf("暂停期间发生了 block 执行：helper=%d takeover=%d",
				counts["block-helper"], counts["block-takeover"])
		}
		if counts["a"] != 1 || counts["a-takeover"] != 0 {
			t.Fatalf("暂停期间任务 a 被重跑：a=%d a-takeover=%d",
				counts["a"], counts["a-takeover"])
		}
		time.Sleep(25 * time.Millisecond)
	}

	// 4) SIGCONT 恢复进程，并落下放行哨兵让它手上的 block 正常成功。
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	gate, err := os.OpenFile(filepath.Join(dir, "release"),
		os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_ = gate.Close()

	// 5) 原进程自行跑完；接管候选也应读到同一个 SUCCEEDED。
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status, err := eng.RunAndWait(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if status != RunSucceeded {
		t.Fatalf("恢复后 status=%s want SUCCEEDED", status)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper 应正常退出：%v", err)
	}

	// 副作用：a 一次、block 由原进程执行一次，接管方零次。
	counts := execLogCounts(t, dir)
	if counts["a"] != 1 || counts["a-takeover"] != 0 {
		t.Fatalf("任务 a 执行计数异常：a=%d a-takeover=%d", counts["a"], counts["a-takeover"])
	}
	if counts["block-helper"] != 1 || counts["block-takeover"] != 0 {
		t.Fatalf("block 必须只由恢复后的原进程执行一次：helper=%d takeover=%d",
			counts["block-helper"], counts["block-takeover"])
	}

	final, err := eng.Result(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	blk := final.Tasks["block"]
	if blk.Status != StatusSucceeded || blk.Attempt != 1 {
		t.Fatalf("block status=%s attempt=%d want SUCCEEDED/1（原有尝试恢复后应直接成功，不得记 INTERRUPTED）",
			blk.Status, blk.Attempt)
	}
	if len(blk.Attempts) != 1 || blk.Attempts[0].Outcome != OutcomeSucceeded {
		b, _ := json.Marshal(blk.Attempts)
		t.Fatalf("暂停恢复后 block 应只有一条 SUCCEEDED，实际 %s", b)
	}
	t.Logf("round=%d | 覆盖任务 a,block：SIGSTOP 静默 %s（≫TTL=300ms）期间零接管、零重跑；"+
		"SIGCONT 后原进程把 block 第 1 次尝试跑完，attempts=[SUCCEEDED]，终态=%s；"+
		"副作用计数 a=1 block-helper=1 block-takeover=0",
		round, 1500*time.Millisecond, final.Status)
}
