package workflow

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// syncWriter 是线程安全的日志缓冲：调度 goroutine 与测试主 goroutine
// 会并发访问，用互斥保护 bytes.Buffer。
type syncWriter struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// String 线程安全地返回当前缓冲内容（测试断言用）。
func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// Contains 线程安全地判断缓冲是否包含子串。
func (w *syncWriter) Contains(s string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Contains(w.buf.Bytes(), []byte(s))
}

var _ io.Writer = (*syncWriter)(nil)

// startFrozenHelper 启动一个 freeze 模式的 helper worker：
// 它触发运行后卡在 block 任务（第 1 次尝试在飞），打印 HELPER_FROZEN。
// 返回的进程随后会被父进程 SIGSTOP。
func startFrozenHelper(t *testing.T, dir, runID string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	cmd.Env = append(os.Environ(),
		"WF_HELPER=1", "WF_HELPER_MODE=freeze",
		"WF_DIR="+dir, "WF_RUN="+runID)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	frozen := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "HELPER_FROZEN" {
				close(frozen)
				return
			}
		}
	}()
	select {
	case <-frozen:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("helper 进程迟迟未进入 block 任务")
	}
	return cmd
}

// secondProcExec 构造接管侧执行器：记录执行次数并向 exec.log 追加副作用。
func secondProcExec(t *testing.T, dir string) (Executor, *int64) {
	t.Helper()
	var blockCalls int64
	exec := MapExecutor{
		"a": func(context.Context, string, string, int) AttemptResult {
			appendExecLog(dir, "a")
			return AttemptResult{}
		},
		"block": func(context.Context, string, string, int) AttemptResult {
			atomic.AddInt64(&blockCalls, 1)
			appendExecLog(dir, "block")
			return AttemptResult{}
		},
	}
	return exec, &blockCalls
}

// TestTakeoverBlockedWhileOwnerFrozen 端到端核心场景：
// 执行方进程在任务在飞时被整个暂停（SIGSTOP，模拟机器卡顿/进程被挂起：
// 任务没动静、心跳也停了），租约因此到期；但进程其实还活着。
//
// 另一个进程在多个租约周期内持续尝试接管，必须全部被存活锁挡回——
// 同一个任务绝不能被第二进程重跑。恢复（SIGCONT）后由原进程自己跑完，
// block 全程只有一次真实执行；随后第二进程读到的终态为 SUCCEEDED。
func TestTakeoverBlockedWhileOwnerFrozen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	runID := "frozen-run"

	cmd := startFrozenHelper(t, dir, runID)

	// 在租约仍新鲜时暂停进程（helper TTL=400ms，心跳间隔约 133ms）；
	// 暂停后其心跳 goroutine 一并冻结，租约随后会在冻结期间到期——
	// 但进程本身仍存活，这正是要区分的关键场景。
	time.Sleep(150 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP: %v", err)
	}
	t.Cleanup(func() {
		// 兜底：避免残留子进程。
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
	})

	// 第二进程在“租约已到期但持有者仍存活”期间反复尝试接管。
	// 用 tee 日志器同时在 -v 下输出决策流并保留缓冲供断言。
	var logBuf bytes.Buffer
	tee := &syncWriter{buf: &logBuf}
	exec, blockCalls := secondProcExec(t, dir)
	eng := newProcWithLogger(t, dir, exec, 200*time.Millisecond, NewLogger(tee))
	mustRegister(t, eng, helperWorkflow())

	if _, existed, err := eng.Trigger(context.Background(), "wf-kill", runID); err != nil {
		t.Fatal(err)
	} else if !existed {
		t.Fatal("frozen 期间 Trigger 必须命中既有运行（existed=true）")
	}

	// 后台持续等待：RunAndWait 会以 pollInterval（15ms）反复尝试获取租约，
	// 因此在“租约到期但持有者仍存活”的整个窗口里都会触发存活探测与接管判定。
	type waitResult struct {
		status RunStatus
		err    error
	}
	waitCh := make(chan waitResult, 1)
	go func() {
		s, err := eng.RunAndWait(context.Background(), runID)
		waitCh <- waitResult{s, err}
	}()

	// 冻结约 1.6s（helper TTL=400ms，覆盖租约到期后的多个周期），
	// 期间租约会到期，但第二进程的接管必须被存活锁挡回。
	waitStart := time.Now()
	var sawExpiredBlocked bool
	for time.Since(waitStart) < 1600*time.Millisecond {
		snap, err := eng.Result(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Tasks["block"].Status != StatusRunning {
			t.Fatalf("frozen 期间 block 状态=%s，必须仍是原进程的在飞 RUNNING，不得被接管",
				snap.Tasks["block"].Status)
		}
		if snap.Lease != nil && snap.Lease.expired(eng.nowFunc()) {
			sawExpiredBlocked = true // 已进入“租约到期但持有者仍存活”的关键窗口
		}
		if got := atomic.LoadInt64(blockCalls); got != 0 {
			t.Fatalf("原进程仍存活（仅暂停）时，第二进程不得重跑 block，实际执行 %d 次", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawExpiredBlocked {
		t.Fatal("测试未观察到“租约到期”窗口：调大冻结时长或检查心跳时序")
	}
	if !tee.Contains(DecisionOwnerAliveUnresponsive) {
		t.Fatalf("frozen 期间应记录 OWNER_ALIVE_UNRESPONSIVE（存活锁确认进程仍存活、拒绝接管）:\n%s", tee.String())
	}
	t.Logf("frozen 期间第二进程决策日志（应只见等待、无 LAUNCH_ATTEMPT）:\n%s", tee.String())
	if tee.Contains(DecisionLaunchAttempt) {
		t.Fatalf("原进程仍存活时第二进程不应发起任何尝试:\n%s", tee.String())
	}

	// 恢复原进程：它手上的 block 尝试依旧永久阻塞（helper 实现 select{}），
	// 因此这里验证第二进程在原进程“恢复并继续心跳”期间依然不能接管。
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT: %v", err)
	}
	resumeStart := time.Now()
	for time.Since(resumeStart) < 600*time.Millisecond {
		if got := atomic.LoadInt64(blockCalls); got != 0 {
			t.Fatalf("原进程恢复后仍由其负责，第二进程不得重跑，实际 %d 次", got)
		}
		select {
		case wr := <-waitCh:
			t.Fatalf("原进程仍存活且任务在飞时，运行不应提前收尾：%+v", wr)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 此时原进程存活且 block 仍在飞（它内部 select{} 永不返回），运行无法自行收尾。
	// 为验证“恢复后由原进程负责、第二进程始终不执行”，让原进程彻底退出（SIGKILL）：
	// 之后第二进程才能接管，并只执行一次 block 收尾。
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait()

	select {
	case wr := <-waitCh:
		if wr.err != nil {
			t.Fatal(wr.err)
		}
		if wr.status != RunSucceeded {
			t.Fatalf("强杀接管后 status=%s want SUCCEEDED", wr.status)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("强杀后第二进程未在合理时间内接管收尾")
	}
	if got := atomic.LoadInt64(blockCalls); got != 1 {
		t.Fatalf("block 被第二进程接管后应恰好执行一次，实际 %d 次", got)
	}

	final, err := eng.Result(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	blk := final.Tasks["block"]
	if blk.Status != StatusSucceeded {
		t.Fatalf("block 终态=%s want SUCCEEDED", blk.Status)
	}
	// 原进程那次在飞尝试记 INTERRUPTED（占第 1 次额度），第二进程成功为第 2 次。
	if len(blk.Attempts) != 2 || blk.Attempts[0].Outcome != OutcomeInterrupted ||
		blk.Attempts[1].Outcome != OutcomeSucceeded {
		t.Fatalf("block 尝试记录应为 [INTERRUPTED, SUCCEEDED]，实际 %+v", blk.Attempts)
	}

	// 跨进程副作用计数：a 一次（helper 已完成），block 一次（仅接管方）。
	logBytes, err := os.ReadFile(filepath.Join(dir, "exec.log"))
	if err != nil {
		t.Fatal(err)
	}
	countA, countBlock := 0, 0
	for _, ln := range strings.Fields(string(logBytes)) {
		switch ln {
		case "a":
			countA++
		case "block":
			countBlock++
		}
	}
	if countA != 1 || countBlock != 1 {
		t.Fatalf("副作用次数 a=%d block=%d，都必须恰为 1（绝不能跑两遍）", countA, countBlock)
	}
	t.Logf("覆盖任务：a(原进程成功一次)、block(原进程在飞→暂停→恢复→强杀，接管方执行一次)；" +
		"终态=SUCCEEDED，副作用 a=1 block=1")
}

// startHelperWithMode 启动指定模式（freeze/recover）的 helper worker。
func startHelperWithMode(t *testing.T, mode, dir, runID string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	cmd.Env = append(os.Environ(),
		"WF_HELPER=1", "WF_HELPER_MODE="+mode,
		"WF_DIR="+dir, "WF_RUN="+runID)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	started, done := make(chan struct{}), make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			done <- line
			if line == "HELPER_FROZEN" || line == "HELPER_BLOCK_STARTED" {
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

// TestOwnerRecoversAfterFreeze 端到端核心场景（需求第一类）：
// 执行方进程在任务在飞时被整个暂停（SIGSTOP：任务无响应、心跳也停了），
// 租约在暂停期间到期；另一个进程多次尝试接管都被存活锁挡回，绝不重跑。
// 随后原进程恢复（SIGCONT + 外部依赖恢复），由它自己把唯一一次在飞尝试跑完，
// 运行收敛到 SUCCEEDED；第二进程在整个过程中一次 block 都没有执行。
func TestOwnerRecoversAfterFreeze(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	runID := "recover-run"

	cmd := startHelperWithMode(t, "recover", dir, runID)
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
	})

	// 租约仍新鲜时暂停整个进程，让租约在冻结期间到期。
	time.Sleep(150 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP: %v", err)
	}

	// 第二进程在整个冻结窗口持续尝试接管。
	var logBuf bytes.Buffer
	tee := &syncWriter{buf: &logBuf}
	exec, blockCalls := secondProcExec(t, dir)
	eng := newProcWithLogger(t, dir, exec, 200*time.Millisecond, NewLogger(tee))
	mustRegister(t, eng, helperWorkflow())
	if _, existed, err := eng.Trigger(context.Background(), "wf-kill", runID); err != nil {
		t.Fatal(err)
	} else if !existed {
		t.Fatal("必须命中既有运行（existed=true）")
	}
	type waitResult struct {
		s   RunStatus
		err error
	}
	waitCh := make(chan waitResult, 1)
	go func() {
		s, err := eng.RunAndWait(context.Background(), runID)
		waitCh <- waitResult{s, err}
	}()

	// 冻结 1.4s（覆盖多个 helper TTL=400ms 周期）：第二进程不得执行 block。
	freezeStart := time.Now()
	var sawExpired bool
	for time.Since(freezeStart) < 1400*time.Millisecond {
		snap, err := eng.Result(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Tasks["block"].Status != StatusRunning {
			t.Fatalf("冻结期间 block=%s，必须仍是原进程在飞 RUNNING", snap.Tasks["block"].Status)
		}
		if snap.Lease != nil && snap.Lease.expired(eng.nowFunc()) {
			sawExpired = true
		}
		if got := atomic.LoadInt64(blockCalls); got != 0 {
			t.Fatalf("原进程仍存活（暂停）时第二进程不得执行 block，实际 %d 次", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawExpired {
		t.Fatal("未观察到租约到期窗口")
	}
	if !tee.Contains(DecisionOwnerAliveUnresponsive) {
		t.Fatalf("冻结期间应记录 OWNER_ALIVE_UNRESPONSIVE:\n%s", tee.String())
	}
	t.Logf("冻结窗口第二进程决策日志:\n%s", tee.String())

	// 恢复：先 SIGCONT，再放置“外部依赖恢复”信号文件，让原进程自己完成尝试。
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT: %v", err)
	}
	if f, err := os.Create(filepath.Join(dir, "recover.flag")); err != nil {
		t.Fatal(err)
	} else {
		_ = f.Close()
	}

	// 运行应由原进程跑完收敛；第二进程的 RunAndWait 只是读到终态。
	select {
	case wr := <-waitCh:
		if wr.err != nil {
			t.Fatal(wr.err)
		}
		if wr.s != RunSucceeded {
			t.Fatalf("恢复后终态=%s want SUCCEEDED", wr.s)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("原进程恢复后未在合理时间内把运行跑完")
	}

	// 第二进程在整个过程中一次 block 都不能执行——这是“不跑两遍”的关键断言。
	if got := atomic.LoadInt64(blockCalls); got != 0 {
		t.Fatalf("恢复后应由原进程完成，第二进程不得执行 block，实际 %d 次", got)
	}

	final, err := eng.Result(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	blk := final.Tasks["block"]
	if blk.Status != StatusSucceeded {
		t.Fatalf("block 终态=%s want SUCCEEDED", blk.Status)
	}
	// 由原进程一次成功：尝试序号为 1，只有一条 SUCCEEDED 记录，没有 INTERRUPTED。
	if blk.Attempt != 1 || len(blk.Attempts) != 1 || blk.Attempts[0].Outcome != OutcomeSucceeded {
		t.Fatalf("恢复应由原进程的第 1 次尝试直接成功，实际 attempt=%d records=%+v",
			blk.Attempt, blk.Attempts)
	}

	// 跨进程副作用：a、block 各恰好一次，且 block 那行来自 helper（原进程）。
	logBytes, err := os.ReadFile(filepath.Join(dir, "exec.log"))
	if err != nil {
		t.Fatal(err)
	}
	countA, countBlock := 0, 0
	for _, ln := range strings.Fields(string(logBytes)) {
		switch ln {
		case "a":
			countA++
		case "block":
			countBlock++
		}
	}
	if countA != 1 || countBlock != 1 {
		t.Fatalf("副作用次数 a=%d block=%d，恢复场景下都必须恰为 1", countA, countBlock)
	}
	t.Logf("覆盖任务：a(成功一次)、block(原进程在飞→暂停→恢复，自己成功一次)；" +
		"第二进程零执行；终态=SUCCEEDED，尝试记录仅 1 条 SUCCEEDED，副作用 a=1 block=1")
}
