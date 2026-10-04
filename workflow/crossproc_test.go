package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 本文件的测试通过启动真实子进程（同一测试二进制，WF_HELPER=1 进入
// TestCrossProcHelper）模拟多 worker 进程共用同一状态目录的场景。

// TestCrossProcHelper 是子进程入口；父进程测试中不会被触发（无 WF_HELPER）。
func TestCrossProcHelper(t *testing.T) {
	if os.Getenv("WF_HELPER") != "1" {
		return
	}
	switch os.Getenv("WF_MODE") {
	case "worker":
		os.Exit(helperWorker())
	case "report":
		os.Exit(helperReport())
	default:
		os.Exit(2)
	}
}

// helperDef 是子进程 worker 注册的工作流：first -> second -> third。
// second 允许 3 次尝试，便于验证“被强杀的尝试计入次数”。
func helperDef() WorkflowDef {
	return WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "first", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "second", Dependencies: []string{"first"}, Retry: RetryPolicy{MaxAttempts: 3}},
		{Name: "third", Dependencies: []string{"second"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}}
}

// helperLogExec 向共享执行日志追加一行（flock 保护，跨进程安全）。
func helperLogExec(path, task string, attempt int) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	fmt.Fprintf(f, "task=%s attempt=%d pid=%d\n", task, attempt, os.Getpid())
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}

// helperWorker 模拟一个 worker 进程：认领并推进运行，打印终态后退出。
// WF_BEHAVIOR=hang 时 second 的尝试永不返回（只能被强杀）。
func helperWorker() int {
	dir := os.Getenv("WF_DIR")
	runID := os.Getenv("WF_RUN")
	execLog := os.Getenv("WF_EXECLOG")
	behavior := os.Getenv("WF_BEHAVIOR")

	store, err := NewFileStore(dir)
	if err != nil {
		fmt.Println("store:", err)
		return 1
	}
	exec := MapExecutor{}
	for _, name := range []string{"first", "second", "third"} {
		exec[name] = func(ctx context.Context, runID, taskName string, attemptNo int) AttemptResult {
			helperLogExec(execLog, taskName, attemptNo)
			if behavior == "hang" && taskName == "second" {
				select {} // 永不返回也不响应取消，模拟卡死的外部调用
			}
			time.Sleep(50 * time.Millisecond)
			return AttemptResult{}
		}
	}
	eng := NewEngine(store, exec, NewLogger(os.Stdout),
		WithOwnerID(fmt.Sprintf("worker-%d", os.Getpid())),
		WithClaimInterval(100*time.Millisecond),
		WithPollInterval(20*time.Millisecond))
	if err := eng.Register(helperDef()); err != nil {
		fmt.Println("register:", err)
		return 1
	}
	if _, _, err := eng.Trigger(context.Background(), "wf", runID); err != nil {
		fmt.Println("trigger:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	status, err := eng.RunAndWait(ctx, runID)
	if err != nil {
		fmt.Println("wait:", err)
		return 1
	}
	fmt.Printf("STATUS=%s\n", status)
	_ = eng.Close()
	return 0
}

// helperReport 以独立进程读取结果视图并输出 JSON。
func helperReport() int {
	store, err := NewFileStore(os.Getenv("WF_DIR"))
	if err != nil {
		return 1
	}
	eng := NewEngine(store, MapExecutor{}, nil)
	rep, err := eng.Report(context.Background(), os.Getenv("WF_RUN"))
	if err != nil {
		fmt.Println("report:", err)
		return 1
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return 1
	}
	os.Stdout.Write(b)
	return 0
}

// startHelper 启动一个子进程助手。
func startHelper(t *testing.T, mode string, env ...string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrossProcHelper")
	cmd.Env = append(os.Environ(), "WF_HELPER=1", "WF_MODE="+mode)
	cmd.Env = append(cmd.Env, env...)
	buf := &bytes.Buffer{}
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	return cmd, buf
}

// waitHelper 等待子进程退出，超时强杀。
func waitHelper(t *testing.T, cmd *exec.Cmd, buf *bytes.Buffer, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("helper exit: %v\noutput:\n%s", err, buf.String())
		}
	case <-time.After(d):
		cmd.Process.Kill()
		<-done
		t.Fatalf("helper timeout after %s\noutput:\n%s", d, buf.String())
	}
}

func workerEnv(stateDir, runID, execLog, behavior string) []string {
	return []string{
		"WF_DIR=" + stateDir,
		"WF_RUN=" + runID,
		"WF_EXECLOG=" + execLog,
		"WF_BEHAVIOR=" + behavior,
	}
}

// execLogCounts 解析共享执行日志：task -> attempt -> 次数。
func execLogCounts(t *testing.T, path string) map[string]map[int]int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read exec log: %v", err)
	}
	out := map[string]map[int]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var task string
		var attempt, pid int
		if _, err := fmt.Sscanf(line, "task=%s attempt=%d pid=%d", &task, &attempt, &pid); err != nil {
			t.Fatalf("bad exec log line %q: %v", line, err)
		}
		if out[task] == nil {
			out[task] = map[int]int{}
		}
		out[task][attempt]++
	}
	return out
}

// TestCrossProcConcurrentTrigger 三个真实进程同时触发同一运行：
// 归属权唯一（CLAIM_ACQUIRED 恰好一次），每个任务恰好执行一次，
// 所有进程拿到同一终态；运行期间父进程轮询结果视图，内容始终自洽。
func TestCrossProcConcurrentTrigger(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	execLog := filepath.Join(dir, "exec.log")
	runID := "shared-run"

	const workers = 3
	cmds := make([]*exec.Cmd, workers)
	bufs := make([]*bytes.Buffer, workers)
	for i := 0; i < workers; i++ {
		cmds[i], bufs[i] = startHelper(t, "worker", workerEnv(stateDir, runID, execLog, "ok")...)
	}

	// 运行期间并发轮询结果视图：任务集合恒定、尝试计数单调不减。
	readerStore, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reader := NewEngine(readerStore, MapExecutor{}, nil)
	t.Cleanup(func() { _ = reader.Close() })
	pollDone := make(chan struct{})
	go func() {
		last := map[string]int{}
		for {
			select {
			case <-pollDone:
				return
			default:
			}
			rep, err := reader.Report(context.Background(), runID)
			if err == ErrRunNotFound {
				continue // 运行尚未创建
			}
			if err != nil {
				t.Errorf("poll report: %v", err)
				return
			}
			if len(rep.Tasks) != 3 {
				t.Errorf("视图任务数=%d，不得读到半截状态", len(rep.Tasks))
				return
			}
			for _, tr := range rep.Tasks {
				if tr.Attempts < last[tr.TaskName] {
					t.Errorf("任务 %s 尝试数回退 %d->%d", tr.TaskName, last[tr.TaskName], tr.Attempts)
					return
				}
				last[tr.TaskName] = tr.Attempts
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for i := 0; i < workers; i++ {
		waitHelper(t, cmds[i], bufs[i], 60*time.Second)
	}
	close(pollDone)

	claimed := 0
	for i := 0; i < workers; i++ {
		if !strings.Contains(bufs[i].String(), "STATUS=SUCCEEDED") {
			t.Fatalf("worker %d 未等到 SUCCEEDED:\n%s", i, bufs[i].String())
		}
		claimed += strings.Count(bufs[i].String(), DecisionClaimAcquired)
	}
	if claimed != 1 {
		t.Fatalf("CLAIM_ACQUIRED=%d want 1，归属权必须唯一", claimed)
	}

	counts := execLogCounts(t, execLog)
	for _, task := range []string{"first", "second", "third"} {
		total := 0
		for _, n := range counts[task] {
			total += n
		}
		if total != 1 {
			t.Fatalf("任务 %s 被执行 %d 次，跨进程归属必须唯一", task, total)
		}
	}

	// 两个独立进程分别读取结果视图，内容必须逐字节一致。
	rep1 := readReportViaHelper(t, stateDir, runID)
	rep2 := readReportViaHelper(t, stateDir, runID)
	if !bytes.Equal(rep1, rep2) {
		t.Fatal("不同进程读到的结果视图不一致")
	}
	var rep RunReport
	if err := json.Unmarshal(rep1, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunSucceeded || rep.TotalAttempts != 3 {
		t.Fatalf("report: status=%s totalAttempts=%d", rep.Status, rep.TotalAttempts)
	}
}

func readReportViaHelper(t *testing.T, stateDir, runID string) []byte {
	t.Helper()
	cmd, buf := startHelper(t, "report", "WF_DIR="+stateDir, "WF_RUN="+runID)
	waitHelper(t, cmd, buf, 30*time.Second)
	return bytes.TrimSpace(buf.Bytes())
}

// TestCrossProcKillAndTakeover 持有进程在执行中被 SIGKILL 强杀后，
// 另一进程在一个巡检周期量级的时间内接手并把运行跑完：
// 已成功任务不重跑、被中断的尝试不计成功但计入次数、不产生重复结果。
func TestCrossProcKillAndTakeover(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	execLog := filepath.Join(dir, "exec.log")
	runID := "killed-run"

	// --- 进程 A：执行到 second 时卡死 ---
	cmdA, bufA := startHelper(t, "worker", workerEnv(stateDir, runID, execLog, "hang")...)

	// 等 second 的第 1 次尝试真正发起（执行日志落行）再强杀。
	watcher, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		b, _ := os.ReadFile(execLog)
		snap, _ := watcher.GetRun(context.Background(), runID)
		if strings.Contains(string(b), "task=second") && snap != nil &&
			snap.Tasks["second"].Status == StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			cmdA.Process.Kill()
			t.Fatalf("等待 second 进入 RUNNING 超时:\n%s", bufA.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 强杀（不等任何收尾）。
	if err := cmdA.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmdA.Wait()

	// 磁盘上 second 必须仍是 RUNNING（中断态），绝不能是成功。
	onDisk, err := watcher.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if onDisk.Tasks["second"].Status != StatusRunning {
		t.Fatalf("强杀后 second=%s，中断中的任务不得被判定为成功", onDisk.Tasks["second"].Status)
	}
	if onDisk.Tasks["first"].Status != StatusSucceeded {
		t.Fatalf("强杀后 first=%s want SUCCEEDED", onDisk.Tasks["first"].Status)
	}

	// --- 进程 B：巡检认领接手，把运行跑完 ---
	cmdB, bufB := startHelper(t, "worker", workerEnv(stateDir, runID, execLog, "ok")...)
	waitHelper(t, cmdB, bufB, 60*time.Second)
	if !strings.Contains(bufB.String(), "STATUS=SUCCEEDED") {
		t.Fatalf("进程 B 未把运行跑到 SUCCEEDED:\n%s", bufB.String())
	}

	// 执行日志：first 恰好 1 次（已成功不重跑）；second 恰好 2 次
	// （A 的第 1 次被中断 + B 的第 2 次）；third 恰好 1 次。
	counts := execLogCounts(t, execLog)
	if counts["first"][1] != 1 || len(counts["first"]) != 1 {
		t.Fatalf("first 执行记录=%v，已成功任务不得重跑", counts["first"])
	}
	if counts["second"][1] != 1 || counts["second"][2] != 1 || len(counts["second"]) != 2 {
		t.Fatalf("second 执行记录=%v，应为第 1 次（被中断）+ 第 2 次（接手）各一次", counts["second"])
	}
	if counts["third"][1] != 1 || len(counts["third"]) != 1 {
		t.Fatalf("third 执行记录=%v", counts["third"])
	}

	// 结果视图：second 共 2 次尝试，第 1 次 INTERRUPTED、第 2 次 SUCCEEDED。
	repBytes := readReportViaHelper(t, stateDir, runID)
	var rep RunReport
	if err := json.Unmarshal(repBytes, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunSucceeded {
		t.Fatalf("status=%s want SUCCEEDED", rep.Status)
	}
	if rep.TotalAttempts != 4 {
		t.Fatalf("TotalAttempts=%d want 4（first1+second2+third1）", rep.TotalAttempts)
	}
	var second *TaskReport
	for i := range rep.Tasks {
		if rep.Tasks[i].TaskName == "second" {
			second = &rep.Tasks[i]
		}
	}
	if second == nil || second.Attempts != 2 || len(second.Records) != 2 {
		t.Fatalf("second 视图=%+v", second)
	}
	if second.Records[0].Outcome != OutcomeInterrupted {
		t.Fatalf("second 第 1 次记录=%s want INTERRUPTED", second.Records[0].Outcome)
	}
	if second.Records[1].Outcome != OutcomeSucceeded {
		t.Fatalf("second 第 2 次记录=%s want SUCCEEDED", second.Records[1].Outcome)
	}

	// 归属锁文件已释放，可再次被认领（这里仅验证锁可获取）。
	c, err := watcher.TryClaimRun(context.Background(), runID, "checker")
	if err != nil || c == nil {
		t.Fatalf("运行结束后归属锁应可获取: c=%v err=%v", c, err)
	}
	_ = c.Release()
}

// TestCrossProcRunIsolation 一个进程里卡死的运行不拖住其他运行：
// 另一进程推进的独立运行照常收敛。
func TestCrossProcRunIsolation(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	execLog := filepath.Join(dir, "exec.log")

	// 进程 A：stuck-run 卡在 second。
	cmdA, _ := startHelper(t, "worker", workerEnv(stateDir, "stuck-run", execLog, "hang")...)
	t.Cleanup(func() { cmdA.Process.Kill(); cmdA.Wait() })

	// 等 A 真正卡住（second 在飞）。
	deadline := time.Now().Add(30 * time.Second)
	for {
		b, _ := os.ReadFile(execLog)
		if strings.Contains(string(b), "task=second") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("等待 stuck-run 卡住超时")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 进程 B：独立运行 free-run 照常推进到终态。
	cmdB, bufB := startHelper(t, "worker", workerEnv(stateDir, "free-run", execLog, "ok")...)
	waitHelper(t, cmdB, bufB, 60*time.Second)
	if !strings.Contains(bufB.String(), "STATUS=SUCCEEDED") {
		t.Fatalf("独立运行被卡住的运行拖住:\n%s", bufB.String())
	}
}
