package workflow

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// captureLogger 收集全部调度决策，供断言“是否打出了等待/接管相关决策”。
type captureLogger struct {
	mu  sync.Mutex
	all []Decision
}

func (c *captureLogger) LogDecision(d Decision) {
	c.mu.Lock()
	c.all = append(c.all, d)
	c.mu.Unlock()
}
func (c *captureLogger) Logf(string, ...any) {}

func (c *captureLogger) kindsContaining(sub string) []Decision {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Decision
	for _, d := range c.all {
		if strings.Contains(d.Kind, sub) || strings.Contains(d.Detail, sub) {
			out = append(out, d)
		}
	}
	return out
}

// newProcWithGate 构造一个带“心跳闸门”的引擎：heartbeatGate 未放行时心跳停摆
// （等价于进程被 SIGSTOP），但引擎 goroutine 与进程存活锁仍然持有。
func newProcWithGate(t *testing.T, dir string, exec Executor, ttl time.Duration,
	heartbeatGate chan struct{}, logger Logger) *Engine {
	t.Helper()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(st, exec, logger,
		WithLeaseTTL(ttl), WithPollInterval(10*time.Millisecond))
	eng.heartbeatGate = heartbeatGate
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func stallWorkflow() WorkflowDef {
	return WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "task", Retry: RetryPolicy{MaxAttempts: 1}},
	}}
}

// testLogWriter 把引擎决策日志转发到 t.Log，-v 时可看到每个用例覆盖了哪些任务。
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// TestTakeoverBlockedWhileHolderAlive 是本次修复的核心场景。
//
// 持有者的任务在飞时，心跳被闸门挂起（模拟进程被 SIGSTOP / 机器卡顿 /
// 调度饿死），租约心跳远超 TTL 没有续上，但：
//   - 执行 goroutine 与整个引擎仍然存活；
//   - 进程存活锁（独占 flock）仍被持有。
//
// 另一个进程在此期间反复尝试接管，必须被存活锁挡住：
//   - 在飞任务绝不允许在别的进程重新执行；
//   - 租约不得易主；
//
// 待闸门打开（进程恢复），持有者自己把唯一一次尝试跑完，全程执行次数为 1。
//
// 重复多轮、开启 -race 须稳定通过。
func TestTakeoverBlockedWhileHolderAlive(t *testing.T) {
	const rounds = 3
	const ttl = 80 * time.Millisecond
	for round := 0; round < rounds; round++ {
		t.Run("", func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			var execCount int64
			hbGate := make(chan struct{}) // 心跳闸门：不放行即“进程暂停”
			execRelease := make(chan struct{})
			inside := make(chan struct{})

			holderExec := MapExecutor{
				"task": func(context.Context, string, string, int) AttemptResult {
					atomic.AddInt64(&execCount, 1)
					close(inside)
					<-execRelease // 进程暂停期间执行函数不返回，但 goroutine 仍存活
					return AttemptResult{}
				},
			}
			// 接管候选若错误重跑，计数立刻变成 2，测试失败。
			challengerExec := MapExecutor{
				"task": func(context.Context, string, string, int) AttemptResult {
					t.Errorf("持有者仍存活（只是没响应），任务绝不能被其他进程重跑")
					atomic.AddInt64(&execCount, 1)
					return AttemptResult{}
				},
			}

			holder := newProcWithGate(t, dir, holderExec, ttl, hbGate, NewLogger(testLogWriter{t}))
			mustRegister(t, holder, stallWorkflow())
			challenger := newProc(t, dir, challengerExec, ttl)
			mustRegister(t, challenger, stallWorkflow())

			if _, _, err := holder.Trigger(context.Background(), "wf", "stalled-run"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-inside:
			case <-time.After(5 * time.Second):
				t.Fatal("持有者的任务迟迟未开始执行")
			}

			// 心跳一旦触发（首次约 ttl/3 后）就阻塞在 hbGate 上。
			// 静默 1.2s ≫ TTL(80ms)：租约心跳早已超时，但进程与存活锁还在。
			time.Sleep(1200 * time.Millisecond)

			// 挑战者在静默窗口内多次触发/轮询：既不能拿到租约，也不能推进状态。
			probeDeadline := time.Now().Add(800 * time.Millisecond)
			for time.Now().Before(probeDeadline) {
				if _, _, err := challenger.Trigger(context.Background(), "wf", "stalled-run"); err != nil {
					t.Fatal(err)
				}
				snap, err := challenger.Result(context.Background(), "stalled-run")
				if err != nil {
					t.Fatal(err)
				}
				if snap.Status != RunRunning {
					t.Fatalf("持有者静默期间运行被推进到终态 %s，疑似被错误接管", snap.Status)
				}
				if snap.Lease == nil || snap.Lease.Owner != holder.owner {
					t.Fatalf("租约在持有者仍存活时易主：lease=%v", snap.Lease)
				}
				if snap.Tasks["task"].Status != StatusRunning {
					t.Fatalf("在飞任务状态被改动：%s", snap.Tasks["task"].Status)
				}
				if got := atomic.LoadInt64(&execCount); got != 1 {
					t.Fatalf("任务被执行了 %d 次，持有者存活时绝不能重跑", got)
				}
				time.Sleep(20 * time.Millisecond)
			}

			// 进程恢复：关闭心跳闸门（已阻塞与后续心跳全部放行），
			// 并让在飞执行返回，持有者自行把唯一一次尝试跑完。
			close(hbGate)
			close(execRelease)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			status, err := holder.RunAndWait(ctx, "stalled-run")
			if err != nil {
				t.Fatal(err)
			}
			if status != RunSucceeded {
				t.Fatalf("恢复后 status=%s want SUCCEEDED", status)
			}
			if got := atomic.LoadInt64(&execCount); got != 1 {
				t.Fatalf("全程任务应恰好执行 1 次，实际 %d 次", got)
			}
			// 挑战者最终也读到同一个确定终态，且任务只成功过一次。
			s2, err := challenger.RunAndWait(ctx, "stalled-run")
			if err != nil || s2 != RunSucceeded {
				t.Fatalf("挑战者读到 status=%s err=%v want SUCCEEDED", s2, err)
			}
			final, _ := holder.Result(context.Background(), "stalled-run")
			tsk := final.Tasks["task"]
			if tsk.Status != StatusSucceeded || tsk.Attempt != 1 {
				t.Fatalf("task status=%s attempt=%d want SUCCEEDED/1（原有尝试应正常跑完）",
					tsk.Status, tsk.Attempt)
			}
			if len(tsk.Attempts) != 1 || tsk.Attempts[0].Outcome != OutcomeSucceeded {
				t.Fatalf("恢复后不应产生 INTERRUPTED 记录，实际 %d 条", len(tsk.Attempts))
			}
			t.Logf("round=%d | 覆盖任务 task：持有者心跳静默 %s（≫TTL=%s）期间未被接管、"+
				"执行计数恒为 1；恢复后原尝试成功，attempts=[SUCCEEDED]，终态=%s",
				round, 1200*time.Millisecond, ttl, tsk.Status)
		})
	}
}

// TestHolderUnresponsiveDecisionLogged 持有者心跳超时但仍存活时，
// 等待方必须打出 HOLDER_UNRESPONSIVE（区别于普通的 LEASE_HELD_BY_OTHER），
// 且期间绝不接管；持有者恢复后等待方读到 SUCCEEDED。
func TestHolderUnresponsiveDecisionLogged(t *testing.T) {
	const ttl = 80 * time.Millisecond
	dir := filepath.Join(t.TempDir(), "state")
	hbGate := make(chan struct{})
	execRelease := make(chan struct{})
	inside := make(chan struct{})

	holder := newProcWithGate(t, dir, MapExecutor{
		"task": func(context.Context, string, string, int) AttemptResult {
			close(inside)
			<-execRelease
			return AttemptResult{}
		},
	}, ttl, hbGate, NewLogger(io.Discard))
	mustRegister(t, holder, stallWorkflow())

	waiterLog := &captureLogger{}
	waiter := newProcWithGate(t, dir, MapExecutor{
		"task": func(context.Context, string, string, int) AttemptResult {
			t.Fatal("等待方不应执行任务")
			return AttemptResult{}
		},
	}, ttl, nil, waiterLog)
	mustRegister(t, waiter, stallWorkflow())

	if _, _, err := holder.Trigger(context.Background(), "wf", "r"); err != nil {
		t.Fatal(err)
	}
	<-inside
	time.Sleep(1200 * time.Millisecond) // 心跳静默远超 TTL

	waitDone := make(chan struct{})
	go func() {
		s, _ := waiter.RunAndWait(context.Background(), "r")
		if s != RunSucceeded {
			t.Errorf("等待方最终 status=%s want SUCCEEDED", s)
		}
		close(waitDone)
	}()
	time.Sleep(500 * time.Millisecond) // 等待方轮询多轮，应打出 HOLDER_UNRESPONSIVE

	// 恢复持有者：关闸放行全部心跳，并让执行返回。
	close(hbGate)
	close(execRelease)
	select {
	case <-waitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("运行未能在持有者恢复后收尾")
	}

	if ds := waiterLog.kindsContaining(DecisionHolderUnresponsive); len(ds) == 0 {
		t.Fatalf("等待方未打出 %s 决策", DecisionHolderUnresponsive)
	}
	if ds := waiterLog.kindsContaining(DecisionLeaseAcquired); len(ds) != 0 {
		t.Fatalf("持有者存活期间等待方不得取得租约，实际：%+v", ds)
	}
	t.Logf("覆盖任务 task：等待方在 %s 静默窗口打出 HOLDER_UNRESPONSIVE 且未接管，恢复后读到 SUCCEEDED",
		1200*time.Millisecond)
}
