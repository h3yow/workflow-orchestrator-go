package workflow

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestPresenceReleasedOnGracefulClose 引擎优雅 Close 后，其进程存活锁立即释放：
// 其他引擎立刻探测为“不存活”，随后可在 TTL 后接管（正常退出的标准路径）。
// 多轮重复验证 Close 不泄漏 flock。
func TestPresenceReleasedOnGracefulClose(t *testing.T) {
	const rounds = 3
	ctx := context.Background()
	for round := 0; round < rounds; round++ {
		t.Run("", func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			st, err := NewFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			eng := NewEngine(st, MapExecutor{}, NewLogger(nil),
				WithLeaseTTL(200*time.Millisecond))
			mustRegister(t, eng, WorkflowDef{Name: "wf", Tasks: []TaskDef{
				{Name: "t", Retry: RetryPolicy{MaxAttempts: 1}},
			}})
			owner := eng.owner
			// 触发一次以确保持有存活锁（惰性获取）。
			if _, _, err := eng.Trigger(ctx, "wf", "close-run"); err != nil {
				t.Fatal(err)
			}
			probe, err := NewFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if alive, err := probe.OwnerAlive(ctx, owner); err != nil || !alive {
				t.Fatalf("Close 前 owner 应存活 alive=%v err=%v", alive, err)
			}
			if err := eng.Close(); err != nil {
				t.Fatal(err)
			}
			// 存活锁应随 Close 立即释放（无需等待 TTL）。
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if alive, _ := probe.OwnerAlive(ctx, owner); !alive {
					break
				}
				time.Sleep(10 * time.Millisecond)
				if time.Now().After(deadline.Add(-10 * time.Millisecond)) {
					t.Fatal("Close 后存活锁未释放，会阻塞后续接管并泄漏 flock")
				}
			}
			_ = probe.Close()
			t.Logf("round=%d | 引擎 Close 后存活锁立即释放（owner=%s）", round, owner)
		})
	}
}

// TestGracefulCloseTakeoverNoDuplicate 优雅退出的持有者留下一个在飞任务时
// （Close 会取消其 ctx 并主动释放租约），另一个进程接管并只执行一次。
func TestGracefulCloseTakeoverNoDuplicate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	var execCount int64
	started := make(chan struct{})
	release := make(chan struct{})

	eng1 := newProc(t, dir, MapExecutor{
		"t": func(ctx context.Context, runID, name string, attemptNo int) AttemptResult {
			atomic.AddInt64(&execCount, 1)
			close(started)
			select {
			case <-release:
				return AttemptResult{}
			case <-ctx.Done():
				// 优雅 Close 取消 ctx：中断执行（本次不算成功）。
				return AttemptResult{Err: ctx.Err()}
			}
		},
	}, 200*time.Millisecond)
	mustRegister(t, eng1, WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 2}},
	}})
	if _, _, err := eng1.Trigger(context.Background(), "wf", "graceful"); err != nil {
		t.Fatal(err)
	}
	<-started

	// 优雅关闭：取消在飞尝试、释放租约与存活锁。
	if err := eng1.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)

	eng2 := newProc(t, dir, MapExecutor{
		"t": func(context.Context, string, string, int) AttemptResult {
			atomic.AddInt64(&execCount, 1)
			return AttemptResult{}
		},
	}, 200*time.Millisecond)
	mustRegister(t, eng2, WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 2}},
	}})
	if _, existed, err := eng2.Trigger(context.Background(), "wf", "graceful"); err != nil {
		t.Fatal(err)
	} else if !existed {
		t.Fatal("应命中既有运行")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if s, err := eng2.RunAndWait(ctx, "graceful"); err != nil || s != RunSucceeded {
		t.Fatalf("接管后 status=%s err=%v want SUCCEEDED", s, err)
	}
	if got := atomic.LoadInt64(&execCount); got != 2 {
		t.Fatalf("中断尝试发起 1 次 + 接管重新执行 1 次，期望 2，实际 %d", got)
	}
	final, _ := eng2.Result(context.Background(), "graceful")
	tsk := final.Tasks["t"]
	if tsk.Attempt != 2 || tsk.Attempts[0].Outcome != OutcomeInterrupted ||
		tsk.Attempts[1].Outcome != OutcomeSucceeded {
		t.Fatalf("尝试记录应为 [INTERRUPTED, SUCCEEDED]，实际 attempt=%d", tsk.Attempt)
	}
	t.Log("覆盖任务 t：优雅 Close 后租约/存活锁立即释放，接管方 [INTERRUPTED,SUCCEEDED]，无重复副作用")
}
