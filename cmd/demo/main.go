// Command demo 在本地跑一个 DAG 工作流，打印每个调度决策与尝试依据。
//
//	go run ./cmd/demo
//
// 图定义：
//
//	start -> flaky(前两次失败，第三次成功)
//	     \-> slow(超时 80ms，实际执行 200ms，超时收尾后二次成功)
//	     \-> doomed(始终失败，2 次后永久失败)
//	doomed -> orphan(因上游永久失败被跳过)
//	start  -> healthy(独立链路，不受 doomed 影响)
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	wf "github.com/h3yow/workflow-orchestrator-go/workflow"
)

func main() {
	store := wf.NewMemoryStore()
	logger := wf.NewLogger(os.Stdout)

	var flakyN int32
	exec := wf.MapExecutor{
		"start": func(context.Context, string, string, int) wf.AttemptResult {
			return wf.AttemptResult{}
		},
		"flaky": func(context.Context, string, string, int) wf.AttemptResult {
			n := atomic.AddInt32(&flakyN, 1)
			if n <= 2 {
				return wf.AttemptResult{Err: fmt.Errorf("transient error #%d", n)}
			}
			return wf.AttemptResult{}
		},
		"slow": func(ctx context.Context, runID, name string, attemptNo int) wf.AttemptResult {
			select {
			case <-time.After(200 * time.Millisecond):
				return wf.AttemptResult{}
			case <-ctx.Done():
				return wf.AttemptResult{Err: ctx.Err()}
			}
		},
		"doomed": func(context.Context, string, string, int) wf.AttemptResult {
			return wf.AttemptResult{Err: fmt.Errorf("permanent downstream outage")}
		},
		"orphan":  func(context.Context, string, string, int) wf.AttemptResult { return wf.AttemptResult{} },
		"healthy": func(context.Context, string, string, int) wf.AttemptResult { return wf.AttemptResult{} },
	}

	eng := wf.NewEngine(store, exec, logger)
	defer eng.Close()

	err := eng.Register(wf.WorkflowDef{
		Name: "demo",
		Tasks: []wf.TaskDef{
			{Name: "start", Retry: wf.RetryPolicy{MaxAttempts: 1}},
			{Name: "flaky", Dependencies: []string{"start"},
				Retry: wf.RetryPolicy{MaxAttempts: 3, Backoff: 50 * time.Millisecond}},
			{Name: "slow", Dependencies: []string{"start"}, Timeout: 80 * time.Millisecond,
				Retry: wf.RetryPolicy{MaxAttempts: 2, Backoff: 20 * time.Millisecond}},
			{Name: "doomed", Dependencies: []string{"start"},
				Retry: wf.RetryPolicy{MaxAttempts: 2, Backoff: 10 * time.Millisecond}},
			{Name: "orphan", Dependencies: []string{"doomed"},
				Retry: wf.RetryPolicy{MaxAttempts: 1}},
			{Name: "healthy", Dependencies: []string{"start"},
				Retry: wf.RetryPolicy{MaxAttempts: 1}},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "register: %v\n", err)
		os.Exit(1)
	}

	// 演示问题：slow 第二次尝试仍然需要 200ms，会再次超时 -> 永久失败。
	// 为了让 demo 展示“超时后重试成功”，这里给 slow 的第二次尝试快速成功。
	exec["slow"] = func(ctx context.Context, runID, name string, attemptNo int) wf.AttemptResult {
		if attemptNo > 1 {
			return wf.AttemptResult{} // 第二次拿到了更快的节点
		}
		select {
		case <-time.After(200 * time.Millisecond):
			return wf.AttemptResult{}
		case <-ctx.Done():
			return wf.AttemptResult{Err: ctx.Err()}
		}
	}

	runID := "demo-run-1"
	if _, _, err := eng.Trigger(context.Background(), "demo", runID); err != nil {
		fmt.Fprintf(os.Stderr, "trigger: %v\n", err)
		os.Exit(1)
	}
	status, err := eng.RunAndWait(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wait: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nRUN FINAL STATUS: %s\n", status)

	snap, _ := store.GetRun(context.Background(), runID)
	for _, name := range []string{"start", "flaky", "slow", "doomed", "orphan", "healthy"} {
		t := snap.Tasks[name]
		fmt.Printf("task=%-8s status=%-18s attempts=%d lastError=%q\n",
			name, t.Status, t.Attempt, t.LastError)
	}
}
