// Command demo 在本地演示 DAG 工作流编排：
//
//	go run ./cmd/demo
//
// 它构造一个菱形 DAG（A -> B/C -> D），其中 A 前两次失败第三次成功，
// 使用 FileStore 将状态持久化到 ./.wf-state，并并发触发两个 run。
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	wf "github.com/h3yow/workflow-orchestrator-go/pkg/workflow"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}

func main() {
	dir := ".wf-state"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	store, err := wf.NewFileStore(dir)
	if err != nil {
		panic(err)
	}

	workflow, err := wf.Load(wf.WorkflowDef{
		Name: "diamond-demo",
		Tasks: []wf.TaskDef{
			{ID: "A", Retries: wf.RetryPolicy{MaxAttempts: 3, Backoff: 20 * time.Millisecond}},
			{ID: "B", Deps: []string{"A"}},
			{ID: "C", Deps: []string{"A"}, Timeout: wf.TimeoutPolicy{Timeout: 2 * time.Second}},
			{ID: "D", Deps: []string{"B", "C"}},
		},
	})
	if err != nil {
		panic(err)
	}

	registry := func() wf.TaskRegistry {
		var aCalls int32
		return wf.TaskRegistry{
			"A": func(ctx context.Context, runID string, attempt int) error {
				n := atomic.AddInt32(&aCalls, 1)
				fmt.Printf("  run=%s task=A attempt=%d\n", runID, n)
				if n < 3 {
					return fmt.Errorf("transient failure #%d", n)
				}
				return nil
			},
			"B": func(ctx context.Context, runID string, attempt int) error {
				fmt.Printf("  run=%s task=B attempt=%d ok\n", runID, attempt)
				return nil
			},
			"C": func(ctx context.Context, runID string, attempt int) error {
				fmt.Printf("  run=%s task=C attempt=%d ok\n", runID, attempt)
				return nil
			},
			"D": func(ctx context.Context, runID string, attempt int) error {
				fmt.Printf("  run=%s task=D attempt=%d ok\n", runID, attempt)
				return nil
			},
		}
	}

	var wg sync.WaitGroup
	for _, runID := range []string{"demo-run-1", "demo-run-2"} {
		wg.Add(1)
		go func(runID string) {
			defer wg.Done()
			eng := wf.NewEngine(workflow, store, registry(), stdLogger{},
				wf.Config{EngineID: "engine-" + runID, RecoveryPoll: 10 * time.Millisecond})
			run, err := eng.Trigger(context.Background(), runID)
			if err != nil {
				fmt.Printf("run %s error: %v\n", runID, err)
				return
			}
			fmt.Printf("run %s finished: state=%s\n", runID, run.State)
			for _, t := range workflow.OrderedTasks() {
				ti := run.Tasks[t.ID]
				fmt.Printf("  %-2s %-9s attempts=%d %s\n", t.ID, ti.State, ti.AttemptCount, ti.ResultReason)
			}
		}(runID)
	}
	wg.Wait()
	fmt.Printf("\nstate snapshots persisted under %s/*.json\n", dir)
}
