// worker 是一个可独立启动的示例 worker 进程：多个实例共用同一状态目录，
// 演示跨进程归属认领、强杀接手与结果视图读取。
//
// 用法：
//
//	worker -dir /tmp/wfstate -run r1 -behavior ok     # 触发并推进运行
//	worker -dir /tmp/wfstate -run r1 -behavior hang   # 卡在 second，用于强杀演练
//	worker -dir /tmp/wfstate -run r1 -report          # 只读结果视图（JSON）
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	workflow "github.com/h3yow/workflow-orchestrator-go/workflow"
)

func demoDef() workflow.WorkflowDef {
	return workflow.WorkflowDef{Name: "demo", Tasks: []workflow.TaskDef{
		{Name: "first", Retry: workflow.RetryPolicy{MaxAttempts: 1}},
		{Name: "second", Dependencies: []string{"first"},
			Retry: workflow.RetryPolicy{MaxAttempts: 3, Backoff: 200 * time.Millisecond}},
		{Name: "third", Dependencies: []string{"second"}, Retry: workflow.RetryPolicy{MaxAttempts: 1}},
	}}
}

func main() {
	dir := flag.String("dir", "", "共享状态目录（必填）")
	runID := flag.String("run", "", "运行 ID（必填）")
	behavior := flag.String("behavior", "ok", "执行行为：ok | hang（second 永久阻塞，用于强杀演练）")
	report := flag.Bool("report", false, "只读取并打印结果视图，不参与调度")
	claim := flag.Duration("claim-interval", 500*time.Millisecond, "巡检认领间隔（接手延迟上界）")
	flag.Parse()
	if *dir == "" || *runID == "" {
		fmt.Fprintln(os.Stderr, "必须提供 -dir 与 -run")
		os.Exit(2)
	}

	store, err := workflow.NewFileStore(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		os.Exit(1)
	}
	logger := workflow.NewLogger(os.Stdout)

	if *report {
		eng := workflow.NewEngine(store, workflow.MapExecutor{}, logger)
		rep, err := eng.Report(context.Background(), *runID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "report:", err)
			os.Exit(1)
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
		return
	}

	exec := workflow.MapExecutor{}
	for _, name := range []string{"first", "second", "third"} {
		exec[name] = func(ctx context.Context, runID, taskName string, attemptNo int) workflow.AttemptResult {
			fmt.Printf("[pid=%d] execute task=%s attempt=%d\n", os.Getpid(), taskName, attemptNo)
			if *behavior == "hang" && taskName == "second" {
				select {} // 永不返回，模拟卡死的外部调用；只能被强杀
			}
			time.Sleep(300 * time.Millisecond)
			return workflow.AttemptResult{}
		}
	}
	eng := workflow.NewEngine(store, exec, logger,
		workflow.WithOwnerID(fmt.Sprintf("worker-%d", os.Getpid())),
		workflow.WithClaimInterval(*claim))
	if err := eng.Register(demoDef()); err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		os.Exit(1)
	}
	if _, existed, err := eng.Trigger(context.Background(), "demo", *runID); err != nil {
		fmt.Fprintln(os.Stderr, "trigger:", err)
		os.Exit(1)
	} else if existed {
		fmt.Println("run 已存在：命中既有运行（去重或接手）")
	}
	status, err := eng.RunAndWait(context.Background(), *runID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wait:", err)
		os.Exit(1)
	}
	fmt.Printf("final status=%s\n", status)
	_ = eng.Close()
}
