package workflow

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestReportViewCompleteAndStable 运行含重试、超时、跳过的工作流，
// 校验结果视图：终态、每任务状态、总尝试次数、每次尝试的收尾方式与依据；
// 并校验终态后重复读取结果一致。
func TestReportViewCompleteAndStable(t *testing.T) {
	store := NewMemoryStore()
	var flakyCalls, slowCalls int32
	exec := MapExecutor{
		"flaky": func(context.Context, string, string, int) AttemptResult {
			n := atomic.AddInt32(&flakyCalls, 1)
			if n == 1 {
				return AttemptResult{Err: errors.New("boom")}
			}
			return AttemptResult{}
		},
		"slow": func(ctx context.Context, s, s2 string, i int) AttemptResult {
			atomic.AddInt32(&slowCalls, 1)
			select {
			case <-ctx.Done():
				return AttemptResult{Err: ctx.Err()}
			case <-time.After(200 * time.Millisecond):
				return AttemptResult{}
			}
		},
		"doomed": func(context.Context, string, string, int) AttemptResult {
			return AttemptResult{Err: errors.New("always fails")}
		},
	}
	eng := NewEngine(store, exec, NewLogger(&bytes.Buffer{}))
	t.Cleanup(func() { _ = eng.Close() })
	def := WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "flaky", Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}},
		{Name: "slow", Timeout: 20 * time.Millisecond, Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "doomed", Retry: RetryPolicy{MaxAttempts: 1}},
		{Name: "downstream", Dependencies: []string{"doomed"}},
	}}
	if err := eng.Register(def); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.Trigger(context.Background(), "wf", "r1"); err != nil {
		t.Fatal(err)
	}
	status, err := eng.RunAndWait(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	if status != RunFailed {
		t.Fatalf("status=%s want FAILED", status)
	}

	rep, err := eng.Report(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != RunFailed {
		t.Fatalf("report status=%s want FAILED", rep.Status)
	}
	// flaky 2 次 + slow 1 次 + doomed 1 次 = 4 次尝试；downstream 未执行。
	if rep.TotalAttempts != 4 {
		t.Fatalf("TotalAttempts=%d want 4", rep.TotalAttempts)
	}
	if len(rep.Tasks) != 4 {
		t.Fatalf("tasks=%d want 4", len(rep.Tasks))
	}
	// 任务按名称排序，保证视图可复现。
	for i := 1; i < len(rep.Tasks); i++ {
		if rep.Tasks[i-1].TaskName >= rep.Tasks[i].TaskName {
			t.Fatalf("任务视图未按名称排序: %v", rep.Tasks)
		}
	}
	byName := map[string]TaskReport{}
	for _, tr := range rep.Tasks {
		byName[tr.TaskName] = tr
	}

	flaky := byName["flaky"]
	if flaky.Status != StatusSucceeded || flaky.Attempts != 2 || len(flaky.Records) != 2 {
		t.Fatalf("flaky: status=%s attempts=%d records=%d", flaky.Status, flaky.Attempts, len(flaky.Records))
	}
	if flaky.Records[0].Outcome != OutcomeFailed || flaky.Records[0].Attempt != 1 {
		t.Fatalf("flaky 第 1 次记录应为 FAILED: %+v", flaky.Records[0])
	}
	if flaky.Records[1].Outcome != OutcomeSucceeded || flaky.Records[1].Attempt != 2 {
		t.Fatalf("flaky 第 2 次记录应为 SUCCEEDED: %+v", flaky.Records[1])
	}
	if flaky.Records[0].Reason == "" {
		t.Fatal("失败记录缺少判定依据")
	}

	slow := byName["slow"]
	if slow.Status != StatusPermanentlyFailed || len(slow.Records) != 1 ||
		slow.Records[0].Outcome != OutcomeTimedOut {
		t.Fatalf("slow: status=%s records=%+v", slow.Status, slow.Records)
	}

	down := byName["downstream"]
	if down.Status != StatusSkipped || down.Attempts != 0 || len(down.Records) != 1 ||
		down.Records[0].Outcome != OutcomeSkipped {
		t.Fatalf("downstream: status=%s attempts=%d records=%+v", down.Status, down.Attempts, down.Records)
	}

	// 终态后视图稳定：连续两次读取完全一致。
	rep2, err := eng.Report(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	if !reportsEqual(rep, rep2) {
		t.Fatal("终态后重复读取的结果视图不一致")
	}
}

func reportsEqual(a, b *RunReport) bool {
	if a.Status != b.Status || a.TotalAttempts != b.TotalAttempts || len(a.Tasks) != len(b.Tasks) {
		return false
	}
	for i := range a.Tasks {
		ta, tb := a.Tasks[i], b.Tasks[i]
		if ta.TaskName != tb.TaskName || ta.Status != tb.Status || ta.Attempts != tb.Attempts ||
			len(ta.Records) != len(tb.Records) {
			return false
		}
		for j := range ta.Records {
			if ta.Records[j].Outcome != tb.Records[j].Outcome ||
				ta.Records[j].Attempt != tb.Records[j].Attempt ||
				ta.Records[j].Reason != tb.Records[j].Reason {
				return false
			}
		}
	}
	return true
}

// TestMultiEngineSameDirSingleOwner 同一状态目录上的两个引擎（模拟两个进程）
// 并发触发同一 runID：归属权唯一，每个任务恰好执行一次，
// 双方等待到的终态一致。
func TestMultiEngineSameDirSingleOwner(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	var executions int32
	makeExec := func() Executor {
		return MapExecutor{
			"a": func(context.Context, string, string, int) AttemptResult {
				atomic.AddInt32(&executions, 1)
				time.Sleep(20 * time.Millisecond)
				return AttemptResult{}
			},
			"b": func(context.Context, string, string, int) AttemptResult {
				atomic.AddInt32(&executions, 1)
				return AttemptResult{}
			},
		}
	}
	def := WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "a"},
		{Name: "b", Dependencies: []string{"a"}},
	}}

	newEngine := func(owner string) *Engine {
		st, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		return NewEngine(st, makeExec(), NewLogger(&bytes.Buffer{}),
			WithOwnerID(owner), WithClaimInterval(20*time.Millisecond),
			WithPollInterval(5*time.Millisecond))
	}
	eng1, eng2 := newEngine("w1"), newEngine("w2")
	t.Cleanup(func() { _ = eng1.Close(); _ = eng2.Close() })
	if err := eng1.Register(def); err != nil {
		t.Fatal(err)
	}
	if err := eng2.Register(def); err != nil {
		t.Fatal(err)
	}

	type res struct {
		status RunStatus
		err    error
	}
	wait := func(eng *Engine, ch chan<- res) {
		if _, _, err := eng.Trigger(context.Background(), "wf", "shared-run"); err != nil {
			ch <- res{err: err}
			return
		}
		s, err := eng.RunAndWait(context.Background(), "shared-run")
		ch <- res{s, err}
	}
	c1, c2 := make(chan res, 1), make(chan res, 1)
	go wait(eng1, c1)
	go wait(eng2, c2)
	r1, r2 := <-c1, <-c2
	if r1.err != nil || r2.err != nil {
		t.Fatalf("wait errors: %v / %v", r1.err, r2.err)
	}
	if r1.status != RunSucceeded || r2.status != RunSucceeded {
		t.Fatalf("statuses=%s/%s want SUCCEEDED/SUCCEEDED", r1.status, r2.status)
	}
	if got := atomic.LoadInt32(&executions); got != 2 {
		t.Fatalf("executions=%d want 2，跨引擎归属必须唯一", got)
	}

	// 双方读到的结果视图一致。
	rep1, err := eng1.Report(context.Background(), "shared-run")
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := eng2.Report(context.Background(), "shared-run")
	if err != nil {
		t.Fatal(err)
	}
	if !reportsEqual(rep1, rep2) {
		t.Fatal("两个引擎读到的结果视图不一致")
	}
	if rep1.TotalAttempts != 2 {
		t.Fatalf("TotalAttempts=%d want 2", rep1.TotalAttempts)
	}
}
