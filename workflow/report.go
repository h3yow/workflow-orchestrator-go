package workflow

import (
	"context"
	"sort"
	"time"
)

// TaskReport 是单个任务的最终结果视图。
type TaskReport struct {
	TaskName string
	Status   TaskStatus
	// Attempts 为已发起的尝试总次数（含被中断的尝试）。
	Attempts int
	// Records 按时间顺序列出每次尝试的收尾方式与判定依据。
	Records   []AttemptRecord
	LastError string
}

// RunReport 是一次运行的完整执行结果视图。
// 同一时刻所有进程读到的内容一致；运行结束后内容不再变化，可重复读取。
type RunReport struct {
	WorkflowName string
	RunID        string
	Status       RunStatus
	// TotalAttempts 为整个运行累计发起的尝试次数。
	TotalAttempts int
	Tasks         []TaskReport
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Report 读取一次运行的完整执行结果视图。
// 视图基于 Store 的一致性快照构造：多进程并发读到的内容一致；
// 运行进入终态后视图不再变化，重复读取结果相同。
func (e *Engine) Report(ctx context.Context, runID string) (*RunReport, error) {
	snap, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	rep := &RunReport{
		WorkflowName: snap.WorkflowName,
		RunID:        snap.RunID,
		Status:       snap.Status,
		CreatedAt:    snap.CreatedAt,
		UpdatedAt:    snap.UpdatedAt,
	}
	names := make([]string, 0, len(snap.Tasks))
	for name := range snap.Tasks {
		names = append(names, name)
	}
	sort.Strings(names) // 固定顺序，保证视图可复现
	for _, name := range names {
		t := snap.Tasks[name]
		tr := TaskReport{
			TaskName:  name,
			Status:    t.Status,
			Attempts:  t.Attempt,
			LastError: t.LastError,
			Records:   make([]AttemptRecord, len(t.Records)),
		}
		copy(tr.Records, t.Records)
		rep.TotalAttempts += t.Attempt
		rep.Tasks = append(rep.Tasks, tr)
	}
	return rep, nil
}
