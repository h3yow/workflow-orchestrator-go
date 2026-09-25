package workflow

import (
	"context"
	"errors"
)

// ErrRunNotFound 表示指定的运行不存在（例如未触发即调度）。
var ErrRunNotFound = errors.New("workflow run not found")

// ErrWorkflowNotRegistered 表示触发了未注册的工作流。
var ErrWorkflowNotRegistered = errors.New("workflow not registered")

// Store 是运行状态的持久化接口。
// 所有实现必须保证：同一个 (RunID, TaskName) 的归属唯一，
// 且 CreateRunIfNotExists 在并发调用下具备幂等语义。
type Store interface {
	// CreateRunIfNotExists 原子地创建运行；已存在时返回 existed=true。
	CreateRunIfNotExists(ctx context.Context, state *RunState) (existed bool, err error)
	// GetRun 读取一次运行的完整状态快照（深拷贝）。
	GetRun(ctx context.Context, runID string) (*RunState, error)
	// PutTask 持久化单个任务实例状态。
	PutTask(ctx context.Context, runID string, task *TaskState) error
	// PutRunStatus 持久化运行聚合状态。
	PutRunStatus(ctx context.Context, runID string, status RunStatus) error
	// ListRuns 列出某工作流（空串表示全部）的运行快照。
	ListRuns(ctx context.Context, workflowName string) ([]*RunState, error)
	// Close 释放底层资源。
	Close() error
}

// snapshotRun 返回运行状态的深拷贝。
func snapshotRun(r *RunState) *RunState {
	if r == nil {
		return nil
	}
	cp := &RunState{
		WorkflowName: r.WorkflowName,
		RunID:        r.RunID,
		Status:       r.Status,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
		Tasks:        make(map[string]*TaskState, len(r.Tasks)),
	}
	for k, t := range r.Tasks {
		cp.Tasks[k] = snapshotTask(t)
	}
	return cp
}

// snapshotTask 返回任务状态的深拷贝（含可空时间指针）。
func snapshotTask(t *TaskState) *TaskState {
	if t == nil {
		return nil
	}
	cp := *t
	if t.LastStartedAt != nil {
		v := *t.LastStartedAt
		cp.LastStartedAt = &v
	}
	if t.LastEndedAt != nil {
		v := *t.LastEndedAt
		cp.LastEndedAt = &v
	}
	return &cp
}
