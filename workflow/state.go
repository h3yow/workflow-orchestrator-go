package workflow

import (
	"context"
	"errors"
	"time"
)

// ErrRunNotFound 表示指定的运行不存在（例如未触发即调度）。
var ErrRunNotFound = errors.New("workflow run not found")

// ErrWorkflowNotRegistered 表示触发了未注册的工作流。
var ErrWorkflowNotRegistered = errors.New("workflow not registered")

// Store 是运行状态的持久化接口，同时承担跨进程协调职责。
// 所有实现必须保证：
//   - 同一个 (RunID, TaskName) 的归属唯一，CreateRunIfNotExists 幂等；
//   - 同一运行任一时刻至多一个持有者拥有有效租约；
//   - 租约的获取/续期/释放与 fenced 写入都是“读-改-写”原子的
//     （内存实现用互斥锁，文件实现用 flock），
//     且 fencing token 单调递增、绝不复用。
type Store interface {
	// CreateRunIfNotExists 原子地创建运行；已存在时返回 existed=true。
	CreateRunIfNotExists(ctx context.Context, state *RunState) (existed bool, err error)
	// GetRun 读取一次运行的完整状态快照（深拷贝）。
	// 该快照即结果视图：任何进程在任意时刻读到的都是一份完整、一致的落盘状态。
	GetRun(ctx context.Context, runID string) (*RunState, error)
	// ListRuns 列出某工作流（空串表示全部）的运行快照。
	ListRuns(ctx context.Context, workflowName string) ([]*RunState, error)

	// AcquireRunLease 原子地获取运行租约。
	// 租约空闲（nil）、已过期、或本就属于 owner（重入续期，token 不变）时成功；
	// 被其他存活持有者占用时返回 acquired=false 及当前快照。
	// 成功时 token 递增（重入除外），并返回获取后的最新快照。
	AcquireRunLease(ctx context.Context, runID, owner string, ttl time.Duration) (acquired bool, snap *RunState, err error)
	// RenewRunLease 在 owner+token 与当前租约匹配时延长租约；
	// 失配（租约已易主）返回 ok=false，调用方必须立即停止推进该运行。
	RenewRunLease(ctx context.Context, runID, owner string, token int64, ttl time.Duration) (ok bool, err error)
	// ReleaseRunLease 在 owner+token 匹配时释放租约；失配时为 no-op。
	ReleaseRunLease(ctx context.Context, runID, owner string, token int64) error

	// PutTask 以 fencing token 条件写入单个任务实例状态：
	// 仅当 token 等于该运行当前租约 token 时生效（ok=true），
	// 否则拒绝（ok=false）——掉线进程的迟到写入不会覆盖新持有者的状态。
	PutTask(ctx context.Context, runID string, task *TaskState, token int64) (ok bool, err error)
	// PutRunStatus 以 fencing token 条件写入运行聚合状态，语义同 PutTask。
	PutRunStatus(ctx context.Context, runID string, status RunStatus, token int64) (ok bool, err error)

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
		LastToken:    r.LastToken,
		Tasks:        make(map[string]*TaskState, len(r.Tasks)),
	}
	if r.Lease != nil {
		lease := *r.Lease
		cp.Lease = &lease
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
	if t.Attempts != nil {
		cp.Attempts = make([]AttemptRecord, len(t.Attempts))
		copy(cp.Attempts, t.Attempts)
	}
	return &cp
}
