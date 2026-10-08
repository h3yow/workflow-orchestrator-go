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
//     且 fencing token 单调递增、绝不复用；
//   - HoldPresence 以“进程生命周期”为粒度持有存活凭证，
//     进程被强杀时由操作系统自动释放（FileStore 用内核级 flock），
//     从而让接管方区分“还活着只是暂时没响应”与“进程已彻底退出”。
type Store interface {
	// HoldPresence 以 owner 名义持有一份“进程存活凭证”。
	// 引擎在生命周期内（启动到 Close）始终持有它；同一进程可重复调用，
	// 返回的 Presence 在 Close（或进程退出）前持续有效。
	// FileStore 的实现是一个被独占 flock 锁住的文件：进程只是暂停/卡顿
	// （如 SIGSTOP、GC、机器抖动）时锁依然在；进程被 kill -9 后由内核释放。
	HoldPresence(ctx context.Context, owner string) (Presence, error)
	// OwnerAlive 非阻塞地探测 owner 是否仍然持有存活凭证：
	//   - alive=true：owner 进程确认还活着（哪怕它的租约心跳已超时），
	//     调用方绝不可接管其运行；
	//   - alive=false：owner 凭证无人持有（进程已退出，或从未注册），
	//     租约心跳又已超时的运行允许接管；
	//   - err 非 nil：探测本身不可用，调用方按“无法确认死亡”处理，继续等待。
	OwnerAlive(ctx context.Context, owner string) (alive bool, err error)
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
