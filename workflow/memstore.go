package workflow

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore 是进程内的 Store 实现，使用互斥锁保证并发触发下
// 任务实例归属唯一（同一 (RunID, TaskName) 只会存在一个状态），
// 并以同一互斥锁实现租约获取/续期/释放与 fencing 校验的原子性。
// 适用于单进程多引擎（或测试）场景；跨进程请使用 FileStore。
type MemoryStore struct {
	mu      sync.Mutex
	runs    map[string]*RunState
	present map[string]int // 持有存活凭证的 owner -> 引用计数
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: make(map[string]*RunState), present: make(map[string]int)}
}

// memPresence 是内存版存活凭证，仅用于单进程内模拟：
// 持有期间 owner 出现在注册表中，Release（引擎 Close）即注销。
type memPresence struct {
	s     *MemoryStore
	owner string
}

func (p *memPresence) Release() error {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	if n := p.s.present[p.owner]; n > 1 {
		p.s.present[p.owner] = n - 1
	} else {
		delete(p.s.present, p.owner)
	}
	return nil
}

// HoldPresence 内存版：把 owner 登记为存活，直到凭证 Release。
func (s *MemoryStore) HoldPresence(ctx context.Context, owner string) (Presence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.present[owner]++
	s.mu.Unlock()
	return &memPresence{s: s, owner: owner}, nil
}

// OwnerAlive 内存版：owner 登记在册即视为存活。
func (s *MemoryStore) OwnerAlive(ctx context.Context, owner string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.present[owner] > 0, nil
}

// CreateRunIfNotExists 原子地创建运行；相同 runID 的并发调用只有一个生效。
func (s *MemoryStore) CreateRunIfNotExists(ctx context.Context, state *RunState) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[state.RunID]; ok {
		return true, nil
	}
	s.runs[state.RunID] = snapshotRun(state)
	return false, nil
}

// GetRun 返回运行状态的深拷贝快照。
func (s *MemoryStore) GetRun(ctx context.Context, runID string) (*RunState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return nil, ErrRunNotFound
	}
	return snapshotRun(r), nil
}

// ListRuns 列出指定工作流（空串表示全部）的运行快照，按 RunID 排序。
func (s *MemoryStore) ListRuns(ctx context.Context, workflowName string) ([]*RunState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*RunState
	for _, r := range s.runs {
		if workflowName != "" && r.WorkflowName != workflowName {
			continue
		}
		out = append(out, snapshotRun(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RunID < out[j].RunID })
	return out, nil
}

// AcquireRunLease 原子地获取或重入续期运行租约。
func (s *MemoryStore) AcquireRunLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, *RunState, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return false, nil, ErrRunNotFound
	}
	// 调用方已持有 s.mu，直接查注册表判断持有者是否仍存活。
	holderAlive := func(holder string) (bool, error) { return s.present[holder] > 0, nil }
	if !acquireLease(r, owner, ttl, timeNow(), holderAlive) {
		return false, snapshotRun(r), nil
	}
	return true, snapshotRun(r), nil
}

// RenewRunLease 在 owner+token 匹配时延长租约。
func (s *MemoryStore) RenewRunLease(ctx context.Context, runID, owner string, token int64, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return false, ErrRunNotFound
	}
	return renewLease(r, owner, token, ttl, timeNow()), nil
}

// ReleaseRunLease 在 owner+token 匹配时释放租约。
func (s *MemoryStore) ReleaseRunLease(ctx context.Context, runID, owner string, token int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return ErrRunNotFound
	}
	releaseLease(r, owner, token, timeNow())
	return nil
}

// PutTask 以 fencing token 条件写入任务实例（深拷贝写入，避免外部共享可变状态）。
func (s *MemoryStore) PutTask(ctx context.Context, runID string, task *TaskState, token int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return false, ErrRunNotFound
	}
	if !fenced(r, token) {
		return false, nil
	}
	cp := snapshotTask(task)
	cp.UpdatedAt = timeNow()
	r.Tasks[task.TaskName] = cp
	r.UpdatedAt = timeNow()
	return true, nil
}

// PutRunStatus 以 fencing token 条件写入运行聚合状态。
func (s *MemoryStore) PutRunStatus(ctx context.Context, runID string, status RunStatus, token int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return false, ErrRunNotFound
	}
	if !fenced(r, token) {
		return false, nil
	}
	r.Status = status
	r.UpdatedAt = timeNow()
	return true, nil
}

// Close 释放资源（内存实现无底层资源）。
func (s *MemoryStore) Close() error { return nil }
