package workflow

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore 是进程内的 Store 实现，使用互斥锁保证并发触发下
// 任务实例归属唯一（同一 (RunID, TaskName) 只会存在一个状态）。
type MemoryStore struct {
	mu   sync.Mutex
	runs map[string]*RunState
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: make(map[string]*RunState)}
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

// PutTask 持久化任务实例（深拷贝写入，避免外部共享可变状态）。
func (s *MemoryStore) PutTask(ctx context.Context, runID string, task *TaskState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return ErrRunNotFound
	}
	cp := snapshotTask(task)
	cp.UpdatedAt = timeNow()
	r.Tasks[task.TaskName] = cp
	r.UpdatedAt = timeNow()
	return nil
}

// PutRunStatus 持久化运行聚合状态。
func (s *MemoryStore) PutRunStatus(ctx context.Context, runID string, status RunStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok {
		return ErrRunNotFound
	}
	r.Status = status
	r.UpdatedAt = timeNow()
	return nil
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

// Close 释放资源（内存实现无底层资源）。
func (s *MemoryStore) Close() error { return nil }
