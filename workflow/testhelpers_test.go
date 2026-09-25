package workflow

import (
	"context"
)

// initialRunOnDisk 直接在 FileStore 中构造一份运行快照，
// 用于模拟崩溃后磁盘上遗留的任意中间状态。
func initialRunOnDisk(dir, workflowName, runID string, taskOrder []string,
	taskFn func(name string) *TaskState) (*FileStore, error) {
	store, err := NewFileStore(dir)
	if err != nil {
		return nil, err
	}
	now := timeNow()
	rs := &RunState{
		WorkflowName: workflowName,
		RunID:        runID,
		Status:       RunRunning,
		Tasks:        map[string]*TaskState{},
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	for _, name := range taskOrder {
		ts := taskFn(name)
		if ts.UpdatedAt.IsZero() {
			ts.UpdatedAt = now
		}
		rs.Tasks[name] = ts
	}
	if _, err := store.CreateRunIfNotExists(context.Background(), rs); err != nil {
		return nil, err
	}
	return store, nil
}
