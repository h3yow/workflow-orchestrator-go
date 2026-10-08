package workflow

import (
	"context"
	"testing"
	"time"
)

// plainLeaseStore 把所有 Store 方法显式委托给 MemoryStore，
// 刻意不暴露 HoldOwner/ProbeOwner，用于模拟“只实现 Store 的第三方存储”。
type plainLeaseStore struct {
	inner Store
}

func (s *plainLeaseStore) CreateRunIfNotExists(ctx context.Context, state *RunState) (bool, error) {
	return s.inner.CreateRunIfNotExists(ctx, state)
}
func (s *plainLeaseStore) GetRun(ctx context.Context, runID string) (*RunState, error) {
	return s.inner.GetRun(ctx, runID)
}
func (s *plainLeaseStore) ListRuns(ctx context.Context, wf string) ([]*RunState, error) {
	return s.inner.ListRuns(ctx, wf)
}
func (s *plainLeaseStore) AcquireRunLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, *RunState, error) {
	return s.inner.AcquireRunLease(ctx, runID, owner, ttl)
}
func (s *plainLeaseStore) RenewRunLease(ctx context.Context, runID, owner string, token int64, ttl time.Duration) (bool, error) {
	return s.inner.RenewRunLease(ctx, runID, owner, token, ttl)
}
func (s *plainLeaseStore) ReleaseRunLease(ctx context.Context, runID, owner string, token int64) error {
	return s.inner.ReleaseRunLease(ctx, runID, owner, token)
}
func (s *plainLeaseStore) PutTask(ctx context.Context, runID string, t *TaskState, token int64) (bool, error) {
	return s.inner.PutTask(ctx, runID, t, token)
}
func (s *plainLeaseStore) PutRunStatus(ctx context.Context, runID string, status RunStatus, token int64) (bool, error) {
	return s.inner.PutRunStatus(ctx, runID, status, token)
}
func (s *plainLeaseStore) Close() error { return s.inner.Close() }

// TestCustomStoreWithoutLivenessFallback 确认未实现 LivenessStore 的自定义 Store
// 仍可正常工作（接管只看租约 TTL），不破坏既有第三方实现。
func TestCustomStoreWithoutLivenessFallback(t *testing.T) {
	st := &plainLeaseStore{inner: NewMemoryStore()}
	if _, ok := any(st).(LivenessStore); ok {
		t.Fatal("plainLeaseStore 不应实现 LivenessStore")
	}

	var calls int64
	exec := MapExecutor{"t": func(context.Context, string, string, int) AttemptResult {
		calls++
		return AttemptResult{}
	}}
	eng := NewEngine(st, exec, nil, WithLeaseTTL(50*time.Millisecond),
		WithPollInterval(5*time.Millisecond))
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.Register(WorkflowDef{Name: "wf", Tasks: []TaskDef{
		{Name: "t", Retry: RetryPolicy{MaxAttempts: 1}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eng.Trigger(context.Background(), "wf", "r"); err != nil {
		t.Fatal(err)
	}
	status, err := eng.RunAndWait(context.Background(), "r")
	if err != nil {
		t.Fatal(err)
	}
	if status != RunSucceeded || calls != 1 {
		t.Fatalf("status=%s calls=%d，退化路径也应正常跑一次", status, calls)
	}
}
