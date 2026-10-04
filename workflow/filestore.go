package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FileStore 将每次运行以一个 JSON 文件落盘，配合 flock 互斥锁，
// 可在多进程/重启场景下保证并发触发的幂等与状态一致。
type FileStore struct {
	dir string
	mu  sync.Mutex // 进程内串行化，配合文件锁覆盖跨进程并发
}

// NewFileStore 打开（必要时创建）状态目录。
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(runID string) string {
	return filepath.Join(s.dir, sanitize(runID)+".json")
}

func sanitize(runID string) string {
	r := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", "..", "__")
	return r.Replace(runID)
}

// fileLock 是目录级互斥文件锁的句柄。
type fileLock struct {
	f *os.File
}

func (s *FileStore) lock() (*fileLock, error) {
	f, err := os.OpenFile(filepath.Join(s.dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) unlock() {
	syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	l.f.Close()
}

func (s *FileStore) readLocked(runID string) (*RunState, error) {
	b, err := os.ReadFile(s.path(runID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrRunNotFound
		}
		return nil, err
	}
	var r RunState
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("decode run state %s: %w", runID, err)
	}
	return &r, nil
}

func (s *FileStore) writeLocked(r *RunState) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(r.RunID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(r.RunID))
}

// CreateRunIfNotExists 以 O_CREATE|O_EXCL 语义原子创建运行文件。
func (s *FileStore) CreateRunIfNotExists(ctx context.Context, state *RunState) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return false, err
	}
	defer l.unlock()

	if _, err := s.readLocked(state.RunID); err == nil {
		return true, nil
	} else if err != ErrRunNotFound {
		return false, err
	}
	if err := s.writeLocked(state); err != nil {
		return false, err
	}
	return false, nil
}

// GetRun 读取运行状态深拷贝快照。
func (s *FileStore) GetRun(ctx context.Context, runID string) (*RunState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer l.unlock()
	r, err := s.readLocked(runID)
	if err != nil {
		return nil, err
	}
	return snapshotRun(r), nil
}

// PutTask 以 fencing token 条件写入单个任务实例。
func (s *FileStore) PutTask(ctx context.Context, runID string, task *TaskState, token int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return false, err
	}
	defer l.unlock()
	r, err := s.readLocked(runID)
	if err != nil {
		return false, err
	}
	if !fenced(r, token) {
		return false, nil
	}
	cp := snapshotTask(task)
	cp.UpdatedAt = timeNow()
	r.Tasks[task.TaskName] = cp
	r.UpdatedAt = timeNow()
	return true, s.writeLocked(r)
}

// PutRunStatus 以 fencing token 条件写入运行聚合状态。
func (s *FileStore) PutRunStatus(ctx context.Context, runID string, status RunStatus, token int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return false, err
	}
	defer l.unlock()
	r, err := s.readLocked(runID)
	if err != nil {
		return false, err
	}
	if !fenced(r, token) {
		return false, nil
	}
	r.Status = status
	r.UpdatedAt = timeNow()
	return true, s.writeLocked(r)
}

// AcquireRunLease 原子地获取或重入续期运行租约（目录锁内读-改-写）。
func (s *FileStore) AcquireRunLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, *RunState, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return false, nil, err
	}
	defer l.unlock()
	r, err := s.readLocked(runID)
	if err != nil {
		return false, nil, err
	}
	if !acquireLease(r, owner, ttl, timeNow()) {
		return false, snapshotRun(r), nil
	}
	if err := s.writeLocked(r); err != nil {
		return false, nil, err
	}
	return true, snapshotRun(r), nil
}

// RenewRunLease 在 owner+token 匹配时延长租约。
func (s *FileStore) RenewRunLease(ctx context.Context, runID, owner string, token int64, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return false, err
	}
	defer l.unlock()
	r, err := s.readLocked(runID)
	if err != nil {
		return false, err
	}
	if !renewLease(r, owner, token, ttl, timeNow()) {
		return false, nil
	}
	return true, s.writeLocked(r)
}

// ReleaseRunLease 在 owner+token 匹配时释放租约；失配为 no-op。
func (s *FileStore) ReleaseRunLease(ctx context.Context, runID, owner string, token int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return err
	}
	defer l.unlock()
	r, err := s.readLocked(runID)
	if err != nil {
		return err
	}
	releaseLease(r, owner, token, timeNow())
	return s.writeLocked(r)
}

// ListRuns 列出指定工作流（空串表示全部）的运行，按 RunID 排序。
func (s *FileStore) ListRuns(ctx context.Context, workflowName string) ([]*RunState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer l.unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []*RunState
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		runID := strings.TrimSuffix(e.Name(), ".json")
		r, err := s.readLocked(runID)
		if err != nil {
			return nil, err
		}
		if workflowName != "" && r.WorkflowName != workflowName {
			continue
		}
		out = append(out, snapshotRun(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RunID < out[j].RunID })
	return out, nil
}

// Close 释放资源。
func (s *FileStore) Close() error { return nil }
