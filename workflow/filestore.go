package workflow

import (
	"context"
	"encoding/json"
	"errors"
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

// presenceDir 存放每个引擎进程的存活锁文件：一个 owner 一个文件，
// 文件上持有独占 flock 即代表该进程还活着。
func (s *FileStore) presenceDir() string { return filepath.Join(s.dir, ".owners") }

func (s *FileStore) presencePath(owner string) string {
	return filepath.Join(s.presenceDir(), sanitize(owner)+".lock")
}

// filePresence 是一个被独占 flock 持有的存活锁文件句柄。
type filePresence struct {
	f *os.File
}

func (p *filePresence) Release() error {
	if p == nil || p.f == nil {
		return nil
	}
	err1 := syscall.Flock(int(p.f.Fd()), syscall.LOCK_UN)
	err2 := p.f.Close()
	p.f = nil
	if err1 != nil {
		return err1
	}
	return err2
}

// HoldPresence 创建/打开 owner 的存活锁文件并以阻塞方式取得独占 flock。
// 锁随返回的 Presence 持有，直到 Release 或进程退出（含 kill -9）由内核释放。
// 取得自己的锁后，尽力清理目录里“无人持有的陈旧锁文件”：
// 能被 LOCK_NB 抢到即证明对应进程已退出（或从未运行完启动），删除安全；
// 抢不到说明对方仍存活，绝不动它的文件。清理失败不影响持有本身。
func (s *FileStore) HoldPresence(ctx context.Context, owner string) (Presence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.presenceDir(), 0o755); err != nil {
		return nil, fmt.Errorf("create presence dir: %w", err)
	}
	f, err := os.OpenFile(s.presencePath(owner), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	p := &filePresence{f: f}
	s.sweepStalePresence(owner)
	return p, nil
}

// sweepStalePresence 删除其他 owner 无人持有的陈旧存活锁文件。
// 仅在调用方已持有自己的存活锁后调用；任何 I/O 错误都忽略（尽力而为）。
func (s *FileStore) sweepStalePresence(self string) {
	entries, err := os.ReadDir(s.presenceDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".lock") {
			continue
		}
		if name := strings.TrimSuffix(e.Name(), ".lock"); name == sanitize(self) {
			continue
		}
		path := filepath.Join(s.presenceDir(), e.Name())
		old, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			continue
		}
		if err := syscall.Flock(int(old.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			// EWOULDBLOCK：对应进程仍存活，保留其锁文件；其他错误同样保守跳过。
			_ = old.Close()
			continue
		}
		// 抢到锁：无人持有，删除后立即释放。
		_ = os.Remove(path)
		_ = syscall.Flock(int(old.Fd()), syscall.LOCK_UN)
		_ = old.Close()
	}
}

// OwnerAlive 非阻塞地探测 owner 的存活锁：
// 抢得到（LOCK_NB）说明无人持有 -> owner 已退出（或从未注册），返回 false；
// EWOULDBLOCK 说明锁被持有 -> owner 进程还活着（暂停也仍持有），返回 true。
// 存活锁文件（或 .owners 目录）尚不存在时，说明该 owner 从未在本目录注册过，
// 同样视为不存活（典型于手工构造的遗留状态或旧版本数据）。
func (s *FileStore) OwnerAlive(ctx context.Context, owner string) (alive bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f, err := os.OpenFile(s.presencePath(owner), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer func() {
		// 关闭本描述即释放本次试探性加的锁。
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return true, nil
	}
	return false, err
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
	now := timeNow()
	holderAlive := func(holder string) (bool, error) {
		return s.OwnerAlive(context.Background(), holder)
	}
	if !acquireLease(r, owner, ttl, now, holderAlive) {
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
