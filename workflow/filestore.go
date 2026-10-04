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

// PutTask 持久化单个任务实例。
func (s *FileStore) PutTask(ctx context.Context, runID string, task *TaskState) error {
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
	cp := snapshotTask(task)
	cp.UpdatedAt = timeNow()
	r.Tasks[task.TaskName] = cp
	r.UpdatedAt = timeNow()
	return s.writeLocked(r)
}

// PutRunStatus 持久化运行聚合状态。
func (s *FileStore) PutRunStatus(ctx context.Context, runID string, status RunStatus) error {
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
	r.Status = status
	r.UpdatedAt = timeNow()
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

// claimHeartbeatInterval 是归属锁文件中心跳信息的刷新周期。
// 心跳仅供观察（ClaimInfo），归属判定只依赖内核文件锁本身。
const claimHeartbeatInterval = 500 * time.Millisecond

// claimInfo 是 .runlock 文件中记录的所有者信息。
type claimInfo struct {
	Owner      string    `json:"owner"`
	PID        int       `json:"pid"`
	AcquiredAt time.Time `json:"acquired_at"`
	Heartbeat  time.Time `json:"heartbeat"`
}

// fileClaim 是基于 per-run 文件锁的跨进程归属权。
// 锁在整个持有期间保持；持有进程被强杀时内核自动释放，
// 其他进程的 TryClaimRun 随后即可成功，实现接手。
type fileClaim struct {
	f     *os.File
	owner string
	done  chan struct{}
	wg    sync.WaitGroup
}

// TryClaimRun 以 LOCK_EX|LOCK_NB 尝试认领 runID 的调度归属权。
// 同一 runID 的锁文件在任一时刻至多被一个进程持有（含跨进程）。
func (s *FileStore) TryClaimRun(ctx context.Context, runID, owner string) (RunClaim, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, sanitize(runID)+".runlock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, nil // 其他进程持有，本进程只读不调度
		}
		return nil, err
	}
	c := &fileClaim{f: f, owner: owner, done: make(chan struct{})}
	c.writeHeartbeat()
	c.wg.Add(1)
	go c.heartbeatLoop()
	return c, nil
}

func (c *fileClaim) Owner() string { return c.owner }

// writeHeartbeat 就地覆写锁文件内容。不能走临时文件 + rename：
// 锁绑定在 inode 上，替换文件会让新打开者拿到未加锁的 inode。
func (c *fileClaim) writeHeartbeat() {
	info := claimInfo{Owner: c.owner, PID: os.Getpid(), Heartbeat: timeNow()}
	if b, err := json.Marshal(info); err == nil {
		_, _ = c.f.WriteAt(b, 0)
		_ = c.f.Truncate(int64(len(b)))
	}
}

func (c *fileClaim) heartbeatLoop() {
	defer c.wg.Done()
	t := time.NewTicker(claimHeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.writeHeartbeat()
		case <-c.done:
			return
		}
	}
}

// Release 停止心跳并释放文件锁。
func (c *fileClaim) Release() error {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	c.wg.Wait()
	syscall.Flock(int(c.f.Fd()), syscall.LOCK_UN)
	return c.f.Close()
}

// ClaimInfo 读取 runID 归属锁文件中的所有者信息，仅供观察与运维排查。
// 无人持有或内容暂不可读时返回 (nil, nil)。
func (s *FileStore) ClaimInfo(runID string) (*claimInfo, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, sanitize(runID)+".runlock"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var info claimInfo
	if err := json.Unmarshal(b, &info); err != nil {
		return nil, nil // 心跳覆写窗口内的半截内容，按无信息处理
	}
	return &info, nil
}
