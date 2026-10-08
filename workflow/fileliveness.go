package workflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ownerLockDir 是状态目录下存放各进程“存活锁”文件的子目录名。
const ownerLockDir = ".owners"

// fileOwnerHold 是 FileStore 上一把进程级排他 flock 的句柄。
// 锁绑定在打开文件描述上：进程被 kill -9 或任何方式退出后，
// 内核自动释放该 flock——这是区分“暂时没响应但还活着”与“彻底退出”的依据。
type fileOwnerHold struct {
	f     *os.File
	store *FileStore
	owner string
}

func (s *FileStore) ownerPath(owner string) string {
	return filepath.Join(s.dir, ownerLockDir, sanitize(owner)+".lock")
}

// HoldOwner 以非阻塞排他 flock 持有 owner 的存活锁，直到 Release 或进程退出。
// 锁文件在释放时保留不删（避免探测方与持有方在文件创建/删除上竞争）。
func (s *FileStore) HoldOwner(ctx context.Context, owner string) (OwnerHold, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if h, ok := s.ownerHolds[owner]; ok {
		return h, nil // 同一 FileStore 上同 owner 重入，复用既有句柄。
	}
	if err := os.MkdirAll(filepath.Join(s.dir, ownerLockDir), 0o755); err != nil {
		return nil, fmt.Errorf("create owner lock dir: %w", err)
	}
	f, err := os.OpenFile(s.ownerPath(owner), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open owner lock: %w", err)
	}
	// 非阻塞排他锁：同名 owner 仍存活时直接失败，而不是等待。
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, ErrOwnerLockHeld
		}
		return nil, fmt.Errorf("flock owner lock: %w", err)
	}
	h := &fileOwnerHold{f: f, store: s, owner: owner}
	s.ownerHolds[owner] = h
	return h, nil
}

// ProbeOwner 以“尝试加非阻塞排他锁”的方式探测 owner 进程是否仍存活：
// 加锁被拒（EWOULDBLOCK）说明存活锁仍被持有——进程还活着，哪怕它暂时
// 没有心跳；加锁成功说明锁已无人持有——进程已彻底退出，随后立即解锁。
func (s *FileStore) ProbeOwner(ctx context.Context, owner string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probeOwnerLocked(owner)
}

// probeOwnerLocked 在已持有 s.mu 时探测（供 AcquireRunLease 在同一临界区复用）。
func (s *FileStore) probeOwnerLocked(owner string) (bool, error) {
	// 本进程（同一 FileStore）自己持有的存活锁必然存活。
	if _, ok := s.ownerHolds[owner]; ok {
		return true, nil
	}
	// 目录可能由本进程首次创建（例如接管一个由旧版本/其他目录写入的遗留状态）。
	if err := os.MkdirAll(filepath.Join(s.dir, ownerLockDir), 0o755); err != nil {
		return false, fmt.Errorf("create owner lock dir: %w", err)
	}
	f, err := os.OpenFile(s.ownerPath(owner), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return false, fmt.Errorf("open owner lock for probe: %w", err)
	}
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		// 加锁成功：无人持有，owner 已退出。立即释放探测锁。
		if uerr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); uerr != nil {
			return false, fmt.Errorf("unlock probe owner lock: %w", uerr)
		}
		return false, nil
	}
	if err == syscall.EWOULDBLOCK {
		return true, nil // 锁仍被某个存活进程持有。
	}
	return false, fmt.Errorf("probe owner lock: %w", err)
}

// Release 主动释放存活锁（引擎优雅关闭时调用）。
func (h *fileOwnerHold) Release() error {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if cur, ok := h.store.ownerHolds[h.owner]; ok && cur != h {
		return nil // 已被替换/释放，避免误关别人的句柄。
	}
	delete(h.store.ownerHolds, h.owner)
	if err := syscall.Flock(int(h.f.Fd()), syscall.LOCK_UN); err != nil {
		_ = h.f.Close()
		return err
	}
	return h.f.Close()
}
