package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store 是 run 状态的持久化抽象。
// 实现必须保证同一进程内并发调用是安全的；返回的 *Run 为独立副本，
// 调用方在副本上的修改不会影响存储内部状态，必须重新 SaveRun 才会生效。
type Store interface {
	// CreateRun 幂等创建一个 run；若同 ID 已存在则返回既有 run。
	CreateRun(ctx context.Context, r *Run) (*Run, error)
	// GetRun 读取一个 run；不存在时错误可被 errors.Is(..., ErrRunNotFound) 判定。
	GetRun(ctx context.Context, id string) (*Run, error)
	// SaveRun 整体保存 run 状态（持久化最新快照）。
	SaveRun(ctx context.Context, r *Run) error
}

// cloneRun 通过 JSON 序列化得到深拷贝，隔离内外状态。
func cloneRun(r *Run) (*Run, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("workflow: clone run %q: %w", r.ID, err)
	}
	var c Run
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("workflow: clone run %q: %w", r.ID, err)
	}
	return &c, nil
}

// MemoryStore 基于内存 map 的 Store，主要用于测试。
type MemoryStore struct {
	mu   sync.Mutex
	runs map[string]*Run
}

// NewMemoryStore 构造内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: make(map[string]*Run)}
}

func (s *MemoryStore) CreateRun(ctx context.Context, r *Run) (*Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.runs[r.ID]; ok {
		return cloneRun(existing)
	}
	cp, err := cloneRun(r)
	if err != nil {
		return nil, err
	}
	s.runs[r.ID] = cp
	return cloneRun(cp)
}

func (s *MemoryStore) GetRun(ctx context.Context, id string) (*Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrRunNotFound, id)
	}
	return cloneRun(r)
}

func (s *MemoryStore) SaveRun(ctx context.Context, r *Run) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cp, err := cloneRun(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[r.ID] = cp
	return nil
}

// FileStore 将每个 run 以独立 JSON 文件保存在本地目录，
// 写入采用 “临时文件 + fsync + rename” 原子替换，
// 进程中断时要么看到旧快照、要么看到完整新快照，不会读到半截文件。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileStore 构造基于目录 dir 的文件存储，目录不存在时自动创建。
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("workflow: empty store directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("workflow: create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func (s *FileStore) CreateRun(ctx context.Context, r *Run) (*Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if data, err := os.ReadFile(s.path(r.ID)); err == nil {
		var existing Run
		if err := json.Unmarshal(data, &existing); err != nil {
			return nil, fmt.Errorf("workflow: read run %q: %w", r.ID, err)
		}
		return &existing, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("workflow: read run %q: %w", r.ID, err)
	}
	if err := s.writeLocked(r); err != nil {
		return nil, err
	}
	return cloneRun(r)
}

func (s *FileStore) GetRun(ctx context.Context, id string) (*Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %q", ErrRunNotFound, id)
		}
		return nil, fmt.Errorf("workflow: read run %q: %w", id, err)
	}
	var r Run
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("workflow: decode run %q: %w", id, err)
	}
	return &r, nil
}

func (s *FileStore) SaveRun(ctx context.Context, r *Run) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(r)
}

// writeLocked 执行原子写盘，调用方必须持有 s.mu。
func (s *FileStore) writeLocked(r *Run) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return fmt.Errorf("workflow: encode run %q: %w", r.ID, err)
	}
	tmp, err := os.CreateTemp(s.dir, ".run-*.tmp")
	if err != nil {
		return fmt.Errorf("workflow: create temp for run %q: %w", r.ID, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("workflow: write run %q: %w", r.ID, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("workflow: fsync run %q: %w", r.ID, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("workflow: close run %q: %w", r.ID, err)
	}
	if err := os.Rename(tmpName, s.path(r.ID)); err != nil {
		cleanup()
		return fmt.Errorf("workflow: rename run %q: %w", r.ID, err)
	}
	return nil
}
