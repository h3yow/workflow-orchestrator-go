package workflow

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// TestPresenceFileLockLiveness 直接验证 FileStore 存活锁的核心语义：
//   - 持有存活锁期间，任何其他 Store 句柄都探测为“存活”；
//   - 主动 Release 后立刻探测为“不存活”（等价于进程退出后内核释放 flock）；
//   - 从未注册过的 owner 也探测为“不存活”，且不报错（遗留状态/旧数据兼容）。
//
// 多轮重复以排除 flock 时序相关的偶发问题。
func TestPresenceFileLockLiveness(t *testing.T) {
	const rounds = 5
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")

	for i := 0; i < rounds; i++ {
		st, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		owner := fmt.Sprintf("owner-%d", i)

		if alive, err := st.OwnerAlive(ctx, owner); err != nil || alive {
			t.Fatalf("round %d：未注册的 owner 应探测为不存活 alive=%v err=%v", i, alive, err)
		}

		p, err := st.HoldPresence(ctx, owner)
		if err != nil {
			t.Fatalf("round %d：HoldPresence: %v", i, err)
		}

		// 另一个独立句柄（独立文件描述）必须看到锁仍被持有。
		probe, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		if alive, err := probe.OwnerAlive(ctx, owner); err != nil || !alive {
			t.Fatalf("round %d：持有期间 owner 应存活 alive=%v err=%v", i, alive, err)
		}

		if err := p.Release(); err != nil {
			t.Fatalf("round %d：Release: %v", i, err)
		}
		if alive, err := probe.OwnerAlive(ctx, owner); err != nil || alive {
			t.Fatalf("round %d：释放后 owner 应不存活 alive=%v err=%v", i, alive, err)
		}
		_ = st.Close()
		_ = probe.Close()
	}
}

// TestPresenceMemoryRegistry 验证 MemoryStore 的进程内存活注册表：
// 持有时存活，释放后不存活，未注册者不存活；引用计数支持同 owner 重复持有。
func TestPresenceMemoryRegistry(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if alive, _ := s.OwnerAlive(ctx, "e1"); alive {
		t.Fatal("未注册 owner 不应存活")
	}
	p1, err := s.HoldPresence(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := s.HoldPresence(ctx, "e1") // 同一 owner 再持有一次（引用计数）
	if err != nil {
		t.Fatal(err)
	}
	if alive, _ := s.OwnerAlive(ctx, "e1"); !alive {
		t.Fatal("持有期间 owner 应存活")
	}
	if err := p1.Release(); err != nil {
		t.Fatal(err)
	}
	if alive, _ := s.OwnerAlive(ctx, "e1"); !alive {
		t.Fatal("还有一个引用时 owner 仍应存活")
	}
	if err := p2.Release(); err != nil {
		t.Fatal(err)
	}
	if alive, _ := s.OwnerAlive(ctx, "e1"); alive {
		t.Fatal("全部释放后 owner 不应存活")
	}
}
