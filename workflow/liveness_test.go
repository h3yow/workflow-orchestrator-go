package workflow

import (
	"context"
	"testing"
	"time"
)

// TestOwnerLivenessGate 单元验证接管门控的核心区分：
//   - 旧持有者的租约到期但存活锁仍在（进程还活着，哪怕暂时无响应）：拒绝接管；
//   - 存活锁释放/进程退出后：允许接管，且 fencing token 递增。
//
// 内存与文件两种 Store 都覆盖；文件实现基于真实 flock，
// 跨 FileStore 实例的语义与真实多进程一致。
func TestOwnerLivenessGate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		newS     func(t *testing.T) Store
		kill     func(owner string)
		alive    func(ctx context.Context, s Store, owner string) (bool, error)
		conflict func(t *testing.T, s Store, owner string)
	}{
		{
			name: "memory",
			newS: func(t *testing.T) Store { return NewMemoryStore() },
			kill: func(owner string) { SimulateOwnerExit(owner) },
			alive: func(ctx context.Context, s Store, owner string) (bool, error) {
				return s.(LivenessStore).ProbeOwner(ctx, owner)
			},
		},
		{
			name: "file",
			newS: func(t *testing.T) Store {
				st, err := NewFileStore(t.TempDir() + "/state")
				if err != nil {
					t.Fatal(err)
				}
				return st
			},
			kill: func(string) {}, // 文件实现：关闭 hold 句柄即等价于进程退出
			alive: func(ctx context.Context, s Store, owner string) (bool, error) {
				return s.(LivenessStore).ProbeOwner(ctx, owner)
			},
			// 文件实现允许同一 FileStore 同 owner 重入（句柄缓存）；
			// “同名存活者重持失败”要用另一个独立实例验证（等价另一个进程）。
			conflict: func(t *testing.T, s Store, owner string) {
				dir := s.(*FileStore).dir
				other, err := NewFileStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				if _, err := other.HoldOwner(context.Background(), owner); err != ErrOwnerLockHeld {
					t.Fatalf("另一实例重持存活 owner 应得到 ErrOwnerLockHeld，实际 %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := tc.newS(t)
			defer st.Close()
			ls := st.(LivenessStore)

			// A 持有自己的存活锁。
			holdA, err := ls.HoldOwner(ctx, "owner-A")
			if err != nil {
				t.Fatalf("HoldOwner A: %v", err)
			}
			if alive, err := ls.ProbeOwner(ctx, "owner-A"); err != nil || !alive {
				t.Fatalf("A 持锁期间探测 alive=%v err=%v，应为存活", alive, err)
			}
			if tc.conflict != nil {
				tc.conflict(t, st, "owner-A")
			} else if _, err := ls.HoldOwner(ctx, "owner-A"); err != ErrOwnerLockHeld {
				t.Fatalf("同名存活 owner 重持应得到 ErrOwnerLockHeld，实际 %v", err)
			}
			// 没人持有的 owner 应探测为已退出。
			if alive, err := ls.ProbeOwner(ctx, "owner-ghost"); err != nil || alive {
				t.Fatalf("ghost owner alive=%v err=%v，应为已退出", alive, err)
			}

			// 构造一份 A 持有的、已到期的租约。
			now := timeNow()
			rs := &RunState{
				WorkflowName: "wf", RunID: "r", Status: RunRunning,
				Tasks: map[string]*TaskState{
					"t": {WorkflowName: "wf", RunID: "r", TaskName: "t", Status: StatusReady},
				},
				CreatedAt: now, UpdatedAt: now,
				Lease:     &LeaseInfo{Owner: "owner-A", Token: 1, Until: now.Add(-time.Millisecond)},
				LastToken: 1,
			}
			if _, err := st.CreateRunIfNotExists(ctx, rs); err != nil {
				t.Fatal(err)
			}

			// A 还活着：B 即使面对已到期租约也不能接管。
			if ok, _, err := st.AcquireRunLease(ctx, "r", "owner-B", time.Second); err != nil || ok {
				t.Fatalf("存活持有者 A 只是心跳暂停时，B 不得接管 ok=%v err=%v", ok, err)
			}
			if alive, err := ls.ProbeOwner(ctx, "owner-A"); err != nil || !alive {
				t.Fatalf("拒绝接管后 A 仍应存活 alive=%v err=%v", alive, err)
			}

			// A 彻底退出（强杀等价：句柄释放/内核回收 flock）。
			if err := holdA.Release(); err != nil {
				t.Fatal(err)
			}
			tc.kill("owner-A")
			if alive, err := ls.ProbeOwner(ctx, "owner-A"); err != nil || alive {
				t.Fatalf("A 退出后 alive=%v err=%v，应为已退出", alive, err)
			}
			// B 接管，token 递增。
			ok, snap, err := st.AcquireRunLease(ctx, "r", "owner-B", time.Second)
			if err != nil || !ok {
				t.Fatalf("A 退出后 B 应接管 ok=%v err=%v", ok, err)
			}
			if snap.Lease.Token <= 1 || snap.Lease.Owner != "owner-B" {
				t.Fatalf("接管后租约 owner=%s token=%d，应属 B 且 token>1", snap.Lease.Owner, snap.Lease.Token)
			}
		})
	}
}
