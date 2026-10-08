package workflow

import (
	"context"
	"sync"
)

// memOwnerRegistry 是跨 MemoryStore 实例共享的“存活登记表”，
// 用于在单进程测试中模拟真实的跨进程 flock 语义：
// 每个 owner 至多被一个存活实例登记；Release 或 SimulateOwnerExit 后
// 其他实例的 ProbeOwner 才能观察到其死亡。
//
// 真实部署中每个进程各自持有内核 flock，无需此类共享表；
// MemoryStore 仅用于单进程/测试场景。
var (
	memRegistryMu sync.Mutex
	memRegistry   = map[string]struct{}{}
)

type memOwnerHold struct {
	store *MemoryStore
	owner string
}

// HoldOwner 为 owner 登记存活；同名 owner 仍存活时失败。
func (s *MemoryStore) HoldOwner(ctx context.Context, owner string) (OwnerHold, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	memRegistryMu.Lock()
	defer memRegistryMu.Unlock()
	if _, ok := memRegistry[owner]; ok {
		return nil, ErrOwnerLockHeld
	}
	memRegistry[owner] = struct{}{}
	return &memOwnerHold{store: s, owner: owner}, nil
}

// ProbeOwner 报告 owner 是否仍登记为存活。
func (s *MemoryStore) ProbeOwner(ctx context.Context, owner string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	memRegistryMu.Lock()
	defer memRegistryMu.Unlock()
	_, alive := memRegistry[owner]
	return alive, nil
}

func (h *memOwnerHold) Release() error {
	memRegistryMu.Lock()
	defer memRegistryMu.Unlock()
	delete(memRegistry, h.owner)
	return nil
}

// SimulateOwnerExit 是测试辅助：强制注销 owner 的存活登记，
// 等价于持有进程被 kill -9 后内核回收 flock。
func SimulateOwnerExit(owner string) {
	memRegistryMu.Lock()
	defer memRegistryMu.Unlock()
	delete(memRegistry, owner)
}
