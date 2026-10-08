package workflow

import "time"

// 本文件实现“进程存活判定”的共享逻辑：
// 租约到期不等于持有进程死亡——只有存活锁确认其已退出后才允许接管。
//
// 接管门控（takeover gate）需要 I/O 探测存活锁，因此放在各 Store 的
// AcquireRunLease 实现中（目录锁/互斥锁内完成），本文件只提供
// 纯判定函数 acquireGate，Store 再根据返回值决定是否做存活探测。

// acquireGateResult 是“本次获取租约请求”在不考虑存活锁时的初步分类。
type acquireGateResult int

const (
	// gateAcquire 租约空闲或持有者正是自己（重入续期）：直接获取/续期。
	gateAcquire acquireGateResult = iota
	// gateHeld 租约由其他存活心跳持有（未到期）：必须拒绝。
	gateHeld
	// gateProbe 租约由他人持有且已到期：心跳已停，必须探测其存活锁，
	// 探测结果为“仍存活”则拒绝接管，“已退出”方可接管。
	gateProbe
)

// acquireGate 对一次租约获取请求做纯判定（不触碰存活锁）。
// probeOwner 仅在结果为 gateProbe 时有意义，表示需要被探测的旧持有者。
func acquireGate(r *RunState, owner string, now time.Time) (gate acquireGateResult, probeOwner string) {
	if r.Lease == nil || r.Lease.Owner == owner {
		return gateAcquire, ""
	}
	if !r.Lease.expired(now) {
		return gateHeld, r.Lease.Owner
	}
	return gateProbe, r.Lease.Owner
}
