package workflow

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// 跨进程协调的默认时间参数。
const (
	// DefaultLeaseTTL 是运行租约的默认“心跳超时”时长：持有方超过该时长没续上心跳，
	// 只代表“暂时联系不上”，并不立即触发接管——接管方还须通过存活锁确认持有进程
	// 已彻底退出（见 acquireLease 与 Store.OwnerAlive）。详见 README「多进程协调」。
	DefaultLeaseTTL = 2 * time.Second
	// leaseRenewDivisor 决定心跳间隔 = TTL / leaseRenewDivisor。
	leaseRenewDivisor = 3
	// DefaultPollInterval 是等待他人持有的运行时，轮询状态库的间隔。
	DefaultPollInterval = 100 * time.Millisecond
)

// LeaseInfo 是一次运行的执行租约，随 RunState 一起持久化。
// 任一时刻至多一个进程持有有效租约并负责推进该运行；
// Token 是单调递增的 fencing token，每次租约易主加一，
// 用于拒绝掉线进程的迟到写入，防止两份状态互相覆盖。
//
// 注意：租约“到期”（Until 已过）只意味着心跳超时；是否允许接管还要看持有
// 进程的存活锁（Presence）。进程暂停/卡顿时心跳会超时但存活锁仍在，
// 此时绝不接管，避免把还活着的进程手上的任务抢过来重跑。
type LeaseInfo struct {
	// Owner 为持有方标识（进程级唯一）。
	Owner string
	// Token 为 fencing token，每次租约被获取（含接管）时递增。
	Token int64
	// Until 为租约到期时刻（UTC）。超过该时刻未续期即视为掉线。
	Until time.Time
}

// expired 报告租约在 now 时刻是否已失效（nil 租约视为可获取）。
func (l *LeaseInfo) expired(now time.Time) bool {
	return l == nil || !now.Before(l.Until)
}

// Presence 是“进程仍存活”的凭证，由 Store.HoldPresence 颁发，
// 生命周期与持有的引擎进程一致。进程正常 Close、崩溃或被强杀后
// 凭证随之失效（FileStore 依赖内核对 flock 的自动释放），
// 因此接管方可以可靠区分“进程还活着只是暂时没响应”与“进程已彻底退出”。
type Presence interface {
	// Release 主动放弃凭证（引擎 Close 时调用）；重复调用安全。
	Release() error
}

// acquireLease 在 r 上为 owner 获取（或重入续期）租约。
// 调用方必须已持有覆盖 r 的互斥原语（内存锁或文件锁）。
//
// 接管条件（owner 与当前持有者不同时）：
//   - 租约仍在 TTL 内：持有者心跳正常，绝不接管，返回 false；
//   - 租约心跳已超时，但 holderAlive=true：持有者进程确认还活着
//     （只是暂停/卡顿没来得及心跳），仍然不接管，返回 false ——
//     宁可让运行等着，也不能把还活着的进程手上的任务抢过来重跑；
//   - 租约心跳已超时且 holderAlive=false：持有者进程已彻底退出
//     （存活凭证已释放），才允许接管并递增 fencing token。
//
// holderAlive 只在“租约心跳已超时且持有者不是自己”时被调用；
// 探测失败（err!=nil）按“无法确认持有者死亡”处理，不接管。
func acquireLease(r *RunState, owner string, ttl time.Duration, now time.Time,
	holderAlive func(holder string) (bool, error)) bool {
	if r.Lease != nil && r.Lease.Owner != owner {
		if !r.Lease.expired(now) {
			return false
		}
		alive, err := holderAlive(r.Lease.Owner)
		if err != nil || alive {
			// 持有者还活着，或存活状态暂时无法确认：一律不接管。
			return false
		}
	}
	if r.Lease != nil && r.Lease.Owner == owner {
		// 重入：同一持有者续期，token 保持不变。
		r.Lease.Until = now.Add(ttl)
		r.UpdatedAt = now
		return true
	}
	r.LastToken++
	r.Lease = &LeaseInfo{Owner: owner, Token: r.LastToken, Until: now.Add(ttl)}
	r.UpdatedAt = now
	return true
}

// renewLease 在 owner+token 匹配时延长租约，失配返回 false。
func renewLease(r *RunState, owner string, token int64, ttl time.Duration, now time.Time) bool {
	if r.Lease == nil || r.Lease.Owner != owner || r.Lease.Token != token {
		return false
	}
	r.Lease.Until = now.Add(ttl)
	r.UpdatedAt = now
	return true
}

// releaseLease 在 owner+token 匹配时释放租约（LastToken 保持不复用）。
func releaseLease(r *RunState, owner string, token int64, now time.Time) {
	if r.Lease == nil || r.Lease.Owner != owner || r.Lease.Token != token {
		return
	}
	r.Lease = nil
	r.UpdatedAt = now
}

// fenced 报告 token 是否与当前租约匹配（持有方才允许写入）。
func fenced(r *RunState, token int64) bool {
	return r.Lease != nil && r.Lease.Token == token
}

// newOwnerID 生成进程级唯一的持有方标识：进程号 + 随机后缀，
// 保证同一台机器上先后启动的进程不会撞名。
func newOwnerID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("pid-%d", os.Getpid())
	}
	return fmt.Sprintf("pid-%d-%s", os.Getpid(), hex.EncodeToString(b[:]))
}
