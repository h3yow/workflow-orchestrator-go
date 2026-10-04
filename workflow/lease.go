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
	// DefaultLeaseTTL 是运行租约的默认存活时长：持有方心跳中断超过该时长，
	// 即被视为掉线，其他进程可接管。详见 README「多进程协调」。
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

// acquireLease 在 r 上为 owner 获取（或重入续期）租约。
// 调用方必须已持有覆盖 r 的互斥原语（内存锁或文件锁）。
// 租约被其他存活持有者占用时返回 false；
// 成功时写入新租约并（非重入时）递增 fencing token。
func acquireLease(r *RunState, owner string, ttl time.Duration, now time.Time) bool {
	if r.Lease != nil && r.Lease.Owner != owner && !r.Lease.expired(now) {
		return false
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
