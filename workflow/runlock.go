package workflow

import "context"

// RunClaim 表示某进程对一次运行的调度归属权。
// 任一时刻同一 runID 至多只有一个持有者；持有者负责推进该运行的调度，
// 其他进程只能读取状态。持有进程被强杀时，底层文件锁由内核自动释放，
// 其他进程即可认领接手。
type RunClaim interface {
	// Owner 返回持有进程的标识。
	Owner() string
	// Release 主动释放归属权（引擎关闭或运行结束时调用）。
	Release() error
}

// ClaimStore 是 Store 的可选扩展：支持跨进程的运行级归属认领。
// 引擎对实现了该接口的 Store 会在调度前认领运行；认领失败表示
// 该运行正由其他进程负责，本进程只读不调度。
type ClaimStore interface {
	Store
	// TryClaimRun 尝试认领 runID 的调度归属权。
	// 认领成功返回 RunClaim；已被其他进程持有时返回 (nil, nil)。
	TryClaimRun(ctx context.Context, runID, owner string) (RunClaim, error)
}
