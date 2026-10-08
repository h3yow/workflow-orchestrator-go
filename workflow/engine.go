package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Engine 是 DAG 工作流编排引擎：加载校验工作流、触发运行，
// 并以“每次运行一个串行调度循环 + 每尝试一个执行 goroutine”的方式
// 推进依赖、处理重试/超时，并把所有状态迁移持久化到 Store。
//
// 多进程协调：多个进程（各自一个 Engine）指向同一个 Store 时，
// 同一运行任一时刻只有一个进程持有租约并负责推进；持有者通过心跳续租。
// 心跳超时不会直接导致接管：接管方还须通过进程存活锁确认持有进程已彻底退出
// （暂停/卡顿但仍存活的进程不会被抢走任务）。所有状态写入都携带 fencing token，
// 掉线进程的迟到写入会被拒绝。
type Engine struct {
	store    Store
	executor Executor
	logger   Logger
	nowFunc  func() time.Time

	owner        string
	leaseTTL     time.Duration
	pollInterval time.Duration

	mu       sync.Mutex
	registry map[string]*dag
	active   map[string]*runLoop
	wg       sync.WaitGroup

	// presence 为本进程（本引擎）的存活凭证，首个运行调度前惰性获取，
	// 随引擎存活一直持有，Close 时释放；其他进程据此判断本进程是否还活着。
	presenceOnce sync.Once
	presence     Presence
	presenceErr  error

	// stallWarn 记录每个 run 上次打印“持有者存活但心跳超时、继续等待”
	// 决策的时刻，避免轮询等待期间日志刷屏。
	stallWarnMu sync.Mutex
	stallWarn   map[string]time.Time

	// heartbeatGate 仅用于测试：非 nil 时每次心跳计时器触发后都会先尝试
	// 从此通道接收（被阻塞即等价于整个进程被 SIGSTOP/调度饿死：
	// 心跳停摆、租约 TTL 到期，但引擎 goroutine 与存活锁都还在）。
	heartbeatGate chan struct{}

	stopOnce sync.Once
	stopped  chan struct{}
}

// Option 配置引擎。
type Option func(*engineConfig)

type engineConfig struct {
	nowFunc      func() time.Time
	leaseTTL     time.Duration
	pollInterval time.Duration
}

// WithClock 注入自定义时间源（主要用于测试）。
// 注意：租约计时始终使用真实墙钟，不受该时间源影响。
func WithClock(f func() time.Time) Option {
	return func(c *engineConfig) { c.nowFunc = f }
}

// WithLeaseTTL 设置租约“心跳超时”时长：持有者超过该时长没有续上心跳，
// 仅被视为“暂时失联”；接管方还会通过进程存活锁确认其已彻底退出才真正接管，
// 因此进程还活着（暂停/卡顿）时任务不会被重跑。心跳间隔为 TTL/3。
// 默认 DefaultLeaseTTL。
func WithLeaseTTL(d time.Duration) Option {
	return func(c *engineConfig) {
		if d > 0 {
			c.leaseTTL = d
		}
	}
}

// WithPollInterval 设置等待他人持有的运行时轮询状态库的间隔。
// 默认 DefaultPollInterval。
func WithPollInterval(d time.Duration) Option {
	return func(c *engineConfig) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

// NewEngine 创建编排引擎。每个引擎实例拥有进程级唯一的 owner 标识，
// 用于租约归属与 fencing。
func NewEngine(store Store, executor Executor, logger Logger, opts ...Option) *Engine {
	cfg := &engineConfig{
		nowFunc:      timeNow,
		leaseTTL:     DefaultLeaseTTL,
		pollInterval: DefaultPollInterval,
	}
	for _, o := range opts {
		o(cfg)
	}
	if logger == nil {
		logger = nopLogger{}
	}
	return &Engine{
		store:        store,
		executor:     executor,
		logger:       logger,
		nowFunc:      cfg.nowFunc,
		owner:        newOwnerID(),
		leaseTTL:     cfg.leaseTTL,
		pollInterval: cfg.pollInterval,
		registry:     make(map[string]*dag),
		active:       make(map[string]*runLoop),
		stallWarn:    make(map[string]time.Time),
		stopped:      make(chan struct{}),
	}
}

// ensurePresence 惰性获取本进程的存活凭证；任何要去抢运行租约之前
// 都必须先持有它，这样本进程日后即使暂停/卡顿，其他进程也能确认“它还活着”。
func (e *Engine) ensurePresence() error {
	e.presenceOnce.Do(func() {
		e.presence, e.presenceErr = e.store.HoldPresence(context.Background(), e.owner)
	})
	return e.presenceErr
}

// noteStallWait 对“持有者存活但心跳超时”的等待日志按 TTL 节流：
// 每个 run 至多每隔一个 TTL 打印一条，返回是否本次应当打印。
func (e *Engine) noteStallWait(runID string) bool {
	now := e.nowFunc()
	e.stallWarnMu.Lock()
	defer e.stallWarnMu.Unlock()
	last, ok := e.stallWarn[runID]
	if ok && now.Sub(last) < e.leaseTTL {
		return false
	}
	e.stallWarn[runID] = now
	return true
}

// Register 加载并注册一份工作流定义；校验失败时返回可区分的错误。
func (e *Engine) Register(def WorkflowDef) error {
	d, err := loadAndValidate(def)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.registry[d.name] = d
	e.mu.Unlock()
	return nil
}

func (e *Engine) lookupWorkflow(name string) (*dag, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	d, ok := e.registry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrWorkflowNotRegistered, name)
	}
	return d, nil
}

// Trigger 触发一次运行。相同 runID 的并发触发（含跨进程）只会产生一个运行
// 与一套任务实例：已存在时返回既有快照（existed=true）。
//
// 触发后本进程会尝试获取该运行的租约：获取成功则开始推进；
// 若租约被其他存活进程持有（快照的 Lease 可见持有方），本进程不重复调度，
// 调用方可通过 RunAndWait 等待其收尾或在其掉线后接管。
func (e *Engine) Trigger(ctx context.Context, workflowName, runID string) (*RunState, bool, error) {
	d, err := e.lookupWorkflow(workflowName)
	if err != nil {
		return nil, false, err
	}
	now := e.nowFunc()
	fresh := &RunState{
		WorkflowName: workflowName,
		RunID:        runID,
		Status:       RunRunning,
		Tasks:        initialTaskStates(workflowName, runID, d.order, now),
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	existed, err := e.store.CreateRunIfNotExists(ctx, fresh)
	if err != nil {
		return nil, false, err
	}
	snap, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, false, err
	}
	if existed {
		e.logger.LogDecision(Decision{RunID: runID, Kind: DecisionAlreadyRunning,
			Detail: fmt.Sprintf("workflow=%s 已存在，触发去重", workflowName)})
	} else {
		e.logger.LogDecision(Decision{RunID: runID, Kind: DecisionTriggered,
			Detail: fmt.Sprintf("workflow=%s 创建运行，任务数=%d", workflowName, len(fresh.Tasks))})
	}
	// 无论新建还是命中既有运行（含重启/他进程掉线后的恢复），都尝试接管调度；
	// 租约被他人持有时 ensureScheduled 返回 nil，本进程不重复调度。
	if _, err := e.ensureScheduled(d, runID); err != nil {
		return snap, existed, err
	}
	return snap, existed, nil
}

// RunAndWait 调度（若尚未调度且租约可用）并阻塞直到运行进入终态或 ctx 取消。
// 若运行由其他进程持有，本进程只轮询状态库等待其收尾；
// 只有在持有者心跳超时且进程存活锁确认其已退出后，本进程才会接管并继续推进；
// 持有者只是暂停/卡顿但仍存活时，本进程一直等待，不会重跑它手上的任务。
func (e *Engine) RunAndWait(ctx context.Context, runID string) (RunStatus, error) {
	for {
		snap, err := e.store.GetRun(ctx, runID)
		if err != nil {
			return "", err
		}
		if snap.Status != RunRunning {
			return snap.Status, nil // 已是终态（含恢复后立刻收尾），直接返回
		}
		d, err := e.lookupWorkflow(snap.WorkflowName)
		if err != nil {
			return "", err
		}
		loop, err := e.ensureScheduled(d, runID)
		if err != nil {
			return "", err
		}
		if loop == nil {
			// 归其他存活进程管：轮询等待其推进或掉线，不干扰对方。
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-e.stopped:
				return e.finalStatus(runID)
			case <-time.After(e.pollInterval):
				continue
			}
		}
		select {
		case <-loop.done:
			// 循环退出（可能已收尾，也可能租约丢失）：回读状态决定下一步。
		case <-ctx.Done():
			final, gerr := e.store.GetRun(context.Background(), runID)
			if gerr == nil && final.Status != RunRunning {
				return final.Status, nil
			}
			return "", ctx.Err()
		case <-e.stopped:
			return e.finalStatus(runID)
		}
	}
}

func (e *Engine) finalStatus(runID string) (RunStatus, error) {
	final, err := e.store.GetRun(context.Background(), runID)
	if err != nil {
		return "", err
	}
	return final.Status, nil
}

// Result 读取一次运行的完整结果视图：终态、每个任务的最终状态、
// 尝试总次数与每一次尝试的结算记录。任何进程在任意时刻调用，
// 读到的都是同一份完整落盘状态（原子快照），多次读取结果一致。
func (e *Engine) Result(ctx context.Context, runID string) (*RunState, error) {
	return e.store.GetRun(ctx, runID)
}

// Close 停止引擎：取消所有在飞尝试、停止退避与心跳计时器、
// 释放持有的租约（其他进程可立即接管），等待调度循环退出，
// 最后释放本进程的存活凭证（向外界宣告本进程不再负责任何运行）。
func (e *Engine) Close() error {
	e.stopOnce.Do(func() { close(e.stopped) })
	e.wg.Wait()
	if e.presence != nil {
		return e.presence.Release()
	}
	return nil
}

// initialTaskStates 为 DAG 中每个任务创建唯一归属的初始实例。
func initialTaskStates(workflowName, runID string, order []string, now time.Time) map[string]*TaskState {
	tasks := make(map[string]*TaskState, len(order))
	for _, name := range order {
		tasks[name] = &TaskState{
			WorkflowName: workflowName,
			RunID:        runID,
			TaskName:     name,
			Status:       StatusPending,
			UpdatedAt:    now,
		}
	}
	return tasks
}
