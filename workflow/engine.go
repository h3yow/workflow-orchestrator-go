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
// 同一运行任一时刻只有一个进程持有租约并负责推进；
// 持有者通过心跳续租，掉线后租约到期，其他进程即可接管。
// 所有状态写入都携带 fencing token，掉线进程的迟到写入会被拒绝。
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
	// waitLogged 记录“等待他人/存活但无响应”决策的最近一次输出时刻，用于限频。
	waitLogged map[string]time.Time
	wg         sync.WaitGroup

	// ownerHold 为本进程的存活锁；仅当 Store 实现 LivenessStore 时非 nil。
	// 持有期间其他进程不得接管本进程租约到期的运行。
	ownerHold OwnerHold

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

// WithLeaseTTL 设置运行租约时长：持有者心跳中断超过该时长后租约到期。
// 到期后并不会立即被接管——其他进程还会通过进程存活锁确认持有者已彻底退出；
// 若持有者只是暂停/卡顿（仍存活），则继续等待而不是重跑。心跳间隔为 TTL/3。
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
//
// 若 Store 实现了 LivenessStore（内置实现均实现），引擎会在首次调度前
// 惰性持有一把进程级“存活锁”：持有期间即使心跳暂时中断，其他进程也不能
// 接管本进程在跑的运行（见 README「多进程协调」）。
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
		waitLogged:   make(map[string]time.Time),
		stopped:      make(chan struct{}),
	}
}

// ensureOwnerHold 惰性持有本进程的存活锁（仅一次）。
// 不实现 LivenessStore 的 Store 跳过：退化为仅按租约 TTL 判定的旧行为。
func (e *Engine) ensureOwnerHold() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ownerHold != nil {
		return nil
	}
	ls, ok := e.store.(LivenessStore)
	if !ok {
		return nil
	}
	h, err := ls.HoldOwner(context.Background(), e.owner)
	if err != nil {
		return fmt.Errorf("hold owner liveness lock: %w", err)
	}
	e.ownerHold = h
	e.logger.Logf("进程存活锁已持有 owner=%s：存活期间即使心跳暂停，其他进程也不会接管", e.owner)
	return nil
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
// 持有者掉线（租约过期）后，本进程会自动接管并继续推进。
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
// 释放持有的租约（其他进程可立即接管），等待调度循环退出后，
// 最后释放进程级存活锁（此后其他进程的存活探测才会判定本进程已退出）。
func (e *Engine) Close() error {
	e.stopOnce.Do(func() { close(e.stopped) })
	e.wg.Wait()
	var closeErr error
	e.mu.Lock()
	h := e.ownerHold
	e.ownerHold = nil
	e.mu.Unlock()
	if h != nil {
		// 存活锁最后释放：先让租约落盘为无人持有，再宣告本进程退出，
		// 保证观察到“存活锁消失”的进程一定能读到释放后的租约。
		closeErr = h.Release()
	}
	return closeErr
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
