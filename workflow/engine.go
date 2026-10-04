package workflow

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// Engine 是 DAG 工作流编排引擎：加载校验工作流、触发运行，
// 并以“每次运行一个串行调度循环 + 每尝试一个执行 goroutine”的方式
// 推进依赖、处理重试/超时，并把所有状态迁移持久化到 Store。
type Engine struct {
	store    Store
	executor Executor
	logger   Logger
	nowFunc  func() time.Time

	ownerID       string
	claimInterval time.Duration // >0 时后台巡检并认领无人负责的 RUNNING 运行
	pollInterval  time.Duration // 非归属方等待终态的轮询间隔

	mu       sync.Mutex
	registry map[string]*dag
	active   map[string]*runLoop
	wg       sync.WaitGroup

	stopOnce sync.Once
	stopped  chan struct{}
}

// Option 配置引擎。
type Option func(*engineConfig)

type engineConfig struct {
	nowFunc       func() time.Time
	ownerID       string
	claimInterval time.Duration
	pollInterval  time.Duration
}

// WithClock 注入自定义时间源（主要用于测试）。
func WithClock(f func() time.Time) Option {
	return func(c *engineConfig) { c.nowFunc = f }
}

// WithOwnerID 设置本进程作为运行归属方的标识（用于日志与锁文件心跳）。
// 缺省为 主机名-PID-随机后缀。
func WithOwnerID(id string) Option {
	return func(c *engineConfig) { c.ownerID = id }
}

// WithClaimInterval 开启后台巡检：每隔 d 扫描一次仍处于 RUNNING 且
// 无人持有的运行并尝试认领接手。多进程部署时各 worker 都应开启；
// 接手延迟上界约为一个巡检周期（持有进程死亡即释放文件锁）。
func WithClaimInterval(d time.Duration) Option {
	return func(c *engineConfig) { c.claimInterval = d }
}

// WithPollInterval 设置非归属方在 RunAndWait 中轮询终态的间隔（缺省 50ms）。
func WithPollInterval(d time.Duration) Option {
	return func(c *engineConfig) { c.pollInterval = d }
}

// NewEngine 创建编排引擎。
func NewEngine(store Store, executor Executor, logger Logger, opts ...Option) *Engine {
	cfg := &engineConfig{
		nowFunc:      timeNow,
		ownerID:      defaultOwnerID(),
		pollInterval: 50 * time.Millisecond,
	}
	for _, o := range opts {
		o(cfg)
	}
	if logger == nil {
		logger = nopLogger{}
	}
	e := &Engine{
		store:         store,
		executor:      executor,
		logger:        logger,
		nowFunc:       cfg.nowFunc,
		ownerID:       cfg.ownerID,
		claimInterval: cfg.claimInterval,
		pollInterval:  cfg.pollInterval,
		registry:      make(map[string]*dag),
		active:        make(map[string]*runLoop),
		stopped:       make(chan struct{}),
	}
	if cfg.claimInterval > 0 {
		e.wg.Add(1)
		go e.claimLoop()
	}
	return e
}

// defaultOwnerID 生成进程级归属标识。
func defaultOwnerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
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

// Trigger 触发一次运行。相同 runID 的并发触发只会产生一个运行与一套任务实例：
// 已存在时返回既有快照（existed），且不会重复启动调度。
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
		// 若该运行尚未在本进程被调度（例如重启恢复后），仍需启动调度。
		if _, err := e.ensureScheduled(d, snap); err != nil {
			return snap, true, err
		}
		return snap, true, nil
	}

	e.logger.LogDecision(Decision{RunID: runID, Kind: DecisionTriggered,
		Detail: fmt.Sprintf("workflow=%s 创建运行，任务数=%d", workflowName, len(fresh.Tasks))})
	if _, err := e.ensureScheduled(d, snap); err != nil {
		return snap, false, err
	}
	return snap, false, nil
}

// RunAndWait 调度（若尚未调度）并阻塞直到运行进入终态或 ctx 取消。
func (e *Engine) RunAndWait(ctx context.Context, runID string) (RunStatus, error) {
	snap, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return "", err
	}
	d, err := e.lookupWorkflow(snap.WorkflowName)
	if err != nil {
		return "", err
	}
	if snap.Status != RunRunning {
		return snap.Status, nil // 已是终态（含恢复后立刻收尾），直接返回
	}
	loop, err := e.ensureScheduled(d, snap)
	if err != nil {
		return "", err
	}
	if loop == nil {
		// 不归本进程调度：要么刚刚被其他调用者收尾，要么归属权在别的进程。
		// 轮询状态直到终态，保证调用方拿到与其他进程一致的结论。
		return e.waitTerminal(ctx, runID)
	}
	select {
	case <-loop.done:
	case <-ctx.Done():
		final, gerr := e.store.GetRun(context.Background(), runID)
		if gerr == nil && final.Status != RunRunning {
			return final.Status, nil
		}
		return "", ctx.Err()
	case <-e.stopped:
	}
	final, err := e.store.GetRun(context.Background(), runID)
	if err != nil {
		return "", err
	}
	return final.Status, nil
}

// waitTerminal 轮询运行状态直到终态或 ctx 取消。
// 用于归属权在其他进程时的等待方：读到的是同一份持久化状态，
// 因此结论与执行方完全一致。
func (e *Engine) waitTerminal(ctx context.Context, runID string) (RunStatus, error) {
	t := time.NewTicker(e.pollInterval)
	defer t.Stop()
	for {
		snap, err := e.store.GetRun(ctx, runID)
		if err != nil {
			return "", err
		}
		if snap.Status != RunRunning {
			return snap.Status, nil
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// claimLoop 周期性认领无人负责的 RUNNING 运行（多进程接手入口）。
func (e *Engine) claimLoop() {
	defer e.wg.Done()
	t := time.NewTicker(e.claimInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.claimPendingRuns()
		case <-e.stopped:
			return
		}
	}
}

// claimPendingRuns 扫描所有运行，对仍处 RUNNING 且本进程未在调度的运行
// 尝试认领归属权；认领成功即启动（恢复）调度循环。
func (e *Engine) claimPendingRuns() {
	runs, err := e.store.ListRuns(context.Background(), "")
	if err != nil {
		e.logger.Logf("claim scan: list runs: %v", err)
		return
	}
	for _, r := range runs {
		if r.Status != RunRunning {
			continue
		}
		e.mu.Lock()
		_, active := e.active[r.RunID]
		e.mu.Unlock()
		if active {
			continue
		}
		d, err := e.lookupWorkflow(r.WorkflowName)
		if err != nil {
			continue // 本进程未注册该工作流，不参与认领
		}
		if _, err := e.ensureScheduled(d, r); err != nil {
			e.logger.Logf("claim scan: schedule run %s: %v", r.RunID, err)
		}
	}
}

// Close 停止引擎，等待在飞调度收尾（在飞尝试的结果仍会被持久化）。
func (e *Engine) Close() error {
	e.stopOnce.Do(func() { close(e.stopped) })
	e.wg.Wait()
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
