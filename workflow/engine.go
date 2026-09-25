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
type Engine struct {
	store    Store
	executor Executor
	logger   Logger
	nowFunc  func() time.Time

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
	nowFunc func() time.Time
}

// WithClock 注入自定义时间源（主要用于测试）。
func WithClock(f func() time.Time) Option {
	return func(c *engineConfig) { c.nowFunc = f }
}

// NewEngine 创建编排引擎。
func NewEngine(store Store, executor Executor, logger Logger, opts ...Option) *Engine {
	cfg := &engineConfig{nowFunc: timeNow}
	for _, o := range opts {
		o(cfg)
	}
	if logger == nil {
		logger = nopLogger{}
	}
	return &Engine{
		store:    store,
		executor: executor,
		logger:   logger,
		nowFunc:  cfg.nowFunc,
		registry: make(map[string]*dag),
		active:   make(map[string]*runLoop),
		stopped:  make(chan struct{}),
	}
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
		// 并发窗口中被其他调用者收尾，回读终态。
		final, gerr := e.store.GetRun(context.Background(), runID)
		if gerr != nil {
			return "", gerr
		}
		return final.Status, nil
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
