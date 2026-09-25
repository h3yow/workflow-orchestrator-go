package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// 尝试结果的固定取值，写入 Attempt.Result，保证日志与状态可解释、可复现。
const (
	resultSucceeded = "succeeded"
	resultFailed    = "failed"
	resultTimeout   = "timeout"
	resultPanic     = "panic"
	// resultInterrupted 表示因调度循环结束/进程取消而中断：
	// 不记为一次尝试结果，不消耗重试预算，任务回到 READY 等待租约回收。
	resultInterrupted = "interrupted"
)

const (
	// 默认领取租约：执行期间会按 lease/3 周期续租；
	// 进程被杀后心跳停止，租约到期即可被恢复方判定为中断。
	defaultLeaseDuration = 5 * time.Second
	// 默认空闲扫描周期（用于回收过期租约）。
	defaultPollInterval = 50 * time.Millisecond
)

// Engine 负责调度并执行某个工作流的 run。
type Engine struct {
	workflow *Workflow
	store    Store
	registry TaskRegistry
	logger   Logger
	engineID string
	lease    time.Duration
	poll     time.Duration
}

// Config 为引擎配置。
type Config struct {
	// EngineID 引擎实例标识，写入每个任务实例的 OwnerID（归属唯一的依据）。
	EngineID string
	// LeaseDuration 任务领取租约；零值使用默认值。
	LeaseDuration time.Duration
	// RecoveryPoll 空闲状态下扫描过期租约的周期；零值使用默认值。
	RecoveryPoll time.Duration
}

// runLocks 保证同一进程内同一 runID 只有一个调度循环。
var runLocks sync.Map // runID -> *sync.Mutex

type runToken struct{}

// NewEngine 构造引擎。
func NewEngine(wf *Workflow, store Store, registry TaskRegistry, logger Logger, cfg Config) *Engine {
	if wf == nil {
		panic("workflow: nil workflow")
	}
	if store == nil {
		panic("workflow: nil store")
	}
	id := cfg.EngineID
	if id == "" {
		id = fmt.Sprintf("engine-%d", time.Now().UnixNano())
	}
	lease := cfg.LeaseDuration
	if lease <= 0 {
		lease = defaultLeaseDuration
	}
	poll := cfg.RecoveryPoll
	if poll <= 0 {
		poll = defaultPollInterval
	}
	if logger == nil {
		logger = nopLogger{}
	}
	return &Engine{
		workflow: wf,
		store:    store,
		registry: registry,
		logger:   logger,
		engineID: id,
		lease:    lease,
		poll:     poll,
	}
}

type nopLogger struct{}

func (nopLogger) Printf(format string, args ...any) {}

// Trigger 并发安全地触发一个 run。
//
//   - run 不存在时创建（全部任务 PENDING）并开始调度；
//   - run 已存在且未结束时，同一进程内并发调用会等待同一个调度循环，
//     不会产生第二个任务执行归属；
//   - run 已结束时直接返回终态快照（成功任务绝不再次执行）；
//   - 进程重启后对既有未完成 run 调用 Trigger 等同 Recover。
func (e *Engine) Trigger(ctx context.Context, runID string) (*Run, error) {
	return e.startOrJoin(ctx, runID, false)
}

// Recover 从既有状态恢复并继续推进；run 不存在时返回 ErrRunNotFound。
func (e *Engine) Recover(ctx context.Context, runID string) (*Run, error) {
	return e.startOrJoin(ctx, runID, true)
}

func (e *Engine) startOrJoin(ctx context.Context, runID string, recoveryOnly bool) (*Run, error) {
	lockAny, _ := runLocks.LoadOrStore(runID, &sync.Mutex{})
	lock := lockAny.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		if recoveryOnly {
			return nil, err
		}
		run, err = e.store.CreateRun(ctx, e.newRun(runID))
		if err != nil {
			return nil, err
		}
	}
	if run.State != RunRunning {
		e.logger.Printf("[run=%s] run already terminal state=%s, no task will be re-executed", runID, run.State)
		return run, nil
	}
	return e.scheduleLoop(ctx, run)
}

// newRun 构造初始 run：所有任务 PENDING。
func (e *Engine) newRun(runID string) *Run {
	now := time.Now()
	r := &Run{
		ID:           runID,
		WorkflowName: e.workflow.Name(),
		State:        RunRunning,
		Tasks:        make(map[string]*TaskInstance, len(e.workflow.OrderedTasks())),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	for _, t := range e.workflow.OrderedTasks() {
		r.Tasks[t.ID] = &TaskInstance{TaskID: t.ID, State: TaskPending, UpdatedAt: now}
	}
	return r
}

// taskEvent 是执行 goroutine / 续租心跳 / 租约回收 投递给调度循环的事件。
type taskEvent struct {
	taskID     string
	attempt    Attempt
	ownerID    string
	generation int
	// heartbeat 为 true 表示仅续租，不携带尝试结果。
	heartbeat bool
}

// scheduleLoop 是每个 run 唯一的决策线程：
// 所有状态迁移只在这里发生（执行 goroutine 仅通过事件回报结果），
// 因而调度决策在并发触发下也是串行确定的。
func (e *Engine) scheduleLoop(ctx context.Context, run *Run) (*Run, error) {
	now := time.Now()
	// 恢复阶段先处理中断遗留：过期的 RUNNING 一律不承认其成功，回退重试。
	e.reapExpiredLeases(run, now)

	events := make(chan taskEvent, 128)
	done := make(chan struct{})
	var wg sync.WaitGroup
	// 循环退出后关闭 done：在飞 goroutine 即便回报也不会阻塞，且 wg 可正常回收。
	defer func() {
		close(done)
		wg.Wait()
	}()

	// 落盘始终使用独立 context：优雅关闭时传入 ctx 已取消，
	// 但中断状态仍必须持久化成功，否则恢复时无法区分中断点。
	persist := func(reason string) error {
		run.UpdatedAt = time.Now()
		if err := e.store.SaveRun(context.Background(), run); err != nil {
			return err
		}
		e.logger.Printf("[run=%s] persisted state=%s (%s)", run.ID, run.State, reason)
		return nil
	}

	// inflight 记录本循环已派出、尚未回报终态的任务（ownerID 即当前引擎）。
	inflight := make(map[string]bool)

	// shutdown 为 true 后进入排空阶段：不再调度新任务，
	// 仅等待在飞尝试以 interrupted 回报并落盘，然后返回。
	shutdown := false
	for {
		if !shutdown && ctx.Err() != nil {
			shutdown = true
			e.logger.Printf("[run=%s] shutdown requested; draining in-flight attempts", run.ID)
		}

		if !shutdown {
			changed := e.evaluate(ctx, run, now, events, done, &wg, inflight)
			if changed {
				now = time.Now()
				if err := persist("state transition after evaluation"); err != nil {
					return run, err
				}
			}
			if e.runDone(run, inflight) {
				return run, nil
			}
		} else if len(inflight) == 0 {
			// 所有在飞尝试的 interrupted 回报均已消费落盘。
			return run, ctx.Err()
		}

		// 结果事件优先，保证执行完成的尝试立刻落库；
		// 心跳仅在未达 poll 周期时用于续租与唤醒退避定时。
		select {
		case ev := <-events:
			now = time.Now()
			e.consumeEvent(run, ev, now, inflight)
			e.convergeRunState(run)
			// 尝试结果与续租都必须落盘，否则崩溃恢复会丢失决策。
			if err := persist("after event " + eventKind(ev)); err != nil {
				return run, err
			}
			continue
		default:
		}

		if shutdown {
			// 排空阶段阻塞等待在飞回报（goroutine 会因 ctx 取消很快回报 interrupted）。
			select {
			case ev := <-events:
				now = time.Now()
				e.consumeEvent(run, ev, now, inflight)
				e.convergeRunState(run)
				if err := persist("after event " + eventKind(ev) + " (draining)"); err != nil {
					return run, err
				}
			case <-time.After(e.poll):
			}
			continue
		}

		// 等待至：poll 周期、最近一个重试退避到期、事件到达或 ctx 取消。
		wait := e.poll
		if t := e.nextWakeup(run, now); t != nil {
			if d := t.Sub(now); d < wait {
				wait = d
			}
		}
		if wait < 0 {
			wait = 0
		}
		select {
		case ev := <-events:
			now = time.Now()
			e.consumeEvent(run, ev, now, inflight)
			e.convergeRunState(run)
			if err := persist("after event " + eventKind(ev)); err != nil {
				return run, err
			}
		case <-time.After(wait):
			now = time.Now()
			if e.reapExpiredLeases(run, now) {
				e.logger.Printf("[run=%s] reaped expired lease(s) at %s", run.ID, now.Format(time.RFC3339Nano))
			}
		case <-ctx.Done():
			// 下一轮进入排空阶段，不立即返回。
			now = time.Now()
		}
	}
}

// nextWakeup 返回最早一个需要唤醒调度器的时间点（重试退避到期）。
func (e *Engine) nextWakeup(run *Run, now time.Time) *time.Time {
	var earliest *time.Time
	for _, ti := range run.Tasks {
		if ti.State == TaskReady && ti.RetryNotBefore.After(now) {
			t := ti.RetryNotBefore
			if earliest == nil || t.Before(*earliest) {
				earliest = &t
			}
		}
	}
	return earliest
}

// consumeEvent 统一消费事件（心跳或尝试结果）。
func (e *Engine) consumeEvent(run *Run, ev taskEvent, now time.Time, inflight map[string]bool) {
	if ev.heartbeat {
		e.applyHeartbeat(run, ev, now)
	} else {
		e.applyAttemptResult(run, ev, now, inflight)
	}
}

// runDone 判断 run 是否可以收尾：无在飞任务且不存在仍可能推进的任务。
func (e *Engine) runDone(run *Run, inflight map[string]bool) bool {
	for _, ti := range run.Tasks {
		if inflight[ti.TaskID] {
			return false
		}
		switch ti.State {
		case TaskPending, TaskReady, TaskRunning:
			return false
		}
	}
	return true
}

// evaluate 做一轮完整调度决策，返回是否发生了状态迁移/任务派出。
func (e *Engine) evaluate(ctx context.Context, run *Run, now time.Time, events chan taskEvent, done <-chan struct{}, wg *sync.WaitGroup, inflight map[string]bool) bool {
	changed := false
	// 拓扑顺序遍历：先处理上游终态传播，再决定下游，单轮即可收敛。
	for _, t := range e.workflow.OrderedTasks() {
		ti := run.Tasks[t.ID]
		switch ti.State {
		case TaskPending:
			ready, blockedBy, failedUp := e.depsStatus(run, t.ID)
			if failedUp != "" {
				ti.State = TaskSkipped
				ti.ResultReason = fmt.Sprintf("upstream task %q did not succeed", failedUp)
				ti.UpdatedAt = now
				e.logger.Printf("[run=%s] decision task=%s PENDING->SKIPPED reason=%q", run.ID, t.ID, ti.ResultReason)
				changed = true
			} else if ready {
				ti.State = TaskReady
				ti.UpdatedAt = now
				e.logger.Printf("[run=%s] decision task=%s PENDING->READY reason=all deps succeeded (%v)", run.ID, t.ID, e.depIDs(t.ID))
				changed = true
			} else {
				e.logger.Printf("[run=%s] decision task=%s stay PENDING reason=waiting for %v", run.ID, t.ID, blockedBy)
			}
		case TaskReady:
			// 重试退避窗口未到则继续等待，不提前领取。
			if now.Before(ti.RetryNotBefore) {
				e.logger.Printf("[run=%s] decision task=%s stay READY reason=backoff until %s",
					run.ID, t.ID, ti.RetryNotBefore.Format(time.RFC3339Nano))
				continue
			}
			// 防御性二次确认：READY 仅可能由全部上游成功而来。
			if _, _, failedUp := e.depsStatus(run, t.ID); failedUp != "" {
				ti.State = TaskSkipped
				ti.ResultReason = fmt.Sprintf("upstream task %q did not succeed", failedUp)
				ti.UpdatedAt = now
				changed = true
				continue
			}
			e.launch(ctx, run, t, ti, now, events, done, wg, inflight)
			changed = true
		case TaskRunning:
			if ti.OwnerID == e.engineID && inflight[t.ID] {
				// 本引擎派出的在飞任务，等待事件回报。
				continue
			}
			if now.Before(ti.LeaseUntil) {
				continue // 被其他存活引擎持有
			}
			e.reapTask(run, ti, now)
			changed = true
		}
	}

	e.convergeRunState(run)
	return changed
}

// convergeRunState 在全部任务到达终态后，确定性地收敛 run 整体状态：
// 全部任务成功才 SUCCEEDED，只要存在 FAILED/SKIPPED 即为 FAILED。
func (e *Engine) convergeRunState(run *Run) {
	if run.State != RunRunning {
		return
	}
	terminal := true
	allSucceeded := true
	for _, ti := range run.Tasks {
		switch ti.State {
		case TaskSucceeded:
		case TaskFailed, TaskSkipped:
			allSucceeded = false
		default:
			terminal = false
			allSucceeded = false
		}
	}
	if terminal {
		if allSucceeded {
			run.State = RunSucceeded
		} else {
			run.State = RunFailed
		}
		e.logger.Printf("[run=%s] decision run -> %s: all tasks reached terminal states", run.ID, run.State)
	}
}

// depsStatus 返回：是否全部成功、未成功的阻塞依赖列表、第一个失败/跳过的上游。
func (e *Engine) depsStatus(run *Run, taskID string) (allSucceeded bool, blockedBy []string, failedUp string) {
	t, ok := e.workflow.Task(taskID)
	if !ok {
		return false, nil, ""
	}
	for _, dep := range uniqueDeps(t.Deps) {
		up := run.Tasks[dep]
		switch up.State {
		case TaskSucceeded:
		case TaskFailed, TaskSkipped:
			if failedUp == "" {
				failedUp = up.TaskID
			}
			blockedBy = append(blockedBy, up.TaskID)
		default:
			blockedBy = append(blockedBy, up.TaskID)
		}
	}
	return failedUp == "" && len(blockedBy) == 0, blockedBy, failedUp
}

func (e *Engine) depIDs(taskID string) []string {
	t, _ := e.workflow.Task(taskID)
	return uniqueDeps(t.Deps)
}

// launch 领取一个 READY 任务（归属写入当前引擎并持久化），随后异步执行。
func (e *Engine) launch(ctx context.Context, run *Run, t TaskDef, ti *TaskInstance, now time.Time, events chan<- taskEvent, done <-chan struct{}, wg *sync.WaitGroup, inflight map[string]bool) {
	ti.State = TaskRunning
	ti.OwnerID = e.engineID
	ti.LeaseUntil = now.Add(e.lease)
	ti.UpdatedAt = now
	ti.AttemptGeneration++
	generation := ti.AttemptGeneration
	inflight[t.ID] = true

	attempt := ti.AttemptCount + 1
	fn := e.registry[t.ID]
	e.logger.Printf("[run=%s] decision task=%s READY->RUNNING owner=%s attempt=%d gen=%d lease_until=%s",
		run.ID, t.ID, e.engineID, attempt, generation, ti.LeaseUntil.Format(time.RFC3339Nano))

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.executeAttempt(ctx, run.ID, t, ti.TaskID, attempt, generation, fn, events, done)
	}()
}

// executeAttempt 在独立 goroutine 中执行一次尝试：
// 先续租（租约覆盖执行窗口），再以超时 context 调用任务体，
// 并将 panic 转化为失败结果，保证任意路径都有且仅有一次事件回报。
func (e *Engine) executeAttempt(parent context.Context, runID string, t TaskDef, taskID string, attemptNo, generation int, fn TaskFunc, events chan<- taskEvent, done <-chan struct{}) {
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(e.lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case events <- taskEvent{taskID: taskID, ownerID: e.engineID, heartbeat: true, attempt: Attempt{Attempt: attemptNo}, generation: generation}:
				case <-done:
					close(heartbeatDone)
					return
				case <-heartbeatCtx.Done():
					close(heartbeatDone)
					return
				}
			case <-heartbeatCtx.Done():
				close(heartbeatDone)
				return
			}
		}
	}()

	started := time.Now()
	ev := taskEvent{taskID: taskID, ownerID: e.engineID, generation: generation}
	ev.attempt.Attempt = attemptNo
	ev.attempt.StartedAt = started

	timeout := t.Timeout.Timeout
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	report := func(result, reason string) {
		stopHeartbeat()
		<-heartbeatDone
		ev.attempt.Result = result
		ev.attempt.Reason = reason
		ev.attempt.FinishedAt = time.Now()
		select {
		case events <- ev:
		case <-done:
		}
	}

	if fn == nil {
		report(resultFailed, "no task function registered")
		return
	}

	// 任务体在内部 goroutine 中运行：即便它不响应 ctx 取消，
	// 超时后外层也立即以 timeout 收尾，不等待、也不接受它迟到的结果。
	type fnResult struct {
		err error
		rvr any
	}
	fnDone := make(chan fnResult, 1)
	go func() {
		res := fnResult{}
		defer func() {
			if r := recover(); r != nil {
				res.rvr = r
			}
			fnDone <- res
		}()
		res.err = fn(ctx, runID, attemptNo)
	}()

	select {
	case res := <-fnDone:
		switch {
		case res.rvr != nil:
			report(resultPanic, fmt.Sprintf("task panicked: %v", res.rvr))
		case res.err == nil:
			report(resultSucceeded, "task returned nil")
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			// 超时收尾优先：即使任务体同时返回了普通错误，也以 timeout 计，
			// 该尝试只产生这一条结果。
			report(resultTimeout, fmt.Sprintf("attempt exceeded timeout %s", timeout))
		default:
			report(resultFailed, res.err.Error())
		}
	case <-ctx.Done():
		// 超时（或调度循环结束）后不等任务体退出，立即唯一收尾。
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			report(resultTimeout, fmt.Sprintf("attempt exceeded timeout %s", timeout))
		} else {
			report(resultInterrupted, "attempt interrupted by shutdown/cancellation")
		}
	}
}

// applyHeartbeat 处理续租：仅当任务仍由本引擎持有时才延长租约。
func (e *Engine) applyHeartbeat(run *Run, ev taskEvent, now time.Time) {
	ti := run.Tasks[ev.taskID]
	// 心跳属于当前在飞尝试，当且仅当：任务仍 RUNNING、归属一致、
	// 且尝试号为 AttemptCount+1（AttemptCount 在结果落库时才递增）。
	if ti == nil || ti.State != TaskRunning || ti.OwnerID != ev.ownerID ||
		ti.AttemptGeneration != ev.generation {
		return // 归属已被回收或属于旧代次，忽略陈旧心跳。
	}
	ti.LeaseUntil = now.Add(e.lease)
	ti.UpdatedAt = now
}

// applyAttemptResult 在唯一决策线程中消费一次尝试的结果并做状态迁移。
func (e *Engine) applyAttemptResult(run *Run, ev taskEvent, now time.Time, inflight map[string]bool) {
	ti := run.Tasks[ev.taskID]
	t, ok := e.workflow.Task(ev.taskID)
	if !ok || ti == nil {
		return
	}
	// 归属校验：只有当前持有方、且对应当前在飞尝试号的回报才被接受，
	// 杜绝重复/陈旧结果落库。
	if ti.State != TaskRunning || ti.OwnerID != ev.ownerID ||
		ti.AttemptGeneration != ev.generation {
		delete(inflight, ev.taskID)
		e.logger.Printf("[run=%s] discard stale result task=%s state=%s owner=%s result=%s reason=%q",
			run.ID, ev.taskID, ti.State, ti.OwnerID, ev.attempt.Result, ev.attempt.Reason)
		return
	}
	delete(inflight, ev.taskID)

	// 中断（优雅关闭/ctx 取消）：不追加尝试记录、不递增 AttemptCount、
	// 不消耗重试预算，任务保持 RUNNING 但归属不变，等待租约到期被回收
	//（崩溃场景下根本没有这条事件；两种路径在恢复时语义一致）。
	if ev.attempt.Result == resultInterrupted {
		ti.UpdatedAt = now
		e.logger.Printf("[run=%s] attempt task=%s attempt=%d result=interrupted reason=%q (not counted as an attempt; lease will expire for recovery)",
			run.ID, ev.taskID, ev.attempt.Attempt, ev.attempt.Reason)
		return
	}

	ti.AttemptCount = ev.attempt.Attempt
	ti.Attempts = append(ti.Attempts, ev.attempt)
	ti.UpdatedAt = now
	e.logger.Printf("[run=%s] attempt task=%s attempt=%d result=%s reason=%q",
		run.ID, ev.taskID, ev.attempt.Attempt, ev.attempt.Result, ev.attempt.Reason)

	if ev.attempt.Result == resultSucceeded {
		ti.State = TaskSucceeded
		ti.ResultReason = "succeeded on attempt " + itoa(ev.attempt.Attempt)
		ti.LeaseUntil = time.Time{}
		e.logger.Printf("[run=%s] decision task=%s RUNNING->SUCCEEDED after %d attempt(s)",
			run.ID, ev.taskID, ev.attempt.Attempt)
		return
	}

	max := t.Retries.MaxAttempts
	if max <= 0 {
		max = 1
	}
	if ev.attempt.Attempt < max {
		// 回到 READY，由后续轮次带退避重新领取；同一任务不会并发重试。
		ti.State = TaskReady
		ti.OwnerID = ""
		ti.LeaseUntil = time.Time{}
		nextWait := t.Retries.Backoff * time.Duration(ev.attempt.Attempt)
		ti.RetryNotBefore = now.Add(nextWait)
		e.logger.Printf("[run=%s] decision task=%s RUNNING->READY attempt=%d failed (%s), will retry (max=%d) backoff=%s",
			run.ID, ev.taskID, ev.attempt.Attempt, ev.attempt.Result, max, nextWait)
		return
	}

	ti.State = TaskFailed
	ti.LeaseUntil = time.Time{}
	ti.ResultReason = fmt.Sprintf("failed after %d attempt(s); last=%s: %s",
		ev.attempt.Attempt, ev.attempt.Result, ev.attempt.Reason)
	e.logger.Printf("[run=%s] decision task=%s RUNNING->FAILED terminal after %d/%d attempts reason=%q",
		run.ID, ev.taskID, ev.attempt.Attempt, max, ti.ResultReason)
}

// reapExpiredLeases 回收所有租约过期的 RUNNING 任务（中断恢复入口）。
func (e *Engine) reapExpiredLeases(run *Run, now time.Time) bool {
	reaped := false
	for _, t := range e.workflow.OrderedTasks() {
		ti := run.Tasks[t.ID]
		if ti != nil && ti.State == TaskRunning && !now.Before(ti.LeaseUntil) {
			e.reapTask(run, ti, now)
			reaped = true
		}
	}
	return reaped
}

// reapTask 将中断在 RUNNING 的任务按“未成功”处理：未超尝试上限则回 READY 重试，
// 否则判 FAILED。绝不在中断点上承认成功。
func (e *Engine) reapTask(run *Run, ti *TaskInstance, now time.Time) {
	t, _ := e.workflow.Task(ti.TaskID)
	max := t.Retries.MaxAttempts
	if max <= 0 {
		max = 1
	}
	e.logger.Printf("[run=%s] decision task=%s lease expired owner=%s lease_until=%s -> interrupted attempt is not counted as success",
		run.ID, ti.TaskID, ti.OwnerID, ti.LeaseUntil.Format(time.RFC3339Nano))
	if ti.AttemptCount < max {
		ti.State = TaskReady
		ti.OwnerID = ""
		ti.LeaseUntil = time.Time{}
		ti.RetryNotBefore = now
	} else {
		ti.State = TaskFailed
		ti.OwnerID = ""
		ti.LeaseUntil = time.Time{}
		ti.ResultReason = fmt.Sprintf("interrupted after %d attempts and retry budget exhausted", ti.AttemptCount)
	}
	ti.UpdatedAt = now
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

func eventKind(ev taskEvent) string {
	if ev.heartbeat {
		return "heartbeat"
	}
	return "attempt-result"
}
