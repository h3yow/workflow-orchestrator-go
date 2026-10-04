package workflow

import (
	"context"
	"fmt"
	"time"
)

// eventKind 区分投递到调度循环的事件。
type eventKind int

const (
	evOutcome  eventKind = iota // 一次尝试收尾（成功/失败/超时）
	evRetryDue                  // 退避结束，可以发起下一次尝试
)

// event 是调度循环处理的唯一事件类型。
type event struct {
	kind     eventKind
	taskName string
	attempt  int
	outcome  AttemptResult
	timedOut bool
}

// inFlight 记录一个在飞尝试的代际信息。
// attempt 序号用于丢弃过期结果：同一尝试只会被结算一次。
type inFlight struct {
	attempt      int
	cancel       context.CancelFunc
	timeoutTimer *time.Timer
}

// runLoop 是单次运行的串行调度循环。所有状态迁移都只在 run() 一个
// goroutine 内发生，因此即便尝试 goroutine 与计时器并发触发事件，
// 调度决策依然确定、可复现。
type runLoop struct {
	engine *Engine
	d      *dag
	runID  string
	claim  RunClaim // 跨进程归属权；为 nil 表示 Store 不支持认领（纯进程内）

	state    map[string]*TaskState // 权威任务状态（每次迁移同步持久化）
	inflight map[string]*inFlight
	// retryTimers[t] 非空表示 t 正处于退避等待。
	retryTimers map[string]*time.Timer
	// blockedLogged 记录已打印过 BLOCKED 决策的任务，避免日志刷屏。
	blockedLogged map[string]bool

	events chan event
	done   chan struct{}
}

// ensureScheduled 保证同一 runID 只有一个调度循环。
// 进程内由 active 表去重；跨进程由 Store 的归属认领（ClaimStore）保证：
// 任一时刻同一 runID 至多一个进程持有归属权并真正调度，
// 其他进程得到 nil 循环（只读等待终态）。
func (e *Engine) ensureScheduled(d *dag, snap *RunState) (*runLoop, error) {
	e.mu.Lock()
	if loop, ok := e.active[snap.RunID]; ok {
		e.mu.Unlock()
		return loop, nil
	}
	select {
	case <-e.stopped:
		e.mu.Unlock()
		return nil, fmt.Errorf("engine is closed")
	default:
	}
	if snap.Status != RunRunning {
		e.mu.Unlock()
		return nil, nil
	}
	e.mu.Unlock()

	// 跨进程归属认领（锁外执行，避免阻塞引擎内其他运行）。
	var claim RunClaim
	if cs, ok := e.store.(ClaimStore); ok {
		c, err := cs.TryClaimRun(context.Background(), snap.RunID, e.ownerID)
		if err != nil {
			return nil, err
		}
		if c == nil {
			e.logger.LogDecision(Decision{At: e.nowFunc(), RunID: snap.RunID, Kind: DecisionClaimDenied,
				Detail: "归属权被其他进程持有，本进程只读不调度"})
			return nil, nil
		}
		claim = c
	}

	// 认领成功后回读最新状态：认领窗口内上一任持有者可能已把运行收尾。
	fresh, err := e.store.GetRun(context.Background(), snap.RunID)
	if err != nil {
		releaseClaim(claim)
		return nil, err
	}
	if fresh.Status != RunRunning {
		releaseClaim(claim)
		return nil, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if loop, ok := e.active[snap.RunID]; ok {
		// 并发窗口：本进程已有调度循环，释放多余认领。
		releaseClaim(claim)
		return loop, nil
	}
	select {
	case <-e.stopped:
		releaseClaim(claim)
		return nil, fmt.Errorf("engine is closed")
	default:
	}
	loop := &runLoop{
		engine:        e,
		d:             d,
		runID:         snap.RunID,
		claim:         claim,
		state:         make(map[string]*TaskState, len(fresh.Tasks)),
		inflight:      make(map[string]*inFlight),
		retryTimers:   make(map[string]*time.Timer),
		blockedLogged: make(map[string]bool),
		events:        make(chan event, 1024),
		done:          make(chan struct{}),
	}
	for k, t := range fresh.Tasks {
		loop.state[k] = snapshotTask(t)
	}
	e.active[snap.RunID] = loop
	e.wg.Add(1)
	if claim != nil {
		e.logger.LogDecision(Decision{At: e.nowFunc(), RunID: snap.RunID, Kind: DecisionClaimAcquired,
			Detail: fmt.Sprintf("owner=%s 认领调度归属权", e.ownerID)})
	}
	go loop.run()
	return loop, nil
}

func releaseClaim(c RunClaim) {
	if c != nil {
		_ = c.Release()
	}
}

func (l *runLoop) log(kind, task string, attempt int, detail string) {
	l.engine.logger.LogDecision(Decision{
		At:       l.engine.nowFunc(),
		RunID:    l.runID,
		TaskName: task,
		Kind:     kind,
		Attempt:  attempt,
		Detail:   detail,
	})
}

func (l *runLoop) run() {
	defer l.engine.wg.Done()
	defer close(l.done)
	defer l.unregister()

	l.recoverInterrupted()

	for {
		l.drainReadyEvents()
		l.propagateSkips()
		l.launchReadyTasks()

		if l.allTerminal() {
			l.finalize()
			return
		}
		if !l.waitForEvent() {
			// 引擎关闭：取消所有在飞尝试、停掉退避计时器后退出。
			l.interruptOnClose()
			return
		}
	}
}

func (l *runLoop) unregister() {
	l.engine.mu.Lock()
	delete(l.engine.active, l.runID)
	l.engine.mu.Unlock()
	if l.claim != nil {
		if err := l.claim.Release(); err != nil {
			l.engine.logger.Logf("release claim of run %s: %v", l.runID, err)
		}
	}
}

// sendEvent 在引擎关闭时放弃投递，避免计时器 goroutine 泄漏。
func (l *runLoop) sendEvent(ev event) {
	select {
	case l.events <- ev:
	case <-l.engine.stopped:
	}
}

// drainReadyEvents 非阻塞地处理所有当前已到达的事件。
func (l *runLoop) drainReadyEvents() {
	for {
		select {
		case ev := <-l.events:
			l.handleEvent(ev)
		default:
			return
		}
	}
}

func (l *runLoop) waitForEvent() bool {
	if len(l.inflight) == 0 && len(l.retryTimers) == 0 {
		// 没有任何可等待项却未终止，属于防御性分支。
		select {
		case ev := <-l.events:
			l.handleEvent(ev)
			return true
		case <-l.engine.stopped:
			return false
		}
	}
	select {
	case ev := <-l.events:
		l.handleEvent(ev)
		return true
	case <-l.engine.stopped:
		return false
	}
}

func (l *runLoop) handleEvent(ev event) {
	switch ev.kind {
	case evRetryDue:
		if t := l.retryTimers[ev.taskName]; t != nil {
			delete(l.retryTimers, ev.taskName)
		}
		l.log(DecisionRetryScheduled, ev.taskName, ev.attempt, "退避结束，准备下一次尝试")
	case evOutcome:
		l.settle(ev.taskName, ev.attempt, ev.outcome, ev.timedOut)
	}
}

// appendRecord 追加一条尝试记录（结果 + 判定依据），随后由 persist 一并落盘。
// 记录是结果视图的唯一事实来源：每次尝试有且仅有一条记录。
func (l *runLoop) appendRecord(t *TaskState, outcome AttemptOutcome, reason string, ended time.Time) {
	rec := AttemptRecord{
		Attempt: t.Attempt,
		Outcome: outcome,
		Reason:  reason,
		EndedAt: ended,
	}
	if t.LastStartedAt != nil {
		v := *t.LastStartedAt
		rec.StartedAt = &v
	}
	t.Records = append(t.Records, rec)
}

// recoverInterrupted 从既有状态恢复：
//   - RUNNING 的任务是上次中断时未记录结果的尝试，绝不判定为成功，
//     重新置为 READY 等待重新调度（Attempt 保留，该尝试计入已发起序号，
//     并补记一条 INTERRUPTED 记录说明去向）；
//   - FAILED/TIMED_OUT 表示崩溃发生在退避窗口，重新置为 READY；
//   - 已成功/永久失败/跳过的终态原样保留。
func (l *runLoop) recoverInterrupted() {
	for _, name := range l.d.order {
		t := l.state[name]
		def := l.d.tasks[name]
		switch t.Status {
		case StatusRunning:
			// 中断时在飞的尝试已经发起，Attempt 已自增，计入尝试次数。
			l.log(DecisionRecoverRunning, name, t.Attempt,
				"检测到中断时仍在执行的尝试，未记录结果，不判定为成功，重置为 READY 重新调度")
			l.appendRecord(t, OutcomeInterrupted,
				"持有进程中断，本次尝试结果未知，不计成功", l.engine.nowFunc())
			if t.Attempt >= maxAttempts(def) {
				t.Status = StatusPermanentlyFailed
				t.LastError = "interrupted after exhausting attempt budget"
			} else {
				t.Status = StatusReady
				t.LastError = "interrupted before result was recorded"
			}
			l.persist(t)
		case StatusFailed, StatusTimedOut:
			// 崩溃可能发生在“末次失败已落盘、终态尚未落盘”的窗口内。
			if t.Attempt >= maxAttempts(def) {
				t.Status = StatusPermanentlyFailed
			} else {
				t.Status = StatusReady
			}
			l.persist(t)
		}
	}
}

// propagateSkips 沿拓扑序单遍传播跳过：任一上游以非成功终态收尾，
// 下游立即标记 SKIPPED，绝不会被静默执行。
func (l *runLoop) propagateSkips() {
	for _, name := range l.d.order {
		t := l.state[name]
		if t.Status.IsTerminal() {
			continue
		}
		if _, waiting := l.retryTimers[name]; waiting {
			continue
		}
		for dep := range l.d.deps[name] {
			switch l.state[dep].Status {
			case StatusPermanentlyFailed, StatusSkipped:
				t.Status = StatusSkipped
				t.LastError = fmt.Sprintf("upstream task %q did not succeed", dep)
				l.appendRecord(t, OutcomeSkipped,
					fmt.Sprintf("上游 %s 状态=%s，未执行直接跳过", dep, l.state[dep].Status),
					l.engine.nowFunc())
				l.persist(t)
				l.log(DecisionSkip, name, t.Attempt,
					fmt.Sprintf("上游 %s 状态=%s，下游不执行直接跳过", dep, l.state[dep].Status))
			}
		}
	}
}

// launchReadyTasks 按拓扑序（同名层按名称排序）发起所有依赖已成功的任务。
func (l *runLoop) launchReadyTasks() {
	for _, name := range l.d.order {
		t := l.state[name]
		if t.Status.IsTerminal() {
			continue
		}
		if _, flying := l.inflight[name]; flying {
			continue
		}
		if _, waiting := l.retryTimers[name]; waiting {
			continue
		}
		if !l.depsSucceeded(name) {
			if !l.blockedLogged[name] && !l.hasUnsuccessfulTerminalUpstream(name) {
				l.blockedLogged[name] = true
				l.log(DecisionBlocked, name, t.Attempt,
					fmt.Sprintf("等待上游成功：%s", l.pendingDeps(name)))
			}
			continue
		}
		l.launchAttempt(name)
	}
}

func (l *runLoop) depsSucceeded(name string) bool {
	for dep := range l.d.deps[name] {
		if l.state[dep].Status != StatusSucceeded {
			return false
		}
	}
	return true
}

func (l *runLoop) hasUnsuccessfulTerminalUpstream(name string) bool {
	for dep := range l.d.deps[name] {
		s := l.state[dep].Status
		if s == StatusPermanentlyFailed || s == StatusSkipped {
			return true
		}
	}
	return false
}

func (l *runLoop) pendingDeps(name string) string {
	var pending []string
	for dep := range l.d.deps[name] {
		if s := l.state[dep].Status; s != StatusSucceeded {
			pending = append(pending, fmt.Sprintf("%s(%s)", dep, s))
		}
	}
	return joinSorted(pending)
}

// launchAttempt 发起一次尝试并记录“在飞代际”。
func (l *runLoop) launchAttempt(name string) {
	def := l.d.tasks[name]
	t := l.state[name]

	t.Attempt++
	started := l.engine.nowFunc()
	t.Status = StatusRunning
	t.LastStartedAt = &started
	t.LastError = ""
	l.persist(t)

	attemptNo := t.Attempt
	l.log(DecisionLaunchAttempt, name, attemptNo,
		fmt.Sprintf("发起第 %d/%d 次尝试，timeout=%s", attemptNo, maxAttempts(def), def.Timeout))

	ctx, cancel := context.WithCancel(context.Background())
	inf := &inFlight{attempt: attemptNo, cancel: cancel}
	l.inflight[name] = inf

	fn, ok := l.engine.executor.Execute(name)
	if !ok {
		// 缺少执行器按一次失败尝试结算，占用重试额度。
		l.settle(name, attemptNo, AttemptResult{Err: fmt.Errorf("no executor registered for task %q", name)}, false)
		return
	}

	if def.Timeout > 0 {
		inf.timeoutTimer = time.AfterFunc(def.Timeout, func() {
			l.sendEvent(event{kind: evOutcome, taskName: name, attempt: attemptNo, timedOut: true})
		})
	}

	go func() {
		res := fn(ctx, l.runID, name, attemptNo)
		l.sendEvent(event{kind: evOutcome, taskName: name, attempt: attemptNo, outcome: res})
	}()
}

// settle 结算一次尝试。通过在飞代际校验，过期结果（已超时/已被新尝试取代）
// 被直接丢弃，因此同一尝试有且仅有一次状态迁移与执行结果。
func (l *runLoop) settle(name string, attempt int, res AttemptResult, timedOut bool) {
	inf, ok := l.inflight[name]
	if !ok || inf.attempt != attempt {
		// 过期结果：该尝试已被结算，丢弃，不产生重复执行结果。
		return
	}
	delete(l.inflight, name)
	inf.cancel()
	if inf.timeoutTimer != nil {
		inf.timeoutTimer.Stop()
	}

	t := l.state[name]
	def := l.d.tasks[name]
	ended := l.engine.nowFunc()
	t.LastEndedAt = &ended

	switch {
	case timedOut:
		t.LastError = fmt.Sprintf("attempt %d timed out after %s", attempt, def.Timeout)
		t.Status = StatusTimedOut
		l.appendRecord(t, OutcomeTimedOut,
			fmt.Sprintf("超过单次尝试超时 %s，超时计时器先于执行返回", def.Timeout), ended)
		l.persist(t)
		l.log(DecisionAttemptTimeout, name, attempt,
			fmt.Sprintf("第 %d 次尝试超过 %s 收尾；结果依据：超时计时器先于执行返回", attempt, def.Timeout))
	case res.Err != nil:
		t.LastError = res.Err.Error()
		t.Status = StatusFailed
		l.appendRecord(t, OutcomeFailed,
			fmt.Sprintf("执行函数返回 error=%v", res.Err), ended)
		l.persist(t)
		l.log(DecisionAttemptFailed, name, attempt,
			fmt.Sprintf("第 %d 次尝试失败；结果依据：执行函数返回 error=%v", attempt, res.Err))
	default:
		t.Status = StatusSucceeded
		t.LastError = ""
		l.appendRecord(t, OutcomeSucceeded, "执行函数成功返回", ended)
		l.persist(t)
		l.log(DecisionAttemptSucceed, name, attempt, "尝试成功；终态=SUCCEEDED")
		return
	}

	if attempt >= maxAttempts(def) {
		t.Status = StatusPermanentlyFailed
		l.persist(t)
		l.log(DecisionRetriesExhausted, name, attempt,
			fmt.Sprintf("已达最大尝试次数 %d，终态=PERMANENTLY_FAILED", maxAttempts(def)))
		return
	}

	// 保持 FAILED/TIMED_OUT 非终态，安排退避后重试。
	backoff := def.Retry.Backoff
	l.log(DecisionRetryScheduled, name, attempt,
		fmt.Sprintf("仍有重试额度（%d/%d），%s 后重试", attempt, maxAttempts(def), backoff))
	at := attempt
	timer := time.AfterFunc(backoff, func() {
		l.sendEvent(event{kind: evRetryDue, taskName: name, attempt: at})
	})
	l.retryTimers[name] = timer
}

func (l *runLoop) allTerminal() bool {
	for _, t := range l.state {
		if !t.Status.IsTerminal() {
			return false
		}
	}
	return true
}

// finalize 聚合运行终态：只要存在未成功任务，整条运行记为 FAILED，
// 但该判定不影响其他独立 run 的推进（运行之间完全隔离）。
func (l *runLoop) finalize() {
	status := RunSucceeded
	for _, t := range l.state {
		if t.Status != StatusSucceeded {
			status = RunFailed
			break
		}
	}
	_ = l.engine.store.PutRunStatus(context.Background(), l.runID, status)
	if status == RunSucceeded {
		l.log(DecisionRunSucceeded, "", 0, "全部任务成功，运行结束")
	} else {
		l.log(DecisionRunFailed, "", 0, "存在永久失败或被跳过的任务，运行结束")
	}
}

// interruptOnClose 在引擎关闭时中断在飞尝试并停退避计时器；
// RUNNING 状态保持已持久化的样子，由下次启动的恢复逻辑处理，
// 因此中断中的任务不会被误判为成功。
func (l *runLoop) interruptOnClose() {
	for name, inf := range l.inflight {
		inf.cancel()
		if inf.timeoutTimer != nil {
			inf.timeoutTimer.Stop()
		}
		delete(l.inflight, name)
	}
	for name, tm := range l.retryTimers {
		tm.Stop()
		delete(l.retryTimers, name)
	}
}

// persist 将一次任务状态迁移原子地写入 Store，保证内存与持久化一致：
// 崩溃后恢复时只会看到迁移前或迁移后的完整状态。
func (l *runLoop) persist(t *TaskState) {
	t.UpdatedAt = l.engine.nowFunc()
	cp := snapshotTask(t)
	l.state[t.TaskName] = cp
	if err := l.engine.store.PutTask(context.Background(), l.runID, cp); err != nil {
		l.engine.logger.Logf("persist task %s of run %s: %v", t.TaskName, l.runID, err)
	}
}

func maxAttempts(def *TaskDef) int {
	if def.Retry.MaxAttempts <= 0 {
		return 1
	}
	return def.Retry.MaxAttempts
}

func joinSorted(xs []string) string {
	// 依赖集合很小；简单选择排序保证日志顺序稳定。
	for i := 0; i < len(xs); i++ {
		for j := i + 1; j < len(xs); j++ {
			if xs[j] < xs[i] {
				xs[i], xs[j] = xs[j], xs[i]
			}
		}
	}
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ", "
		}
		out += x
	}
	return out
}
