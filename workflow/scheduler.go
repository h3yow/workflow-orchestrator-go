package workflow

import (
	"context"
	"fmt"
	"time"
)

// eventKind 区分投递到调度循环的事件。
type eventKind int

const (
	evOutcome   eventKind = iota // 一次尝试收尾（成功/失败/超时）
	evRetryDue                   // 退避结束，可以发起下一次尝试
	evHeartbeat                  // 租约心跳到期，需要续期
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
//
// 跨进程归属：循环持有运行租约（owner+token），周期性心跳续租；
// 所有状态写入都携带 fencing token，一旦被其他进程接管（token 失配），
// 本循环立即停止推进，保证同一运行任一时刻只有一个进程真正负责。
type runLoop struct {
	engine *Engine
	d      *dag
	runID  string

	owner string
	token int64

	state    map[string]*TaskState // 权威任务状态（每次迁移同步持久化）
	inflight map[string]*inFlight
	// retryTimers[t] 非空表示 t 正处于退避等待。
	retryTimers map[string]*time.Timer
	// blockedLogged 记录已打印过 BLOCKED 决策的任务，避免日志刷屏。
	blockedLogged map[string]bool

	// heartbeat 驱动租约续期；leaseLost 置位后循环尽快退出。
	heartbeat *time.Timer
	leaseLost bool

	events chan event
	done   chan struct{}
}

// ensureScheduled 保证同一 runID 在本进程内只有一个调度循环，
// 且全集群任一时刻只有一个进程持有租约推进该运行。
// 租约被其他存活进程持有时返回 (nil, nil)，调用方只读结果即可。
func (e *Engine) ensureScheduled(d *dag, runID string) (*runLoop, error) {
	e.mu.Lock()
	if loop, ok := e.active[runID]; ok {
		e.mu.Unlock()
		return loop, nil
	}
	e.mu.Unlock()

	select {
	case <-e.stopped:
		return nil, fmt.Errorf("engine is closed")
	default:
	}

	// 租约获取不持有引擎锁，避免目录锁等待阻塞其他运行的调度。
	acquired, snap, err := e.store.AcquireRunLease(context.Background(), runID, e.owner, e.leaseTTL)
	if err != nil {
		return nil, err
	}
	if !acquired {
		holder := "unknown"
		if snap.Lease != nil {
			holder = snap.Lease.Owner
		}
		e.logger.LogDecision(Decision{At: e.nowFunc(), RunID: runID, Kind: DecisionLeaseHeldByOther,
			Detail: fmt.Sprintf("租约由 %s 持有，本进程只读结果、等待其收尾或掉线", holder)})
		return nil, nil
	}
	if snap.Status != RunRunning {
		// 已收敛到终态：让出租约，无需调度。
		_ = e.store.ReleaseRunLease(context.Background(), runID, e.owner, snap.Lease.Token)
		return nil, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if loop, ok := e.active[runID]; ok {
		// 同进程并发获取（同一 owner 重入续期，token 相同），复用既有循环。
		return loop, nil
	}
	loop := &runLoop{
		engine:        e,
		d:             d,
		runID:         runID,
		owner:         e.owner,
		token:         snap.Lease.Token,
		state:         make(map[string]*TaskState, len(snap.Tasks)),
		inflight:      make(map[string]*inFlight),
		retryTimers:   make(map[string]*time.Timer),
		blockedLogged: make(map[string]bool),
		events:        make(chan event, 1024),
		done:          make(chan struct{}),
	}
	for k, t := range snap.Tasks {
		loop.state[k] = snapshotTask(t)
	}
	e.active[runID] = loop
	e.wg.Add(1)
	e.logger.LogDecision(Decision{At: e.nowFunc(), RunID: runID, Kind: DecisionLeaseAcquired,
		Detail: fmt.Sprintf("取得运行租约 token=%d ttl=%s，开始负责推进", loop.token, e.leaseTTL)})
	go loop.run()
	return loop, nil
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
	l.armHeartbeat()

	for {
		if l.leaseLost {
			l.abortInFlight()
			return
		}
		l.drainReadyEvents()
		l.propagateSkips()
		l.launchReadyTasks()

		if l.leaseLost {
			l.abortInFlight()
			return
		}
		if l.allTerminal() {
			l.finalize()
			l.stopTimers()
			return
		}
		if !l.waitForEvent() {
			// 引擎关闭：取消所有在飞尝试、停掉计时器、释放租约后退出。
			l.interruptOnClose()
			return
		}
	}
}

func (l *runLoop) unregister() {
	l.engine.mu.Lock()
	delete(l.engine.active, l.runID)
	l.engine.mu.Unlock()
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
	case evHeartbeat:
		l.renewLease()
	case evRetryDue:
		if t := l.retryTimers[ev.taskName]; t != nil {
			delete(l.retryTimers, ev.taskName)
		}
		l.log(DecisionRetryScheduled, ev.taskName, ev.attempt, "退避结束，准备下一次尝试")
	case evOutcome:
		l.settle(ev.taskName, ev.attempt, ev.outcome, ev.timedOut)
	}
}

// armHeartbeat 安排下一次租约续期；间隔为 TTL 的 1/3，
// 即连续错过约 3 次心跳后租约才会到期，留给其他进程接管。
func (l *runLoop) armHeartbeat() {
	interval := l.engine.leaseTTL / leaseRenewDivisor
	l.heartbeat = time.AfterFunc(interval, func() {
		l.sendEvent(event{kind: evHeartbeat})
	})
}

// renewLease 续租；失败说明租约已易主，本循环必须停止推进。
func (l *runLoop) renewLease() {
	if l.leaseLost {
		return
	}
	ok, err := l.engine.store.RenewRunLease(context.Background(), l.runID, l.owner, l.token, l.engine.leaseTTL)
	if err != nil {
		// 存储暂时不可用：按丢失处理，停止推进，避免无租约裸奔。
		l.leaseLost = true
		l.log(DecisionLeaseLost, "", 0, fmt.Sprintf("租约续期出错：%v，停止推进", err))
		return
	}
	if !ok {
		l.leaseLost = true
		l.log(DecisionLeaseLost, "", 0, "租约已被其他进程接管，本进程立即停止推进")
		return
	}
	l.armHeartbeat()
}

// recoverInterrupted 从既有状态恢复：
//   - RUNNING 的任务是上次中断时未记录结果的尝试，绝不判定为成功，
//     记一条 INTERRUPTED 结算（占用尝试额度）后重新置为 READY 等待重新调度；
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
			t.Attempts = append(t.Attempts, AttemptRecord{
				Attempt:   t.Attempt,
				Outcome:   OutcomeInterrupted,
				Reason:    "持有进程掉线，尝试结果未知，不判定为成功",
				Owner:     t.Owner,
				StartedAt: t.LastStartedAt,
				EndedAt:   l.engine.nowFunc(),
			})
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
		if l.leaseLost {
			return
		}
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
				t.Attempts = append(t.Attempts, AttemptRecord{
					Attempt: 0,
					Outcome: OutcomeSkipped,
					Reason:  fmt.Sprintf("上游 %s 状态=%s，下游不执行直接跳过", dep, l.state[dep].Status),
					Owner:   l.owner,
					EndedAt: l.engine.nowFunc(),
				})
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
		if l.leaseLost {
			return
		}
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
// RUNNING 状态先持久化（携带 fencing token）再启动执行，
// 因此崩溃后磁盘上的 RUNNING 表示“结果未知的在飞尝试”。
func (l *runLoop) launchAttempt(name string) {
	def := l.d.tasks[name]
	t := l.state[name]

	t.Attempt++
	started := l.engine.nowFunc()
	t.Status = StatusRunning
	t.Owner = l.owner
	t.LastStartedAt = &started
	t.LastError = ""
	l.persist(t)
	if l.leaseLost {
		// 租约已失：RUNNING 未落盘（写入被 fencing 拒绝），
		// 不启动执行，循环随即退出，内存态随之废弃。
		return
	}

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
// 每次结算追加一条 AttemptRecord，构成结果视图的尝试历史。
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

	rec := AttemptRecord{Attempt: attempt, Owner: l.owner, StartedAt: t.LastStartedAt, EndedAt: ended}
	switch {
	case timedOut:
		rec.Outcome = OutcomeTimedOut
		rec.Reason = fmt.Sprintf("超过单次尝试超时 %s，超时计时器先于执行返回", def.Timeout)
		t.LastError = fmt.Sprintf("attempt %d timed out after %s", attempt, def.Timeout)
		t.Status = StatusTimedOut
		t.Attempts = append(t.Attempts, rec)
		l.persist(t)
		l.log(DecisionAttemptTimeout, name, attempt,
			fmt.Sprintf("第 %d 次尝试超过 %s 收尾；结果依据：超时计时器先于执行返回", attempt, def.Timeout))
	case res.Err != nil:
		rec.Outcome = OutcomeFailed
		rec.Reason = fmt.Sprintf("执行函数返回 error=%v", res.Err)
		t.LastError = res.Err.Error()
		t.Status = StatusFailed
		t.Attempts = append(t.Attempts, rec)
		l.persist(t)
		l.log(DecisionAttemptFailed, name, attempt,
			fmt.Sprintf("第 %d 次尝试失败；结果依据：执行函数返回 error=%v", attempt, res.Err))
	default:
		rec.Outcome = OutcomeSucceeded
		rec.Reason = "执行函数成功返回"
		t.Status = StatusSucceeded
		t.LastError = ""
		t.Attempts = append(t.Attempts, rec)
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
// 写入携带 fencing token，随后释放租约，其他进程立即可读到终态。
func (l *runLoop) finalize() {
	status := RunSucceeded
	for _, t := range l.state {
		if t.Status != StatusSucceeded {
			status = RunFailed
			break
		}
	}
	ok, err := l.engine.store.PutRunStatus(context.Background(), l.runID, status, l.token)
	if err != nil {
		l.engine.logger.Logf("finalize run %s: %v", l.runID, err)
	}
	if !ok && err == nil {
		l.leaseLost = true
		l.log(DecisionLeaseLost, "", 0, "终态写入被 fencing token 拒绝，租约已易主")
	}
	// 释放租约（若仍持有），让等待中的其他进程立即看到终态/接管。
	_ = l.engine.store.ReleaseRunLease(context.Background(), l.runID, l.owner, l.token)
	if status == RunSucceeded {
		l.log(DecisionRunSucceeded, "", 0, "全部任务成功，运行结束")
	} else {
		l.log(DecisionRunFailed, "", 0, "存在永久失败或被跳过的任务，运行结束")
	}
}

// interruptOnClose 在引擎关闭时中断在飞尝试并停掉全部计时器；
// RUNNING 状态保持已持久化的样子，由下次启动的恢复逻辑处理，
// 因此中断中的任务不会被误判为成功。租约被主动释放，
// 其他进程可立即接管，无需等待 TTL 到期。
func (l *runLoop) interruptOnClose() {
	l.abortInFlight()
	_ = l.engine.store.ReleaseRunLease(context.Background(), l.runID, l.owner, l.token)
}

// abortInFlight 取消在飞尝试并停掉全部计时器（不释放租约，
// 用于租约已丢失的场景：租约归新持有者，释放操作本就会失配）。
func (l *runLoop) abortInFlight() {
	for name, inf := range l.inflight {
		inf.cancel()
		if inf.timeoutTimer != nil {
			inf.timeoutTimer.Stop()
		}
		delete(l.inflight, name)
	}
	l.stopTimers()
}

// stopTimers 停掉退避与心跳计时器。
func (l *runLoop) stopTimers() {
	for name, tm := range l.retryTimers {
		tm.Stop()
		delete(l.retryTimers, name)
	}
	if l.heartbeat != nil {
		l.heartbeat.Stop()
		l.heartbeat = nil
	}
}

// persist 将一次任务状态迁移原子地写入 Store（携带 fencing token），
// 保证内存与持久化一致：崩溃后恢复时只会看到迁移前或迁移后的完整状态。
// 写入被 fencing 拒绝说明租约已易主，本循环立即停止推进。
func (l *runLoop) persist(t *TaskState) {
	t.UpdatedAt = l.engine.nowFunc()
	cp := snapshotTask(t)
	l.state[t.TaskName] = cp
	ok, err := l.engine.store.PutTask(context.Background(), l.runID, cp, l.token)
	if err != nil {
		l.engine.logger.Logf("persist task %s of run %s: %v", t.TaskName, l.runID, err)
		return
	}
	if !ok {
		l.leaseLost = true
		l.log(DecisionLeaseLost, t.TaskName, t.Attempt,
			"状态写入被 fencing token 拒绝，租约已易主，本进程停止推进")
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
