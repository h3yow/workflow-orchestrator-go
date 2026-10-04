package workflow

import (
	"fmt"
	"io"
	"log"
	"os"
)

// 调度决策种类，出现在每条决策日志中，使调度行为可解释。
const (
	DecisionTriggered        = "TRIGGERED"         // 运行被创建
	DecisionAlreadyRunning   = "ALREADY_RUNNING"   // 相同 runID 已存在，触发被去重
	DecisionRecoverRunning   = "RECOVER_RUNNING"   // 恢复时发现中断中的尝试，不计成功，重新调度
	DecisionLaunchAttempt    = "LAUNCH_ATTEMPT"    // 发起一次尝试
	DecisionBlocked          = "BLOCKED"           // 上游未全部成功，暂不调度
	DecisionAttemptSucceed   = "ATTEMPT_SUCCEEDED" // 单次尝试成功
	DecisionAttemptFailed    = "ATTEMPT_FAILED"    // 单次尝试失败
	DecisionAttemptTimeout   = "ATTEMPT_TIMEOUT"   // 单次尝试超时收尾
	DecisionRetryScheduled   = "RETRY_SCHEDULED"   // 已安排退避后重试
	DecisionRetriesExhausted = "RETRIES_EXHAUSTED" // 重试额度耗尽
	DecisionSkip             = "SKIP"              // 上游非成功终态，下游跳过
	DecisionRunSucceeded     = "RUN_SUCCEEDED"
	DecisionRunFailed        = "RUN_FAILED"
	DecisionClaimAcquired    = "CLAIM_ACQUIRED" // 本进程认领运行的调度归属权
	DecisionClaimDenied      = "CLAIM_DENIED"   // 归属权被其他进程持有，本进程只读不调度
)

// nopLogger 丢弃所有日志。
type nopLogger struct{}

func (nopLogger) LogDecision(Decision) {}
func (nopLogger) Logf(string, ...any)  {}

// stdLogger 基于标准库 log 输出调度决策与每次尝试的结果依据。
type stdLogger struct {
	l *log.Logger
}

// NewLogger 创建输出到指定 Writer 的 Logger；传 nil 使用 stdout。
func NewLogger(w io.Writer) Logger {
	if w == nil {
		w = os.Stdout
	}
	return &stdLogger{l: log.New(w, "[workflow] ", log.LstdFlags|log.Lmicroseconds)}
}

func (sl *stdLogger) LogDecision(d Decision) {
	if d.At.IsZero() {
		d.At = timeNow()
	}
	sl.l.Printf("decision=%s run=%s task=%-12s attempt=%d %s",
		d.Kind, d.RunID, d.TaskName, d.Attempt, d.Detail)
}

func (sl *stdLogger) Logf(format string, args ...any) {
	sl.l.Output(2, fmt.Sprintf(format, args...))
}
