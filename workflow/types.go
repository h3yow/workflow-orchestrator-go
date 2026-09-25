package workflow

import (
	"context"
	"time"
)

// TaskStatus 是任务实例的状态取值。
//
// 状态迁移条件：
//
//	PENDING -> READY               加载完成后的初始可调度状态
//	READY   -> RUNNING             调度器领取（归属唯一）后执行尝试
//	RUNNING -> SUCCEEDED           本次尝试成功（终态，结果唯一）
//	RUNNING -> FAILED              本次尝试失败，但仍有剩余重试额度
//	FAILED  -> RUNNING             退避结束后发起下一次尝试
//	RUNNING -> TIMED_OUT           尝试超过任务超时时间（按失败计入额度）
//	TIMED_OUT -> RUNNING           仍有重试额度，退避后再次尝试
//	RUNNING/READY -> PERMANENTLY_FAILED 重试额度耗尽（终态）
//	PENDING/READY -> SKIPPED       任一上游以非成功终态收尾（终态）
type TaskStatus string

const (
	StatusPending           TaskStatus = "PENDING"
	StatusReady             TaskStatus = "READY"
	StatusRunning           TaskStatus = "RUNNING"
	StatusSucceeded         TaskStatus = "SUCCEEDED"
	StatusFailed            TaskStatus = "FAILED"
	StatusTimedOut          TaskStatus = "TIMED_OUT"
	StatusPermanentlyFailed TaskStatus = "PERMANENTLY_FAILED"
	StatusSkipped           TaskStatus = "SKIPPED"
)

// IsTerminal 报告状态是否为终态。
func (s TaskStatus) IsTerminal() bool { return terminalStatus[s] }

var terminalStatus = map[TaskStatus]bool{
	StatusSucceeded:         true,
	StatusPermanentlyFailed: true,
	StatusSkipped:           true,
}

// RunStatus 是一次工作流运行的聚合状态。
type RunStatus string

const (
	RunRunning   RunStatus = "RUNNING"
	RunSucceeded RunStatus = "SUCCEEDED"
	RunFailed    RunStatus = "FAILED"
)

// RetryPolicy 描述任务级重试策略。
type RetryPolicy struct {
	// MaxAttempts 为最大尝试总次数（含首次）；<=0 视为 1。
	MaxAttempts int
	// Backoff 为相邻两次尝试之间的固定退避时长。
	Backoff time.Duration
}

// TaskDef 是工作流定义中的单个任务。
type TaskDef struct {
	Name         string
	Dependencies []string
	Retry        RetryPolicy
	// Timeout 为单次尝试的超时；<=0 表示不限制。
	Timeout time.Duration
}

// WorkflowDef 是一份完整的工作流定义。
type WorkflowDef struct {
	Name  string
	Tasks []TaskDef
}

// dag 是校验通过后的内部 DAG 表示。
type dag struct {
	name  string
	tasks map[string]*TaskDef
	// deps[t] 为 t 的直接上游集合。
	deps map[string]map[string]struct{}
	// children[t] 为 t 的直接下游集合。
	children map[string]map[string]struct{}
	// order 为拓扑序（同层按名称排序，保证调度决策可复现）。
	order []string
}

// TaskState 是单个任务实例的持久化状态。
type TaskState struct {
	WorkflowName  string
	RunID         string
	TaskName      string
	Status        TaskStatus
	Attempt       int // 已发起的尝试序号（从 0 开始，首个在飞尝试为 1）
	LastError     string
	LastStartedAt *time.Time
	LastEndedAt   *time.Time
	UpdatedAt     time.Time
}

// RunState 是一次工作流运行的持久化状态。
type RunState struct {
	WorkflowName string
	RunID        string
	Status       RunStatus
	Tasks        map[string]*TaskState
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// AttemptResult 是任务执行函数返回的单次尝试结果。
type AttemptResult struct {
	Err error
}

// ExecuteFunc 是用户提供的任务执行逻辑。
// attemptNo 从 1 开始；ctx 在超时或引擎关闭时被取消，
// 用户实现应当尊重 ctx 的取消信号以便超时及时收尾。
type ExecuteFunc func(ctx context.Context, runID, taskName string, attemptNo int) AttemptResult

// Executor 按任务名解析执行函数。
type Executor interface {
	Execute(name string) (ExecuteFunc, bool)
}

// MapExecutor 是基于 map 的 Executor 简易实现。
type MapExecutor map[string]ExecuteFunc

// Decision 描述调度器做出的一次决策，用于可解释的日志输出。
type Decision struct {
	At       time.Time
	RunID    string
	TaskName string
	Kind     string
	Attempt  int
	Detail   string
}

// Logger 为引擎日志接口。
type Logger interface {
	LogDecision(Decision)
	Logf(format string, args ...any)
}
