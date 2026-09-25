package workflow

import (
	"context"
	"time"
)

// TaskState 为单个任务实例的生命周期状态。
type TaskState string

const (
	// TaskPending 尚未被调度（等待依赖满足）。
	TaskPending TaskState = "PENDING"
	// TaskReady 依赖已满足，具备执行条件。
	TaskReady TaskState = "READY"
	// TaskRunning 已被某个引擎领取并开始执行。
	TaskRunning TaskState = "RUNNING"
	// TaskSucceeded 执行成功（终态）。
	TaskSucceeded TaskState = "SUCCEEDED"
	// TaskFailed 重试耗尽后失败（终态）。
	TaskFailed TaskState = "FAILED"
	// TaskSkipped 因上游失败/跳过而被跳过（终态，不会静默执行）。
	TaskSkipped TaskState = "SKIPPED"
)

// RunState 为一次工作流触发（run）的整体状态。
type RunState string

const (
	// RunRunning 仍有任务在推进。
	RunRunning RunState = "RUNNING"
	// RunSucceeded 全部任务成功。
	RunSucceeded RunState = "SUCCEEDED"
	// RunFailed 存在失败或被跳过的任务（确定性终态）。
	RunFailed RunState = "FAILED"
)

// RetryPolicy 描述任务级重试策略。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次），<=0 视为 1。
	MaxAttempts int
	// Backoff 两次尝试之间的固定退避。
	Backoff time.Duration
}

// TimeoutPolicy 描述任务级超时策略。
type TimeoutPolicy struct {
	// Timeout 单次尝试的超时时间，<=0 表示不限制。
	Timeout time.Duration
}

// TaskDef 是工作流中的任务定义。
type TaskDef struct {
	// ID 任务唯一标识。
	ID string
	// Deps 直接上游任务 ID 列表。
	Deps []string
	// Retries 重试策略。
	Retries RetryPolicy
	// Timeout 单次尝试超时策略。
	Timeout TimeoutPolicy
}

// WorkflowDef 是一份 DAG 工作流定义。
type WorkflowDef struct {
	// Name 工作流名称。
	Name string
	// Tasks 任务列表（加载时按 ID 索引并校验）。
	Tasks []TaskDef
}

// Attempt 记录一次尝试的结果依据。
type Attempt struct {
	// Attempt 第几尝试（从 1 开始）。
	Attempt int
	// StartedAt 尝试开始时间。
	StartedAt time.Time
	// FinishedAt 尝试结束时间。
	FinishedAt time.Time
	// Result 本次尝试的结果：succeeded / failed / timeout / panic。
	Result string
	// Reason 结果的可解释依据（错误信息或超时说明）。
	Reason string
}

// TaskInstance 是某个任务在某次 run 中的运行时状态。
type TaskInstance struct {
	// TaskID 任务 ID。
	TaskID string
	// State 当前状态。
	State TaskState
	// Attempts 已发生的尝试记录。
	Attempts []Attempt
	// AttemptCount 已开始的尝试次数。
	AttemptCount int
	// OwnerID 当前/最后一次领取该任务的引擎实例 ID。
	OwnerID string
	// LeaseUntil 领取租约到期时间（用于中断恢复判定）。
	LeaseUntil time.Time
	// UpdatedAt 最近一次状态更新时间。
	UpdatedAt time.Time
	// ResultReason 终态依据说明。
	ResultReason string
	// RetryNotBefore 重试退避：早于该时间不得再次领取（仅 READY 重试时有意义）。
	RetryNotBefore time.Time `json:"RetryNotBefore,omitempty"`
	// AttemptGeneration 领取代次：每次进入 RUNNING（含回收后重试）单调加一，
	// 用于识别被回收尝试的陈旧心跳/结果，保证结果不会错位归属。
	AttemptGeneration int `json:"AttemptGeneration,omitempty"`
}

// Run 是一次工作流触发的完整可持久化状态。
type Run struct {
	// ID run 唯一标识。
	ID string
	// WorkflowName 所属工作流名称。
	WorkflowName string
	// State 整体状态。
	State RunState
	// Tasks 以任务 ID 为键的任务实例。
	Tasks map[string]*TaskInstance
	// CreatedAt 创建时间。
	CreatedAt time.Time
	// UpdatedAt 最近更新时间。
	UpdatedAt time.Time
}

// TaskFunc 是任务执行体。
// 返回 nil 表示成功；返回的 error 为本次尝试失败的依据。
// ctx 会在超时时被取消，执行体应当尊重 ctx 的取消信号；
// 即便执行体不响应取消，引擎也会在超时后以 timeout 收尾，且不会产生重复结果。
type TaskFunc func(ctx context.Context, runID string, attempt int) error

// TaskRegistry 提供任务 ID 到执行体的映射。
type TaskRegistry map[string]TaskFunc

// Logger 输出调度决策与尝试结果依据（测试中用于断言可解释性）。
type Logger interface {
	Printf(format string, args ...any)
}
