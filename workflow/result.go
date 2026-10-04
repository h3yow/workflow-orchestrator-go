package workflow

import "time"

// AttemptOutcome 是一次尝试（或跳过决策）的结算类别，
// 用于结果视图中解释每一次尝试的依据。
type AttemptOutcome string

const (
	// OutcomeSucceeded 尝试成功。
	OutcomeSucceeded AttemptOutcome = "SUCCEEDED"
	// OutcomeFailed 尝试失败（执行函数返回错误或缺少执行器）。
	OutcomeFailed AttemptOutcome = "FAILED"
	// OutcomeTimedOut 尝试超时收尾。
	OutcomeTimedOut AttemptOutcome = "TIMED_OUT"
	// OutcomeInterrupted 尝试在飞时持有进程掉线，结果未知，
	// 不判定为成功，但占用一次尝试额度。
	OutcomeInterrupted AttemptOutcome = "INTERRUPTED"
	// OutcomeSkipped 任务因上游非成功终态被跳过（未发起尝试，Attempt=0）。
	OutcomeSkipped AttemptOutcome = "SKIPPED"
)

// AttemptRecord 记录一次尝试的完整结算信息，是结果视图的最小单元。
// 记录只在结算（或恢复判定）时追加，绝不修改或删除，
// 因此多次读取、多进程读取得到的序列一致。
type AttemptRecord struct {
	// Attempt 为尝试序号（从 1 开始）；跳过记录为 0。
	Attempt int
	// Outcome 为结算类别。
	Outcome AttemptOutcome
	// Reason 为人类可读的结算依据（错误信息、超时阈值、跳过原因等）。
	Reason string
	// Owner 为发起该次尝试的进程标识。
	Owner string
	// StartedAt 为尝试开始时刻；跳过记录为 nil。
	StartedAt *time.Time
	// EndedAt 为结算（或恢复判定）时刻。
	EndedAt time.Time
}

// TotalAttempts 返回该运行所有任务已发起的尝试总次数（含在飞与中断的尝试）。
func (s *RunState) TotalAttempts() int {
	total := 0
	for _, t := range s.Tasks {
		total += t.Attempt
	}
	return total
}

// Terminal 报告运行是否已收敛到终态。
func (s *RunState) Terminal() bool { return s.Status != RunRunning }
