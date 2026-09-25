# DAG 工作流编排引擎（本地可运行）

一个纯本地状态模拟的 DAG 工作流编排引擎：加载与校验工作流定义，按依赖推进任务，
处理任务级重试与超时，支持同一工作流的并发触发与进程中断后的状态恢复。
核心目标是：**在并发触发与重启恢复下，调度结果可解释、可复现**。

- 纯 Go 标准库，无外部依赖。
- 每次运行由**单个串行调度 goroutine** 驱动，所有状态迁移只在该 goroutine 内发生；
  任务执行在独立 goroutine 中，但结果通过事件队列回到调度循环结算。
- 每次尝试带**代际（attempt 序号）校验**：过期结果（超时先收尾后迟到的返回、
  被中断的尝试）一律丢弃，保证同一次尝试只产生一次状态写入。
- 状态持久化抽象为 `Store` 接口，提供内存实现与 JSON 文件实现（`flock` 跨进程互斥）。

## 快速开始

```bash
go run ./cmd/demo        # 运行内置 demo，打印全部调度决策
go test ./... -race -v   # 全量测试（竞态检测 + 决策日志）
```

## 包结构

| 文件 | 职责 |
| --- | --- |
| `workflow/types.go` | 任务/运行状态、策略、执行函数、决策日志等类型定义 |
| `workflow/validate.go` | 工作流加载与校验（重复任务/缺失依赖/成环） |
| `workflow/engine.go` | 引擎入口：注册、触发（去重）、等待、关闭 |
| `workflow/scheduler.go` | 串行调度循环：依赖推进、跳过传播、重试退避、超时结算、中断恢复 |
| `workflow/state.go` | `Store` 接口、错误定义、状态深拷贝 |
| `workflow/memstore.go` | 内存持久化（互斥锁，用于并发触发测试） |
| `workflow/filestore.go` | JSON 文件持久化（`flock` + 原子 rename，用于重启恢复） |
| `workflow/log.go` / `clock.go` | 决策日志与可替换时间源 |

## 定义工作流

```go
err := eng.Register(workflow.WorkflowDef{
    Name: "demo",
    Tasks: []workflow.TaskDef{
        {Name: "start", Retry: workflow.RetryPolicy{MaxAttempts: 1}},
        {Name: "flaky", Dependencies: []string{"start"},
            Retry: workflow.RetryPolicy{MaxAttempts: 3, Backoff: 50 * time.Millisecond}},
        {Name: "slow", Dependencies: []string{"start"}, Timeout: 80 * time.Millisecond,
            Retry: workflow.RetryPolicy{MaxAttempts: 2}},
    },
})
```

执行逻辑通过 `Executor` 按任务名解析（提供 `MapExecutor`）：

```go
type ExecuteFunc func(ctx context.Context, runID, taskName string, attemptNo int) AttemptResult
```

执行函数应当尊重 `ctx.Done()`：超时或引擎关闭时 ctx 会被取消，以便尝试及时收尾。

## 加载与校验

加载阶段（`Register`）依次拒绝以下问题，返回 `*ValidationError`，
其 `Kind` 可通过 `errors.Is` 区分三种原因，`TaskName` 指出相关任务：

| 错误哨兵 | 触发条件 |
| --- | --- |
| `ErrDuplicateTask` | 同名任务被定义多次（含空任务名/空工作流名） |
| `ErrMissingDependency` | 依赖指向不存在的任务 |
| `ErrCycleDetected` | 依赖图存在环（含任务依赖自身）；Kahn 拓扑排序判定 |

同一条依赖被重复声明（如 `Deps: ["a","a"]`）会被去重，不算错误。
校验通过后，邻接表与拓扑序均按名称稳定排序，作为调度确定性的基础。

## 依赖与跳过语义

- 任务只有在**全部直接上游都为 `SUCCEEDED`** 时才会被发起尝试；
  否则记录一条 `BLOCKED` 决策并等待。
- 任一上游进入非成功终态（`PERMANENTLY_FAILED` 或 `SKIPPED`），
  下游在同一调度轮内沿拓扑序被标记为 `SKIPPED`，**绝不会被静默执行**（执行次数为 0）。
- 运行之间彼此隔离：一条链路永久失败只会导致该运行 `FAILED` 并跳过其下游，
  不影响同工作流下其他 run 或无依赖关系的独立分支继续推进（见 `TestIndependentRunsIsolated`）。

## 重试与超时语义

- `RetryPolicy.MaxAttempts` 为**最大尝试总次数（含首次）**；`<=0` 按 1 处理。
  相邻尝试间隔固定退避 `Backoff`。
- 失败与超时都**占用尝试额度**：`attempt >= MaxAttempts` 时进入确定终态
  `PERMANENTLY_FAILED`，不再补跑。
- 超时只作用于**单次尝试**：超时计时器先于执行返回时，该尝试以 `TIMED_OUT`
  收尾；即使执行函数之后迟到返回（成功或失败），结果也会因代际不匹配被丢弃，
  **同一次尝试不会产生重复执行结果**。若仍有额度，退避后发起下一次尝试。
- 缺少执行器（未注册的任务名）按一次失败尝试处理，同样占用额度。
- 重试成功后任务即为 `SUCCEEDED` 终态，不会有任何额外尝试。

## 状态取值与迁移条件

### 任务状态 `TaskStatus`

| 状态 | 含义 | 迁移条件 |
| --- | --- | --- |
| `PENDING` | 初始态 | 触发运行时创建，恢复前占位 |
| `READY` | 可被调度 | 加载后/恢复后等待发起；依赖未满足时继续等待 |
| `RUNNING` | 某次尝试在飞 | 调度器领取并发起尝试（`Attempt` 自增，先持久化后执行） |
| `SUCCEEDED` | 成功（终态） | 某次尝试返回成功；恢复时原样保留，**不再执行** |
| `FAILED` | 本次尝试失败，仍有额度 | 执行返回 error 且 `attempt < MaxAttempts`；退避后回 `RUNNING` |
| `TIMED_OUT` | 本次尝试超时收尾，仍有额度 | 超时计时器先触发；退避后回 `RUNNING` |
| `PERMANENTLY_FAILED` | 额度耗尽（终态） | 失败/超时发生在末次尝试；恢复时若发现末次失败已落盘也直接收敛于此 |
| `SKIPPED` | 上游非成功终态（终态） | 任一上游为 `PERMANENTLY_FAILED`/`SKIPPED`，执行次数保持 0 |

迁移图（简化）：

```text
PENDING/READY ──领取──▶ RUNNING ──成功──────────────────▶ SUCCEEDED
                         │  ├─失败且有余量─▶ FAILED ─退避─▶ RUNNING
                         │  └─超时且有余量─▶ TIMED_OUT ─退避▶ RUNNING
                         └─末次失败/超时──────────────────▶ PERMANENTLY_FAILED
PENDING/READY（任一上游非成功终态）──────────────────────▶ SKIPPED
```

### 运行状态 `RunStatus`

- `RUNNING`：至少一个任务未到终态。
- `SUCCEEDED`：全部任务为 `SUCCEEDED`。
- `FAILED`：存在 `PERMANENTLY_FAILED` 或 `SKIPPED` 的任务。
  运行失败不影响其他 run。

## 并发触发与归属唯一

- `Trigger(workflowName, runID)` 是幂等的：相同 `runID` 的并发触发通过
  `Store.CreateRunIfNotExists` 原子创建，只有一个调用真正建运行（返回 `existed=false`），
  其余全部命中共存快照（`existed=true`），并记录 `ALREADY_RUNNING` 决策。
- 任务实例的归属键是 `(RunID, TaskName)`：一个 run 只有一套任务状态，
  每个任务在任一时刻最多只有一个在飞尝试（`inflight` 表 + attempt 代际）。
- 不同 `runID` 拥有完全独立的调度循环、状态与执行 goroutine。

## 中断恢复

- 发起尝试前先把任务持久化为 `RUNNING`（含 `Attempt` 与开始时间），
  再启动执行；因此崩溃后磁盘上的 `RUNNING` 表示“结果未知的在飞尝试”，
  **绝不会被判定为成功**。
- 新引擎对同一 `runID` 调用 `Trigger` 时会命中既有运行并启动恢复调度：
  - `SUCCEEDED` / `PERMANENTLY_FAILED` / `SKIPPED`：原样保留，成功任务不再执行；
  - `RUNNING`：记录 `RECOVER_RUNNING` 决策后重置为 `READY` 重新调度。
    该中断尝试已发起，计入尝试次数；若它本就是末次尝试，则直接收敛为
    `PERMANENTLY_FAILED`（崩溃可能发生在“末次失败已落盘、终态未落盘”的窗口）；
  - `FAILED` / `TIMED_OUT`：崩溃发生在退避窗口，按剩余额度决定重试或收敛终态。
- `FileStore` 用目录级 `flock` 串行化跨进程读写，每次写入走临时文件 + 原子 rename。
- 引擎 `Close()` 会取消所有在飞尝试的 ctx、停止退避计时器并等待调度循环退出；
  已写入的状态保持一致，由下次启动恢复。

## 决策日志（可解释性）

`NewLogger(io.Writer)` 输出形如：

```text
decision=LAUNCH_ATTEMPT run=r task=flaky attempt=2 发起第 2/3 次尝试，timeout=0s
decision=ATTEMPT_FAILED run=r task=flaky attempt=2 第 2 次尝试失败；结果依据：执行函数返回 error=...
decision=RETRY_SCHEDULED run=r task=flaky attempt=2 仍有重试额度（2/3），50ms 后重试
```

决策种类包括：`TRIGGERED`、`ALREADY_RUNNING`、`RECOVER_RUNNING`、`LAUNCH_ATTEMPT`、
`BLOCKED`、`ATTEMPT_SUCCEEDED`、`ATTEMPT_FAILED`、`ATTEMPT_TIMEOUT`、
`RETRY_SCHEDULED`、`RETRIES_EXHAUSTED`、`SKIP`、`RUN_SUCCEEDED`、`RUN_FAILED`。
每条日志都包含 run、任务、尝试序号与结果依据；测试在 `-v` 下打印完整决策流。

## 本地验证方法

```bash
# 1. 全量单测（依赖阻塞、失败重试、超时、并发触发、中断恢复），开启竞态检测
go test ./... -race -v

# 2. 重复运行排查偶发竞态
go test ./... -race -count=20

# 3. 静态检查
go vet ./...

# 4. 可观察的端到端 demo
go run ./cmd/demo
```

测试与需求点的对应：

| 测试 | 覆盖点 |
| --- | --- |
| `TestLoadAndValidate_*` | 重复定义、缺失依赖、成环的可区分错误 |
| `TestDependentBlockingAndSkip` | 依赖阻塞、上游失败后下游跳过（执行 0 次）、独立链路继续推进 |
| `TestRetrySuccessThenNoDuplicate` | 失败重试成功，执行次数恰为 N，且无重复结果 |
| `TestRetriesExhaustedDeterministic` | 达到重试上限后的确定终态 |
| `TestTimeoutClosesAttemptOnce` / `TestTimeoutExhaustsBudget` | 超时收尾唯一、迟到结果丢弃、超时占额度 |
| `TestConcurrentTriggerUniqueOwnership` | 并发触发恰好创建一次、任务执行次数唯一 |
| `TestIndependentRunsIsolated` | 独立 run 互不影响 |
| `TestRecoveryAfterCrash` | 重启后成功任务不重跑、中断任务不算成功、重调度后收敛 |
| `TestRecoveryExhaustedDuringCrash` | 崩溃于终态落盘窗口时恢复到确定终态且不补跑 |
