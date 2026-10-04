# DAG 工作流编排引擎（本地可运行）

一个纯本地状态模拟的 DAG 工作流编排引擎：加载与校验工作流定义，按依赖推进任务，
处理任务级重试与超时，支持同一工作流的并发触发、**多进程互斥执行**与进程中断后的状态恢复。
核心目标是：**在并发触发、多进程竞争与重启恢复下，调度结果可解释、可复现**。

- 纯 Go 标准库，无外部依赖，不引入任何外部服务或中间件。
- 每次运行由**单个串行调度 goroutine** 驱动，所有状态迁移只在该 goroutine 内发生；
  任务执行在独立 goroutine 中，但结果通过事件队列回到调度循环结算。
- 每次尝试带**代际（attempt 序号）校验**：过期结果（超时先收尾后迟到的返回、
  被中断的尝试）一律丢弃，保证同一次尝试只产生一次状态写入。
- 跨进程协调靠**文件锁 + 租约（lease）+ fencing token**：任一时刻只有一个进程
  负责推进某次运行；持有者心跳续租，掉线后租约到期由其他进程接管；
  掉线进程的迟到写入被 fencing token 拒绝，绝不会覆盖新持有者的状态。
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
| `workflow/engine.go` | 引擎入口：注册、触发（去重）、等待、结果视图、关闭 |
| `workflow/scheduler.go` | 串行调度循环：依赖推进、跳过传播、重试退避、超时结算、租约心跳、中断恢复 |
| `workflow/lease.go` | 租约类型与获取/续期/释放/fencing 判定（两种 Store 共用） |
| `workflow/result.go` | 结果视图：尝试结算记录（`AttemptRecord`）与汇总辅助 |
| `workflow/state.go` | `Store` 接口（含租约与 fencing 写入）、错误定义、状态深拷贝 |
| `workflow/memstore.go` | 内存持久化（互斥锁，用于单进程/测试） |
| `workflow/filestore.go` | JSON 文件持久化（`flock` + 原子 rename，用于多进程与重启恢复） |
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

## 多进程协调（租约 + fencing token）

多个 worker 进程可以指向**同一个状态目录**（各自 `NewFileStore(同一目录)` + `NewEngine`），
引擎仅靠文件锁与本地状态文件完成协调，不需要任何外部服务：

- **执行租约**：每次运行有一份随状态落盘的租约（`RunState.Lease`：
  持有者、 fencing token、到期时刻）。进程要推进某个运行，必须先通过
  `Store.AcquireRunLease` 原子地拿到租约；拿不到（被其他存活进程持有）时，
  本次触发只读既有结果（记录 `LEASE_HELD_BY_OTHER` 决策），`RunAndWait`
  转为轮询等待，绝不重复调度。
- **心跳与掉线判定**：持有者每 `TTL/3` 续一次租约。TTL 默认 `2s`
  （`WithLeaseTTL` 可调），即**一个进程约 2 秒没有任何心跳即被视为掉线**，
  租约到期后任何进程都可接管。优雅 `Close()` 会主动释放租约，接管立即发生，
  无需等待 TTL。
- **fencing token**：租约每次易主 token 单调递增（`RunState.LastToken` 只增不减）。
  所有任务/运行状态写入都携带持有者的 token，存储层在校验不匹配时拒绝写入——
  被强杀或失联进程的迟到写入**绝不会**覆盖新持有者的状态，
  因此不可能出现两份互相打架的结果。
- **接管语义**：接管方恢复调度时——已 `SUCCEEDED` 的任务不再执行；
  中断时在飞的尝试（磁盘上的 `RUNNING`）不判定为成功，记一条 `INTERRUPTED`
  结算（占用一次尝试额度）后重新调度；额度已耗尽的直接收敛到
  `PERMANENTLY_FAILED`，不补跑。整个运行最终收敛到确定终态，
  重复触发多少次拿到的结论都一样。
- **运行间隔离**：租约是运行级的，某个运行卡住或失败不会拖住其他运行；
  目录级文件锁只在单次读-改-写期间持有，执行期间不持锁。

多进程用法与单进程完全一致（同一套 API），只是多个进程打开同一目录：

```go
st, _ := workflow.NewFileStore("/shared/state/dir")   // 每个进程各开一份
eng := workflow.NewEngine(st, executor, logger,
    workflow.WithLeaseTTL(2*time.Second))             // 掉线判定时长
eng.Register(def)
snap, existed, _ := eng.Trigger(ctx, "demo", "run-42") // existed=true 表示已存在
status, _ := eng.RunAndWait(ctx, "run-42")             // 不归自己管时会等对方收尾或掉线接管
view, _ := eng.Result(ctx, "run-42")                   // 任意进程可读完整结果视图
```

## 结果视图（跨进程一致）

运行结束后（以及运行期间的任意时刻），任何进程都可以通过
`Engine.Result(ctx, runID)` 或 `Store.GetRun` 读到**同一份完整结果视图**：

- 运行终态（`RunState.Status`）与每个任务的最终状态（`TaskState.Status`）；
- 尝试总次数（`RunState.TotalAttempts()`，含在飞与中断的尝试）；
- 每一次尝试的结算记录 `TaskState.Attempts`（只增序列，绝不修改或删除）：
  `SUCCEEDED` / `FAILED` / `TIMED_OUT` / `INTERRUPTED`（持有者掉线，结果未知）
  / `SKIPPED`（上游非成功终态，未发起尝试），每条都带结算依据（`Reason`）、
  发起进程（`Owner`）与起止时间。

读取走文件锁 + 原子 rename 后的完整文件，因此并发读写时看到的永远是完整快照，
不会读到半截状态；同一运行的视图可以被任意进程直接读取，多次读取结果一致。

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
- 引擎 `Close()` 会取消所有在飞尝试的 ctx、停止退避与心跳计时器、释放租约，
  并等待调度循环退出；已写入的状态保持一致，由下次启动（或接管进程）恢复。
  被 `kill -9` 的进程没机会做这些清理：租约随 TTL 到期失效后由其他进程接管，
  其迟到写入被 fencing token 拒绝（见 `TestTakeoverAfterSIGKILL`）。

## 兼容性

- **单进程用法不变**：API 与之前完全相同（`Register`/`Trigger`/`RunAndWait`/`Close`），
  租约的获取、续期、释放全部由引擎内部完成，调用方无感知；
  `MemoryStore` 同样实现了租约语义，单进程多引擎行为一致。
- **已有状态数据可直接使用**：旧版本落盘的 JSON 没有 `Lease`/`LastToken`/
  `Attempts` 字段，加载后分别为 `nil`/`0`/空序列——`nil` 租约即“无人持有”，
  新引擎首次触发即可正常获取租约并恢复调度，无需任何迁移。
- **时间源**：`WithClock` 注入的时间源只影响业务时间戳；
  租约计时始终使用真实墙钟（跨进程可比），不受注入时钟影响。

## 决策日志（可解释性）

`NewLogger(io.Writer)` 输出形如：

```text
decision=LAUNCH_ATTEMPT run=r task=flaky attempt=2 发起第 2/3 次尝试，timeout=0s
decision=ATTEMPT_FAILED run=r task=flaky attempt=2 第 2 次尝试失败；结果依据：执行函数返回 error=...
decision=RETRY_SCHEDULED run=r task=flaky attempt=2 仍有重试额度（2/3），50ms 后重试
```

决策种类包括：`TRIGGERED`、`ALREADY_RUNNING`、`LEASE_ACQUIRED`、`LEASE_HELD_BY_OTHER`、
`LEASE_LOST`、`RECOVER_RUNNING`、`LAUNCH_ATTEMPT`、`BLOCKED`、`ATTEMPT_SUCCEEDED`、
`ATTEMPT_FAILED`、`ATTEMPT_TIMEOUT`、`RETRY_SCHEDULED`、`RETRIES_EXHAUSTED`、`SKIP`、
`RUN_SUCCEEDED`、`RUN_FAILED`。
每条日志都包含 run、任务、尝试序号与结果依据；测试在 `-v` 下打印完整决策流。

## 本地验证方法

```bash
# 1. 全量单测（依赖阻塞、失败重试、超时、并发触发、多进程互斥与接管、中断恢复），开启竞态检测
go test ./... -race -v

# 2. 重复运行排查偶发竞态
go test ./... -race -count=20

# 3. 静态检查
go vet ./...

# 4. 可观察的端到端 demo
go run ./cmd/demo
```

多进程行为在本地复现的方式：

- **单进程内模拟多进程**：`workflow/multiprocess_test.go` 在同一状态目录上创建多组
  独立的 `FileStore` + `Engine`。`flock` 作用于独立的打开文件描述，
  因此这些实例之间的互斥语义与真实多进程一致。
- **真实子进程 + kill -9**：`TestTakeoverAfterSIGKILL` 用 `os/exec` 启动一个
  真实 worker 进程，等它的任务在飞后发送 SIGKILL，再由本进程接管跑完，
  并跨进程校验执行次数（共享日志文件）与结果视图。
- **手工复现**：也可以自己起两个进程指向同一目录——一个跑 `Trigger` 后 `kill -9`，
  另一个对同一 `runID` 调用 `Trigger` + `RunAndWait`，约一个租约 TTL（默认 2s）
  后即可观察到接管与 `RECOVER_RUNNING` 决策。

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
| `TestMultiProcessConcurrentTrigger` | 多进程并发触发：恰好创建一次、任务只执行一次、各进程结果视图逐字节一致 |
| `TestLeaseExpiryTakeover` | 持有者掉线后按租约 TTL 接管：不提前、不超时、成功任务不重跑、中断尝试记 INTERRUPTED |
| `TestFencedWritesRejected` | fencing token：易主后旧持有者的写入/续期/释放全部失效（内存与文件两种 Store） |
| `TestResultViewCompleteAndConsistent` | 结果视图完整（终态/任务状态/尝试次数/逐次结算依据）、并发读取只见完整快照、多次读取一致 |
| `TestMultiProcessRunsIsolated` | 某运行被他进程卡住时，其他运行照常推进 |
| `TestTakeoverAfterSIGKILL` | 真实子进程被 kill -9 后跨进程接管：成功任务不重跑、中断占额度、收敛到确定终态 |
