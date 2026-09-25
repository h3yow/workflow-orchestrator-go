# DAG 工作流编排引擎（Go，本地可运行）

一个用本地状态模拟任务调度与执行的轻量 DAG 工作流引擎。目标是在**并发触发、失败重试、
超时收尾与进程中断恢复**下给出**可解释、可复现**的结果：

- 每个 run 只有一个串行决策线程，所有状态迁移都有决策日志；
- 任务实例归属唯一（owner + 领取代次 generation）；
- 状态以快照原子写入本地 JSON 文件，崩溃/重启后可从既有状态恢复。

## 目录结构

```
pkg/workflow/
  types.go    状态、定义、尝试记录等核心类型
  errors.go   加载/运行期可区分的错误哨兵
  loader.go   DAG 加载与校验（重复/缺依赖/成环）
  store.go    状态持久化抽象：MemoryStore 与 FileStore（原子写）
  engine.go   调度循环、重试、超时、租约与中断恢复
cmd/demo      本地可运行的演示程序
```

## 快速开始

```bash
# 运行演示（状态写入 ./.wf-state/*.json）
go run ./cmd/demo

# 运行全部测试（含竞态检测，建议反复运行）
go test ./... -race -count=20 -v
```

## 基本用法

```go
wf, err := workflow.Load(workflow.WorkflowDef{
    Name: "demo",
    Tasks: []workflow.TaskDef{
        {ID: "A", Retries: workflow.RetryPolicy{MaxAttempts: 3, Backoff: 20 * time.Millisecond}},
        {ID: "B", Deps: []string{"A"}, Timeout: workflow.TimeoutPolicy{Timeout: 2 * time.Second}},
    },
})
if err != nil { /* errors.Is 区分错误原因 */ }

store, _ := workflow.NewFileStore("./.wf-state")
eng := workflow.NewEngine(wf, store, workflow.TaskRegistry{
    "A": func(ctx context.Context, runID string, attempt int) error { return nil },
    "B": func(ctx context.Context, runID string, attempt int) error { return nil },
}, logger, workflow.Config{EngineID: "engine-1"})

run, err := eng.Trigger(ctx, "run-001")   // 触发（不存在则创建，已结束则直接返回终态）
run, err = eng.Recover(ctx, "run-001")   // 进程重启后恢复（run 不存在返回 ErrRunNotFound）
```

## 依赖语义

- 任务通过 `TaskDef.Deps` 声明**直接上游**；引擎按 Kahn 拓扑排序推进，
  同入度节点按**定义顺序**打破次序，因此调度顺序在任何机器上确定、可复现。
- 一个任务只有在**所有上游均为 SUCCEEDED** 时才可被领取执行。
- 任一上游为 `FAILED` 或 `SKIPPED`，下游迁移到 `SKIPPED`，**永远不会被静默执行**；
  跳过沿依赖链向下传播（B 因 A 失败而跳过，则依赖 B 的 C 也跳过）。
- 彼此独立的链路互不影响：一条链路失败时，其他链路继续推进至各自终态。
- run 的整体状态在所有任务到达终态后确定：**全部成功才 `SUCCEEDED`，否则 `FAILED`**。

### 加载校验（拒绝于加载阶段，错误可用 `errors.Is` 区分）

| 错误哨兵 | 触发条件 |
| --- | --- |
| `ErrEmptyWorkflow` | 不包含任何任务 |
| `ErrInvalidTask` | 任务 ID 为空等非法定义 |
| `ErrDuplicateTask` | 同一工作流内任务 ID 重复 |
| `ErrDependencyNotFound` | 依赖指向不存在的任务（含自依赖前的缺名校验） |
| `ErrCyclicDependency` | 依赖关系成环（含自依赖），由拓扑排序无法覆盖全部节点判定 |

## 重试与超时语义

- `RetryPolicy.MaxAttempts` 为**最大尝试次数（含首次）**，`<=0` 视为 1；
  第 N 次失败后若 `N < MaxAttempts`，任务回到 `READY`，退避
  `Backoff * N` 后再次领取；同一任务任意时刻最多只有一个尝试在执行。
- 一次尝试的结果固定为四类之一，且**每次尝试有且仅有一条结果记录**
  （`TaskInstance.Attempts` 中不会出现重复结果）：

  | 结果 | 含义 |
  | --- | --- |
  | `succeeded` | 任务体返回 nil |
  | `failed` | 任务体返回普通错误 / panic（panic 记为 `panic` 子结果） |
  | `timeout` | 尝试超过 `TimeoutPolicy.Timeout` |
  | `interrupted` | 优雅关闭导致尝试中断（**不计入尝试次数、不消耗预算、不写 Attempts**） |

- 达到 `MaxAttempts` 仍未成功时，任务以确定终态 `FAILED` 结束，不会无限重试。
- 超时以引擎侧判定为准：即使任务体不响应 `ctx` 取消，引擎也会在超时后
  立即以 `timeout` 唯一收尾；它迟到的返回值会因**归属/代次不符而被丢弃**，
  不会把终态改回成功。不配合取消的任务体其 goroutine 会自然脱离，
  因此**任务体应当尊重 ctx**。

## 状态取值与迁移条件

任务实例状态 `TaskState`：

| 状态 | 含义 | 进入条件 |
| --- | --- | --- |
| `PENDING` | 初始态 | run 创建 |
| `READY` | 依赖已满足（或等待重试退避） | 全部上游 SUCCEEDED；或失败尝试仍有重试预算 |
| `RUNNING` | 已被某引擎领取执行 | READY 被领取，写入 `OwnerID`、续租 `LeaseUntil`、`AttemptGeneration+1` |
| `SUCCEEDED` | 终态：成功 | 某次尝试 `succeeded` |
| `FAILED` | 终态：失败 | 尝试预算耗尽，或中断时预算已耗尽 |
| `SKIPPED` | 终态：跳过 | 任一上游 FAILED/SKIPPED |

```
PENDING ──全部上游成功──> READY ──领取──> RUNNING ──succeeded──────────> SUCCEEDED
   │                        ^              │
   │上游 FAILED/SKIPPED      │失败且有余量  │──failed/timeout 且有余量──> READY(退避)
   v                        │              │
SKIPPED                     └──────────────┴──失败/超时且预算耗尽──────> FAILED
                                          └──租约过期（进程中断）
                                              ├─ 预算有余 -> READY 重试
                                              └─ 预算耗尽 -> FAILED
```

run 状态 `RunState`：`RUNNING`（仍有任务可推进）→ `SUCCEEDED` / `FAILED`（终态，不可逆）。

## 并发触发与归属唯一

- 同一进程内对同一 `runID` 的并发 `Trigger` 由 run 级互斥串行化：只有一个调度循环，
  其余调用等待同一结果，因此一个任务**只会被一个执行体领取一次**。
- 每次领取写入 `OwnerID` 并对 `AttemptGeneration` 单调加一；执行 goroutine 只回报事件，
  不直接改状态。调度器只接受「状态为 RUNNING、owner 一致、generation 一致」的结果，
  被回收尝试的迟到结果/心跳一律丢弃。
- 不同 `runID` 相互独立，可安全并发。

## 中断恢复

- 每个 run 序列化为 `state 目录/<runID>.json`，写入采用
  「临时文件 + fsync + rename」原子替换，崩溃时只会读到完整旧快照或完整新快照。
- 领取 RUNNING 后引擎按 `LeaseDuration/3` 周期续租。进程被杀则心跳停止，
  租约到期后恢复方回收该任务：
  - **中断点绝不被判定为成功**；
  - 中断的尝试不计数、不耗预算（优雅关闭落 `interrupted`；硬崩溃则无该记录，二者等价）；
  - 仍有重试预算 -> 回到 READY 重新执行；预算已耗尽 -> FAILED；
  - 已经 SUCCEEDED 的任务恢复后不会再次执行。
- 优雅取消（`cancel(ctx)`）时，调度器会先排空在飞尝试、把中断状态落盘后再返回。

## 本地验证方法

```bash
# 1) 单元测试 + 竞态检测，反复运行确认稳定
go test ./... -race -count=20

# 2) 查看详细调度决策与每次尝试的结果依据（-v 打印到测试日志）
go test ./pkg/workflow -race -v

# 3) 运行 demo，检查落盘快照
go run ./cmd/demo
cat .wf-state/demo-run-1.json
```

测试覆盖：

- **依赖阻塞/跳过传播**：上游失败下游不执行、独立链路继续推进；
- **失败重试**：重试成功的尝试次数、无重复结果、预算耗尽的确定终态、超时唯一收尾、panic 恢复；
- **并发触发**：同 runID 16 路并发下每个任务恰好执行一次，多 runID 相互隔离；
- **中断恢复**：内存快照恢复、FileStore 模拟进程重启、中断任务重试/判失败、
  成功任务不重放、迟到陈旧结果被丢弃。

每条用例均通过 `-race`，日志中打印 `decision ...`（调度决策）与
`attempt ... result=... reason=...`（每次尝试的结果依据）。

## 设计边界

- `FileStore` 的原子写保证**单进程**并发安全与崩溃快照完整；多进程/多机同时写同一文件
  不在本实现范围内（跨引擎归属仍由 owner + generation + 租约在调度层防护）。
- 任务体应配合 `ctx` 取消；不配合时超时/关闭在引擎侧依然正确收尾，但任务 goroutine
  会脱离直到自行返回。
