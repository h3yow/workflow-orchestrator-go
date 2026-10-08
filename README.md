# DAG 工作流编排引擎（本地可运行）

一个纯本地状态模拟的 DAG 工作流编排引擎：加载与校验工作流定义，按依赖推进任务，
处理任务级重试与超时，支持同一工作流的并发触发、**多进程互斥执行**与进程中断后的状态恢复。
核心目标是：**在并发触发、多进程竞争与重启恢复下，调度结果可解释、可复现**。

- 纯 Go 标准库，无外部依赖，不引入任何外部服务或中间件。
- 每次运行由**单个串行调度 goroutine** 驱动，所有状态迁移只在该 goroutine 内发生；
  任务执行在独立 goroutine 中，但结果通过事件队列回到调度循环结算。
- 每次尝试带**代际（attempt 序号）校验**：过期结果（超时先收尾后迟到的返回、
  被中断的尝试）一律丢弃，保证同一次尝试只产生一次状态写入。
- 跨进程协调靠**文件锁 + 租约（lease）+ fencing token + 进程存活锁**：
  任一时刻只有一个进程负责推进某次运行；持有者每 `TTL/3` 心跳续租。
  心跳超时**不会**立刻导致接管——接管方还必须确认持有进程已经彻底退出
  （基于内核级 `flock` 的存活锁：进程暂停/卡顿不释放，被 `kill -9` 才释放），
  因此“还活着只是暂时没响应”不会造成任务被重跑；
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
| `workflow/filestore.go` | JSON 文件持久化（`flock` + 原子 rename）与**进程存活锁**（`.owners/` 下的内核级 flock） |
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

## 多进程协调（租约 + 进程存活锁 + fencing token）

多个 worker 进程可以指向**同一个状态目录**（各自 `NewFileStore(同一目录)` + `NewEngine`），
引擎仅靠文件锁与本地状态文件完成协调，不需要任何外部服务。

### 三层协调机制

1. **执行租约（心跳）**：每次运行有一份随状态落盘的租约（`RunState.Lease`：
   持有者、fencing token、到期时刻）。进程要推进某个运行，必须先通过
   `Store.AcquireRunLease` 原子地拿到租约；持有者每 `TTL/3` 续一次租约。
   心跳的作用是**快速发现正常关闭/掉线**，但它**不是**接管的唯一条件。
2. **进程存活锁（liveness flock）**：每个引擎在自己的生命周期内（首次调度前到
   `Close()`）始终持有 `<状态目录>/.owners/<owner>.lock` 上的一把**内核级独占
   `flock`**。这把锁的关键性质是：
   - 进程只是**暂停或卡顿**（被 `SIGSTOP`、机器抖动、GC/调度饿死、暂时写不动心跳），
     锁**依然被持有**——其他进程抢不到，就知道“它还活着”，**绝不接管**；
   - 进程**彻底退出**（`kill -9`、崩溃、掉电）时，操作系统会**自动释放**这把锁，
     与进程有没有机会执行清理无关——其他进程能立刻抢到，确认它真的没了。
3. **fencing token**：租约每次易主 token 单调递增（`RunState.LastToken` 只增不减）。
   所有任务/运行状态写入都携带持有者的 token，存储层校验不匹配即拒绝写入。

### 到底什么时候别的进程才可以接管？

接管方尝试获取一个他人持有运行的租约时，必须**同时**满足两个条件才会成功：

- **条件一：心跳超时。** 租约的 `Until` 已过，即持有者超过一个 TTL 没有续上心跳
  （默认 `2s`，`WithLeaseTTL` 可调；测试中常用更短值）。心跳正常（TTL 内）一律不接管。
- **条件二：持有进程已确认死亡。** 接管方对持有者的存活锁做一次**非阻塞**
  `flock(LOCK_EX|LOCK_NB)` 探测：
  - 抢不到（`EWOULDBLOCK`）→ 存活锁仍被持有 → 进程还活着（哪怕它已经很多个 TTL
    没心跳）→ **拒绝接管，继续等待**，并打出 `HOLDER_UNRESPONSIVE` 决策
    （每 TTL 至多一条，避免刷屏）。宁可让这个运行一直等，也不允许任务跑两遍。
  - 抢到了 → 存活锁无人持有 → 进程已退出 → 允许接管，fencing token 加一。
  - 探测本身出错（锁文件所在目录不可访问等“无法确认”的情况）→ 按**保守**处理，
    视同“还活着”，继续等待，绝不冒险接管。

因此两种情况被明确区分开：

| 现场 | 心跳 | 存活锁 | 结论 |
| --- | --- | --- | --- |
| 正常运行 | 按时续 | 持有 | 不接管 |
| **进程还活着，只是暂时没响应**（SIGSTOP / 卡顿 / 饿死） | 超时 | **仍持有** | **不接管，等它恢复**（任务绝不重跑） |
| **进程被 kill -9 / 崩溃 / 掉电** | 超时 | **被内核释放** | **立即允许接管** |
| 优雅 `Close()` | 主动释放租约 + 存活锁 | 释放 | 立即接管，无需等 TTL |

> 失联时长的含义：一个 TTL（默认 **2 秒**）没有心跳只表示“暂时联系不上”，
> 并不代表进程死了；真正的死亡以存活锁被内核释放为准。所以“暂停很久但最终恢复”
> 的进程会一直保有它手上的任务，恢复后由它自己把那次尝试跑完，全程只有一份副作用。
> 只有在进程被强杀时，接管才会发生，时机约为“一个 TTL + 一次轮询间隔”。

### 接管后的结算语义（保持不变）

接管方恢复调度时——已 `SUCCEEDED` 的任务**不再执行**；中断时在飞的尝试
（磁盘上的 `RUNNING`）**不判定为成功**，记一条 `INTERRUPTED` 结算
（**占用一次尝试额度**，`Owner` 记为原持有者）后重新调度，接管后的重新执行
只产生**一份**结果；额度已耗尽的直接收敛到 `PERMANENTLY_FAILED`，不补跑。
整个运行最终收敛到确定终态，重复触发多少次结论都一样。

### 其余约定

- 拿不到租约时，本次触发只读既有结果（记录 `LEASE_HELD_BY_OTHER` 或
  `HOLDER_UNRESPONSIVE` 决策），`RunAndWait` 转为轮询等待，绝不重复调度。
- **运行间隔离**：租约与存活判定都是运行/进程级的，某个运行卡住或失败不会拖住
  其他运行；目录级文件锁只在单次读-改-写期间持有，执行期间不持锁。
- 存活锁文件放在 `<状态目录>/.owners/` 下；引擎取得自己的存活锁后会**尽力清理**
  目录里无人持有的陈旧锁文件（能非阻塞抢到锁才删，对方还活着则绝不触碰）。

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
- 引擎 `Close()` 会取消所有在飞尝试的 ctx、停止退避与心跳计时器、释放租约与
  本进程存活锁，并等待调度循环退出；已写入的状态保持一致，由下次启动（或接管进程）
  恢复。**优雅退出无需等待 TTL，其他进程立即接管。**
  被 `kill -9` 的进程没机会做这些清理：它的存活锁由操作系统自动释放，
  其他进程在“租约心跳超时（TTL）+ 存活锁可被抢到”后接管，
  其迟到写入被 fencing token 拒绝（见 `TestTakeoverAfterSIGKILL`）。
  反之，进程若只是被 `SIGSTOP`/卡顿而**仍然存活**，即使心跳超时，存活锁也不会
  释放，其他进程不会接管（见 `TestPausedProcessNotTakenOver`）。

## 兼容性

- **单进程用法不变**：API 与之前完全相同（`Register`/`Trigger`/`RunAndWait`/`Close`），
  租约的获取/续期/释放与存活锁的持有/释放全部由引擎内部完成，调用方无感知；
  `MemoryStore` 同样实现了存活语义（进程内注册表，引擎 `Close` 即注销），
  单进程多引擎行为一致。
- **多进程可直接滚动升级，旧版本状态数据无需迁移**：
  - 旧版本落盘的运行 JSON 没有 `Lease`/`LastToken`/`Attempts` 字段，
    加载后分别为 `nil`/`0`/空序列——`nil` 租约即“无人持有”，
    新引擎首次触发即可正常获取租约并恢复调度。
  - 存活锁是**新增的旁路文件**（`.owners/` 目录），不改动运行 JSON 的结构。
    若运行的租约持有者是一个“从未来过本目录注册存活锁”的 owner（例如手工构造的
    遗留快照、旧版本进程），探测其存活锁会得到“无人持有”，行为退化为
    “心跳超时即可接管”——与旧版本语义一致，不会把旧数据永久卡死。
  - 混用部署时唯一需要注意：**旧版本进程不会持有存活锁**，因此一个正在运行的
    旧进程在心跳超时后仍可能被新进程接管。让所有指向同一状态目录的 worker
    都升级到本版本后，“存活不接管”的保证才对全体进程生效。
- **时间源**：`WithClock` 注入的时间源只影响业务时间戳；
  租约计时始终使用真实墙钟（跨进程可比），不受注入时钟影响。
- **平台**：存活锁与跨进程互斥都基于 POSIX `flock`（Linux/macOS/BSD 等）；
  纯 Windows 上跨进程互斥不在支持范围内，`MemoryStore` 可用于单进程场景。

## 决策日志（可解释性）

`NewLogger(io.Writer)` 输出形如：

```text
decision=LAUNCH_ATTEMPT run=r task=flaky attempt=2 发起第 2/3 次尝试，timeout=0s
decision=ATTEMPT_FAILED run=r task=flaky attempt=2 第 2 次尝试失败；结果依据：执行函数返回 error=...
decision=RETRY_SCHEDULED run=r task=flaky attempt=2 仍有重试额度（2/3），50ms 后重试
```

决策种类包括：`TRIGGERED`、`ALREADY_RUNNING`、`LEASE_ACQUIRED`、`LEASE_HELD_BY_OTHER`、
`HOLDER_UNRESPONSIVE`、`LEASE_LOST`、`RECOVER_RUNNING`、`LAUNCH_ATTEMPT`、`BLOCKED`、
`ATTEMPT_SUCCEEDED`、`ATTEMPT_FAILED`、`ATTEMPT_TIMEOUT`、`RETRY_SCHEDULED`、
`RETRIES_EXHAUSTED`、`SKIP`、`RUN_SUCCEEDED`、`RUN_FAILED`。

- `LEASE_HELD_BY_OTHER`：租约还在 TTL 内、由别的进程正常持有，本进程只读等待。
- `HOLDER_UNRESPONSIVE`：持有者心跳已超过 TTL，但**存活锁仍在**——进程确认还活着
  （暂停/卡顿），本进程**不接管**、继续等待；每个运行每 TTL 至多打印一条。

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
- **存活锁语义单测**：`TestPresenceFileLockLiveness`（多轮）直接验证
  “持锁=存活、释放=死亡、未注册视为无人持有”；`TestPresenceMemoryRegistry`
  验证内存版注册表与引用计数。
- **进程内“心跳静默但进程存活”**：`TestTakeoverBlockedWhileHolderAlive` 用测试专用
  心跳闸门把持有者的心跳 goroutine 挂起超过若干个 TTL（等价于进程被冻结），
  验证另一进程始终抢不到租约、任务计数恒为 1；恢复后由原进程把唯一一次尝试跑完。
  `TestHolderUnresponsiveDecisionLogged` 验证等待方打出 `HOLDER_UNRESPONSIVE`。
- **真实子进程 + SIGSTOP/SIGCONT**：`TestPausedProcessNotTakenOver`（3 轮）用
  `os/exec` 启动真实 worker，等任务在飞后发送 `SIGSTOP` 把整个进程冻结
  （远超一个 TTL 没有心跳），确认另一进程零接管、零重跑；再 `SIGCONT` 恢复，
  由**原进程自己**把任务跑完（副作用计数文件区分 `block-helper` / `block-takeover`）。
- **真实子进程 + kill -9**：`TestTakeoverAfterSIGKILL`（3 轮）用 `os/exec` 启动
  真实 worker，等它的任务在飞后发送 SIGKILL，再由本进程在 TTL 后接管跑完，
  并跨进程校验执行次数（共享日志文件）与结果视图（`[INTERRUPTED, SUCCEEDED]`）。
- **手工复现**：也可以自己起两个进程指向同一目录——
  - 暂停恢复：对正在跑任务的进程按 `kill -STOP <pid>`，几秒后另一个进程的
    `RunAndWait` 只会打印 `HOLDER_UNRESPONSIVE` 而**不会**重跑任务；
    再 `kill -CONT <pid>`，原进程继续跑完。
  - 强杀接管：`kill -9 <pid>` 后，另一个对同一 `runID` 调用 `Trigger` +
    `RunAndWait` 的进程，会在约一个租约 TTL（默认 2s）后观察到接管与
    `RECOVER_RUNNING` 决策。

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
| `TestTakeoverAfterSIGKILL` | 真实子进程被 kill -9 后跨进程接管（多轮）：成功任务不重跑、中断占额度、收敛到确定终态 |
| `TestPausedProcessNotTakenOver` | 真实子进程被 SIGSTOP 冻结（心跳远超 TTL）时零接管零重跑，SIGCONT 后由原进程自己跑完（多轮） |
| `TestTakeoverBlockedWhileHolderAlive` | 进程内复现“心跳静默但进程存活”：另一进程拿不到租约、任务计数恒为 1，恢复后原尝试成功（多轮） |
| `TestHolderUnresponsiveDecisionLogged` | 等待方打出 `HOLDER_UNRESPONSIVE` 且持有者存活期间绝不取得租约 |
| `TestPresenceFileLockLiveness` / `TestPresenceMemoryRegistry` | 存活锁：持锁=存活、释放=死亡、未注册=无人持有；内存注册表与引用计数（多轮） |
