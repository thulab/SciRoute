# FS 公共查询任务调度器

`QueryTaskScheduler` 为同一个 `QueryRuntime` 中的独立查询请求提供固定工作线程、有界等待队列、资源预约和就绪任务攒批派发。它与 decoded fragment cache 的生产者线程池、训练查询的 `FairWorkScheduler`、owner reader pool 分别控制不同资源；不创建每请求专用线程，也不拆分单个查询、GRIB Message 或数据库 SQL。

## 配置与派发

| 启动配置 | 默认值 | 有效范围 |
| --- | --- | --- |
| `query.tasks.worker_threads` | 4 | 1–256 |
| `query.tasks.max_queued_tasks` | 64 | 1–65536 |
| `query.tasks.max_batch_size` | `min(4, worker_threads)` | 1–`worker_threads` |

C++ `QueryTaskSchedulerConfig` 默认是 4 个 worker、64 个等待任务、最大批次 4；直接构造配置时，调用方改变 worker 数量后也须提供不超过 worker 数量的 batch 大小。无效构造配置抛出 `std::invalid_argument`。

专用派发线程按提交次序扫描等待队列，跳过资源暂不可用的任务，每次派发最多 `min(空闲 worker 数, max_batch_size, 可运行任务数)` 个任务。已有可运行任务和空闲 worker 时立即派发；没有等待凑满批次的定时窗口。批次只表示一起分配执行机会，不合并查询语义或结果，也不保证同批任务同时完成。多 worker 的完成顺序可不同于提交顺序。

dispatcher 使用独立条件变量，每个固定执行槽另有只供其 worker 等待的条件变量；所有谓词和槽状态仍由同一个 scheduler mutex 保护。提交、任务完成及资源重新分类只通知 dispatcher，派发仅通知本轮变成 Ready 的槽。通知可以早于 worker 进入等待，Ready 谓词仍保证任务不会丢失；不同槽不共享 `notify_one`，避免唤错 worker。正常 shutdown 由 dispatcher drain 完全部已接纳任务后设置退出谓词，再逐一通知所有 worker；部分线程构造失败时也设置退出谓词并通知已启动 worker。资源不可用时的 10ms 外部探测、原接纳上限和任务完成顺序不变。

`max_queued_tasks` 仅计算尚未分配给 worker 的任务；正在执行或已经分配的任务另外受 worker 数量限制。`submit_batch` 原子接纳整个输入批次：剩余等待空间不足时返回 `ResourceExhausted`，其中任何一个任务都不执行。即使所有 worker 空闲，单次批次超过等待队列容量也会拒绝。空 task 返回 `InvalidQuery`。成功提交返回每个任务各自的 `future<QueryStatus>`。

## 资源预约与重新分类

`Submission::resources` 接受最多 32 个资源需求，每项由共享 `QueryTaskResource` 和正整数 units 组成。资源容量不可变；空资源、重复资源、零 units 或超过容量的需求以 `InvalidQuery` 拒绝整批。派发前非阻塞尝试全部预约，任一失败立即归还已取得的部分预约，任务留队，不占执行槽。逻辑 Lease 支持跨线程归还，不转移真实 mutex 所有权。资源释放后重新扫描；外部释放或 probe 状态变化最多按 10 ms 间隔观察，同池任务完成会主动通知。

Native HDF5 入口使用进程级容量 1 的逻辑资源，并通过同线程 `try_lock/unlock` 探测实际 HDF5 库锁。其他 HDF5 请求留队，显式 Arrow 请求可以越过它们。Indexed HDF5 不申请该资源。缺省 all、混合或不确定的格式选择在 native 模式保守申请；调度提示不执行 source select、prepare、权限检查或元数据计费。纯 prepare/release、dataset artifact 与 Arrow window 操作不申请 native 资源。

模板/selector 使用不续 TTL、不清理的 `inspect`，等待任务只保留 weak ownership。实际执行仍在原 `acquire` 点、同一 registry mutex 内核对对象身份，再续 TTL；ID 在排队期间复用时，执行前抛内部 `ObservationChanged`，重新分类资源，保留原客户端错误和过期行为。

`ResourcesChanged` 仅允许用于查询 I/O、计费、发布等副作用之前的观察失效。调度器归还旧预约，保留同一 task/future，并将其置为 Parked。该状态只保留原接纳名额，不占执行线程；可与后续可运行任务交换已有 queue 单元，不扩大等待容量。始终 `pending <= max_queued_tasks`，`active + parked <= worker_threads`，总接纳任务不超过二者上限之和。取消或过期的 Parked 任务无需资源即可终结。不会重复完成 future 或重复接纳请求。

实际 HDF5 库锁继续负责句柄安全。库外调用可能在 probe 后抢先持锁，最多一个已预约 HDF5 请求仍可能阻塞在执行层；probe 不保证完全消除这种等待。通用多资源组合不承诺无饥饿。外部资源永久不可用且未取消/无截止时间时，drain 也可能持续等待。

## 服务入口

控制 socket 的四个 transport worker 负责认证对端、读取完整 frame 和顶层 JSON 语法解析，随后异步提交整个控制请求，立即继续收包。正式查询验证、规划、读取、编码和原有发布清理在公共任务池中执行；队列满时返回结构化 `ResourceExhausted`。多个 socket server 使用同一个 runtime 时共享执行上限。

顶层 JSON 的语法解析在 transport 分类时完成一次；只读解析树由控制任务持有，资源提示、实际执行和结果 fetch 复用它。正式结构／语义验证、权限、版本与预算仍在原公共 worker 边界执行。确定的 JSON 语法异常在原验证边界重抛；非语法的瞬时解析异常保留原资源提示和 worker 重新解析机会。正常任务不同时保留原 wire 字符串，排队树仍受原帧长、队列和 worker 上限约束；树可能比 wire 大。`typed_batch` 的内部逐项解析保持原行为，句柄观察失效时重新检查资源，不缓存授权或版本结果。

普通 profile 的 `control_json.transport_parse_ns` 单独记录执行前的完整语法解析，`json_parse` 只记录 worker 的复用／异常重试；不能仅凭 `json_parse` 或 `server_total` 下降声称完整查询提速。协议继续为每连接一请求，不复用连接身份。

本轮粒度是独立控制请求。一个 `typed_batch` 或 prepared batch 仍是一个任务，其内部 items 顺序执行，保留共享计费、错误顺序和整体回滚。同步客户端只有一个在途请求时，服务端不会凭空产生多个查询；可并行的请求须已经到达。此处攒批是对已就绪请求批量派发，不等待后续请求、不合并 SQL，也不拆分查询空间。

metadata/profile scope 在实际执行线程建立，已有执行 lease、授权与冻结版本选择继续由原查询路径维护。`stop()` 先停止接收，再等待本 server 的已接纳请求清理完成，最后清理注册表；不会关闭其他使用者共享的 runtime。socket 目前没有断连取消协议，也不将客户端响应超时映射为 task deadline；排队或执行中的已接纳请求会继续运行，并沿用原有发布失败回滚逻辑。

profile 的 `runtime_config.query_task_workers/query_task_max_queued/query_task_max_batch` 记录池配置，`counters.query_tasks.queue_nanoseconds` 记录从完整 frame 入队到开始执行的等待时间。原有 `server_total_ns` 仍表示执行及编码阶段，队列等待单列，不能将两者混作同一个阶段。

## 生命周期与语义

- 调度器在调用 task 前检查共享取消信号和单调时钟 deadline，分别返回 `Cancelled` 或 `DeadlineExceeded`，不调用已失效任务。两者同时满足时先返回 `Cancelled`。
- 已运行 task 接收原取消信号并自行合作退出。调度器不强制中断，不因运行期间取消或超时而改写 task 已返回的状态。deadline 不是运行中定时器；查询内部继续负责自己的截止时间检查。
- task 返回的 `QueryStatus` 原样传递，包括 phase、message、source、operation 和 retriable。抛出的标准或未知异常转换为 `Internal/Execution`，后续任务仍可运行。
- `shutdown()` 停止接纳新任务并等待所有已接纳任务结束；重复调用安全。析构执行同样的 drain。调用方应在池外停止服务；不得从该池的 task 调用 `shutdown()`，或同步等待提交回同一池的子任务。
- 调度器不继承提交线程的 TLS。task 在执行线程中建立并恢复自己的版本读取、预算、profile 和取消 scope；共享状态通过显式捕获的对象传递，避免跨请求错误归属。请求持有的读取 lease、结果和资源必须覆盖其实际使用周期。
- `QueryRuntime` 在销毁缓存和 admission 服务之前先 drain 查询任务，保证已接纳请求可以完成清理。其他拥有者仍须保证 task 所引用对象活到 future 完成或调度器 drain 结束。

## 观测

`stats()` 提供互斥保护的一致快照。`submitted` 为已接纳任务数，`completed` 为所有终态任务数，`failed` 为非 OK 终态数；`cancelled` 与 `expired` 是其中相应错误码的子集。`rejected` 按因容量不足或停止接纳而拒绝的任务数累计，输入验证失败不计入。

`queued` 是 pending 队列任务数，`active` 包括已分配但尚未进入用户函数的任务，`parked` 是保留接纳名额、已释放执行线程的重新分类任务数。`resource_waits` 按首次资源阻塞任务计数，`resource_bypasses` 统计越过前面资源等待者的派发，`resource_blocked` 是最近扫描观察到的 pending/Parked 阻塞 gauge，`resource_requeues` 统计观察失效后的重新分类。`dispatch_batches`、`dispatched_tasks` 和 `max_dispatch_batch` 描述真实派发尝试，重分类任务可多次派发但只接纳和完成一次；这些数字不代表数据库 SQL 并发度、物理 CPU 使用率或性能提升。完成 future 就绪前已释放该任务的函数捕获和资源预约、更新终态计数并释放执行槽位；全部已接纳任务的最终状态以 drain 后的快照为准。

## 验证边界

`tests/unit/query/core/test_QueryTaskScheduler.cpp` 使用条件变量门闩验证 1/2/4 个实际运行任务、FIFO、攒批上限、单任务即时派发、有界队列与全批拒绝、取消与过期、异常隔离、TLS scope、共享 runtime 和 drain。等待超时只用于暴露死锁或未派发，不据此推断吞吐量或提速。这里不产生 GRIB/HDF5/Arrow 端到端性能结论。
