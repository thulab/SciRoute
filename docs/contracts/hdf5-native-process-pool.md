# Native HDF5 有界进程执行（已撤回，历史契约）

2026-10-26：依据小文件与扩大规模探针，该实验后端已从当前生产构建撤回；配置 `query.hdf5.native_processes`、专用 helper、进程传输与公共调度器异步接点均已移除。Native HDF5 恢复进程内执行和既有资源感知排队。旧配置中的该键（包括值0）会在连接数据库及挂载前明确拒绝。本文保留当时的实现与验收边界，供[原探针](../reports/hdf5-native-process-probe-20261026.md)及[规模对照](../reports/hdf5-native-indexed-scale-probe-20261026.md)复核，不代表当前可用功能。

`query.hdf5.native_processes=0` 默认关闭，保留既有进程内 Native HDF5 路径。设为 1–16 时，要求 `query.hdf5.mode=native-hdf5`、`query.hdf5.native_cache=false`，且进程数不超过 `query.tasks.worker_threads`。专用 `iotdbfs_native_query_worker` 安装在 daemon 同一目录，使用相同构建和依赖，通过 `posix_spawn/exec` 独立初始化。启动握手失败明确报错，不回退执行，也不在运行中的多线程 daemon 中执行 fork 后的 C++ 查询代码。

## 查询边界

父进程执行原来源选择、授权、owner admission 和 prepare，持有完整精确版本能力及 execution lease。现有 HDF5 物理值查询语法要求单个 Exact 路径和 Exact 版本；该请求的整个冻结执行计划，包括全部 slab、通用 residual/projection、累计行数限制和 Arrow IPC 编码，交给一个子进程顺序执行。不会拆分 message、chunk、slab 或数据库 SQL，也不会创建客户端 worker。

父进程再次执行原结果发布 admission、结果缓存和响应交付。子进程不重做 prepare，不访问 owner journal、数据库连接或自身 FUSE 路径，不获得 owner 身份，也不保存授权。空结果、非 Native HDF5、混合格式和 `typed_batch` 保留原执行路径；批内累计预算及整体回滚继续由原路径管理。普通单查询和单查询模板使用公共进程资源，catalog/批请求仍使用原进程内 Native HDF5 资源。

## 一个等待队列

`QueryTaskScheduler::Submission::async_task` 释放实际 launch worker，继续占用原逻辑在途名额。所有尚未获得进程资源的请求留在同一个公共等待队列，dispatcher 可以越过它们执行 Arrow 等可运行任务。进程池每个 cell 只有一个已准入任务交接位和一个固定父端协调线程，没有额外等待队列；该线程维持原请求 TLS、代理能力检查和发布，并非新增查询额度。

总在途上限继续为公共 `worker_threads`，不变成“线程数加进程数”。进程资源租约、请求捕获、预算、连接和版本 pin 保留到真实执行与父端处理结束；cell 已空闲后才发送完成通知，future 在捕获/预约清理后就绪。异步资源重新分类只允许在原有副作用之前，沿用同一请求、接纳名额和 future。shutdown drain 全部已接纳任务。

如果公共额度为 4 且 4 个 Native 查询已经执行，Arrow 等待是总容量限制。混合负载需要给其他格式保留机会时，可用 2 个 Native 子进程、4 个公共额度。进程池不提供优先级、预占或无饥饿保证。

## 版本、预算和错误

首版通过父端 RPC 代理原 `NativeFileHandle::validate/read`，保留每次受管读取的 owner guard、文件与目录项检查、范围检查及读取前后校验。不会把路径或裸 fd 当作授权能力。子进程 VFD 的逻辑累计范围/字节预留仍由父端原请求账本处理；不因进程隔离获得另一份预算。代价是原生文件内容经过 IPC，尚未采用跨进程共享 payload 或零复制。

Native 工作缓冲、执行 capsule 副本、编码容量及读回传输缓冲通过父端 `DecodedFragmentCache::reserve_external_temporary` 计入同一个 runtime 内存账本，并与现有 fragment 内存竞争。Native cursor 在子进程交付结果前销毁，因此编码/传输预留按阶段峰值 `max(native_bound + 2 × encoded_cap, 3 × encoded_cap) + 2 × read_bound` 计算，另加执行 capsule 副本预留，避免将互不重叠的缓冲相加。该非阻塞预留只能回收可回收缓存，不能回收仍被使用的数据；不足明确返回 `ResourceExhausted`。它保留既有 HDF5 过滤器 scratch 只有逻辑上限的限制，不是完整 RSS 硬限制。进程独立的库/运行时基础 RSS 由服务部署资源限制覆盖。较大的 capsule/读取/输出上限仍可能使多个查询超出共享预算，不能将服务预算乘给每个子进程。固定实验两侧使用同一 2 GiB runtime 预算及同一 2 GiB 查询 cgroup。

私有执行 capsule 有 64 MiB 上限，封送前估计并预留容量；超限明确失败。协议仅限同主机相同构建的私有继承 socketpair，不是公开网络/持久格式。结果、状态及 profile 帧受各自上限和请求代次校验。子进程不继承 owner 锁、控制 listener 等无关描述符。

子进程执行使用原 Native kernel，并保留 source、operation、phase、retriable 等错误归属。当前查询发生 helper/传输故障时失败，不自动重放；下一独立查询可以重新启动 helper。取消经 IPC 传播，在原 cursor 的合作检查点观察；已经完成的成功结果不会被父端迟到取消覆盖。迟到取消不污染下一代查询。终态收到且缓冲/执行清理完成前不归还进程、预算或版本资源。

## 观测

服务 profile 报告 `server_hdf5_execution.native_processes`、`hdf5.process.jobs`、子进程 PID、generation、`child_begin_ns`/`child_end_ns` 和 round-trip，并汇总子进程的原 kernel/IPC 计时。子进程区间使用同主机 `CLOCK_MONOTONIC`，包含 native setup、读取、父端 RPC 等待和编码；重叠证明独立子进程查询同时在途，不等同于 H5Dread CPU 时间同时运行。保留外层父端和客户端完整时钟，不能将嵌套计时相加推导总延迟。
