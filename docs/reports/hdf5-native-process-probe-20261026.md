# Native HDF5 有界进程池实现与探针（2026-10-26）

已实现完整独立 Native HDF5 值查询的有界子进程后端，保留一个公共等待队列与统一在途额度。真实 1/2/4 子进程执行重叠已验证，版本、完整输出、预算、取消、错误与资源回收测试通过。但本次小文件首次读取探针没有提速：相对原进程内路径，4 进程整批耗时增加 **23.09%**、平均查询延迟增加 **23.82%**、吞吐下降 **18.76%**。该功能保持默认关闭。

## 实现与范围

父进程保留原来源选择、授权、owner execution lease、prepare 和结果发布。一个子进程顺序执行一个完整冻结计划的全部 slab、通用过滤／投影／行数限制及 Arrow IPC 编码；不重新 prepare、不拆分 chunk/slab、不增加客户端查询 worker，也不接入 IoTDB 查询线程池。现有 HDF5 物理值查询语法要求一个 Exact 路径和 Exact 版本。空集、其他格式、混合格式及 `typed_batch` 继续原路径，保留批内累计预算和回滚。

公共调度器新增异步启动／完成及资源重分类：释放实际启动线程时，继续保留原逻辑在途额度、请求捕获和资源租约；最终清理后才兑现 future。子进程池每个 cell 只有一个已准入任务交接位及固定父端协调线程，没有第二个等待队列。资源未就绪的任务留在公共队列，其他格式可越过它们。4 个已运行 Native 查询会合法占满 4 个公共额度；混合场景采用 Native 子额度 2、公共额度 4。

首版原生文件读取通过父端 RPC 调用原 `NativeFileHandle::validate/read`，保留每次读取的 owner guard、文件／目录项、范围、读取前后校验和父端逻辑预算。子进程不继承 owner 锁、数据库连接或控制 listener。工作缓冲、封送／传输副本和编码容量计入同一 runtime 内存账本；不足返回原类 `ResourceExhausted`。helper/传输故障不自动重放当前查询，后续独立查询可以重启 helper。取消使用代次隔离的合作协议，迟到取消不覆盖已完成成功结果，也不污染下一请求。详见[执行契约](../contracts/hdf5-native-process-pool.md)。

启用示例（owner 和受管文件根沿用现有配置）：

```properties
query.hdf5.mode=native-hdf5
query.hdf5.native_cache=false
query.hdf5.native_processes=4
query.tasks.worker_threads=4
query.cache.memory_budget_bytes=2147483648
```

默认 `native_processes=0`；有效进程数 1–16 且不得超过公共额度。首版明确拒绝与 native session cache 同时启用。helper `iotdbfs_native_query_worker` 随 daemon 构建／安装。该共享预算不是每个进程各得 2 GiB，也不等同于完整进程 RSS 硬限制。

## 构建和正确性

Linux GCC 13 Release 使用与冻结基线相同的 SDK、Arrow 21.0.0、线程安全串行 HDF5 1.14.4、编译／链接参数及运行时库字节。5 个目标构建通过：daemon、web、native helper、混合探针和 unit_tests。

- 最终定向测试 **440/440**、全部单元测试 **2091/2091** 通过，零失败、禁用或跳过。
- 启动配置 **16 + 41 + 16 = 73** 项通过，不连接数据库或挂载。
- 通用异步调度器 **48/48** 本地测试及其 ASan/UBSan 通过；这不是整个进程后端的 sanitizer 声明。
- 7 个 Native 进程测试覆盖真实受管文件和三种 dataset 类型、多批次完整 IPC／schema／mask／来源、2/4 个 helper 的 Open RPC 屏障及独立 PID、资源等待者绕行、取消／撤销、详细错误、payload／行／编码／共享内存预算、协调线程 `bad_alloc` 后恢复，以及 helper 死亡时当前查询不重试、下一查询重启。

基线 daemon SHA-256 为 `98f8501491a71bfb83e16ccaad7c2f211ce49591831ff6a46d37e841180da044`；最终候选为 `de6aae71090d36822d9d1da0e9474dbafb53aaec449c9cc505c038b30e996127`。最终 helper 为 `b0c97d0d10c3fb0ce0a1075242342556ba27e18c4a953b200e34424e625f49fc`。相对基线只有 23 个生产构建输入变化，未混入其他 IoTDB／Owner 优化。初版构建和未使用的中间冻结稿均保留；任何性能查询开始前已完成最终候选的全部测试。

## 主实验：一个客户端查询四个不同 dataset

复用已封印 N128/V1 corpus 中一个 **63,760 B** HDF5 文件的 `/data/u0000`–`/data/u0003`，每个 dataset 形状为 `1×1×64×64`、4096 个 Float32。每查询 owned 值与 mask 共 20,480 B，四查询共 81,920 B。精确版本 `1792569496136`；文件 SHA-256 `ffea25c3f287d5d80b552fac48500fd5c80d49561b0799739beea846aabe6c46`。这是 **N128 corpus 的 C1/K1 诊断**，不是第 4.3 节原 C128/K2 整组查询。

单客户端使用一个 asyncio 事件循环发出四个不同内容请求，没有独立客户端查询 worker。每查询保留普通 exact-version catalog、typed、异步结果 fetch 和 owned 数组交付。T 从实际进入查询到值／mask 完全 owned；D 为发现阶段，Q 为其余完整链路，`T=D+Q`。整批时间从最早进入到最后 owned，吞吐为总查询数除以整批时间之和。oracle／hash、profile 收集、资源核查及启动均在钟外。

公共额度／Owner reader／sender 为 4/4/1。CPU 2、3 上共用 2 核 quota，覆盖 DataNode、FS、helper 和客户端；FS／helper／客户端 query cgroup 为 2 GiB，两侧 runtime 预算同为 2 GiB，native cache 均关闭。DataNode JVM 保留原 15232 MiB heap／2176 MiB direct 配置。每次挂载新 daemon 和原生会话，OS／DataNode／磁盘缓存未清；不能称作冷盘。

预先冻结的资格序列 P0/P1/P2/P4 各 1 波、共 16 查询，全部排除性能统计。其整批单次观测为 73.822/33.543/31.399/30.010 ms；P0 首波较慢，不能拿它与后续 P4 计算收益。资格验证观测到子进程执行重叠峰值 **1/2/4**。P1/P2 仅有一次资格波，不作统计性能结论。

性能为两轮 ABBA（P0/P4/P4/P0），每侧 4 波、16 查询，全部保留：

| 指标 | P0 原进程内路径 | P4 有界进程池 | 变化 |
|---|---:|---:|---:|
| 整批均值 ms | 26.189 | 32.235 | +23.09% |
| 查询 T 均值 ms | 25.038 | 31.002 | +23.82% |
| D 均值 ms | 4.668 | 5.188 | — |
| Q 均值 ms | 20.370 | 25.814 | — |
| 吞吐 QPS | 152.736 | 124.088 | −18.76% |

P0 原始整批 ms：`25.001693, 26.313508, 27.444445, 25.996041`；P4：`31.937759, 37.148394, 30.474652, 29.380169`。两轮退化分别为 **34.63%／12.00%**，没有删除慢样本。所有 P4 性能波均有四个不同 PID、执行区间峰值 4，实际 cgroup／线程亲和性检查通过。

## 为什么有并行但没有提速

以下为每个 typed 查询的均值，均是包含关系或可重叠阶段，**不得相加作为总时间**：

| 观测 ms | P0 | P4 |
|---|---:|---:|
| 公共任务排队 | 11.768 | 1.528 |
| typed 服务执行区间 | 4.590 | 17.898 |
| query_materialize | 3.948 | 16.990 |
| native.setup | 1.460 | 5.607 |
| native.read | 0.545 | 2.977 |
| typed_ipc_encode | 0.529 | 0.808 |
| 客户端 fetch | 0.872 | 2.522 |

P4 process round-trip 均值 **15.170 ms**；child 完整执行墙钟均值 **14.383 ms**、进程 CPU 均值 **5.925 ms**。子进程执行区间包含受管文件 RPC 等待、调度和编码，峰值 4 表示独立查询同时在途，不能解释为 4 核 CPU 同时使用，也不能从 CPU／墙钟比断言整个机器空闲。

排队下降是真实的，但每查询 materialize 增加约 **13.04 ms**，抵消了约 **10.24 ms** 的排队下降。原 native 值读取本身平均只有 0.545 ms；两侧均是每查询 **18 次、平均 19,114 B** 原生读取，物理需求没有减少。首版新增了逐读取预算／文件能力的控制往返和文件内容 IPC，fresh helper 还各自承担库／编码初次使用成本。观测支持“进程／IPC 路径增加了服务时间”，不能精确把各毫秒归因于单个 RPC、初始化或 CPU 争用，当前未做这几项的独立消融。

## HDF5／Arrow 混合辅助验证

使用生产 runtime／公共调度器／控制 socket／NativeFileStore／Arrow adapter 的 C++ 夹具，仍由一个客户端线程多路发送，不增加查询线程。每波两个不同 HDF5 dataset 查询和两个相同 Arrow 内容查询。P0/P2 均使用**候选构建**，区别仅为 0／2 个 Native 子进程；不是旧基线 98 的跨二进制对照。

该夹具没有 IoTDB 或 FUSE；控制面走 socket，结果经 `bridge.acquire_exact` 和 immutable handle 读取后复制为 owned 数组，交付路径与主实验的 result-fetch 不同。每个 runtime 连续 2 波，第二波保留 Arrow 缓存状态；字段 `warm=false` 仅表示无显式 warmup，不意味着第二波首次读取。8 cells、16 波、64 查询，两个 ABBA 周期全部保留。

| 指标 | 候选 P0 | 候选 P2 | 变化 |
|---|---:|---:|---:|
| 整批均值 ms | 4.859 | 8.853 | +82.17% |
| 吞吐 QPS | 823.148 | 451.846 | −45.11% |
| HDF5 owned 延迟均值 ms | 3.936 | 8.193 | +108.18% |
| HDF5 延迟中位数 ms | 3.871 | 6.407 | +65.51% |
| Arrow owned 延迟均值 ms | 3.998 | 3.392 | −15.17% |
| Arrow 延迟中位数 ms | 3.661 | 3.409 | −6.87% |

所有 P2 混合波的 child 峰值为 2，**全部 16 个 Arrow 结果都在至少一个 Native child 执行区间尚未结束时完成 owned 交付**。因此资源等待没有阻止 Arrow 推进，但混合整体吞吐仍退化。该夹具不能代替真实 IoTDB/FUSE 的混合负载性能结论。

## 恢复、证据和后续边界

48 条主查询和 64 条辅助查询均通过完整内容校验，零失败、零科学查询重跑。原 corpus、128 份 raw／FS／native 文件、index、12 组 head 和 private journal 一致，没有新发布语料、重建索引或补绑定。1024 个资源样本未出现 OOM／OOM kill；query cgroup 整窗口 `memory.peak` 为 68,849,664 B，这是 cgroup 计费峰值，不是进程 RSS 或逐波峰值。

控制器恢复后另做一次只读 live 检查：实际 DataNode PID 499005，224 个线程恢复 CPU 0–31，实际 `ASYNC/true`，原配置字节、JVM、cgroup、root controllers 和三个受保护文件一致；12 个 FUSE 挂载及记录到的 51 个 daemon/helper 身份全部失活，临时 cgroup 不存在。检查仅执行一次 `SHOW ALL CONFIGURATION`，未追加科学查询或服务变更。

本轮已完成实现和实验，不将“可真实并行”当作提速结论。对这个小查询场景应保留默认关闭。后续优化优先测清并减少预算／读取／校验控制往返，并将纯库／编码运行时初始化放在 helper 启动阶段；这些方向尚未实现或验证，不应声称可以抵消全部退化。若优化文件描述符交付，仍需保留原每次读取的 owner guard 及受管文件检查，不能仅传裸 fd 后删掉这些校验。

证据位于 `experiments/cross_version/work/native-process-20261026/`：

- [主统计](../../experiments/cross_version/work/native-process-20261026/evidence/collection-r1/analysis.json)、[全部主 rows](../../experiments/cross_version/work/native-process-20261026/evidence/collection-r1/queries.jsonl)、[waves](../../experiments/cross_version/work/native-process-20261026/evidence/collection-r1/waves.jsonl)。
- [最终构建](../../experiments/cross_version/work/native-process-20261026/evidence/products/NEW/build-report.json)、[2091 单元测试](../../experiments/cross_version/work/native-process-20261026/evidence/products/NEW/unit-tests.xml)。
- [独立统计](../../experiments/cross_version/work/native-process-20261026/evidence/collection-r1/independent.json)、[独立复核 r2](../../experiments/cross_version/work/native-process-20261026/independent-review-r2.json)、[实际恢复](../../experiments/cross_version/work/native-process-20261026/evidence/collection-r1/live-restoration-audit.json)、[本地包核查](../../experiments/cross_version/work/native-process-20261026/local-evidence-audit.json)。

可移植证据包 33,849,257 B，800 个产物逐项字节数／hash 校验通过；SHA-256 `5fc80d17f1aa02944657d267e6aeff3716444b302df5d243ae1556fa8a3d035e`。首次导出缺少元数据文件的失败收据及不完整导出保留，补齐后只重新打包，未重跑科学实验。工作区原有改动保留，未执行 Git 写操作。
