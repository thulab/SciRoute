# Native HDF5 进程回退与读取优化（2026-10-26）

已撤回 Native 多进程后端，并在原进程内 `H5Dread` 路径实现 chunk 对齐和物理数组转移。约 6.3 MB 样本的首次整批耗时下降 **17.20%**，后续读取下降 **18.65%**、有效吞吐增加 **22.92%**；每查询受管读取 **816→600 次**，读取字节下降 **26.71%**。小样本后续整批耗时反而增加 **7.90%**，本轮不能主张普遍提速。

**160 条查询／40 波／8 次挂载**全部通过完整值、mask、精确版本及实际模式校验，独立统计和资源／服务恢复核查通过。保留此前公共资源感知调度、定向唤醒、owner reader 和 GRIB 优化；不改变 Indexed 路径，不新增客户端 worker、查询内并行或 HDF5 解码线程。

## 回退范围

以冻结 daemon `98f8501491a71bfb83e16ccaad7c2f211ce49591831ff6a46d37e841180da044` 的源码为基准，25 个受进程后端影响的既有文件先精确恢复；7 个专有头／源／测试移到忽略的实验归档。helper 构建和打包、私有 RPC、父端代理能力、执行 capsule、进程内存预留与公共调度器专用异步接点均撤回。其他已存在的工作区优化保留，未使用 Git reset 或提交操作。

旧 `query.hdf5.native_processes` 键（包括 0 或空值）在连接数据库及挂载前明确拒绝，要求删除配置；不静默退回另一条执行路径。历史实现、测试、失败与性能证据保留，当前[模式契约](../contracts/hdf5-query-modes.md)及[历史进程契约](../contracts/hdf5-native-process-pool.md)已标记撤回。

归档和逐文件 hash 见 `experiments/cross_version/work/native-process-rollback-20261026/rollback-static-audit.json`。回退后新增的拒绝旧配置检查，以及下述 Native 读取优化，属于本轮后续改动，不要求这些文件继续与旧基线逐字相同。

## 两项读取优化

Native chunked dataset 的 slab 切分在原 `target_batch_rows`、`target_batch_bytes` 和 Native 输出工作集上限内向下对齐 chunk；非对齐 ROI 按绝对 chunk 边界截短。如果边界不能在上限内容纳，就保留原受限切分。不扩大请求区域，不改变 C 顺序、schema、domain 或来源；单批边界及 Arrow IPC 分批数允许变化。连续布局和 Indexed 使用原切分。

例如 S2 的 768×768 dataset、32×32 chunk，在 65,536 行上限下原来每 slab 85 行、共 10 个 slab，累计与 792 个 chunk 区域相交；新方案每 slab 64 行、共 12 个 slab，目标为 576 次相交，对应原 576 个唯一 chunk。这是读取计划的变化，实际物理调用／字节和完整耗时另由探针验证。

物理视图直接转移 `H5Dread` 返回的 owned Float32／Float64／Int32 数组，取消重复预分配和复制；Int16 已由 `H5T_NATIVE_INT32` 扩展，不再进行 pack16→expand32。返回后仍检查取消、完整类型、行数、ROI、descriptor 和 validity。科学值视图保留原 packed 拷贝、缺失值掩码及 CF 转换，未为减少复制改变数值语义。

受管文件准入、读取前后 guard／文件身份验证、实际 VFD 累计范围／字节预算、结果行数和编码上限、sticky failure 与资源生命周期保留。不新增数据缓存，不依赖放宽内存或请求预算制造收益。Native 会话缓存开关维持原默认 false；其选择身份包含对齐参数，以区分 slab 工作流。

新增 profile：`chunk_aligned.cursors`、`selected_chunk.visits`、`output.moved.bytes`、`output.copied.bytes`，均在 `hdf5.native.` 域下。chunk visits 是 SourceDriver 预检的候选相交次数，不等于实际解压次数；VFD 物理需求须使用 `native.source.calls/bytes` 和 `native.filesystem.calls/bytes`。复制计数按实际 packed 宽度 2／4／8 B，不能用科学结果 Float64 宽度替代。

## 并行边界

当前链接 HDF5 1.14.4 线程安全串行构建，SourceDriver 及库 API 仍受进程级锁约束；把 dataset/node 句柄传给多个线程不会自动带来并发 `H5Dread`。HDF5 官方的[线程安全机制说明](https://portal.hdfgroup.org/documentation/hdf5/latest/thread-safe-lib.html)区分了线程安全与 API 并发。独占的已读取数组可以交给锁外代码处理，但当前 Native 公共任务仍使用原完整任务资源准入，本轮没有实现读取／处理流水线，也不声称新增了查询执行重叠。

[`H5Dread_multi`](https://portal.hdfgroup.org/documentation/hdf5/latest/group___h5_d.html) 可为同一文件上的多 dataset 操作提供合并 API，但本轮保留四份独立查询的版本能力、预算、取消与错误边界，未合并成一个跨请求读取。直接读原始压缩 chunk 后使用自建过滤器解码属于另一种执行路径，不混入本轮 Native 对照。

## 验证与实验方法

代码独立审查未发现阻塞问题。审查的 Python 算术参考枚举通过 10,000 个随机 rank／ROI／cap／chunk 案例及 168 个高位坐标案例；这不替代编译后 C++ 和真实 HDF5 测试，部分极端坐标案例仅用于算术压力检查。

新增 3 项 slab 模型测试及 2 项真实 Native adapter 测试，强化既有 Int16／Float32／Float64、大小端、过滤器、物理／科学输出比对。覆盖 ROI 首段、跨 pivot C 顺序、cap 小于 chunk、完整值和来源、取消／累计预算失败及失败后已交付数组的独立所有权。另有旧配置拒绝检查。

Linux GCC 13 Release 使用与原基线相同的 SDK、Arrow 21 和 HDF5 1.14.4。daemon／web／unit_tests 构建通过；**442 项定向测试、2078 项完整单元测试**全部通过，无失败、禁用或跳过；旧多进程配置拒绝、查询池和结果发送配置分别 **16／41／16** 例通过，均不连接数据库或挂载。

新 daemon SHA-256 为 `64672d18371ce3c734e836d53f3a96dde00cd976b9e0ce6c23e998713a715357`。产品仅在原 98 基线的 1034 文件源码闭包上覆盖 8 个明确文件，其中生产输入 5 个，其余为测试。冻结输入 SHA-256 `e72e513308758f49c93de109e4c93c89e6a36d2f1a5ea2d608eb79c416112d4e`。独立代码 review 的 6 个 Native 实现／测试 hash 与冻结文件一致，其他工作区变更未加入候选。只读 prepare／verify 校验 192 个实验输入，未修改服务或资源，然后才启动性能窗口。

性能使用两个既有封印文件：S0 为 N128 corpus 的 63,760 B 文件、每 dataset 4096 个 Float32；S2 为 N8 corpus 的 6,338,165 B 文件、每 dataset 589,824 个 Float32。每个文件查询 `/data/u0000`–`/data/u0003` 四个不同 dataset，仍是单文件 C1 诊断，不替代第 4.3 节多文件 case，目录 N 也不同，不能称纯文件大小消融。

每种大小固定 ABBA 四次全新挂载，A 为原 98 基线，B 为冻结的新进程内实现；每次挂载首次 4 条查询，随后 4 波共 16 条后续查询。两种大小总计 8 次挂载、160 条查询／40 波，每个 arm／大小有两次 daemon 挂载。首波与后续读取分别统计，所有慢／失败结果保留，不按结果挑选重跑。

一个 asyncio 客户端事件循环维持每波四个独立请求。T 从普通查询入口至值／mask 完全 owned，`T=D+Q`；波时钟从最早入口至最后 owned。oracle、profile、资源采样和写证据在波钟外，sequence QPS 另含这些间隙。OS／DataNode 缓存不清空，首次新 daemon 不等于冷盘。

资源与前轮一致：DataNode＋FS＋客户端共用 CPU 2、3 和总两核 quota；query cgroup 和 runtime 均为 2 GiB，公共 worker／owner reader／cache worker／sender 为 4／4／4／1，`read_parallelism=1/native_cache=false`，原 DataNode JVM 配置保留。语料、index、当前 head／journal、native 文件绑定复用，不新发布、不重建索引、不补绑定。

## 性能结果

每个 arm／大小的首次段有 2 波／8 查询，后续段有 8 波／32 查询。所有波均纳入预定分段，无删除慢样本。下表单位 ms。

| 样本 | 组 | 首次整波均值 | 后续整波均值 | 后续查询 T 均值 | 后续查询 T p95 | 后续有效 QPS | 后续 sequence QPS |
|---|---|---:|---:|---:|---:|---:|---:|
| S0 63,760 B | A 原路径 | 51.201 | 11.014 | 8.600 | 11.349 | 363.180 | 63.201 |
| S0 63,760 B | B 优化 | 26.986 | 11.884 | 9.362 | 13.634 | 336.574 | 65.070 |
| S2 6,338,165 B | A 原路径 | 253.532 | 244.848 | 159.792 | 250.595 | 16.337 | 11.138 |
| S2 6,338,165 B | B 优化 | 209.937 | 199.189 | 130.996 | 200.450 | 20.081 | 13.240 |

S2 首次整批 −17.20%、T −15.31%；后续整批 −18.65%、T −18.02%、有效 QPS +22.92%、包含核验间隙的 sequence QPS +18.87%。两次 A 挂载的后续波均值分别为 **244.766／244.931 ms**，两次 B 为 **201.218／197.160 ms**，两个 B 挂载均有改善。后续单波原始范围 A 为 240.087–254.401 ms，B 为 195.165–212.436 ms；样本仍很少，不作长期稳定性或显著性主张。

S0 后续整批 **+7.90%**、T **+8.85%**、有效 QPS **−7.33%**。两次 A 挂载的后续波均值为 **11.267／10.761 ms**，两次 B 为 **10.291／13.478 ms**；B 的第二次挂载含 13.971 和 17.002 ms 慢波，全部保留。读取次数／字节在两组相同；当前没有独立消融，不能把小样本差异精确归因于移动逻辑、计时计数、数据库状态或调度噪声，也不能将其忽略为零退化。S0 每查询只减少 16 KiB 中间数组转移的复制工作，本轮未显示稳定端到端收益。

S0 首次波原始值 A 为 **74.844819／27.557166 ms**，B 为 **26.456441／27.515312 ms**。首个 A 明显较慢，原因未隔离；不能用其导致的首次均值下降 47.29% 宣称小样本 Native 稳定提速。S2 首次 A 为 **252.652711／254.412165 ms**，B 为 **212.097012／207.776984 ms**。这些是新 daemon 首次查询，文件页缓存此前可能已被校验和其他实验访问。

## 读取量、复制与耗时的对应关系

| 每查询观测 | S0 A | S0 B | S2 A | S2 B |
|---|---:|---:|---:|---:|
| Native slab 数 | 1 | 1 | 10 | 12 |
| VFD 实际读取调用 | 18 | 18 | 816 | 600 |
| VFD 实际读取字节均值 | 19,114 | 19,114 | 2,166,373.75 | 1,587,715.25 |
| 新增 chunk 预检 visits 计数 | 未记录 | 4 | 未记录 | 576 |
| owned 物理值转移字节 | 未记录 | 16,384 | 未记录 | 2,359,296 |

全部 B 查询满足 chunk-aligned cursor 1、预检 visits 4／576、slabs 1／12、moved bytes 为值元素数×4。SourceDriver 仍走 filesystem VFD，数据库 payload 调用为 0，进程树前后只有 daemon，无 helper。预检 visits 不等于解压次数；原 A 的 792 次 chunk 相交来自旧 85 行 slab 几何推导，新 B 的 576 为实际预检计数。

S2 受管读取调用减少 **26.47%**、字节减少 **26.71%**，同时所有值／mask 和来源完全一致，支持对齐消除了跨 slab 的重复供数。VFD 读取可由 OS 页缓存供给，这些计数不是磁盘块 I/O。IPC 分批增加为 12，说明收益不依赖放大单批预算。旧构建未提供新增移动／复制计数；候选的物理路径只记录移动，复制分支未进入，原始缺失字段保留为 null，不能把旧构建未记录的复制量视作零。

以下为后续段每查询均值，单位 ms；计时包含或重叠，不应相加：

| 观测 | S0 A | S0 B | S2 A | S2 B |
|---|---:|---:|---:|---:|
| D 发现阶段 | 2.532 | 2.750 | 2.948 | 3.043 |
| Q 其余完整交付 | 6.068 | 6.611 | 156.844 | 127.953 |
| 公共任务排队 | 2.225 | 2.426 | 87.695 | 70.615 |
| server_total | 1.652 | 1.866 | 57.842 | 46.376 |
| query_materialize | 1.514 | 1.711 | 57.556 | 46.144 |
| native.setup | 0.307 | 0.315 | 0.375 | 0.319 |
| native.read | 0.368 | 0.420 | 43.047 | 32.316 |
| native.filesystem.read | 0.349 | 0.394 | 14.475 | 10.819 |
| typed_ipc_encode | 0.119 | 0.168 | 7.296 | 6.948 |

S2 原生读取阶段减少约 **10.73 ms／查询**，materialize 减少约 **11.41 ms／查询**；串行资源服务时间下降也降低后续请求排队。原 Native 锁和资源准入仍在，全部 40 波 typed 实际执行峰值均为 1，profile lock_wait 很小不能说明资源串行瓶颈消失，等待已主要体现在公共队列。此次改善是减少串行工作量，没有新增 `H5Dread` 并行度。

对齐与数组转移同时改变，本轮没有分别消融；不能将完整耗时改善全归因于减少复制。大文件物理读取需求下降提供了明确机制证据，小文件物理需求不变且结果波动，因此现阶段支持“大规模、会跨 chunk 边界切批的读取值得采用”，不支持“所有 Native 查询均提速”。

## 恢复、证据与边界

科学窗口完成后原 DataNode PID **718189**，293 个线程恢复 CPU 0–31，原 cgroup、JVM、配置字节、受保护文件及实际 `ASYNC/true` 核验通过。8 个 FUSE 挂载与 8 个记录到的 daemon 身份失活，临时 cgroup 消失。独立 live 核查仅执行一次 `SHOW ALL CONFIGURATION`，没有追加科学查询、服务重启、配置写入或资源变更。

462 个周期资源样本保留，257 个覆盖查询区间。统计中的逐 wave `resource_before` 为挂载开始时快照，不能解释为每波局部资源差分；周期资源证据用于核验配置和包络，包含钟外审计开销，不是查询独占 CPU／I/O 计量。

证据位于 `experiments/cross_version/work/native-read-20261026/`：

- [正式统计](../../experiments/cross_version/work/native-read-20261026/evidence/collection-r1/analysis.json)、[全部查询](../../experiments/cross_version/work/native-read-20261026/evidence/collection-r1/queries.jsonl)、[全部波](../../experiments/cross_version/work/native-read-20261026/evidence/collection-r1/waves.jsonl)。
- [构建及测试](../../experiments/cross_version/work/native-read-20261026/evidence/products/NEW/build-report.json)、[2078 项单元测试 XML](../../experiments/cross_version/work/native-read-20261026/evidence/products/NEW/unit-tests.xml)、[冻结输入](../../experiments/cross_version/work/native-read-20261026/implementation-inputs.json)。
- [独立统计](../../experiments/cross_version/work/native-read-20261026/evidence/collection-r1/independent.json)、[实际恢复](../../experiments/cross_version/work/native-read-20261026/evidence/collection-r1/live-restoration-audit.json)、[本地证据包核查](../../experiments/cross_version/work/native-read-20261026/local-evidence-audit.json)。
- [回退核查](../../experiments/cross_version/work/native-process-rollback-20261026/rollback-static-audit.json)、[独立代码审查](../../experiments/cross_version/work/native-process-rollback-20261026/review-native-read.json)、[独立结果审查](../../experiments/cross_version/work/native-process-rollback-20261026/review-native-read-results.json)。结果审查另外只读核对 408 份原 raw／FS／native 文件字节摘要，head 与原／私有 journal 均匹配。

证据包 **169,514,429 B／733 个产物**逐项字节数与 SHA-256 通过，包 SHA-256 `1003a92563f29b4c888c45896b04d81a266bf7e35256c436f4c1f630f6c7bc8c`；本地标准库独立重算与远端完全一致。首次与后续全部保留，无失败请求重试或性能挑选重跑。未修改或重新发布任何科学数据，未做 Git 写操作。下一步若推进读出对象后的多线程处理，仍需把 HDF5 资源租约和锁外处理阶段明确分开；本轮不把这种尚未实现的流水线计入收益。
