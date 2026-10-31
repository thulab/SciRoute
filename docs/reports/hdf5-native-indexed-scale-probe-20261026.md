# HDF5 大小规模、进程复用与 Indexed 查询间并行探针（2026-10-26）

扩大至约 6.3 MB 文件并连续复用 helper 后，当前 Native 四进程仍未提速：256 查询段的整波耗时相对原进程内路径增加 **25.38%**。`iotdb-indexed` 四路已验证真实查询执行重叠，相对 Indexed 一路，该段整波耗时下降 **30.85%**、有效吞吐增加 **44.62%**。但 Indexed 大样本首次读取仍明显慢于 Native，后续缓存收益必须单独解释。

本轮 `collection-r2` 的 **2592 条查询／648 波／8 次挂载**全部通过完整值、mask、精确版本及资源核查，独立复算和服务恢复检查通过。实验复用既有封印数据和原二进制，不改变生产实现，不新增客户端 worker，不引入查询内并行。

## 比较对象与计时

| 组 | 引擎 | 公共查询 worker | 最大派发批大小 | Native helper |
|---|---|---:|---:|---:|
| N0 | native-hdf5，原进程内路径 | 4 | 4 | 0 |
| N4 | native-hdf5，有界进程池 | 4 | 4 | 4 |
| I1 | iotdb-indexed | 1 | 1 | 0 |
| I4 | iotdb-indexed | 4 | 4 | 0 |

N0 为原基线 `98f8501491a71bfb83e16ccaad7c2f211ce49591831ff6a46d37e841180da044`；其余为已通过 2091 项单元测试的同一候选 `de6aae71090d36822d9d1da0e9474dbafb53aaec449c9cc505c038b30e996127`。本次不重新编译。`iotdb-indexed` 是现有正式模式，不是新引擎或隐式回退 Native 的别名。

所有组均由一个 asyncio 客户端事件循环，在同一文件上发出四个不同 dataset 的完整独立查询。每波四个请求完成 owned Float32 值和 mask 后，再在钟外核对全部内容、精确版本及 profile，然后进入下一波。每次新挂载依次运行 4／64／256 条查询段，共 324 条；八次挂载共 2592 条。第一段只有一个首次波，后两段复用同一 daemon／helper。S0 顺序 N0/N4/I1/I4，S2 顺序 I4/I1/N4/N0。

T 从进入普通查询到客户端完整 owned 输出，包含 catalog、typed、结果 fetch 和数组交付；`T=D+Q`。整波时间为最早查询进入至最后结果 owned。有效 QPS 为查询数除以整波时间之和；另报包含 oracle、profile、资源采样和写证据间隙的 sequence QPS，不能将前者当作无间隙持续负载吞吐。启动、挂载和 helper spawn/handshake 在查询钟外。OS、DataNode 和磁盘缓存未清空，首次波不是冷盘实验。

两条 CPU 核 2、3 和总两核 quota 覆盖 DataNode、FS、helper 和客户端；FS/helper/client query cgroup 上限 2 GiB，runtime 共享预算同为 2 GiB。Owner reader 4，缓存 worker 4，sender 1，`query.hdf5.read_parallelism=1`，`native_cache=false`。四个 helper 同时在途不能解释为四个 CPU 核同时计算。

## 既有样本与成本假设

| 样本 | 来源 corpus | 文件字节 | 每个 dataset 形状 | 每个 dataset 唯一 chunk | 每查询 owned 字节 |
|---|---|---:|---|---:|---:|
| S0 | FM1-hdf5-N128-V1-S0-base | 63,760 | 1×1×64×64 | 4 | 20,480 |
| S2 | FM1-hdf5-N8-V1-S2-base | 6,338,165 | 1×1×768×768 | 576 | 2,949,120 |

每文件均选 `/data/u0000`–`/data/u0003` 四个不同内容 dataset，Float32、32×32 chunk、shuffle＋deflate。S0 精确版本 `1792569496136`，SHA-256 `ffea25c3f287d5d80b552fac48500fd5c80d49561b0799739beea846aabe6c46`；S2 精确版本 `1792567785061`，SHA-256 `c406a494dfacd7bdbc54950201eeed4a0774d1a364e35d112b6dde464309bd58`。

这是两个既有 corpus 上的单文件四 dataset 诊断；目录规模也从 N128 变为 N8，不是纯文件大小消融，也不替代第 4.3 节原多文件 case。原 raw/FS/native 文件、索引、manifest、发布收据、head 和 owner journal 全部保留。

进程池构造已在查询前完成 spawn 和握手；连续请求主要复用库首次使用状态，不会免除每个请求的执行上下文、文件和 dataset 打开。普通 VFD 读取增加 Reserve 和 Read 控制往返；父端 Read 仍执行原 guard、文件身份及读取前后检查。输出 IPC 随结果字节增长。因此更大查询能否摊薄成本必须实测。

默认每批最多 65,536 行；S2 的宽度 768 对应每个完整 slab 85 行，共 10 个 slab，与 32 行 chunk 边界不对齐。理论上各 slab 共与 792 个 chunk 区域相交，唯一 chunk 只有 576 个；Native raw chunk cache 关闭可能导致重复读取和解压。这个推导不是逐 chunk 物理读取的测量结果。

Indexed 通过既有 layout/chunk 索引和有界自有解码器执行，不经过 Native `H5Dread` 全局锁；仍受 owner reader、数据库 payload、共享预算和 CPU 资源限制。重复查询可能命中 decoded cache，Native 会话缓存则关闭。I4/I1 用于观察同一 Indexed 路径的查询间并行，I4/N0 或 I4/N4 是完整路线对比，不能把全部差异归因于并行。

## 首次启动失败与修正

`collection-r1` 的 S0 N0/N4 共 648 条成功查询保留，均不拼入正式统计。I1 在任何科学查询开始前因实验 launcher 配置 `worker_threads=1/max_batch_size=4` 退出；服务明确要求 batch 不超过 worker。修复仅为 I1 使用 batch 1，并增加预检非法配置拒绝及每条 profile 的实际 batch 验证。原失败日志、冻结输入和恢复证据保留；退出码未被旧挂载 harness 持久化，不补写推测值。

原服务恢复后，`preflight-r3` 重新冻结 190 个输入并通过 prepare/verify，随后以 `collection-r2` 独立执行完整矩阵。顺序、查询内容、规模和统计规则不根据 r1 的性能选择。

## 全部规模段的整波结果

下表单位为 ms；4 条段是一波首次读取，64／256 条段分别为同一挂载后续 16／64 波的均值，未删除慢样本。它们不是独立 daemon 重复实验。

| 样本 | 组 | 首次 4 条 | 后续 64 条 | 后续 256 条 |
|---|---|---:|---:|---:|
| S0 | N0 | 72.242 | 10.048 | 10.160 |
| S0 | N4 | 30.025 | 13.295 | 13.645 |
| S0 | I1 | 54.726 | 8.073 | 9.909 |
| S0 | I4 | 43.292 | 7.000 | 6.903 |
| S2 | N0 | 256.595 | 248.328 | 248.454 |
| S2 | N4 | 299.693 | 308.779 | 311.521 |
| S2 | I1 | 1340.645 | 98.108 | 100.034 |
| S2 | I4 | 741.956 | 69.066 | 69.173 |

S0 首次 N0 先运行且共享 OS／DataNode 状态未清空，不能用单个 72.242／30.025 ms 比值认定 Native 进程池冷启动提速。连续复用后，N4 在 S0 的 64／256 段仍慢 **32.31%／34.29%**，S2 仍慢 **24.34%／25.38%**。在此次范围内，增加查询次数与字节规模没有反转 Native 进程池的收益。

I4/I1 的首次波 S0 下降 20.89%、S2 下降 44.66%，仅为单次探针观察；不能据此给出独立重复统计或稳定冷读加速主张。S2 首次 I4 的 741.956 ms 仍为 Native N0 的 2.89 倍。该场景只读一次且需比较首次交付时，不能仅用 Indexed 稳态结果选择引擎。

## 延迟与吞吐

以下为后续 256 查询段，T 包含完整 owned 交付。p95 使用该段 256 条请求的经验分位数。有效 QPS 仅以波内 owned 时钟为分母，sequence QPS 还包含所有波间核验工作。

| 样本 | 组 | 整波均值 ms | 查询 T 均值 ms | 查询 T p95 ms | 有效 QPS | sequence QPS |
|---|---|---:|---:|---:|---:|---:|
| S0 | N0 | 10.160 | 7.770 | 10.559 | 393.687 | 68.840 |
| S0 | N4 | 13.645 | 12.545 | 14.188 | 293.155 | 63.678 |
| S0 | I1 | 9.909 | 9.147 | 12.456 | 403.668 | 68.924 |
| S0 | I4 | 6.903 | 5.912 | 7.195 | 579.417 | 85.440 |
| S2 | N0 | 248.454 | 160.946 | 250.932 | 16.100 | 10.990 |
| S2 | N4 | 311.521 | 281.495 | 322.480 | 12.840 | 9.384 |
| S2 | I1 | 100.034 | 94.817 | 105.461 | 39.986 | 19.339 |
| S2 | I4 | 69.173 | 61.152 | 72.358 | 57.826 | 22.718 |

S2 I4/I1：整波 −30.85%、T −35.51%、有效 QPS +44.62%；包含核验间隙的 sequence QPS 为 +17.47%。S0 相应为 −30.33%、−35.36%、+43.54%、+23.96%。S0 的 I1 在 64／256 段分别为 8.073／9.909 ms，说明同次挂载内也存在波动；全部样本保留，不挑较快段替换。

S2 I4/N0 的稳态整波下降 72.16%，有效 QPS 为 3.59 倍；其中既有引擎差异，也有缓存差异，不能称作公共查询池四路本身达到 3.59 倍加速。每波还有 oracle、profile、资源采样与 JSON 写盘间隙；此探针未测无间隙滚动并发的长期吞吐。

## 真实并行与缓存资格

仅用 `operation=typed` 的服务执行区间计算重叠，排除排队和 catalog。N0／I1 在两种大小下均 **81/81 波峰值为 1**；S2 I4 为 **81/81 波峰值为 4**。S0 I4 的峰值分布为 1／2／3／4 对应 4／19／33／25 波，小任务结束较快，并非每波都达到四路执行重叠。

两种大小的 N4 均 **81/81 波实际 child 峰值为 4**，共 162 波；每种大小的四个独立 helper PID 跨段保持不变，各 helper 代次依次为首次 1、64 段 2–17、256 段 18–81，没有在每条查询中重启进程。执行区间包括 RPC 等待、编码和调度；这不是解码 CPU 阶段四核并发的证明。

Indexed 首次查询均实际报告 `iotdb-indexed/database`、无 Native helper、每查询 decoder compile 1，S0／S2 各查询读取布局页 1／3 次。I1 的串行执行允许精确归属共享 runtime 增量：S0 每查询实际解码 4 个 chunk、payload read 1 次；S2 为 576 个 chunk、payload read 15 次。全部内容校验成功，证明本次已发布布局与 chunk 索引可用于所选完整查询，不只依赖原文件布局检查。

| 样本 | I1 和 I4 各自全挂载实际解码总数 | 首次后 hits | 64 段后累计 hits | 256 段后累计 hits |
|---|---:|---:|---:|---:|
| S0 | 16 | 0 | 256 | 1280 |
| S2 | 2304 | 0 | 36864 | 184320 |

64／256 两段没有新增 decoded fragment 解码或 payload 读取。I4 实际 payload read 峰值 4，I1 为 1。S2 末端缓存占用约 14.3 MB，全部解码块可保留。因此本轮 Indexed 稳态结果表示缓存复用下仍保留授权、版本、逻辑预算、组装及交付的查询性能，不能代表每条查询都从数据库重新读完整 payload。

并发 profile 的 `runtime_delta`／`iotdb_delta` 是共享前后快照，可能包含同波其他查询，不能相加当作唯一物理解码／SQL 总量。上表使用各挂载累计 runtime 计数；Native 读取使用逐请求 profile counter。

## Native 成本为何没有被分担

以下为 256 段每查询均值，单位 ms。多行计时互相包含或重叠，不应相加；N4 的 `typed_encode` 未记录，不将缺失解释为零。

| 观测 | S0 N0 | S0 N4 | S2 N0 | S2 N4 |
|---|---:|---:|---:|---:|
| 公共任务排队 | 2.357 | 0.533 | 88.880 | 0.574 |
| server_total | 1.611 | 7.993 | 59.038 | 262.971 |
| query_materialize | 1.460 | 7.799 | 58.794 | 262.610 |
| native.setup | 0.241 | 1.206 | 0.358 | 1.737 |
| native.read | 0.358 | 2.453 | 44.510 | 232.305 |
| native.source.read | 0.355 | 2.076 | 15.291 | 85.567 |
| process round-trip | — | 7.088 | — | 253.688 |
| child 墙钟 | — | 6.859 | — | 250.194 |
| child CPU | — | 1.639 | — | 75.322 |
| typed IPC encode | 0.116 | 0.135 | 7.024 | 12.087 |
| 客户端 fetch | 0.782 | 0.948 | 4.851 | 8.075 |
| 客户端 decode＋owned | 0.207 | 0.218 | 4.706 | 5.570 |

S2 排队减少约 88.31 ms，但 materialize 增加约 203.82 ms，完整 T 增加约 120.55 ms。主要抵消发生在执行与受管读取链路，不能归结为结果 fetch 或首次 helper 初始化。helper 已连续复用 81 个请求代次，服务时间仍维持高位。

两条 Native 路径物理需求完全相同：S0 每查询 18 次／平均 19,114 B，S2 每查询 **816 次／平均 2,166,373.75 B**。S2 实测每查询 10 个 slab，与前述 792 次 chunk 相交推导相符，且原 dataset 压缩 chunk 字节约 1.54 MB，支持存在重复供数的解释；但 816 次中混有元数据读取，不能将其全称为 816 个 chunk，当前没有逐 chunk trace 分解。

普通受管读取的 Reserve／Read 往返随调用数增加，不会因多跑几轮消失；两核总资源下四个查询又同时竞争父端协调、校验和 CPU。`native.source.read`／代理路径的 filesystem 计时包含通信等待，不能解释为磁盘 `pread` 独占时间变慢。当前证据支持 IPC／受管读取链路放大服务时间，尚不能精确划分每一次 RPC、内核调度与锁等待的独占贡献。

## 结论与下一步范围

当前 Native 库锁已通过独立进程绕过，但性能瓶颈转移到逐读取 RPC 与共享执行资源；只扩大到本轮大小或增加连续请求，尚不足以使现实现获益。保持 `native_processes=0` 默认值，保留四进程作为可选实验后端，不将其作为当前推荐性能路径。本轮没有大样本 1／2／4 子进程完整消融，也不能断言进程数继续增加会提速。

对既有解码器支持、已发布完整 chunk/layout 索引的数据，`iotdb-indexed + 公共查询池` 已是可用的查询间并行路径，无需解除 libhdf5 内部锁。所选样本已通过实际资格查询。重复区域／dataset 查询可优先考虑这一方向；首次大查询仍需保留 Native 对照，不能由热缓存结果推断 Indexed 在所有访问模式中更快。

若继续优化 Native，优先减少单查询内部重复供数及跨进程往返，例如在现有批大小上限内对齐 slab 与 chunk、对已验证不可变范围做有界请求内复用，或合并预算／读取消息。它们都需要保留原逻辑需求计费、取消点、owner/file 前后检查、错误及资源释放语义，且尚未实现和验证。本轮不引入单查询多 message/chunk 并行，也不修改 IoTDB 查询池。

## 恢复与证据

最终只读 live 核查确认 DataNode PID 561047、227 个线程恢复 CPU 0–31、原 cgroup/JVM/配置字节与 `ASYNC/true`；8 个临时 FUSE 挂载和记录的 16 个 daemon/helper 身份失活，临时 cgroup 不存在。此核查仅执行一次 `SHOW ALL CONFIGURATION`，没有追加科学查询、配置写入或服务重启。r1 的独立恢复核查也保留，不冒称 r1 科学矩阵完成。

冻结计划、源证明、失败记录、全部 2592 行／648 波、24 段、独立统计、进程代次和实际恢复证据位于 `experiments/cross_version/work/native-process-scale-20261026/`：

- [正式统计](../../experiments/cross_version/work/native-process-scale-20261026/evidence/collection-r2/analysis.json)、[全部查询](../../experiments/cross_version/work/native-process-scale-20261026/evidence/collection-r2/queries.jsonl)、[全部波](../../experiments/cross_version/work/native-process-scale-20261026/evidence/collection-r2/waves.jsonl)。
- [可移植独立重算](../../experiments/cross_version/work/native-process-scale-20261026/evidence/collection-r2/independent.json)、[补充独立复核](../../experiments/cross_version/work/native-process-scale-20261026/independent-review-r2.json)、[实际服务恢复](../../experiments/cross_version/work/native-process-scale-20261026/evidence/collection-r2/live-restoration-audit.json)。
- [缓存与 helper 代次派生表](../../experiments/cross_version/work/native-process-scale-20261026/derived-cache-helper-summary-r2.json)、[Native 阶段派生表](../../experiments/cross_version/work/native-process-scale-20261026/derived-native-stage-summary-r2.json)、[本地包核查](../../experiments/cross_version/work/native-process-scale-20261026/local-evidence-audit.json)。两份派生表在审计后从已冻结原始结果只读重算，不属于证据包原始产物。

证据包 **224,473,997 B／935 个产物**逐项字节数和 SHA-256 核对通过；包 SHA-256 为 `7b66a02b1c8115b6696a2b343c88ee485dc0c654d94d37e616e56437a1111e83`。本地标准库独立重算与远端结果一致，补充复核所绑定的六个原始输入 hash 与本地包逐一匹配。原工作区改动保留，未执行 Git 写操作。
