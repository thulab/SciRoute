# HDF5 H10：固定 trace 与成本归属

H10 提供可复验的查询成本观测，不实施 H11 的完整 A–E 性能对照，也不以正收益作为验收条件。两条观测路径使用相同数据字节、dataset、矩形选择和稳定 sample ID：

| 路径 | 实际执行 | 可以解释什么 |
|---|---|---|
| `recording-memory` | 原始文件一次性摄取到 recording backend 的内存 B/L；生产 QueryModule、HDF5 adapter、公共 cache 和 IPC encoder | 布局读取、范围合并、chunk 解码、组装和编码；计数器与独立 source trace 的一致性 |
| `iotdb_live` | 隔离数据库中已发布的精确版本；Unix socket 注册；IoTDB 取数；只读不可变 FUSE IPC 路径 | 注册、准备、授权、后端 I/O、编码、发布，以及客户端读取和重建的实际成本 |

Recording backend 不是普通文件系统速度基线。Live trace 前的源文件 SHA 校验通过普通挂载读取，会影响 OS、IoTDB 或底层存储缓存；它不会预热 typed decoded cache。两条路径均不声称冷磁盘条件。

## 固定输入与正确性

`run_h10.py prepare` 复用 H08 原始压缩 fixture 和锁定的 GWOSC H1 产品，由独立 h5py 进程生成值 oracle。小 fixture 查询 `/array` 的 `[1:6,2:10]`，重叠查询 `[2:5,3:8]`。真实产品查询 `/strain/Strain` 的 `[2047:10241]`，重叠查询 `[4096:8192]`。每个 source 的顺序固定为首次查询、精确重复、被首次选择覆盖的重叠选择、再次精确重复。

每个 source 产生 `manifest.json`、五列 `trace.tsv` 及逐样本 canonical little-endian 值文件。Sample ID 是源字节 SHA-256、dataset 路径、origin、shape、phase 的规范 JSON 的 SHA-256；不含 pid、backend namespace、source version、输出目录或进程内随机值。不同 backend 和配置可以使用同一 ID，首次请求与重复请求具有不同 phase，因而不会混为同一样本。

每次运行保存原始 `raw.json`、独立 `client.json`、每样本 `.values.bin` 和 `.ipc`。Recording 还保存原生 probe 输出 `probe-raw.json`、调用参数和进程状态；live 保存逐样本原始 `.server.json`。`summarize` 不调用 probe，可以单独重建 summary。它检查完整的有序 ID 列表、重复与缺失、坐标、源字节身份、精确 source/version、原生和客户端值哈希、artifact 哈希及字节数、指标类型、oracle 未被修改；失败时返回非零，不把缺测当作零。自测包含这些负例。

## 缓存条件

默认 trace 使用 `retain`。Recording 每次运行创建新的 QueryRuntime，因此首次请求没有该 runtime 的 decoded cache 条目。Live 只允许独立 daemon/runtime 与未查询过的 source；`--fresh-runtime` 是运行者对该环境的声明，脚本同时要求首次请求发生解码，并检查 profile sequence 连续、查询区间不重叠。不能用该选项让共享生产 runtime 获得“冷缓存”标签。

`--expect-resident` 明确声明完整相关工作集能够驻留。在该条件且 `retain` 下，后续三次选择才要求零 payload I/O、零 decode。容量不足的配置不传此选项，保留正常 evictions、redecodes 和实际读数；不会把暖遍解码错误判为 correctness failure。

Recording 的 `--evict-before-request` 在每次请求前调用 `evict_all()`。它称为 **evict-before-request**，不称为 cache-off：一次请求内部仍有 cache、single-flight 和共享解码。当前没有配置 encoded-only cache 或真正绕过 decoded cache 的模式，报告明确列为未配置。未清空 OS page cache 或设备缓存。

## 时间和计数口径

| 观测 | 归属与限制 |
|---|---|
| `setup.ingestion_ns`、ingestion profile/counters | 一次性原始摄取、HDF5 库访问与索引构建；不混入逐查询 wall time。Live daemon/摄取准备由外部实验记录，缺少的 setup 不填零。 |
| `timings_ns.total` | Recording 的 prepare、cursor 执行和 IPC 编码父区间；不含保存 artifact 或独立正确性校验。 |
| `prepare`、`execute_setup`、`cursor_consume`、`ipc_encode` | 顺序客户端调用区间；更细的 `stages_ns` 与 runtime 累计计数保留原始名称。 |
| `server_total`、`client_registration_roundtrip` | Live server 注册与物化父区间，及包含 socket 往返的客户端区间；不能相加为总耗时。 |
| layout/source、decode、filter/inflate/shuffle/checksum、conversion、assembly、queue/admission/session 等 | 细阶段可能嵌套或在不同 worker 上重叠。累计 wall durations 不是可相加的执行 wall time；CPU 采样与缺失样本需保留。 |
| `payload_read_bytes/ranges/calls` | 实际 source API **尝试请求**的字节、范围形状及调用数，包含失败尝试，不是成功返回字节；与逻辑预算 reservation `read_bytes/read_ranges` 区分。Recording 与独立 source trace 交叉核对。 |
| IoTDB `payload_requested_bytes`、`payload_returned_bytes` | 前者为通过参数校验的 backend 尝试请求字节，包含失败；后者仅为成功 backend 调用返回字节。验收 trace 必须无 payload failure，才要求 runtime attempted bytes 与成功 returned bytes 相等。均不是物理磁盘 I/O。 |
| `chunk_decode_count`、`unique_decoded_fragments_cumulative` | Probe 的有界 canonical key 观测用于区分实际 chunk/tile 解码与不同 fragment 数；同时检查 `unique_decode_trace_complete`，不把截断的集合称为完整。 |
| `unique_payload_spans` | 本请求不同 `(offset,length)` 数，不是独立 chunk decode 数；合并/分割后不能一一对应 chunk。 |
| `unique_payload_union_bytes`、`repeated_payload_bytes` | 前者为本请求实际 payload 范围的区间并集大小；后者为累计范围字节减去并集字节。它们描述 source 地址覆盖与重复，不是磁盘访问量，也不表示跨请求 cache 去重率。 |
| `merge_gap_overread_bytes` | 当前 planner 只合并相邻或重叠范围，不跨缺口，因此该策略值为 0。它不是解码整个 native chunk 造成的有效载荷放大，也不是磁盘预读量。 |
| cache、pinned、inflight、temporary、peak、active/peak calls | gauges 保留绝对值；不能把 peak 作差或把相互重叠的 pinned/cache-owned 相加。 |
| `process_peak_rss_bytes` | Probe 和 Python 客户端分别从 OS `getrusage().ru_maxrss` 读取其进程生命周期 RSS 高水位；Linux KiB 转为 bytes，macOS 原值为 bytes。包含库、输入、索引、Arrow buffers 等其他驻留内存，不等于 runtime 声明预算的 `peak_bytes`，两者不作差，也不把进程 lifetime peak 当作请求增量。 |

IoTDB 计数器的范围是整个 daemon 的所有 IoTDB client/backend 实例，而非一个请求专属连接。顺序隔离 trace 才能把该时段增量用于本请求解释；并发请求的增量会相互覆盖。`session_acquire_nanoseconds` 包含 pool mutex 和可用连接等待、包括超时；`session_leases_active` 持续覆盖结果读取和关闭，`session_leases_peak` 是 daemon 生命周期高水位。它们不等同于 IoTDB 服务器内部并行度。

`query_nanoseconds` 测量客户端 `executeQuery`，包含连接获取和初始 SDK 提交，**不含之后的结果消费**。`metadata_query_nanoseconds` 是其中由 backend 元数据查询产生的子集，不能与 query 时间相加。`payload_nanoseconds` 测量完整 backend 读取，包括 query、结果消费和 close，与 query/session 时间重叠；layout 的完整 backend 调用另有对应 counters/timer。所有这些时间都不能直接解释为数据库服务器执行时间或内部 RPC 时间。

可解释的 wall-time 模型采用**同一调用线程上互不重叠的直接子区间**：

```text
server_total
  = registration_catalog + registration_authorize + route_register
    + query_materialize + server_residual
query_materialize
  = typed_prepare + query_authorize + typed_result_metadata
    + typed_execute_setup + typed_encode + result_publish + materialize_residual
typed_encode
  = typed_cursor_execute + typed_ipc_encode + encode_residual
```

残差包含未单独命名的解析、控制流、时钟和包装成本。只有以上父子计时关系才能用于相减计算残差；worker queue、payload、codec、metadata 和 session 时间用作归因，不能再加到模型右侧。以 p=1 GWOSC 精确重复为例，三个残差依次约为 0.224、0.033、0.013 ms；这只证明该样本的计时归属可核对，不是拟合出的常数。当前数据不足以识别独立 per-byte/per-range 系数，不进行该拟合。

`summary.json` 保留每请求的 native 顶层 timings、细 stages、counters、runtime metrics、IoTDB delta/gauges、客户端计时与 faults。没有把它们强制归一成百分之百的堆叠成本。`payload_bytes_per_call`、`ranges_per_call` 在零调用时为 null。区分 bytes 或 ranges 主导需要结合固定 trace 下的 transport 配置与这些测量；单个 wall time 或一次样本不足以给出规模外推结论。

## 客户端测量

客户端按以下顺序运行，所有阶段都单独计时：

1. `mmap_setup`：`pyarrow.memory_map` 建立视图。
2. `page_touch`：每个 OS 页读取一个字节；记录该进程区间的 minor/major fault 增量。此区间包括 Python 遍历开销，不能把它当作纯 page-fault handler 时间。
3. `arrow_reconstruct`：打开 IPC stream、读取 RecordBatch/Table。
4. `numpy_materialize_copy`：合并值列 chunk、转 NumPy 并显式拷贝。
5. `validation`：值哈希、dtype、形状和 schema descriptor 校验，独立于前面阶段。
6. `ordinary_read_after_touch`：再次普通读取完整 IPC。它已受前面触页影响，不是和 mmap 独立的冷态 A/B 对照。

Recording 映射的是 probe 落盘的本地 IPC artifact；live 映射实际不可变 FUSE IPC 路径。Live 还将实际解码值和 IPC 存为证据，保存成本不计入上述区间。两种客户端 surface 在 `client.json` 中明确标识。

## 执行方式

见 [benchmark README](../../tests/benchmarks/hdf5_query/README.md#h10-fixed-traces-and-cost-profiles)。本阶段实验结果应连同 native probe、daemon 配置、source manifest、逐请求 raw/client/server 和独立 summary 保存。本轮实测与解释见下文；它不替代 H11 的强文件系统对照。


## 2026-10-03 验收结果

Linux x86_64、GCC 13.3、固定 HDF5 1.14.6 上，**18 个配置、72 次请求全部通过**独立 summary 验证。其中 recording 12 个配置／48 次请求，live 6 个独立 daemon 配置／24 次请求。每个配置只执行一次四请求 trace，没有独立重复试验或统计显著性结论。实验清单和所有逐请求证据见 [experiment.json](../results/hdf5-h10-20261003/verified-runs/experiment.json)；独立重新汇总 [18/18 结果一致](../results/hdf5-h10-20261003/independent-summary-validation.json)；[verification.json](../results/hdf5-h10-20261003/verification.json) 保存平台、409 个源码文件一致性检查、二进制及配置 SHA-256。

共同 runtime 预算为 64 MiB、最多 65,536 个条目。Recording 对每个 source 运行 `p=1/2/4,ranges=32,retain`、`p=1/4,ranges=1,retain` 和 `p=1,ranges=32,evict-before-request`；live 对每个 source 运行 `p=1/2/4,retain`。没有在本轮增加容量压力、cache-off、encoded-only 或完整 H11 A–E 实验。

以下是 **p=1** 的逐样本观察，单位为 ms。Recording total 与 live server total 属于不同执行边界，不能据此计算普通 FS 对 IoTDB-FS 的加速比。

| source／路径 | 首次 total | 精确重复 total | 首次 payload：调用／范围／字节 | 首次实际 chunk 解码 | 首次 codec 累计时间 |
|---|---:|---:|---:|---:|---:|
| [small recording](../results/hdf5-h10-20261003/verified-runs/recording-small-p1-r32-evict0/summary.json) | 2.276 | 0.286 | 1／1／226 | 6 | 0.041 |
| [GWOSC recording](../results/hdf5-h10-20261003/verified-runs/recording-gwosc-p1-r32-evict0/summary.json) | 1.881 | 0.526 | 3／3／95,342 | 6 | 0.516 |
| [small live](../results/hdf5-h10-20261003/verified-runs/live-small-p1/run/summary.json) | 82.311 | 73.178 | 1／1／226 | 6 | 0.045 |
| [GWOSC live](../results/hdf5-h10-20261003/verified-runs/live-gwosc-p1/run/summary.json) | 89.296 | 69.480 | 3／3／95,342 | 6 | 0.599 |

所有 retain 配置的精确重复、重叠和再次重复都满足零 payload 调用／字节、零解码；后端与 runtime 实际 I/O 计数交叉一致，全部 live payload 调用成功。强制每次驱逐时，[small](../results/hdf5-h10-20261003/verified-runs/recording-small-p1-r32-evict1/summary.json) 四次请求产生 22 次 chunk 解码、6 个唯一 decoded key、16 次 redecode；[GWOSC](../results/hdf5-h10-20261003/verified-runs/recording-gwosc-p1-r32-evict1/summary.json) 对应为 20、6、14。`unique_decode_trace_complete=1`。主动 `evict_all()` 在请求计时及 delta 起点之前执行，因此其删除数没有被计入该请求的 `evictions`；此处零 evictions 不意味着未驱逐。结果说明 profile 能区分共享复用和重复解码，不能解释为压力驱逐性能测试。

本轮无法用 `p=2/4` 配置推断并行收益：所有实际 payload 活动峰值仍为 **1**，live session lease 高水位也为 **1**。首次小文件已合并为一个连续范围；GWOSC 三个调用在当前 cursor 迭代中仍顺序发生。单范围与多范围配置在这些首次选择上没有改变调用数。强制驱逐后的 small 重叠请求确实通过一次多范围调用读取两个非连续范围，但没有针对它建立独立重复的性能对照。所有 recording 的 `merge_gap_overread_bytes=0`，符合不跨缺口合并策略；不能把当前 trace 的并行未被利用推广为实现不支持并行。

### 当前成本主要在哪里

对这两个局部查询，**live 暖请求的主要剩余成本是重复元数据／layout 访问和服务路径，并非值解压**。暖请求仍各有 7 次 backend metadata query 和 4 次 layout read，即 11 次 IoTDB query；small 每次读取 1,292 B layout，GWOSC 每次 9,620 B。暖请求观测范围如下；范围来自不同 phase 和 p 配置，不是置信区间。

| live 暖请求观测 | small | GWOSC |
|---|---:|---:|
| server total | 66.437–74.456 ms | 58.691–69.480 ms |
| metadata query 累计区间 | 45.135–52.921 ms | 38.301–47.296 ms |
| 完整 layout 调用累计区间 | 17.668–19.859 ms | 17.064–19.973 ms |
| IPC encoder 子阶段 | 0.111–0.136 ms | 0.326–0.776 ms |
| payload I/O／decode | 0／0 | 0／0 |

这些区间存在父子包含关系，不能逐行相加或直接作为 server total 百分比。p=1 的精确重复中，small 注册授权与执行授权分别为 12.139、12.055 ms，GWOSC 为 12.373、10.460 ms；它们包含相应 inode／父目录元数据访问。重复 catalog/source 打开、layout 目录／页读取与授权是下一步可检验的固定成本来源。此处不提出跳过授权或版本验证。

首次 live small 的 226 B payload 用约 5.13 ms，GWOSC 的 95,342 B／三次调用用约 15.33 ms，而 codec 分别约 0.045／0.599 ms。结合暖态仍存在的 11 次 query，证据更支持**本轮小 trace 受调用与元数据固定成本影响较大**。它没有控制更多载荷大小与范围数，尚不能从这两点拟合独立的 per-byte 与 per-range 系数，也不能推断大工作集吞吐。

客户端 warm `mmap_setup` 为 small 11.60–13.15 ms、GWOSC 10.12–12.50 ms，均大于其后测得的触页区间（分别 0.072–0.138 ms、0.160–0.312 ms）。此处 mmap setup 包含打开实际 FUSE 路径的用户可见成本，不能等同于单个 mmap syscall。每个新 Python 进程的首次 `numpy_materialize_copy` 约 237–252 ms，后续 small 约 0.070–0.211 ms、GWOSC 约 0.110–0.183 ms。首次区间可能含 PyArrow/Pandas 等惰性初始化；当前未独立剥离，所以**不能把约 240 ms 宣称为数组拷贝本身的成本**。

p=1 首次 small 的有效值为 320 B，IPC 为 4,424 B；GWOSC 对应 65,552 B 和 70,944 B。小结果的 schema/provenance 固定开销占比高，但本轮 encoder 时间仍不足 1 ms。其后是否值得优化应结合更多结果规模验证。两条 p=1 live runtime 预算峰值分别为 1,049,605 B 和 1,225,871 B，远未触及 64 MiB；Python RSS 生命周期峰值约 115 MB，属于不同测量对象。

### 测试与未覆盖范围

Linux CTest **1,264/1,264** 通过；本地针对性 [普通测试](../results/hdf5-h10-20261003/hdf-cache-focused.json) 和 [ASan/UBSan 测试](../results/hdf5-h10-20261003/hdf-cache-focused-asan.json) 各 **10/10** 通过，覆盖实际读尝试、codec mask、profile 传播和 cache 行为。严格 summary 的负例自测单独检查缺失／重复／错样本、错误数据哈希、改动 artifact、计数不一致及旧成功 summary 失效。

额外的 IoTDBClient 写入型生命周期 live 集成在新隔离数据库的 seed 阶段收到 IoTDB **906：无可用 SchemaRegionGroups**，尚未进入租约断言，因此不计为通过；也没有因此修改集群配置。该环境限制不影响上述复用 H09 已发布精确版本的只读 H10 24 次 live 查询及其租约观测。原 Arrow／GRIB 挂载保留，H10 临时挂载已清理。H10 由此完成测量与正确性验收；普通 FS 强基线、扩大工作集、真正并发以及重复统计试验仍属于 H11。
