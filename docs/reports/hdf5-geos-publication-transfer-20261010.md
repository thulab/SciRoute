# GEOS/HDF5 有界传输块 2／4／8 MiB 消融

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-10。相同 16 个 GEOS 原件、每臂 7,142,773,610 B，同一生产 binary，以同轮 1 MiB 为对照，完整发布为 **84.733／108.249／106.891／117.271 s**（1／2／4／8 MiB）。较大三组相对 1 MiB 分别增加 **27.75%／26.15%／38.40%**。**本轮没有显示调大传输块的收益，默认保持 1 MiB。**

payload Tablet 数明显减少，但 OBJECT 编码和 SDK insert 的整体 elapsed 增加；单靠减少请求次数无法预测当前 SDK 链路的 wall time。结果来自固定顺序、每臂一轮，不能作为跨运行状态的稳定性能比例。

## 实现与实验边界

新增 `writer.transfer_chunk_bytes`，严格正整数字节，默认 1048576，上限 8 MiB，且必须容纳于有效 `writer.db_request_max_bytes` 和 manifest checkpoint 预算。非法值明确拒绝，不静默 clamp。Generic ingestion 使用该大小；legacy GRIB stream 内部 chunk 和普通读取 chunk 不改。资源配置明确区分 payload、协议 framing 与 checkpoint。

此参数控制 source read、OBJECT transfer、Adapter consume 的共同边界，**不是仅改变 SDK 网络 packet 的实验**。本轮 checkpoint 全部固定 8 MiB，因此 1／2／4／8 MiB 分别为每完整 checkpoint 8／4／2／1 个传输块。原文件字节、科学 layout、值语义和发布生命周期不变。SDK、数据库 schema、服务器参数均未修改。

8 MiB 指 payload 大小，OBJECT 另加 9 B EOF／offset 头，实际 Thrift 请求还含其他字段。DataNode properties 未显式设置 `dn_thrift_max_frame_size`，根据已读取的运行堆和源码默认规则推导其默认值为 64 MiB；没有动态读取生效限额。远端 client 默认帧限额也未直接核实。8 MiB 在本轮短路径负载的完整发布、查询、全 SHA 和跨格式 smoke 中实际通过，不据此承诺任意长度路径／其他 SDK 环境的帧安全。

构建使用独立 `geos-publication-transfer-20261010/source` 与 `build`，基于上一轮冻结生产 source，仅 overlay 6 项任务文件。4 臂使用同一 Release daemon，SDK／依赖构建路径与上一轮相同。数据库仍为 `geostime20261007`，每臂新逻辑 prefix 和精确版本；不新建／删除数据库，不改普通挂载或原实验 build。

## 可比计时与结果

原 `geos_pilot.py` 逐文件 `shutil.copyfile` 计时不变：包括创建、暂存写入、close-time 摄取发布，排除 mount、父目录 mkdir、源 SHA、stat、查询与完整 SHA 验证。8 MiB checkpoint、attribute/entry TTL 为零、256 MiB query cache／65536 entries、p=1、raw/full prefetch 为 1 B 均一致。

执行顺序为 1／2／4／8 MiB，不清 OS／IoTDB cache。每臂原驱动的 64 项正确性查询在该臂发布后、下一臂之前运行，所以不能声称各臂 cache 状态相同。所有 64 项完整 payload SHA 及三组多格式 smoke 均在四臂发布计时结束后运行。

| 指标，秒 | 1 MiB | 2 MiB | 4 MiB | 8 MiB |
| --- | ---: | ---: | ---: | ---: |
| 完整发布 | 84.733 | 108.249 | 106.891 | 117.271 |
| payload 阶段 wall | 40.923 | 67.511 | 67.414 | 75.612 |
| payload SQL 接口 | 4.785 | 4.396 | 4.639 | 8.702 |
| payload 外层 Tablet 接口 | 25.640 | 35.474 | 37.499 | 42.980 |
| payload OBJECT 编码 | 4.870 | 16.329 | 18.064 | 17.129 |
| payload SDK insert（嵌套） | 25.620 | 35.459 | 37.490 | 42.976 |
| payload 源读取 | 4.751 | 7.488 | 3.730 | 4.387 |
| payload 基础 Tablet 构造 | 0.051 | 0.038 | 0.020 | 0.007 |
| payload 细分后未归属 | 0.825 | 3.783 | 3.459 | 2.405 |
| 暂存同步 | 14.698 | 14.646 | 14.489 | 14.703 |
| HDF5 finish／索引构建 | 2.171 | 2.195 | 2.285 | 2.268 |
| 全部发布 root SQL 接口 | 11.704 | 8.903 | 9.525 | 15.355 |

阶段 wall 包含其分项。`sdk_insert` 是 SDK 内部请求构造／序列化、网络、服务器等待的综合调用时间，已含于外层 Tablet；`session_acquire` 也嵌套于 SQL／Tablet，不再相加。残余公式沿用上一轮：payload wall − SQL（submit/fetch/close/nonquery）− 外层 Tablet − 基础 Tablet 构造 − OBJECT 编码 − 源读取 − consume − mutation 锁等待。本轮均无 backend 输入复制。

SDK insert 每个 payload Tablet 的平均 elapsed 为 **3.76／10.39／21.90／49.80 ms**。更大请求的平均时间上升幅度抵消了请求数量减少的收益。OBJECT 编码是调用 `insertTablet` 之前的 inline 内存工作，没有 RPC；2／4／8 MiB 该项约 16–18 s，1 MiB 为 4.87 s，说明本轮退化不能全部解释为服务器等待。SDK 头文件中已有分配、初始化及多次复制；buffer 的分配／缺页／调度是后续可以验证的假设，**本轮没有单独测量或确认具体 allocator 阈值**。

暂存同步约 14.5–14.7 s，HDF5 索引构建约 2.17–2.28 s，基本一致。8 MiB payload SQL 为 8.70 s，高于其他三组 4.40–4.78 s，但 SQL 次数相同；这一差异是接口 elapsed 波动，不能称调大块导致更多 SQL。

上一轮 1 MiB 为 79.407 s，本轮 84.733 s；主对照使用本轮同 binary 的 84.733 s，不从跨轮总时间推导因果收益。没有自动切换默认值，也没有进一步修改 SDK／算法来掩盖较大块的结果。

## 调用数

| 操作 | 1 MiB | 2 MiB | 4 MiB | 8 MiB |
| --- | ---: | ---: | ---: | ---: |
| payload OBJECT Tablet | 6,820 | 3,413 | 1,712 | 863 |
| manifest SELECT／frontier INSERT（各） | 863 | 863 | 863 | 863 |
| 全部发布 root Tablet | 6,916 | 3,509 | 1,808 | 959 |
| 全部 SQL SELECT＋nonquery | 2,430 | 2,430 | 2,430 | 2,430 |
| payload SessionAcquire | 8,546 | 5,139 | 3,438 | 2,589 |

块数逐文件计算 `sum(ceil(file_bytes/transfer_bytes))`，checkpoint 同样逐文件计算，不能用总容量向上取整。每臂 32 个 root（16 empty create＋16 content commit），payload 外 Tablet 固定 96 次，Build／Encode 各比 payload 多 64 次；source read、Adapter consume、SDK insert 与 payload chunk 数逐一一致。四臂所有诊断 failures 为零。

## 取值延迟诊断

以下为原正确性负载中每 case 的 16 个不同文件各一次查询的中位数，不是独立重复 query benchmark。IoTDB-FS 时间包含 socket 注册＋IPC mmap／解码／值复制；native 时间包含 `h5py.File` 打开、子集提取和复制。point／roi／multichunk／levels 顺序固定，cache 有关联，不与旧直连 warm-kernel 指标混用。

| IoTDB-FS，ms | 1 MiB | 2 MiB | 4 MiB | 8 MiB |
| --- | ---: | ---: | ---: | ---: |
| point | 59.74 | 61.24 | 62.00 | 66.48 |
| roi | 48.89 | 49.74 | 50.90 | 53.67 |
| multichunk | 46.08 | 46.10 | 47.22 | 49.52 |
| levels | 56.64 | 58.58 | 59.48 | 62.02 |

| native FS，ms | 1 MiB 臂 | 2 MiB 臂 | 4 MiB 臂 | 8 MiB 臂 |
| --- | ---: | ---: | ---: | ---: |
| point | 0.99 | 0.74 | 0.96 | 1.15 |
| roi | 1.43 | 1.33 | 1.44 | 1.67 |
| multichunk | 1.47 | 1.34 | 1.45 | 1.70 |
| levels | 3.92 | 3.82 | 4.01 | 4.39 |

这一诊断没有显示更大传输块带来查询收益；8 MiB 的 IoTDB-FS 和 native 两侧时间均较高，不能把变化全部归因于 IoTDB 内部存储粒度或断言已测得物理读放大。原值、行数与 digest 均一致。

## 验证与复验

- 独立 Linux Release 构建通过；**10 项配置探针**通过：0、负数、单位后缀、超过 8 MiB、超过 DB 预算、超过 checkpoint 预算均拒绝；1／2／4／8 MiB 均通过配置检查。探针在 migration CLI 的 usage 返回之前检查配置，不连接／写数据库。
- **19 项 Python validator 单测**通过，覆盖新增预算限制、逐文件尾块、较大块完整 trace 计数与嵌套诊断。runtime trace 严格按实际 transfer 大小验收，不沿用固定 6820 次。
- 四臂 **256/256 GEOS 取值结果**正确，**64/64 layout digest** 与旧 `/5` 精确匹配；**64/64 完整 payload SHA**匹配原件，总覆盖 28,571,094,440 B。
- 2／4／8 MiB 三组格式 smoke 均 complete=true：每组 12,000,400 B Arrow stream、9,439,332 B／12 messages GRIB、3 B text、empty。合计 12 项 Arrow 切片、9 项 GRIB exact message、12 项全 SHA，精确版本 EOF、identity/layout 和正常卸载全部通过；所选 message 跨越 8 MiB，也跨越 2／4 MiB 的对应物理边界。
- 本轮 C++ 改动仅 main 配置接线，未修改 backend／ingestion 库；库的上一轮 1441 项 unit 结果已有保存，本轮不将它称为重新执行。main 新边界由实际 binary 的配置探针和实时挂载覆盖。
- 6 项 overlay hash 与本地匹配；服务器源码 manifest 共 429 项。所有临时实验挂载卸载，普通挂载保留。

摘要 JSON（服务器归档：`docs/hdf5-geos-publication-transfer-20261010.json`）、[原始汇总](../results/hdf5-geos-publication-transfer-20261010/summary.json)、[64 项完整 SHA](../results/hdf5-geos-publication-transfer-20261010/payload-verification.json)、[配置探针](../results/hdf5-geos-publication-transfer-20261010/config-probes.json)、[validator 日志](../results/hdf5-geos-publication-transfer-20261010/validator-tests.log)。原始 `mib1/2/4/8` 目录保存 provenance、pilot 和 JSONL，根目录保留命令、构建日志、严格验证输出及 source hash；三组 `format-smoke-mib*` 保存结果和配置。

脚本：[profile wrapper](../../tests/benchmarks/hdf5_query/geos_publication_profile.py)、[严格 validator](../../tests/benchmarks/hdf5_query/geos_publication_profile_validate.py)、[格式 smoke](../../tests/benchmarks/hdf5_query/publication_batch_format_smoke.py)。wrapper 显式 `--transfer-bytes 1048576|2097152|4194304|8388608 --manifest-batch-bytes 8388608`；validator 对应 `--chunk-bytes` 同值及 `--construction-profile`。保留的 build/run/collect 脚本可在新隔离目录复验，须另选 output root 与逻辑 prefix，避免覆盖既有结果。

生产 daemon SHA-256：`c8659065eaa70f22025d1538e547f282dc0d31663f211b206c41162681ee4a5c`。远端原始证据保留 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-publication-transfer-20261010/`。证据不含凭据或 mount.log。
