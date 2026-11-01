# H1–H3 科学数据格式候选实验方案

研究对象是当前 IoTDB-FS-C 的 GRIB message 级布局与三个候选扩展格式。本文档是可执行 protocol；截至 2026-10-06，HDF5 Adapter 已支持 OISST 的属性目录与 CF 科学查询，并完成七天同条件性能实验 (local archive: `docs/reports/hdf5-scientific-query-benchmark.md`)。mzML 和 SEG-Y 适配器尚未实现。

配套综述见 [scientific-format-candidates-h1-h3-2026-09-30.md](../scientific-format-candidates-h1-h3-2026-09-30.md)。综述中的 C1–C8 是仓库源码证据，R1–R22 是在线来源。本文档只把已核验的事实写成“已知”，把需要下载后检查的属性写成“待测”。

## 1. 排名和决策

| 优先级 | 格式与输入 | 主要正例 | 为什么适合验证 H1/H2/H3 | 主要风险 |
|---|---|---|---|---|
| 1 | netCDF-4/HDF5；首选 NOAA OISST，第二数据集为 NASA MERRA-2 | 少量经纬度点或小 ROI 的多日/多月时间序列 | 规则网格语义直接对应现有 DenseGrid；生产常按大空间 chunk 写入，而点时间序列跨很多 chunk；可分别测 chunk 元数据索引和重分块 | 原生 HDF5 chunk cache、Kerchunk、DMR++、chunkindex 已很强；不能把已有成熟实现当作弱基线 |
| 2 | mzML；必要时以 mzMLb/mzDB 作为成熟派生基线 | centroid MS1 提取离子色谱图（XIC），窄 m/z 窗跨许多 scan | scan-major 的完整谱图与跨 scan 的 m/z 谓词存在明显语义错位；可检验 Rows、目录索引和派生列/块布局能否跨领域复用 | indexed offset 只能定位 scan，不能保证 m/z 数组内部少读；不等长谱图、profile/centroid 和单位语义容易造成错误比较 |
| 3 | SEG-Y；首选 USGS NAMSS B-30f-90-LA 3D 迁移体 | 小三维子体积、薄深度切片、跨 trace 窄深度窗 | 工业场景清楚地体现 trace-major 与横向切片错位；OpenVDS bricks/Fast Slices 提供强工业参照 | segyio 等原生工具可按 trace offset 高效读取；IBM 浮点、非规则道和几何缺失道必须公平处理 |

FITS 图像可作为替代第三候选；它只有在能取得实际大块或整文件压缩输入、且与 CFITSIO tile subset 基线形成错位时才进入主实验。TIFF、LAS/LAZ、COPC、imzML 先作为边界或后续扩展，不把候选数量当作论文贡献。

推荐执行顺序是 netCDF-4/HDF5 → mzML → SEG-Y。只有第一阶段同时通过正确性门槛和公平基线审查，才投入第二阶段适配；每一阶段都保留低选择率、完整扫描和源布局已匹配的负例。

## 2. 已确认的数据入口与启动时核验

### 2.1 netCDF-4/HDF5

**已确认的 OISST 输入**：

- 文件：[NOAA OISST 2024-01-01](https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/202401/oisst-avhrr-v02r01.20240101.nc)。
- 文件大小：1,528,923 bytes；SHA-256：`fd2f772cbe1633b20ab0c53ed76b874406f933bbd97062d611827c5d8a73bc4c`。
- HDF5 维度为 `time=1, zlev=1, lat=720, lon=1440`；`sst`、`anom`、`err`、`ice` 的已核验 chunk 为 `[1,1,720,1440]`，使用 shuffle 与 deflate level 4。
- 该单日文件只能证明“一个逻辑点可能落在整幅空间 chunk 中”的 H1 机制，不能单独证明端到端加速；长时间序列需要下载同一产品的多日文件并现场记录每个文件的 chunk、过滤器、变量和维度。

2024-01-01 至 2024-01-07 七个连续原始文件已经用于 A–E 与跨文件属性目录对照；
结果见独立性能报告 (local archive: `docs/reports/hdf5-scientific-query-benchmark.md`)。七天结果仍不能外推到
365 天或不同 chunk 布局。

**已确认的 MERRA-2 入口元数据**：

- [CMR collection M2I1NXASM 5.12.4](https://cmr.earthdata.nasa.gov/search/collections.json?short_name=M2I1NXASM&version=5.12.4&page_size=1)。
- [2024-01-01 granule 查询](https://cmr.earthdata.nasa.gov/search/granules.json?collection_concept_id=C1276812820-GES_DISC&temporal=2024-01-01T00:00:00Z%2C2024-01-02T00:00:00Z&page_size=1)。
- Granule 文件名为 `MERRA2_400.inst1_2d_asm_Nx.20240101.nc4`，CMR 报告约 198.49 MB，逐小时、单层、全球 0.625° × 0.5°。
- chunk 形状、压缩过滤器、变量列表、缺测编码和真实文件 hash 必须在下载后读取；不能从产品说明推断。

启动时用 HDF5/netCDF 工具记录：格式版本、变量、维度、dtype、属性、chunk、过滤器、压缩级别、fill value、文件 hash、字节大小，以及是否存在外部索引或源产品内部目录。若 OISST 单文件过小，不扩展其结论；改用 MERRA-2 或同产品多时刻归档。

### 2.2 mzML

- [作者复现仓库的 `manuscript_things` 分支](https://github.com/wkumler/mzsql/tree/manuscript_things)，已核验分支 SHA `13ab9742e89e9a2d6317b79f14a907c4d1d24b02`。
- 文件 `demo_data/180205_Poo_TruePoo_Full1_idx.mzML`，已核验大小 27,955,346 bytes，`spectrumList count="1365"`，文件头含 MS1、centroid、scan start time；首批谱图数组长度出现 34、32、254。
- 目前只核验了文件前部和仓库元数据，未把全部谱图下载、解码或建立完整 m/z/RT 统计；indexed offset、压缩方式、profile/centroid 比例和每个数组的编码参数均为启动时待测。
- 参考格式与基线：[mzML 规范论文](https://pmc.ncbi.nlm.nih.gov/articles/PMC3013463/)、[mzMLb](https://pmc.ncbi.nlm.nih.gov/articles/PMC7871438/)、[mzDB](https://pubmed.ncbi.nlm.nih.gov/25505153/)、[简单数据库比较](https://pubs.acs.org/doi/10.1021/acs.jproteome.5c00721)。

启动时必须记录 XML 是否 indexed、binaryDataArray 的压缩/precision、m/z 与 intensity 数组长度、RT 单位、MS level、polarity、扫描顺序、缺失数组和重复谱图标识。mzMLb、mzDB 或数据库派生表示须保留同一原始元数据，并分别计入转换与存储成本。

### 2.3 SEG-Y

- [USGS 数据集页](https://walrus.wr.usgs.gov/namss/survey/b-30f-90-la/)、[文件清单](https://walrus.wr.usgs.gov/namss/survey/b-30f-90-la/dataset/)、[官方 header dump](https://walrus.wr.usgs.gov/namss/data/1990/namss.B-30f-90-LA.mcs3d.airgun/Images/G3D201503-07_l90_030_header_dump.txt)。
- 已核验为 3D Multichannel Seismic、3D Migration、SEG-Y、嵌入导航；`.sgy` 约 934.7 MB，ZIP 约 796.7 MB，解压约 935.1 MB。
- header dump 显示每道 1001 个样点、IBM 32-bit 浮点、inline 4180–4910 步长 2、CDP 1330–1960 步长 1、采样间隔 30 ft、深度域。
- 由 header 范围推得约 366 × 631 的矩形、约 230,946 traces 和约 2.31 亿样点；这些是规模估算，必须在真实文件上验证实际 trace 数、排序、缺失/重复道、坐标缩放、采样范围和导航一致性。
- 工业参照为 [OpenVDS Deep Dive](https://osdu.pages.opengroup.org/platform/domain-data-mgmt-services/seismic/open-vds/vds/deepdive/deepdive.html)；原生读取参照为 [segyio](https://github.com/equinor/segyio)。

启动时记录文本/二进制 header、trace header 字段映射、endianness、IBM→IEEE 转换、每道实际样点数、道间距、坐标缩放、缺失道比例、数据域和文件偏移表。原生基线必须支持按 offset 定位、合并相邻读、只转换所需样点。

## 3. 研究问题与可证伪判据

1. **H1**：给定生产表示和语义谓词，触达的物理单元、读取字节、解码值数或重复处理量是否显著高于答案所需量？
2. **H2**：仅增加已有结构索引，或改变为工作负载适配布局，是否降低上述放大？收益是否在至少两个后端上保持？
3. **H3**：把索引、派生、数据库服务和维护成本纳入后，IoTDB-FS 是否在明确的选择率、复用度、并发度和缓存状态下改善端到端时间？

结果必须报告正例和负例，不预设候选一定获益。论文结论使用“在某一数据布局和工作负载下观察到”这一范围，不能写成格式级必然性质。

## 4. A–E 因果对照

| 组 | 数据表示 | 后端 | 目的 |
|---|---|---|---|
| A | 原始文件，开启原生已有索引/最佳合法缓存 | 调优原生库 | 最佳可行原生基线 |
| B | 同一原始字节，外置结构索引（地址、目录、几何或 scan 索引） | 普通范围读/本地文件 | 隔离 H2 的“索引已有结构”收益 |
| C | 工作负载适配的派生布局（重分块、转置、bricks、m/z 块） | 与 B 相同的非 IoTDB 后端 | 隔离 H2 的“改变物理单元”收益 |
| D | 与 B 完全相同的字节、索引、缓存预算和查询计划 | IoTDB-FS | 测相同表示下的数据库/系统增量 |
| E | 与 C 完全相同的派生字节、索引和缓存预算 | IoTDB-FS | 测派生表示下的数据库/系统增量 |

必须另列每种格式的成熟最佳实践基线：netCDF/HDF5 原生、Kerchunk/DMR++/chunkindex（适用时）、mzML indexed、mzMLb/mzDB、segyio/OpenVDS。B 与 D、C 与 E 的输入表示和选择谓词必须逐字相同；服务器实际磁盘读量要单独采集，不能用 API 返回字节替代。

差值解释固定为：`B − A` 是结构索引收益，`C − B` 是派生表示收益，`D − B` 是 IoTDB-FS 在相同表示上的增量，`E − C` 是派生布局下的 IoTDB-FS 增量。

## 5. 查询工作负载矩阵

### 5.1 netCDF-4/HDF5

对每一组输入固定变量、时间、空间选择，并记录逻辑与物理选择率的组合：

| 维度 | 取值 |
|---|---|
| 时间跨度 | 1、7、30、365 天（数据集不足时报告不可用，不补造数据） |
| 变量 | 1 个、少量相关变量、全部变量 |
| 空间 | 单点、约 1/16 网格、约 1/256 网格、中等 ROI、全场 |
| 正例 | 固定点/小窗口跨时间序列；少变量小 ROI |
| 负例 | 全场、完整扫描、源 chunk 已匹配、单次查询、热缓存低选择率失效 |

输出是带原坐标、单位、时间戳、变量名和缺测语义的数值数组；边界按明确闭区间/半开区间协议处理，保持经纬度顺序、fill value/NaN、dtype 转换规则和时间日历。不得用插值、重采样、降分辨率或改变缺测值来换取速度。

跨文件拼接必须显式记录 calendar、time units、时区/参考历元、文件排序、重复时间戳和缺失时间步；重复时间戳按预注册规则报错、去重或保留来源标识，不能由不同后端自行决定。跨文件目录准备和单文件读取要分别计时。

### 5.2 mzML

主任务为 centroid MS1 XIC：对每个原始 scan，计算闭区间 m/z 窗内 intensity 的总和，按原始 scan 顺序返回 `(scan_id, RT, MS level, polarity, intensity_sum)`；无命中峰返回零；不合并重复峰、不平滑、不插值、不归一化。保留原始单位和时间轴。

| 维度 | 取值 |
|---|---|
| m/z 相对窗 | ±5、±10、±20 ppm |
| m/z 绝对敏感性 | ±0.005、±0.01、±0.05 Da |
| RT 选择率 | 有效时长约 1%、10%、100% |
| 谱图层 | MS1、MS2、混合；centroid、profile 分层 |
| 正例 | 窄 m/z 跨大量 scan、重复 RT 窗查询 |
| 负例 | 完整谱图顺序扫描、单个已可 offset 定位的 scan、低复用、全 m/z 窗 |

ppm 与绝对 Da 不能混为同一查询；对每个 scan 必须以其实际 m/z 值判断边界。若 profile 谱图需要峰提取，峰提取时间和参数必须固定并计入各组；不能把 profile 与 centroid 的结果直接比较。

### 5.3 SEG-Y

| 任务 | 选择参数 | 正/负对照 |
|---|---|---|
| 局部三维子体积 | inline、CDP、深度三维范围；选择率 0.01%、0.1%、1%、10% | 正例为小子体积；负例为完整体 |
| 固定薄切片 | 单个或少量深度样点，跨许多 trace | 与原生 offset 直接读取对照 |
| 窄深度窗 | 多 trace 内相同深度窗，窗宽 1%、10%、50% | 全 trace 顺序读、低复用 |
| 坐标裁剪 | 导航/inline/CDP 范围与几何 mask | 已按空间排序的输入、全扫描 |

输出必须保留 trace 标识、inline/CDP、原始坐标、采样间隔、深度/时间域、缺失道、道顺序和 IEEE 数值。IBM→IEEE 只转换真正需要的样点；若某库只能整道转换，要把整道解码成本计入该基线并明确记录。不能把头信息范围推断成无缺失矩形；实际 trace mask 必须成为结果的一部分。

## 6. 正确性和等价性门槛

每个候选先建立一个小规模、全量解码的 oracle。所有 A–E 输出逐元素或逐字段比较，并检查：

- 维度、坐标、单位、时间/深度域、排序和边界包含规则；
- 缺测值、NaN、无命中零值、重复标识和记录数量；
- 精度、舍入、压缩有损选项和 endian/IBM 转换；
- 结果校验 hash、最大绝对/相对误差和允许误差来源；
- 同一查询在冷缓存、暖缓存和重复运行中的结果一致。

任何组在语义不等价、缺少坐标/元数据、改变分辨率、隐含插值或无法解释缺失数据时，停止比较并标记为“未通过正确性”，不得用速度结果补救。转换输出必须保存源文件 hash、格式版本、工具版本、命令行和参数。

## 7. 指标、缓存与资源预算

至少记录以下原始计数和墙钟时间，不用单一“压缩比”替代：

- 客户端、网络、服务器和磁盘实际读取字节；触达的 chunk、trace、spectrum、HDU 或其他物理单元数；
- 扫描记录数、索引命中数、解码值/点数、返回值数、请求数、失败重试数；
- 索引/catalog/导入/转换/派生准备时间、数据库服务时间、结果编码时间、后续计算时间；
- CPU、峰值内存、并发吞吐、p50/p95/p99 延迟、首查询与重复查询；
- 额外存储、版本更新后的重建时间、索引失效和维护频率；
- 端到端 `T_online` 与阶段时间的重叠关系。

缓存协议固定为两种：冷缓存（清空应用、OS、数据库和对象存储缓存，并记录无法清空的层）和暖缓存（同一查询预热指定次数后测量）。每组至少 5 次预热后测量 30 次，报告中位数、p95 和离散度；若运行环境不足，缩减次数必须在 manifest 中说明。并发先测 1、4、16 个客户端，超过资源上限时记录上限而不是静默降低对照组。

建议最小资源预算：单一固定硬件和容器镜像；每次实验保留 2 个 CPU 核、8 GiB 内存、至少 2 倍最大输入的本地临时空间；SEG-Y 解压和派生阶段另预留输入大小的 1.5 倍。预算不足时优先保留完整原始输入、oracle 和日志，减少并发或数据跨度，不删除基线。

## 8. H3 的成本模型与判定

设 `P` 为索引、转换、派生布局、导入和 catalog 准备的增量成本，`M` 为版本维护和重建成本，`N` 为布局有效期内的实际复用查询数，`T_online` 为包含目录、IoTDB 服务、范围读、解码、结果编码和必要后处理的在线端到端时间。单查询平均获益的必要条件是：

```text
T_online + P / N < T_best_native
```

当 `Delta = T_best_native - T_online > 0` 时，估计临界复用次数 `N_break_even = P / Delta`，并用实际查询序列验证；当 `Delta <= 0` 时，增加 N 不能凭空创造收益。带维护成本的总序列判据为：

```text
DeltaP + DeltaM + sum(T_IoTDB_i - T_baseline_i) < 0
```

其中 `DeltaP`、`DeltaM` 只计相对方案新增成本；共同的数据获取和必须保留的原始归档不能重复计入。空间放大、版本更新、索引失效和重建时间单列。若流水线阶段重叠，使用实测端到端墙钟时间，不机械相加阶段时长。

H3 通过必须同时满足：结果等价；至少一个预注册正例在冷和暖缓存下都显示正的在线节省；节省在实测复用次数下覆盖 `P+M`；并且同一表示的 B–D 或 C–E 差值能在重复运行和置信区间内解释。若只在单次热缓存或故意弱化原生基线下获益，标记为负例或不确定。

## 9. 复现 manifest 和记录格式

每次运行写一个不可变 JSON/YAML manifest，至少包含：

```yaml
run_id: <UTC timestamp + git working tree hash>
source:
  uri: <stable URL or local accession>
  sha256: <hash>
  bytes: <integer>
  format: <format and version>
  dimensions_and_layout: <dims/chunks/traces/spectra>
software:
  iotdb_fs_revision: <revision or dirty-tree hash>
  native_reader: <name/version>
  index_or_rechunk_tool: <name/version/commit>
  container: <image digest>
hardware:
  cpu: <model and cores>
  memory_gib: <value>
  storage: <device/filesystem>
  network: <link or object-store endpoint>
query:
  semantic_predicate: <serialized predicate>
  projection: <fields>
  boundary_and_units: <explicit rules>
  expected_result_hash: <oracle hash>
execution:
  cache_state: cold|warm
  concurrency: <integer>
  repetitions: <integer>
  raw_counters: <path>
  correctness_report: <path>
  preparation_costs: <path>
```

结果表每行对应一个 `run_id × group(A–E) × query × cache_state × concurrency`，禁止只保存平均值。源文件、派生布局、索引和容器镜像必须可追溯；在线 URL 需要记录访问日期和 HTTP/仓库版本，动态 `latest` 链接不能作为唯一版本标识。

## 10. 失败、停止与负结果规则

立即停止某个候选的扩展实现，但保留已完成的负结果，若发生以下任一情况：

1. 无法获得可再分发或可合法复现实验输入，或源文件 hash/版本不稳定；
2. A–E 无法构成同一语义查询，或 oracle 比较失败；
3. 原生最佳基线已用等价索引和缓存消除主要物理放大，B–E 只增加服务/准备成本；
4. 派生布局改变精度、单位、分辨率、缺失值、坐标或记录顺序，且无法给出严格等价定义；
5. 读字节、解码值数、服务器磁盘 I/O 等关键计数无法观测，导致无法区分网络、数据库和格式收益；
6. 在预注册正例和最大允许复用次数内，`T_online + P/N >= T_best_native`；
7. 资源预算不足以同时运行原生基线和 IoTDB-FS，或后台缓存/并发状态无法保持对照公平。

停止条件不是“没有加速”本身：没有加速的可解释负例仍用于检验 H3 的边界。不得把估算的规模、文献中的性能或单文件 chunk 事实写成 IoTDB-FS 已测得的收益。

## 11. 分阶段执行计划

**阶段 0：审计与冻结（0.5–1 天）**。锁定输入 URL、hash、格式/工具版本、硬件和查询语义；在任何性能运行前预注册每个候选的正例、负例、选择率、重复次数、缓存状态和并发配置（保存带时间戳的查询清单）；建立 oracle 与 manifest 模板；确认生产 FUSE/CLI 路径与 typed test adapter 分开记录。

**阶段 1：netCDF-4/HDF5（2–4 天）**。下载 OISST 多日或 MERRA-2；现场读取 chunk/filter；实现 A、B、C 的独立脚本和 D、E 的同表示接入；先跑点时间序列、小 ROI、全场和一次查询负例；通过正确性与计数可观测性门槛后再扩大时间跨度和并发。

**阶段 2：mzML（3–5 天）**。完整解析复现文件；确认 indexed offsets、binary arrays、RT/m/z 单位；实现 XIC oracle、indexed 基线和一种派生布局；按 MS1/MS2、centroid/profile 和 m/z/RT 矩阵运行；若 m/z 内部物理裁剪无法保持精确语义，停止派生方案并保留 scan-index 负例。

**阶段 3：SEG-Y（3–5 天）**。下载并校验 USGS 文件；验证实际 trace 几何和 IBM 转换；实现 offset 合并读取基线、header/geometry 索引和一种 bricks/slice 派生布局；先测薄切片、子体积与完整 trace 负例；无法证明非规则道等价时不进入 IoTDB-FS 比较。

**阶段 4：跨格式分析（1–2 天）**。按 A–E 差值拆分索引、表示和后端收益，计算 `N_break_even` 和带维护成本总账；报告正例、负例、资源限制和未测属性；只把通过正确性和成本判据的结果写入论文。

## 12. 当前仓库衔接与实现缺口

- `include/iotdbfs/QueryPlanning.h` 的 `QuerySource` 只提供 `read_payload(offset,length)` 与 `read_layout(offset,length)`；默认 `max_sources=1024`、`max_ranges=65536`、`max_layout_bytes=64 MiB`、`max_payload_bytes=1 GiB`。这些是规划边界，不代表新格式已接入。
- `include/iotdbfs/QueryModel.h` 已有 `Rows`、`DenseGrid`、`TimeSeries` 逻辑域，可承载三候选的抽象结果；它不提供格式解析、chunk/trace/spectrum 索引或派生布局实现。
- `src/Ingestion.cpp` 会保留原始 payload，并写入适配器生成的 metadata/layout；新格式需要 `FormatAdapter`/`FormatIngestionSession`，并决定 layout 是地址目录、解码块目录还是派生数组索引。
- `src/GribQueryKernel.cpp` 当前选择 GRIB message 后先完整解码，再进行空间裁剪；该事实只能作为 GRIB 的 H1 对照，不能假定 netCDF、mzML 或 SEG-Y 可以复用 GRIB 解码代码。
- 必须分别实现并测量：原始 payload 的范围读、结构布局的范围读、派生布局的范围读、结果拼接和错误/缺失语义。IoTDB 服务器实际磁盘 I/O 需要独立观测。
- 当前未发现新格式生产适配器或本方案 benchmark；文档完成不等于代码完成。任何涉及布局持久化、缓存策略、查询边界或兼容性行为的不确定设计，应先由用户确认后实现。

## 13. 交付检查清单

在论文或性能报告中引用某个候选前，逐项勾选：

- [ ] 数据 URL、版本、hash、物理布局和工具版本已冻结。
- [ ] A–E 使用同一语义谓词、投影、边界、精度和缓存预算。
- [ ] oracle、输出 hash、缺失/单位/坐标/顺序检查通过。
- [ ] 原生最佳基线、成熟索引/派生方案和负例已运行。
- [ ] 读取字节、触达物理单元、解码值数、CPU、服务时间和准备/维护成本可审计。
- [ ] 冷/暖缓存、首次/重复查询、并发和 p95 已记录。
- [ ] `N_break_even` 和总成本判据已用实际序列计算。
- [ ] 结果明确区分测量、文献事实、源码推断和待测属性。
- [ ] 未把性能预期、文献数字或估算规模写成 IoTDB-FS 实测结果。
