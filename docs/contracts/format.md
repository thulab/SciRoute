# 面向 H1–H3 的科学数据格式候选研究

研究日期：2026-09-30（Asia/Shanghai）。本报告结合当前仓库源码、环境中的 GRIB 样本元数据，以及工业项目、格式规范和原始学术论文。候选优先级是机制与工程可行性判断，不是已测得的成功概率或 IoTDB-FS 加速比。本次未修改运行代码、未运行新的性能基准、未执行 Git 写操作。

## 1. 结论与推荐组合

**首选新增 netCDF-4/HDF5 中真实存在布局错位的规则多维数组；如需更有说服力的跨领域验证，优先考虑 mzML 的跨谱图查询。** SEG-Y 是工业场景很强的补充；FITS 可作为范围较易控制的备选；传统 TIFF 和非空间组织的 LAS/LAZ 适合补充实验。候选应定义为“格式版本＋物理布局＋压缩设置＋工作负载”，不能只按扩展名选择。[R1–R4,R9–R19]

| 优先级与角色 | 应选择的实际输入 | 最有希望的目标访问 | H2 需要的机制 | 工程判断 |
|---|---|---|---|---|
| A：主候选 | 按大空间块压缩的 netCDF-4/HDF5 | 少量点或小区域的长时间序列；少量变量和层次 | 原块索引＋面向时间序列的派生分块，或适用压缩流的子块索引 | 与现有网格语义较接近；中等工作量 |
| A：跨领域主候选 | 按 scan/spectrum 组织的 mzML | 窄 m/z 窗口跨大量 scan 的提取离子色谱图；保留时间窗口 | 谱图目录＋保留时间/m/z 派生索引或分块 | 泛化价值较高；领域语义与适配成本较高 |
| B：质谱进一步扩展 | imzML 等逐像素谱图数据 | 指定 m/z 范围跨像素生成 ion image | 面向 m/z 的列向或分块稀疏布局 | 强错位；不规则质量轴和重采样需特别处理 |
| B：工业补充 | 以 trace 为主要组织单元的 SEG-Y | 时间/深度薄切片、跨 trace 的局部子体积 | trace/几何索引＋三维块或切片派生布局 | 机制明确；坐标和格式变体增加成本 |
| B：较易限定范围的备选 | 大图像/数据立方体 FITS；实际归档中的整文件压缩版本 | 小窗口、波段/谱线切片、跨曝光筛选 | HDU/坐标索引；必要时 tile 化或重分块 | 可先限定图像 HDU；收益高度依赖原压缩形式 |
| C：补充与边界验证 | strip TIFF、大 tile TIFF；未空间重排的 LAS/LAZ | 小空间窗口、跨景/跨文件查询 | strip/tile 索引或空间重排 | 成熟优化格式很强，宜同时做边界对照 |

上述工程成本为本次评估，不是实现承诺。性能优先级与论文新颖性不是同一维度：已有成熟解法的场景往往更容易验证 H1/H2，但也必须采用更强基线。

建议的最小论文组合：

- **GRIB＋netCDF-4/HDF5**：优先完成“已有结构索引”和“派生布局”两种机制的因果验证。
- **GRIB＋netCDF-4/HDF5＋mzML**：在前一组合稳定后，检验规则网格之外的跨领域适用性。
- 若质谱领域成本不适合当前周期，可用 **FITS 图像**替代第三种；若强调工业解释性，可选择 **SEG-Y**。这些是研究取舍建议，尚未决定或修改代码设计。

netCDF-4 的存储层是 HDF5，不能把它们当作两个独立物理格式来充数；netCDF classic 则具有不同的连续/记录布局，必须分开讨论。[R2]

## 2. 当前 GRIB 能证明什么，尚不能证明什么

### 2.1 已实现的是 message 级索引

当前摄入流程保留原 payload B，GRIB 私有布局 L 保存 message 的 offset、length 和变量、层次、时效等标签。它可以跳过不相关 message，减少重复扫描原文件和无关消息的读取、解码。首次读取 L 仍需解析整个索引，筛选消息也仍需遍历记录，不能将其描述为已实现倒排树或任意维度数据库索引。[C1,C2]

**冷查询的空间裁剪仍读取并解码整条 message。** typed kernel 在打开消息后调用不带裁剪窗口的 decode_values(F64, {}, {})，再从完整原生场提取区域。若完整场有 N 个点、答案有 n 个点，可能存在 N/n 的点数解码放大；这是源码推出的工作量关系，不是时延倍数。[C3]

typed adapter 有 parsed-index 和 decoded native-field 内存缓存，可让重复区域请求复用解码结果；这不等于已经建立持久化空间瓦片。此次符号审计未找到生产入口构造 typed adapter/QueryModule 的调用，不能将这些优化自动归于所有现有 FUSE、CLI 和 CrossFileClient 路径。[C4]

### 2.2 通用接口不是已经完成的多格式或多后端性能证据

QuerySource 只暴露 manifest 和 payload/layout 范围读；逻辑模型已有 Rows、DenseGrid、TimeSeries。它们为扩展提供了边界，但当前 GRIB 解码器、坐标处理和生产入口仍需分别核查。接口不能替代新适配器实现或真实后端性能实验。[C5]

FormatIngestionArtifacts 只返回 metadata 与不透明 layout，原字节由摄入模块复制。接口没有把 L 限死为地址索引，但当前 GRIB 的 L 确实只是指向原 B 的消息索引；尚未发现其持久化解码数组、时空转置或 tile 副本的实现。因此，“索引已有结构”与“构建派生数值布局”应作为不同工作量评估，不能说后者已完成。[C6]

IoTDB 客户端使用 READ_OBJECT(data,offset,length) 请求范围。该接口不能独自证明数据库磁盘层实际读取了同样少的字节；论文要另测服务器 I/O。旧跨文件筛选与新 typed catalog 的下推行为也不同，不能笼统声称所有语义谓词已在数据库执行。[C7]

### 2.3 当前真实样本的代表性

仓库常见 GRIB 后缀递归搜索没有发现二进制样例，单元测试多由 ecCodes 样例生成。环境的 /mnt/local 有三组相关文件，各组 _0/_1/_2 三份同大小文件；本次只检查每组 _0 的元数据，未验证副本哈希相同，也未测性能。

文件共同前缀：/mnt/local/paper_check_KWBCGFS_CMRA_。

| 文件后缀 | 字节数 | message 数 | 内容概况 | 零位宽 message 数 |
|---|---:|---:|---|---:|
| 100_2024031800_GLB_0_0_0P25_0.grib2 | 2,446,828 | 22 | 22 个等压层；step=0 | 3 |
| 100_2024031800_GLB_1_2_0P25_0.grib2 | 549,606,683 | 4,598 | 22 个等压层；209 个 step | 209 |
| 105_2024031800_GLB_1_2_0P25_0.grib2 | 19,054,300 | 209 | hybrid level=1；209 个 step | 0 |

三组均为 clwmr、regular_ll、1440×721，每条 message 对应 1,038,240 个点。前两组含 179 B、bitsPerValue=0 的消息。论文应单列这种极小编码情况，不能用其代表一般压缩场；这些文件也尚不能支持跨变量泛化。检查方法为 grib_get 读取 shortName、typeOfLevel、level、step、Ni、Nj、numberOfDataPoints、totalLength、packingType、bitsPerValue、gridType，未读取 values 或以命令耗时充当基准。

## 3. 首选：netCDF-4/HDF5 的块布局错位

### H1：空间场生产，点时间序列消费

应找原本就按时间步输出大空间场、且使用大压缩 chunk 的气候/海洋产品。后续任务只需少数位置跨许多时间步的数据时，选择虽小，却可能与大量大块相交。普通 HDF5 压缩过滤按 chunk 工作；选择子区域不自动意味着只解压子区域。块形状与缓存配置因此直接影响成本。[R1,R4]

### H2：两个层次必须分别验证

1. **已有结构索引**：记录变量、维度、chunk 地址和过滤器信息，减少文件打开、元数据遍历与多文件准备。Kerchunk 和 DMR++ 已提供这类核心能力。[R3,R6]
2. **改变可访问单元**：当所有时间步的大块都与查询相交，外置地址表无法消除块内放大。需要面向点/小区域时间序列的重分块，或适用编码的压缩流内部索引。Rechunker 与 chunkindex 是直接相关工作。[R4,R5]

2026-02-20 正式发表的 chunkindex 论文针对压缩 netCDF-4 的子块访问，评估了本地、远程和云环境。它需要索引准备，且不是对所有读取模式都占优；论文的特定实验索引约为原数据的 16%，不能将这个比例泛化。应把它作为紧邻相关工作，而不是将“压缩块内选择读取”描述成未经研究的空白。[R4]

### H3：机会与反例

我的判断是：大源块、低物理选择率、重复分析充分、跨文件元数据成本明显时，值得优先试验。整场读取、源块已匹配、热缓存充分或只查一两次时，额外服务与准备成本可能不值得。

**必须比较**：调优原生 netCDF/HDF5、Kerchunk、适用的 DMR++/chunkindex，以及同精度、适配工作负载的重分块表示。不能把原生 chunk cache 故意设小，也不能用低效逐点调用代表原生库的最佳可行用法。[R1,R3–R6]

建议首个工作负载采用规则经纬度网格、少数变量/层次、点或小 ROI 的长时间序列；用较大 ROI 和完整场作为反例。这是范围控制建议，不要求立刻实现任意曲线网格。

### 生产案例：HDF5 本身也在改善

NSIDC、NASA 和 HDF Group 参与的 ICESat-2 技术研究显示，数据布局和 I/O 设置共同调优后，原生 HDF5 可以接近 Kerchunk/Zarr 的访问表现；单独改变某一设置并不保证更快。该材料应标作作者技术研究，不擅称同行评审结果。[R7]

NSIDC 于 2026-01-12 的官方报道明确，云优化 ATL03 已于 **2025-07-31** 发布。因此，若选 ATL03，应区分旧归档与当前云优化版本，不把历史问题泛化到全部现行 HDF5 产品。沿轨 photons/segments 也不能直接套用 GRIB 规则二维网格。[R8]

netCDF classic 可作为低成本控制项：头信息与规则布局能推导地址，但没有 netCDF-4 压缩块的同类解码收益。其机会主要在跨记录小读和请求组织，不能与 netCDF-4 合并成一个性能结论。[R2]

## 4. 跨领域强候选：mzML；进一步延伸到 imzML

### H1：一个谱图的存储顺序，不适合跨谱图抽取

mzML 按谱图表达数据，indexed mzML 可直接定位特定谱图。因此，“给 scan 加 offset”本身不是充分增量。更合适的目标是提取离子色谱图：选窄 m/z（质荷比）范围，跨很多 scan 读取强度随保留时间的变化。若原表示以完整谱图为处理单元，目标只占每幅谱图的很小一部分。[R9,R10]

建议从 MS1、保留时间与 m/z 范围查询开始，保留原始峰和值的语义。单谱图读取作为对照；profile/centroid、MS1/MS2 和不同采集方式分组，避免混成一个结果。

### H2 与已有强基线

已有结构层可提取 scan、保留时间、MS level 等目录。派生层则按保留时间与 m/z 组织索引或分块，让查询少触达峰值数据。这是研究建议，具体稀疏表示与物理布局尚未决定。

该方向有强先例：mzDB 将 LC-MS 数据存入 SQLite 并使用多种索引；mzMLb 利用 HDF5 的分块、缓存与随机访问。2025-11-24 发表的研究进一步比较 SQLite、DuckDB、Parquet 的六类质谱子集查询。这些工作同时支持可行性并提高了基线要求。[R10–R12]

**不能只与逐行 XML 全扫描比较。** 至少包括 indexed mzML、mzMLb，以及与目标查询直接相关的 mzDB 或 SQL/列式方案。若某派生方案不保留完整源元数据，要将其作为辅助分析表示，并公平计入原文件保留与额外空间，而不是视作无损替换归档。

### H3 与 imzML 的扩展边界

多文件、反复窄窗口提取以及昂贵的重复谱图处理，可能提供系统收益空间；一次顺序读全谱或只取少数已可随机定位的 scan，数据库服务开销可能占上风。这是待验证推断。

质谱成像把类似错位扩展到“逐像素存完整谱图”与“取一个 m/z 范围跨所有像素”。2026 年 Thyra 论文明确区分 CSR 的逐像素优势与 CSC 的跨像素 m/z 优势，并使用 Zarr v3。因此该扩展有清晰机制，也已有最近相关工作。其转换包含重采样，不能把重采样结果与原始峰值精确查询未经检查就视为等价。[R13]

与规则网格相比，这一领域需要处理不等长谱图、稀疏值、质量轴和仪器元数据；通用 Rows 接口提供基础，但不使适配成为零成本。[C5]

## 5. 工业强例：SEG-Y 的 trace 与横向切片

对按 trace 顺序组织的三维地震数据，完整 trace 读取可能很自然，而横跨大量 trace 的薄时间/深度切片或小子体积可能产生离散读取。建议分别测试 trace 筛选与 trace 内样点访问，不预设每次必须读完整 trace。成熟原生读取器可直接定位或内存映射，必须纳入对照。[R14,R15]

H2 的第一层是 trace/header/几何关系索引；第二层是面向子体积或切片的三维块、方向化派生数据。OSDU/OpenVDS 官方文档已经提供三维 bricks 与二维 Fast Slices，并支持不同存储方式。这是工业界直接承认工作负载驱动布局价值的证据，但不是 IoTDB 已优于这些方案的证据。[R14]

建议基线为调优 segyio/同类原生读取与 OpenVDS。比精确查询时应使用同等精度、分辨率和压缩条件，不能用低分辨率 LOD 或有损配置获得的速度代表等价查询。准备成本、额外方向切片的空间开销、几何坐标映射和非规则 trace 分布均应计入。我的评估：H1/H2 机制强，但实现成本通常高于先限定规则 netCDF 数组。

## 6. FITS、TIFF、点云：适合作为补充，也适合检验边界

### FITS

建议先限定图像 HDU 或规则数据立方体：小窗口、少数波段/谱线通道、跨曝光筛选。HDU/坐标目录可减少定位与重复准备；大块或整文件压缩输入可能需要 tile 化，才能改变局部访问成本。

NASA/HEASARC 的 fpack 已使用 tile-compressed FITS，允许读取部分图像时只解压相关 tiles。因此强基线应包含 CFITSIO 的 subset 读取与适当 tile 压缩，而不只是整文件 gzip 的全解压。对未压缩且已方便随机读取的 FITS，或已经匹配查询的 tile 布局，额外服务未必有收益。浮点量化和压缩精度必须统一。[R16]

### GeoTIFF 与 OME-TIFF

真实 strip TIFF 或过大 tile 与小窗口任务可能错位；已有 IFD/offset 索引能定位 strip/tile，但不能消除大单元内部的读取与解码。若重铺瓦片后再由 IoTDB 存储，应把 tile 化收益与数据库收益拆开。

OGC COG 标准已经组合 tiles、分辨率层次和 HTTP Range；USGS Landsat Collection 2 已按 COG 分发。因此不能拿“下载整幅 Landsat 后裁剪”作唯一基线，且原分辨率数值查询不能与降采样 overview 混比。[R17,R18]

OME-TIFF 的 Z/C/T 选择和局部体数据可扩大至显微领域，但应比较成熟 tiled/pyramidal OME-TIFF 与 OME-Zarr。相关论文与 Zarr v3 sharding 规范已经覆盖分块、层次与小对象合并问题；不能以“分块必然导致海量小文件”为数据库必要性的前提。[R20–R22]

### LAS/LAZ 与 COPC

若输入点顺序不具空间局部性，小三维 ROI 可能触达许多区段；空间索引可以筛候选，空间重排与独立可解码块才能进一步减少数据处理。该机制是否成立，须检查真实原文件的顺序和压缩块。

COPC 已将 LAZ 数据按 clustered octree 组织，并保存层次与块地址供选择读取。它应是强基线，甚至可作为“源布局已经匹配查询”的边界组。不能将 COPC 当作缺乏空间索引的传统点云，也不宜把重新实现其核心思想作为论文新颖性。[R19]

## 7. 能分别验证 H1、H2、H3 的实验设计

以下是研究方案建议，尚未编写适配器或更改存储设计。先选一个真实数据集和两类相反访问，再决定是否扩大实现。

### 7.1 分解表示收益与后端收益

| 组别 | 表示与后端 | 主要用途 |
|---|---|---|
| A | 原文件＋调优原生库，保留原生已有索引 | 公平的原始表示基线 |
| B | 同源文件＋外置结构索引，本地文件或普通范围读后端 | 与 A 比较结构索引收益 |
| C | 工作负载适配派生布局，同一非 IoTDB 后端 | 与 B 比较重排/分块收益 |
| D | 与 B 相同的表示、索引和缓存预算，IoTDB-FS | 与 B 比较系统增量 |
| E | 与 C 相同的派生布局和缓存预算，IoTDB-FS | 与 C 比较派生布局下的系统增量 |

另与每种格式的成熟优化方案作端到端对照；H2 的对照不应停留在内存 mock。保持数据位置、硬件资源与连接条件可比，否则把远近访问差异算到格式或数据库上。

H1 不要求所有科学数据都存在错位；记录哪些生产布局在哪些任务上有放大。H2 不要求每个格式都构造两种优化；应说明哪一层实际改变了搜索、触达物理单元或解码工作量。H3 应允许“不获益”的范围，并解释原因。

### 7.2 摊销与盈亏平衡

令 P 为相对基线额外的索引/派生准备成本，N 为在该版本或布局失效前复用的查询次数，T_online 为包含目录、数据库服务、范围读、解码、输出和文件系统开销的实测在线时间。只有在

    T_online + P / N < T_best_native

时，才有含准备成本的平均端到端优势。若 Delta = T_best_native - T_online > 0，简化模型中的临界条件为 N > P / Delta；若 Delta <= 0，单靠增加 N 不能获得优势。对混合工作负载应比较实际查询序列的总成本，不用一个平均查询替代全部分布。

P 应使用两组方案的增量差，扣除共同必须承担的源数据获取等成本，并计入重建、版本变化和额外复制。空间放大单独报告。若流水化阶段重叠，应使用端到端实测时间，不能把重叠阶段的墙钟时间机械相加。

### 7.3 至少测量这些项目

- 候选文件/物理单元数、实际触达数、返回记录或数值数；区分逻辑选择率与物理选择率。
- 客户端、网络与服务器实际读字节；范围 API 返回量不等于磁盘读量。
- 解码值/点数量、解码 CPU、索引和 catalog 准备、请求次数、数据库服务、结果编码与后续处理。
- 冷缓存与热缓存，首次与重复查询，单并发与适当并发，延迟分布与 p95。缓存预算及预加载成本保持可比。
- 摄入/索引/派生耗时、额外存储、版本更新后的再准备，以及不同 N 的总耗时。
- 输出的数值、精度、缺失值、单位、坐标、顺序、边界条件与分辨率一致性。

不要把“压缩读字节/未压缩输出字节”不加说明就称作通用读取放大系数。优先同时报告原始计数；解码放大可明确用已解码值数/答案所需值数，并解释中间必要计算。

每个主候选至少加入大范围/完整扫描、已优化原布局、很低复用次数三种反例。对当前 GRIB，还应区分零位宽消息与一般压缩消息，并准确标记实际调用的 FUSE/CLI/typed 路径。

## 8. 投稿中的相关工作与贡献边界

H1 和 H2 有广泛一手先例；这增强了立题依据，却意味着三句话本身不能自动构成首创性。尤其 Kerchunk 已覆盖 GRIB、netCDF/HDF5、TIFF、FITS 等格式，抽取范围与压缩信息，支持多文件聚合和多种后端；“多格式外置索引＋后端无关”需要和它正面对比。[R3]

更可信的论文论证，应具体回答：IoTDB-FS 支持哪些此前对照方案不能同样方便或有效完成的查询；哪些语义真正映射到可裁剪单元；何时选择已有结构索引、何时必须改变布局；相同表示跨后端是否仍有效；IoTDB 的额外服务在何种规模、选择率、复用度和并发下值得承担。FUSE 接口、数据库存储或格式数量本身都不能代替这些证据。

本次建议优先完成 netCDF-4/HDF5 的小区域长时间序列验证，再决定是否投资 mzML 的跨谱图适配；这样既补上当前 GRIB 尚未完成的持久化派生布局证据，也能避免在未确认物理剪枝效果前扩展过多格式。

## 9. 仓库证据索引

以下行号来自此次读取的工作区，后续代码修改后可能移动。完整临时审计在 /tmp/iotdbfs-grib-audit.md；本报告已保留决策所需主要事实，不依赖该临时文件才能阅读。

- [C1] src/GribWriteStreamer.cpp:149、249；src/gribUtils.cpp:703、752；src/GribLayout.cpp:19：扫描、消息标签和 offset/length 布局。
- [C2] src/GribQueryKernel.cpp:422；src/GribFormatQueryAdapter.cpp:639、1071：完整布局读取、解析与消息筛选。
- [C3] src/gribUtils.cpp:619、1229；src/GribQueryKernel.cpp:485、502、550：整 message 读取、全场解码、后续区域选择。
- [C4] include/iotdbfs/GribFormatQueryAdapter.h:10；src/GribFormatQueryAdapter.cpp:740；tests/unit/test_GribFormatQueryAdapter.cpp:602、645、673：缓存及相关测试。生产路径边界另见 src/GribFormatAdapter.cpp:135、src/CrossFileClient.cpp:341。
- [C5] include/iotdbfs/QueryPlanning.h:94；include/iotdbfs/QueryModel.h:279：范围读与逻辑域接口；tests/unit/test_GribFormatQueryAdapter.cpp:42：内存测试源。
- [C6] include/iotdbfs/FormatAdapter.h:23；src/Ingestion.cpp:75、92、125；src/GribFormatAdapter.cpp:61；src/GribFormatQueryAdapter.cpp:589：原字节与适配器产物边界。
- [C7] src/IoTDBStructuredBlobBackend.cpp:406、621、679；include/iotdbfs/MetadataSetIndex.h:58；src/QueryPlanning.cpp:137、216：范围请求及不同目录/下推路径。
- [C8] tests/unit/test_GribFormatQueryAdapter.cpp:142、177：生成式小网格测试；tests/performance/read_perf.cpp:113：历史整文件读取测试。

docs/reports/grib-query-migration-acceptance.md:72 记载的是 2026-09-25 的历史功能验收，不是本次新跑的性能实验。

## 10. 一手文献与工业资料

所有在线资料的本次访问日期为 2026-09-30。动态文档的“current/latest”不是论文应固定的软件版本；实验时还需锁定版本、配置和数据产品版本。

1. [R1] The HDF Group. Chunking in HDF5. 官方文档；重点 partial I/O、compression、chunk cache。https://portal.hdfgroup.org/documentation/hdf5/latest/hdf5_chunking.html
2. [R2] Unidata. NetCDF-C File Format Specifications. 官方规范；区分 classic 与 netCDF-4/HDF5。https://docs.unidata.ucar.edu/netcdf-c/current/file_format_specifications.html
3. [R3] fsspec/kerchunk. 官方项目文档；统一范围引用、多格式、多后端与虚拟聚合。https://fsspec.github.io/kerchunk/
4. [R4] Penard et al. (2026-02-20). A new sub-chunking strategy for fast netCDF-4 access in local, remote and cloud infrastructures, chunkindex V1.1.0. Geoscientific Model Development 19, 1519–1535. https://gmd.copernicus.org/articles/19/1519/2026/
5. [R5] Rechunker. Algorithm. 官方算法说明；重分块与中间存储。https://rechunker.readthedocs.io/en/latest/algorithm.html
6. [R6] OPeNDAP. How to create DMR++ sidecar files? 官方指南；数据块地址与压缩信息。https://www.opendap.org/how-to-create-dmr-sidecar-files/
7. [R7] Lopez et al. (2025). Evaluating Cloud-Optimized HDF5 for NASA’s ICESat-2 Mission. 作者技术研究；不是本报告确认的同行评审论文。https://nsidc.github.io/cloud-optimized-icesat2/
8. [R8] NSIDC (2026-01-12). Cloud-Optimized HDF5 Boosts Access to ICESat-2 Data. 官方生产实践报道，包含 2025-07-31 发布日期。https://nsidc.org/news-analyses/news-stories/cloud-optimized-hdf5-boosts-access-icesat-2-data
9. [R9] Martens et al. mzML—a Community Standard for Mass Spectrometry Data. Molecular & Cellular Proteomics 10(1), 2011；2010 年先行在线。DOI:10.1074/mcp.R110.000133. https://pmc.ncbi.nlm.nih.gov/articles/PMC3013463/
10. [R10] Bouyssié et al. (2015). mzDB: A File Format Using Multiple Indexing Strategies for the Efficient Analysis of Large LC-MS/MS and SWATH-MS Data Sets. 原始论文摘要记录。https://pubmed.ncbi.nlm.nih.gov/25505153/
11. [R11] Bhamber et al. mzMLb: A Future-Proof Raw Mass Spectrometry Data Format Based on Standards-Compliant mzML and Optimized for Speed and Storage Requirements. Journal of Proteome Research 20(1), 2021；2020 年先行在线。DOI:10.1021/acs.jproteome.0c00192. https://pmc.ncbi.nlm.nih.gov/articles/PMC7871438/
12. [R12] Kumler, LaRue and Ingalls (2025-11-24). Storing Mass-Spectrometry Data in Simple Databases Enables Flexible and Intuitive Exploration without Time or Space Penalties. Journal of Proteome Research 24, 6174–6185. DOI:10.1021/acs.jproteome.5c00721. https://pubs.acs.org/doi/abs/10.1021/acs.jproteome.5c00721
13. [R13] Native, Memory-Bounded Conversion of Mass Spectrometry Imaging Data into the SpatialData Ecosystem (2026). Journal of the American Society for Mass Spectrometry. DOI:10.1021/jasms.6c00169. https://pubs.acs.org/jamsef/article/doi/10.1021/jasms.6c00169/5435687/Native-Memory-Bounded-Conversion-of-Mass
14. [R14] OSDU/OpenVDS. VDS Deep Dive. 官方文档；bricks、Fast Slices、分辨率及存储机制。https://osdu.pages.opengroup.org/platform/domain-data-mgmt-services/seismic/open-vds/vds/deepdive/deepdive.html
15. [R15] Equinor/segyio. 官方读取器项目及文档。https://github.com/equinor/segyio
16. [R16] NASA HEASARC/CFITSIO. fpack Home Page. 官方文档；tile-compressed FITS、局部解压与量化选项。https://heasarc.gsfc.nasa.gov/docs/software/fitsio/fpack/fpack.html
17. [R17] OGC (2023). Cloud Optimized GeoTIFF Standard 1.0, 21-026. https://docs.ogc.org/is/21-026/21-026.html
18. [R18] USGS. Landsat Collection 2. 官方产品资料，包含 COG 分发。https://www.usgs.gov/landsat-missions/landsat-collection-2
19. [R19] Cloud Optimized Point Cloud Specification 1.0. COPC 官方规范。https://copc.io/
20. [R20] Zarr. Sharding codec specification. 官方规范；inner chunks、范围读取与 shard 索引。https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/index.html
21. [R21] Moore et al. (2021). OME-NGFF: a next-generation file format for expanding bioimaging data-access strategies. Nature Methods. https://www.nature.com/articles/s41592-021-01326-w
22. [R22] Moore et al. (2023). OME-Zarr: a cloud-optimized bioimaging file format with international community support. Histochemistry and Cell Biology. https://link.springer.com/article/10.1007/s00418-023-02209-1

部分 PMC/出版社直连遇到访问限制时，本次使用原始论文的公开索引、作者版本或项目资料交叉核查；本报告不引用无法核实的图表加速比。全文没有将其他论文的性能测量当作 IoTDB-FS 的实测结果。
