# HDF5 语义子结构与真实场景证据

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

_交付对象：IoTDB-FS VLDB Research 合作者 · 2026-10-07 · 状态：候选资料已复核；GEOS 单文件及 16 时次跨文件时间筛选已实测_

---

## 📋 结论摘要

真实场景存在，但要把**文件内 chunk 剪枝**和**跨文件候选剪枝**分成两个命题。已经取得可复现实测证据的是公开 GEOS-5.7.2 `inst3_3d_asm_Np`：一个文件的 `/T` 有 2,688 个 chunk，变量、时间、压力层和经纬度绑定已由生产 Adapter 验证，单点仅命中一个 chunk；其单文件 A–E 复测见[报告](../reports/hdf5-geos-adapter-review-rerun.md)。因此首轮规模实验应沿用这一**真实连续时次、未改写的原文件**，而不是先切换到尚未核验物理布局的产品。

以下三类仍是后续扩展候选：

1. **NASA HIRDLS/Aura L2 HDF-EOS5**：每文件约 5,600 个 profile scans，同时包含多个大气变量、pressure、time、latitude 和 longitude，最贴近“变量 × 层次 × 时间”的单文件语义选择场景[^1]。
2. **MPAS-Atmosphere NetCDF-4/HDF5 或 NASA MERRA-2**：真实天气模式／再分析输出包含多变量、垂直层、时间和空间 cell/grid，可作为 IoTDB-FS 的主天气 workload[^2][^3]。
3. **NASA GEDI HDF5**：单文件约 2 GB，8 个 beam，每 beam 有 765 个 SDS layer；NASA 已提供按 ROI、beam 和 SDS 读取的实际工具，最能证明多 dataset 语义筛选需求[^4][^5]。

NOAA NARR 可作为大空间 chunk 的负对照：服务端元数据显示 `air[time=570][level=29][y=277][x=349]` 的 `_ChunkSizes=[1,1,277,349]`，按声明的尺寸约有 16,530 个 time×level chunk[^6]。这还是**远端元数据证据**，并非本地原件的 HDF5 chunk-offset manifest；下载并校验原件前，不应把它记为已测的物理读取结果。

现阶段应把“真实语义需求”“样本 manifest”“配对性能”分开报告。GEOS 已有单文件生产 layout、结果正确性和 A–E 计时，以及[16 个完整时次的跨文件时间筛选实验](../reports/hdf5-geos-time-scale-results.md)。该实验实证了 IoTDB-FS 内部 catalog 剪枝，但没有发现相对普通 FS 的端到端速度优势。HIRDLS、MPAS、MERRA-2、GEDI 和 NARR 仍需逐个下载原件、验证格式与 Adapter 能力，再升级证据等级。

## 🎯 研究对象与证据等级

### HDF5 子结构定义

论文中的 HDF5 子结构建议定义为适配器私有的 **语义物理单元**：

```text
u = (object_token, dataset_path, chunk_coord,
     logical_extent, typed_attributes, physical_locator)
```

- `object_token`、`dataset_path`：前者标识对象，后者携带 group 层次与用户选用的路径；路径不应作为唯一身份，因为 HDF5 支持 hard link。
- `chunk_coord`、`logical_extent`：描述 chunk 在 time、level、latitude、longitude 或 scan 轴上的范围。
- `typed_attributes`：变量名、单位、pressure/model level、valid time、beam、scan 或元素等语义属性。
- `physical_locator`：文件偏移、存储长度、filter pipeline 和可合并的 byte range。
- chunk 级 min/max、空值数或 ROI 统计**不是当前 layout 的通用能力**；若未来引入，须另列索引构建、空间与正确性成本，不能将其算入现有剪枝率。

在 Structured Blob 中保持以下映射：

```text
B          = 原始 HDF5 字节
Sigma_H5   = 当前文件级格式、完整性和数量等汇总；尚无通用时间覆盖
L_HDF5     = dataset/chunk manifest、语义范围和物理 locator
```

HDF-EOS5 标准将 swath 组织为 data fields、geolocation fields、dimensions 和 dimension maps；压缩字段需要先采用 chunking，但具体产品是否采用何种 chunk 形状仍需读取样本确认[^7]。

```mermaid
flowchart LR
    accTitle: HDF5 剪枝证据流程
    accDescr: 查询先用文件级和 dataset 级语义摘要缩小候选，再用 chunk manifest 定位少量物理单元，最后读取并解码原始 HDF5 字节

    query([📥 语义查询]) --> sigma[🔍 Sigma_H5 文件筛选]
    sigma --> dataset[🏷️ Dataset 与属性筛选]
    dataset --> layout[📋 L_HDF5 chunk manifest]
    layout --> ranges[📦 合并 byte ranges]
    ranges --> decode[⚙️ HDF5 解码]
    decode --> result([📤 Query Batch])

    classDef process fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#1e3a5f
    classDef output fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#14532d
    class query,result output
    class sigma,dataset,layout,ranges,decode process
```

### 证据等级

| 等级 | 已证明内容 | 尚未证明内容 |
| --- | --- | --- |
| A | 官方资料证明真实数据结构和用户查询语义 | 具体 chunk 数量、offset 和读取成本 |
| B | 样本 manifest 证明 dataset、shape、chunk、filter 和 locator | 配对性能和端到端收益 |
| C | 相同原件与结果的 raw／index-only 配对计时和物理 I/O；若测试 derived，另配同一布局的 FS 对照 | 跨文件索引收益、跨格式迁移性须另证 |

GEOS 单文件已有 **B** 级 layout/offset 和正确性证据，以及单文件 A–E 的 **C** 级配对计时；16 个真实时次的跨文件时间筛选又取得独立的 **C** 级配对计时与身份校验，结果显示目录剪枝但未显示相对 FS 的速度优势。HIRDLS、MPAS、MERRA-2、GEDI 主要处于 **A**；NARR 的远端 chunk 元数据只是部分 B 的线索。不要把 A 级资料直接写成实测物理剪枝结果。

## 📊 真实场景候选

### GEOS-5.7.2 `inst3_3d_asm_Np`：首轮规模实验主负载

[NASA GMAO 公开目录](https://gmao.gsfc.nasa.gov/gmaoftp/ops/GEOS572_Sample_Data/inst3_3d_asm_Np/)提供 2011-07-01 与 2011-07-02 的 16 个三小时时次文件。已核验的 09:00 原件为 445,984,379 B、SHA-256 `fd763bf40ed34e0e4f91b9da1bd94e272e2145c95a0323b292f7b48b143a723e`；文件有 18 个 dataset，`/T` 的 `[1,42,721,1152]` 值布局采用 `[1,1,91,144]` chunk。生产 layout `/5` 已从属性及 dimension-scale 证据绑定 `/time`、`/lev`、`/lat`、`/lon`，查询 `standard_name=air_temperature` 和物理 Float32 值视图；详情见样本审计（服务器归档：`docs/hdf5-main-workload-candidates.md`）与[语义验收](../reports/hdf5-geos-semantic-grib-control.md)。该公开样本的 `valid_range` 与正常温度值冲突，所以主实验不得把严格 CF 科学值视图与物理 Float32 视图混在一起。

GEOS 的边界也必须写清楚：每个时次几乎共享同一变量表，`standard_name=air_temperature` 是**文件内 dataset 选择**，不是高选择性的**跨文件筛选**；每个文件的时间坐标以该文件的参考时刻为单位。新摄取的精确版本现在可用经验证的 `utc-point/1` 覆盖标记，在目录阶段按统一 UTC 范围淘汰未命中文件；旧版没有该标记时不得假装已完成时间剪枝。N=16→K=1 的实际候选缩减见[规模实验](../reports/hdf5-geos-time-scale-results.md)。

### HIRDLS/Aura L2 HDF-EOS5：后续 swath 扩展候选

NASA 的 HIRDLS2 产品每个文件包含一天的垂直 profile 数据，约 5,600 个 profile scans；变量包括 O₃、HNO₃、H₂O、CFC、N₂O、温度、位势高度和气溶胶等，数据同时带有 time、latitude、longitude、pressure 和 swath 元数据[^1]。

适合的查询是：

```text
variable ∈ {O3, H2O, temperature}
pressure ∈ [50, 100] hPa
time ∈ [t1, t2]
latitude/longitude ∈ ROI
```

它同时具备多变量、多 profile、多压力层和时间轴，候选物理单元可以定义为：

```text
(swath_field, profile_range, pressure_range, time/geolocation range, chunk)
```

局限是公开产品页面没有给出每个 dataset 的实际 chunk shape 和 byte offset，需要下载一个文件后用 `h5py` 或 HDF5 工具生成 manifest。其 swath/geolocation 与逐 profile 坐标关系也不属于当前 CF 规则网格自动绑定的已验证范围；必须先审计 HDF-EOS5 对象关系、dtype/filter、目录完整性和可访问性，再决定是否实施 swath 语义组件。

GEOS 目录计时[复核](../reports/hdf5-geos-time-scale-results.md#fs-预建目录计时口径复核)后，HIRDLS2 更适合作为**下一项只读负载试点**，而非立即替换主基线：NASA Earthdata 列出 1,141 个 V007 granule，每日文件约 5,600 个 profile；时间、经纬度与目标变量位于同一 swath 的不同字段，因而可测试逐 profile 的时间／空间选择以及变量、压力层和物理 chunk 的联合剪枝。只按文件日期筛选仍会退化成与 GEOS 类似的小型时间目录，不构成新的系统优势。NASA 的[HIRDLS 产品说明](https://avdc.gsfc.nasa.gov/PDF/aura_validation_v1.0.pdf)将逐 profile Time 标为 TAI93；不能直接套用 GEOS 的 UTC 单点转换。真正运行前须取得 V007 原件，核验 swath 关联、时间转换、chunk/filter、稀疏命中数及原生 HDF-EOS5/HDF5 对照，并为 FS 建立**相同语义**的 profile 索引。当前生产语义组件只含 CF 规则网格与 GEOS 专用规则，不自动支持 HIRDLS swath。

### MPAS-Atmosphere NetCDF-4/HDF5：有条件的天气模式候选

MPAS 的 I/O 配置**可以**选择 `io_type='netcdf4'`，即使用 HDF5 支撑的 NetCDF-4 文件；stream 可以组织多次时间输出，并选择具体变量或变量数组[^2]。官方文档同时说明默认 I/O 为 `pnetcdf`，因此不能仅凭 `.nc` 扩展名或产品名称推定为 HDF5。字段表包含 `theta`、`theta_m`、`u`、`w`、`qv` 等 `nVertLevels × nCells × Time` 结构[^8]。

适合的查询是：

```text
variable ∈ {theta, temperature, u, qv}
Time ∈ [t1, t2]
nVertLevels ∈ {selected pressure/model layers}
cell ∈ spatial ROI
```

它与 IoTDB-FS 的气象主线接近，但当前 Adapter 不自动将非结构化 `nCells` 映射到规则经纬度 ROI。需要先取得真实的、经 magic bytes 验证的 NetCDF-4 history/diagnostics 原件，确认 chunk 是否沿 `Time` 和 `nVertLevels` 对齐，并界定 cell→ROI 的独立语义组件和边界放大。

### MERRA-2 NetCDF-4/HDF5：真实再分析多变量场

MERRA-2 提供 576×361 的全球网格、72 个 native model levels 或 42 个 pressure levels；2D 输出按小时、3D 输出按 3 小时提供[^3]。标准产品的[官方文件规范][^9]列出 `inst3_3d_asm_Np` 及其压力层变量，并说明文件格式、经纬度、时间和变量级 metadata。MERRA-2 数据目录还列出温度、风、水汽、臭氧、垂直速度等 3D pressure-level 变量[^10]。

适合的查询是：

```text
variable ∈ {T, U, V, QV, OMEGA}
pressure_level = 500 hPa
valid_time = t
lat/lon ∈ ROI
```

MERRA-2 的语义候选数量很强，适合展示跨变量、压力层和空间 tile 的选择；但不同 collection 的时间组织可能不同，实际样本必须检查是否为单时刻文件、多个 time records，或跨文件时间分片。

### GEDI02_A/02_B：多 dataset 语义筛选的强实证

GEDI L2A 用户指南给出约 2 GB 的 HDF5 文件、8 个 beam 和每 beam 765 个 SDS layer；每个 shot 有唯一 shot identifier，并带有地理位置、时间、height/RH 等信息[^4]。NASA 提供的 `GEDI_Subsetter` 已支持按 ROI、beam 和 SDS 选择，并明确指出每个文件包含 thousands of layers，精确 ROI 和 layer 选择可以减少处理开销[^5]。

适合的查询是：

```text
beam ∈ selected beams
SDS ∈ {rh, elevation, quality_flag}
delta_time ∈ [t1, t2]
latitude/longitude ∈ ROI
```

GEDI 最适合证明“多 dataset + 层次/时间/空间属性确实产生了真实用户需求”。局限是官方资料没有公布产品 chunk shape，且 shot 级不规则地理坐标、可变长度字段和跨 beam 对齐不能直接假定由当前规则网格绑定承载。先做只读 inventory 与支持类型审计，再决定是否投入该负载。

### NARR NetCDF：待下载原件的 chunk 级负对照

NOAA PSL 的 NARR `air.mon.mean.nc` 服务端元数据显示，`air` 的逻辑形状为 `time=570 × level=29 × y=277 × x=349`，属性 `_ChunkSizes=[1,1,277,349]`[^6]。这暗示每个 time×level 组合覆盖一个完整空间 chunk，约有 `570 × 29 = 16,530` 个逻辑位置；实际已分配 chunk、过滤器和偏移必须从下载原件核验。服务端同时报告 NetCDF-4/HDF5 来源信息，但远端 OPeNDAP 属性不能替代文件 magic 与 HDF5 API 验证。

适合的控制查询是：

```text
level = 500 hPa
time = one month
lat/lon = small ROI
```

若原件验证了该形状，它可以测量 time/level 选择对 chunk 数和读取字节的影响；由于空间维度完整包含在一个 chunk 中，ROI 不会带来强空间物理剪枝。其经纬度还是二维曲线网格，当前自动规则网格语义绑定不适用。因此它应作为 ordinal chunk 微基准和布局负对照，而不是多 dataset 主案例。

### Eiger/NeXus 与 NXtomo：跨领域物理单元证据

Diamond Eiger2 的真实采集示例包含 3,600 帧，数据被分成 1–1000、1001–2000 等多个 HDF5 文件，并支持 rotation、grid scan 和 line scan[^11]。NeXus `NXtomo` 标准则把 detector data 定义为 `[nFrames,xSize,ySize]`，并明确提供 `image_key[nFrames]` 和 `rotation_angle[nFrames]`[^12]。TomoBank 的公开数据包含 1,500 projections、10 white fields、10 dark fields 和 2448×1080 detector[^13]。

适合的查询是：

```text
frame/rotation_angle ∈ interval
image_key = projection
pixel region ∈ ROI
```

这个场景证明了真实采集系统中“扫描轴 → frame/file/chunk”的物理定位需求，但变量和压力层语义较弱，适合作为跨领域补充。

## 🔬 推荐交付实验包

### 优先级与进入门槛

| 优先级 | 数据族 | 用途 | 进入计时的门槛 |
| --- | --- | --- | --- |
| P0 | GEOS-5.7.2 真实连续时次 | 现有多变量／压力／规则网格的文件内剪枝，以及跨文件候选成本 | 至少 4 个完整不同时间原件；16 个为首轮规模上限；hash、layout `/5`、时间单位和 h5py oracle 均通过 |
| P1 | HIRDLS2 | swath/profile 与规则网格不同的语义扩展 | 获得原件；证明 HDF-EOS5 对象和不规则地理坐标可有界索引 |
| P1 | GEDI02_A | 大量 dataset、beam、shot 的跨层次选择 | 获得原件；核验支持的 dtype/filter、shot 坐标和 chunk locator |
| P2 | MPAS NetCDF-4/HDF5 | 非结构化 cell 的天气模式补充 | 确认实际文件是 HDF5，而非默认 pnetcdf；核验 cell→ROI 规则 |
| P2 | MERRA-2 | 长时次压力层补充 | 获得可复现的原件和 hash；核验 chunk 与过滤器 |
| 控制 | NARR | 大空间 chunk 的 ROI 负对照 | 下载原件并验证 magic、chunk offset 与已分配数 |

主实验的原件、IoTDB 数据和输出放在 `/data_01`。16 个原件现已逐个验证，合计 7,142,773,610 B，冻结大小、SHA-256 和 UTC 时次见[manifest](../results/hdf5-geos-time-scale-20261007/source-manifest.json)。早期试点目录中几个同名但只有 3.6–5.3 MB 的文件是不完整下载，**未**计入这 16 个原件或计时。

### 规模实验的三个独立命题

1. **文件内物理剪枝（已具备实验能力）**：固定 1 个 GEOS 文件，以单点、小 ROI、多 chunk ROI、整层和多变量投影改变目标 chunk 数。报告所选 `/T` chunk／2,688、实际压缩字节、解码字节和逻辑输出字节；同一原件的 native `H5Dread` 与 IoTDB-FS 物理 Float32 查询逐值核对。原生 HDF5 本身也利用 chunk 索引，不能把 native 与 typed 的差额直接称为 IoTDB-FS 的布局收益。
2. **当前跨文件目录成本（现状基线）**：用 1、4、8、16 个真实不同时次，执行 `standard_name=air_temperature`、无命中属性和精确 dataset 路径的目录发现。GEOS 的这个 `standard_name` 在各文件都会命中，因此主要测的是**文件数 N 增长时的目录开销**，不是 1/N 文件剪枝。现有后端先冻结 published manifest，再对每个已选文件加载和验证完整 dataset catalog，做属性索引计数校验及 SQL 过滤；`dataset_catalog` 的属性谓词下推仍有 O(N) 文件级工作，且逐文件验证过程持有本机 mutation mutex。分别记录初始 manifest 扫描、每文件 SQL/目录字节、授权、候选 dataset、返回 dataset、响应大小和并发写入等待；不得把“属性 SQL 已下推”写成“跨文件数据库索引已跳过未命中文件”。
3. **跨文件时间剪枝（16 时次已实测）**：GEOS 不同时次的 `/time` 使用各自的 `minutes since …`。新摄取的 GEOS 原件在验证 dimension-scale 与 GEOS UTC 产品规则后，将单点时间覆盖及 layout digest 存入精确版本 manifest；`dataset_catalog.time_range` 在加载 dataset catalog 前筛除未命中版本。缺少覆盖证据的旧版明确报错，需重新摄取为新版本。普通 FS 配有同样 `utc-point/1` 语义的预建目录，详见[时间目录契约与对照](hdf5-geos-time-directory.md)及[规模实验结果](../reports/hdf5-geos-time-scale-results.md)。N=16 的 IoTDB-FS 目录命中中位数为 165.520 ms，普通 FS 原生逐文件扫描为 46.593 ms，预建目录为 0.066 ms；首版 IoTDB 实现仍扫描 manifest 并逐版本验证，仅减少 catalog 读取，不能将其称为全局 SQL 时间索引或 FS 性能优势。两侧目录构建、空间和版本更新成本另计。

首轮 N 只取公开目录中真实的 1/4/8/16 个时次；不复制同一原件、伪造时间属性或重复注册版本来制造候选规模。若要检验 N≥64 的拐点，须另选含足够真实时次的产品并重新完成格式及语义验收。现有控制协议默认 `max_sources=1024`、`max_datasets=1024`，最大 1 MiB 帧；大规模实验需把分页／响应截断与实际发现能力分开，不能把限额失败计为查询性能。

### 对照路径与测量

在相同原件、切片、结果形状和缓存状态下至少保留：A 原生文件+`H5Dread`；B 本地预建 HDF5 layout+Adapter；C 挂载内原文件+`H5Dread`；D IoTDB typed 直连；E 注册+IPC。跨文件目录再增加 FS 原件逐文件扫描、FS 同构预建目录、IoTDB 目录发现三条路径。A/B 比较 reader/layout；D/B 主要比较存储后端；E/D 包含注册、授权、编码、发布、FUSE 和客户端读取；同一派生布局若将来引入，必须在普通 FS 与 IoTDB-FS 两边读取，不能将两边共有的重排收益算为系统独有收益。

每条结果保存文件数 N、命中文件数 K、候选／命中 dataset、候选／命中 chunk、实际 payload 范围调用数及合并后范围数、压缩返回字节、解码字节、输出字节、目录／manifest/layout SQL 和各阶段墙钟时间。`P_chunk=1−命中 chunk/候选 chunk` 只描述物理单元比例；`A_byte=实际 payload 字节/命中 chunk 的压缩字节` 描述读取放大；`A_decode=实际解码字节/逻辑输出字节` 描述解码放大。它们不能替代端到端延迟，也不能把文件大小直接当作实际读取字节。

至少做 3 个独立 campaign，各含首次冷查询与多次稳态重复；平衡 A–E 路径顺序，并分别报告按 campaign 聚合的中位数及范围。冷态定义必须注明是进程缓存、HDF5 raw-chunk cache、Linux 页缓存还是 IoTDB 服务缓存；不要用一次 `drop_caches` 代替所有层次。详细 profile 与 syscall/存储 I/O 计数可另开观测轮，以免 tracing 改变微秒级计时。把索引构建、原文件摄取、派生构建、额外空间与在线查询分别报告，禁止把首次构建混入暖态查询，或把它完全从成本模型中隐藏。

### 负对照和补充

- **连续布局或空间大 chunk**：验证逻辑 ROI 不一定带来物理 byte pruning。
- **变量打包在同一 dataset 的布局**：验证变量谓词只能减少输出，不能减少 chunk。
- **Eiger/NXtomo**：验证 frame/scan 物理定位能否迁移到非气象科学文件。
- **XRF Ghost Imaging**：单个 176 MB HDF5 文件包含 896 个扫描 realization 和元素能谱范围，可作为小型多 dataset/多轴 fixture[^14]。

## 🧪 样本核验协议

对每个候选文件生成一份不可变 manifest，并保存文件 hash：

```json
{
  "file_sha256": "...",
  "source": {"storage_endpoint": "...", "database": "...",
              "path": "...", "version": 0, "revision": 0,
              "layout_protocol": "IOTDBFS-HDF5-LAYOUT/5", "layout_digest": "..."},
  "hdf5_library": "...",
  "datasets": [
    {
      "object_token": "...",
      "path": "...",
      "shape": ["..."],
      "dtype": "...",
      "chunks": ["..."],
      "compression": "...",
      "filter_pipeline": ["..."],
      "dimension_scales": ["time", "level", "lat", "lon"],
      "semantic_attributes": {"standard_name": "...", "units": "..."},
      "time_coverage": {"native_units": "...", "calendar": "...",
                        "normalized_lower": "...", "normalized_upper": "..."},
      "chunk_records": [
        {"coord": ["..."], "logical_extent": "...", "offset": 0, "stored_length": 0}
      ]
    }
  ]
}
```

核验顺序：

1. 对原件记录字节数、SHA-256、HDF5 magic、来源 URL 与采集时间；拒绝截断下载。统计每文件 dataset 数、group 深度、已分配 chunk 数、filter 和坐标证据；核对生产 layout 完整性及限制。GEOS 当前 layout 限额为单文件 64 MiB、最多一百万 chunk，样本数值不能替代实际摄取验收。
2. 确认变量、压力、时间和空间条件能在 dataset/chunk 粒度判定；归一化时间要保存原单位与 calendar，不能只依赖文件名、根属性或直接比较不同文件的原始 time 值。
3. 用原生 `H5Dread` 独立校验每个物理 Float32 查询的 shape、mask、值和精确 source/version；无命中目录查询也须与冻结 manifest 的 FS oracle 一致。当前 GEOS 异常 `valid_range` 要在报告中明确记录。
4. 分别记录原始文件、仅加索引、以及未来可能的派生布局路径的构建时间、额外空间、候选和命中单元、payload 字节、解码字节与完整延迟。只有 manifest 和原始日志可重算时，才报告物理剪枝率和收益。

## ⚠️ 交付边界

- 当前仓库**已经**注册 HDF5 Adapter，支持 layout `/5`、有界 chunk 读取、DEFLATE/shuffle 等已验证过滤器、dataset 属性目录及 GEOS 压力层规则网格绑定；这不等于支持所有 HDF-EOS5 swath、MPAS 非结构化 cell、GEDI shot 几何或任意 NetCDF 文件。现有 HDF5 事实和限制见[实现计划](../deliverables/hdf5-query-implementation-plan.md)、[GEOS 试点](../reports/hdf5-geos-pilot-results.md)和[语义验收](../reports/hdf5-geos-semantic-grib-control.md)。
- “官方文档给出变量、层次、时间和子集工具”只能证明真实访问需求，不能自动证明 chunk 级 byte pruning。
- HDF-EOS5 或 NetCDF-4 的格式规范不能替代具体文件 manifest；压缩方式、chunk shape、filter 和 offset 必须按样本核验。
- 不应把“增加 HDF5 索引”单独写成创新。论文贡献应落在可验证的语义到少量物理单元映射、跨文件候选的真实缩减，以及 raw/index-only/derived 在 FS 与 IoTDB-FS 上的成本边界。当前目录按文件验证的 O(N) 工作若不消除，也必须作为负结果报告。

## ✍️ 合作者下一步清单

- [x] 从 NASA 目录取得 16 个**完整且不同时次**的 GEOS 原件，并冻结 URL、大小、SHA-256 和时间坐标；见[原件 manifest](../results/hdf5-geos-time-scale-20261007/source-manifest.json)。
- [x] 在 `/data_01` 的隔离 namespace 摄取并验收 layout `/5`、catalog 完整性、版本身份、时间/压力/空间绑定与原生 H5Dread oracle；见[规模实验结果](../reports/hdf5-geos-time-scale-results.md)。
- [ ] 完成 N=1/4/8/16 的现状目录与 A–E 查询，保存每文件 SQL、候选数、chunk/range/字节和冷暖原始记录；先发表当前 O(N) 边界。
- [x] 实现经 GEOS 语义验证、绑定精确版本与 layout digest 的 UTC 单点时间目录，以及同语义的 FS 预建目录；真实原件的两侧构建证据已核对。
- [x] 在未扩容 DataNode 的情况下恢复可用 SchemaRegionGroup 后，于新版本运行 `dataset_catalog.time_range` 与 FS 候选集、耗时对照；当前 IoTDB 路径仍扫描 manifest，不宣称全局 SQL 时间索引收益。
- [ ] 仅在 GEOS 结果显示需要不同语义/布局时，依次选 HIRDLS2、GEDI、MPAS/MERRA-2 或 NARR 原件做支持性审计，决定是否加入第二主负载。

## 🔗 参考资料

[^1]: ../NASA Earthdata Search. “HIRDLS/Aura Level 2 Daily Geophysical Parameters V007 (HIRDLS2).” https://search.earthdata.nasa.gov/search?q=HIRDLS2_007
[^2]: ../MPAS-Atmosphere. “Configuring Model Input and Output.” https://www2.mmm.ucar.edu/projects/mpas/site/documentation/users_guide/configuring_io.html
[^3]: ../NASA GMAO. “MERRA-2 System Characteristics.” https://gmao.gsfc.nasa.gov/gmao-products/merra-2/system-characteristics_merra-2/
[^4]: ../LP DAAC. “GEDI Level 2 User Guide.” https://lpdaac.usgs.gov/documents/986/GEDI02_UserGuide_V2.pdf
[^5]: ../NASA GEDI Data Resources. “GEDI Spatial and Band/Layer Subsetting.” https://github.com/nasa/GEDI-Data-Resources/blob/main/python/scripts/GEDI_Subsetter/README.md
[^6]: ../NOAA PSL. “NARR Monthly Pressure Air NetCDF Metadata.” https://psl.noaa.gov/thredds/dodsC/Datasets/NARR/Monthlies/pressure/air.mon.mean.nc.html
[^7]: ../NASA Earthdata. “ESDS-RFC-008: HDF-EOS5 Standard.” https://www.earthdata.nasa.gov/s3fs-public/imported/ESDS-RFC-008-v1.1.pdf
[^8]: ../MPAS-Atmosphere. “Appendix D: Fields.” https://www2.mmm.ucar.edu/projects/mpas/mpas_website/build/html/documentation/users_guide/appD_fields.html
[^9]: ../NASA GMAO. “MERRA-2: File Specification.” https://gmao.gsfc.nasa.gov/media/publications/zbly36ziNFDFbmYmvhQeVqPhUo/Bosilovich785.pdf
[^10]: ../NASA Data.gov. “MERRA-2 inst3_3d_asm_Np.” https://catalog.data.gov/dataset/merra-2-inst3_3d_asm_np-3d3-hourlyinstantaneouspressure-levelassimilationassimilated-meteo
[^11]: ../Diamond Light Source. “Introduction to Eiger2 X Detectors.” https://www.diamond.ac.uk/Instruments/Mx/I04/I04-Manual/Eiger2-Detectors.html
[^12]: ../NeXus. “NXtomo Application Definition.” https://manual.nexusformat.org/classes/applications/NXtomo.html
[^13]: ../TomoBank. “Lorentz Tomography Dataset.” https://tomobank.readthedocs.io/en/latest/source/data/docs.data.lorentz.html
[^14]: ../ESRF / Zenodo. “X-ray Fluorescence Ghost Imaging.” https://zenodo.org/records/7828494
