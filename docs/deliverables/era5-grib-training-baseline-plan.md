# ERA5 区域预测：GRIB 查询与数据管理实验基线

日期：2026-09-30。状态更新（2026-10-07）：D0/D1 已下载并校验，样本与 split 合同已冻结；D0 的本地 reader 和 IoTDB-FS 精确版本 typed 查询完成跨两类文件的 20 通道样本逐值对照，见 [D0 单样本验收](../reports/era5-d0-single-sample-acceptance-20261007.md)。D1 的 F1/Z1/Q2 50-step、1／2／4 视图训练及 loader-only replay 已完成，见 [D1 实验记录](../reports/era5-d1-50step-acceptance-20261007.md)。完整 D0 流式对照、D1 全 train split epoch、F2 本地共享服务、GRIB 原生训练适配器及 EG03 仍未完成。原始资源核查见 [资源清单](../knowledge/era5-grib-training-resources-20260930.md)。本计划独立于已运行的 Arrow/Electricity 基线，不覆盖其代码、数据或结果。

## 1. 要验证的主张

按以下顺序推进，不以性能胜出作为正确性验收条件：

1. **EG01：单实验正确性与供给。** 同一 ERA5 原始 GRIB，经本地索引 reader 与 IoTDB-FS 查询生成相同区域预测样本，拆分读取、解码、组装、传输和训练时间。
2. **EG02：2／4 个独立视图实验。** 验证同一精确源版本的基础气象场能否在不同变量、区域和预测时距之间复用；与本地共享解码服务比较。
3. **EG03：更新与恢复。** 验证新增时次或修订源版本后，旧 snapshot、样本身份、训练恢复和来源记录是否仍正确，新视图是否仅准备必要增量。

待检验假设是“省去的重复解码、真实 I/O 与准备工作足以覆盖查询服务成本”，不是“GRIB 或大模型必然加速”。Electricity 的单实验暖 mmap 结果继续保留。论文中的 GRIB 子集／跨文件／跨版本查询数字不能作为 ERA5 训练性能的预期值。

主数据路径保留 GRIB 消息边界；不先转成 Arrow 再称为 GRIB 查询实验。当前裁剪发生在完整气象场解码之后，空间选择率不能当作压缩字节读取选择率。适用性统计沿用 [采集与训练采样模型](../knowledge/fs-query-workload-mathematical-model.md)。

## 2. 数据冻结与下载范围

### 2.1 官方源和变量

使用 CDS 的 [ERA5 single levels](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels) 与 [ERA5 pressure levels](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels)。选择 `reanalysis`、GRIB、每天 UTC `00:00/06:00/12:00/18:00`。这是从小时数据选择四个时次，**不做六小时平均**。

首轮使用 CDS 规则经纬度 0.25° 全球网格；下载后必须实测 `Ni=1440,Nj=721`、坐标和扫描方向，不能从文件名推断。这里“原始 GRIB”指保留 CDS 返回的原文件字节，不宣称它是 ERA5 原生模式网格。保留实际 GRIB edition，不为适配器统一转成 GRIB2。

| 固定通道位置 | CDS variable | 物理量／单位 | 层次 |
| --- | --- | --- | --- |
| 0 | `2m_temperature` | 温度 K | 2 m |
| 1 | `10m_u_component_of_wind` | 纬向风 m/s | 10 m |
| 2 | `10m_v_component_of_wind` | 经向风 m/s | 10 m |
| 3 | `mean_sea_level_pressure` | 气压 Pa | 海平面 |
| 4–19 | `geopotential`, `temperature`, `u_component_of_wind`, `v_component_of_wind` | 依次 m²/s²、K、m/s、m/s | 对每个变量，按 1000、850、700、500 hPa 排列 |

共 20 通道。位势不改称位势高度；GRIB 中的 paramId、shortName、typeOfLevel、level、units 均保存并核对，不只依赖 shortName 字符串。首轮排除累计降水等区间量，不推断压力层缺失值为零，也不自动增加地下掩码等派生变换。

### 2.2 三档数据规模

| ID | 时间 | 用途 | 预期场数 | 全部值展开为 Float32 的体积 |
| --- | --- | --- | ---: | ---: |
| D0 | 2018-01-01 至 01-02 | 下载、摄取、单样本和 forward/backward 正确性 | 160 | 0.619 GiB |
| D1 | 2018-01 全月 | 50-step smoke、loader-only、完整供给 epoch、1／2／4 视图 | 2,480 | 9.592 GiB |
| D2，可选扩展 | 2017–2019 三个完整年份 | 跨季节、跨年预测质量及长期训练 | 87,600 | 338.814 GiB |

体积公式为 `days × 4 × 20 × 721 × 1440 × 4`，不含 mask、索引和副本；**不是 GRIB 下载大小或 Zarr 压缩后体积**。D0/D1 足以验收系统供给，不能证明天气预报质量。D2 在 D1 完成和资源重新核查后再下载，不把数十年 ERA5 作为起步要求。

请求按“年月 × single/pressure 类别”组织，保持下载返回的文件和消息顺序；D0 是同规则的两天子集。若 CDS 请求限制要求进一步拆分，保存实际请求和原因，两侧使用相同文件。不得为制造跨文件收益按训练窗口拆文件。D1 完整文件准备好后不把 D0 重复文件加入同一 snapshot。

全球数据用于验证真实气象档案服务多区域的场景；固定单区域任务可以只下载区域数据。若声称对这种专用任务也有收益，必须另设区域下载或区域 Zarr 对照，不能让它承担不需要的全球档案成本。

### 2.3 文件与语义 manifest

每个文件保存 dataset ID／DOI、完整请求 JSON、下载时间、实际字节、SHA-256、许可证与引用信息、GRIB edition／packing 分布。每条消息保存 offset/length、变量与层次、reference time、valid time、step/stepType/stepRange、expver（存在时）、成员（存在时）、网格身份、bitmap 和单位。

消息目录由 ecCodes 独立建立；IoTDB-FS 绑定另保存 namespace 与精确 source/version。文件版本时间不是气象有效时间；ERA5 analysis 的 step 不能充当时间轴。重复逻辑键、缺时次、非同网格、非预期单位须在准备阶段报告并拒绝进入首轮完整窗口集。

冻结内容身份与源访问身份分别记录。样本身份使用稳定内容 snapshot＋view＋逻辑坐标；进程 PID／随机 namespace 不得进入跨运行 sample ID。映射变更或源不可用应显式失败，不静默读取 latest。

## 3. 样本、视图与模型

### 3.1 张量与采样

以 `init_time=t` 命名样本，输入为 `[t−6h,t]`，目标为 `[t+lead]`；默认 lead=6h。张量为 `X:[B,2,C,128,128]`、`Y:[B,1,C,128,128]`，另有有效位 mask。若实现沿用现有 target-anchor API，必须显式转换为 `anchor=t+lead`，不能混淆两个时间。

纬度统一北到南，经度为 `[0,360)`，从声明的规则网格选整数行列，不插值。原 GRIB 扫描方向的转换成本计入对应阶段。通道和时间顺序固定，Float64 ecCodes 解码后按相同规则转 Float32。原值、标准化值与 mask 分开验收；缺失和 NaN 不等于零。

每个区域定义 16 个 patch：给定左上角基础索引 `(r0,c0)`，加笛卡尔积偏移 `dr,dc ∈ {0,16,32,48}`，每个 patch 取半开区间 `[r,r+128)×[c,c+128)`。East Asia 使用 `(156,400)`，North America 使用 `(100,920)`；前提是网格坐标与上述约定完全一致。patch 表保存实际经纬度边界及 hash；这里不跨日期变更线。

样本集合为合法 init_time × patch ID；标准 PyTorch RandomSampler 无放回遍历，评估按固定时间／patch 顺序。各后端共享冻结的 trace，每个实验 seed 独立，不同步不同实验以制造命中，不把顺序评估冒充随机训练。批内允许物理需求合并，但恢复原样本顺序并计预取内存。

D1 划分：训练 `[2018-01-01,01-22)`；验证 `[01-22,01-27)`；测试 `[01-27,02-01)`，均 UTC。一个样本的全部输入和目标必须在同一 split 内，窗口不跨 split。无缺失时，lead=6h 每个 patch 的训练／验证／测试样本数应为 82／18／18，即 16 个 patch 合计 1,312／288／288；lead=24h 为 79／15／15。D0 只做正确性，不作独立质量评估。D2 使用 2017 train、2018 validation、2019 test，采用同一边界规则。

每个空间域在训练时段、patch 覆盖区域的**唯一网格点集合**上拟合逐通道 mean/stddev；不因 patch 重叠重复计权。输入和目标共用统计，缺失值不参与，零方差或非有限统计显式拒绝。保存 double 统计、拟合域、算法和 artifact hash。变换规则先 double `(x−mean)/std` 再 cast Float32，各后端一致。相同拟合域的通道子集可引用同一统计 artifact；其他区域单独拟合。

首轮只接收输入和目标均完整的窗口，准备时冻结合法样本表并记录排除原因；不在计时训练中临时跳过。缺失策略功能回归另用小 fixture，不能向真实训练集静默注入数据。

### 3.2 公开小模型

首模型 `RegionalCNN-v1` 是拟在本仓库公开实现的系统实验模型，**不是 WeatherBench/GraphCast 官方模型的复现**。将输入时间与通道展平为 `[B,2C,H,W]`：三层 3×3 Conv2d（`2C→64→64→64`），每层 ReLU、stride=1、reflection padding=1；末层 1×1 Conv2d（`64→C`），直接预测一个目标时刻。不用 BatchNorm、dropout、周期经度 padding 或预训练权重。

训练共用 PyTorch Dataset/DataLoader 和同一训练循环；首轮 batch=2、num_workers=0、FP32、AdamW(lr=1e−3, weight_decay=1e−4)、固定 LR，seed 基数 20260930。关闭 TF32，先做确定性数值验收；性能模式若改变数值设置，所有后端同步改变并重新验收。预取从零额外 lookahead 起步；异步 H2D、worker 和预取优化单列。

loss 使用标准化空间上按 `cos(latitude)` 加权的 MSE，按有效权重归一化再对通道平均；首轮合法窗口无缺失。评估逆标准化后分别报告各通道原单位 RMSE/MAE，并保留 persistence（目标等于 t 时刻）参照，不把不同单位的 RMSE 直接平均为一个物理指标。50 steps 只验收供给和训练闭环；D2 才开展多 epoch、固定训练预算的质量比较。

训练代码位于 `tests/benchmarks/grib_training/`：已提供 `model.py`、`dataset.py`、`train.py`、`prepare_era5.py`、本地／IoTDB reader、Zarr 转换器与 reader、固定 trace replay 和多视图资源采样器。F2 共享本地 reader、checkpoint/恢复与完整原生 GRIB 训练 adapter 待实现。参考 [WeatherBench CNN 与评分代码](https://github.com/pangeo-data/WeatherBench)，不直接安装其旧 Keras 环境到现有 Arrow venv。U-Net／完整天气模型留作模型规模扩展。

### 3.3 多视图矩阵

| View | 区域 | 通道 | lead | seed |
| --- | --- | --- | ---: | ---: |
| V0 | East Asia | 全 20 | 6h | 20260930 |
| V1 | North America | 全 20 | 6h | 20260931 |
| V2 | East Asia | 4 地表＋z500/t850/u850/v850，共 8 | 6h | 20260932 |
| V3 | East Asia | 全 20 | 24h | 20260933 |

J=1 使用 V0；J=2 使用 V0/V1；J=4 使用全部。数字 seed 是整数，不解释为日期。每个 view 单独模型和训练统计引用，跨后端保持一致；V2 的输入层维度不同，不与 V0 混用 checkpoint。

先以多个进程 replay 各自随机 trace 测供给，后做训练。J=4 的总 samples/s 受通道数差异影响，必须同时给出每 view 吞吐、有效值字节/s、归一化到各自单跑的 slowdown。比较 J 间共享规模效应时，补充相同 V0、独立 seed 的同任务对照，避免混合任务难度混淆。

## 4. 必须保留的后端

| ID | 实现 | 所需能力与证据 |
| --- | --- | --- |
| F1 | 本地原 GRIB＋持久消息 offset 索引＋ecCodes＋有界私有解码缓存 | 索引只建一次，不逐样本从头扫描；各进程缓存之和等于公共预算 |
| F2 | 本地原 GRIB＋共享解码服务 | 与 Q2 同类基础场、总预算、single-flight 和输出方式；分离共享收益与后端收益 |
| Z1 | 同一冻结 GRIB 转成 Float32 Zarr 基础数组 | `[time,channel,y,x]`，初始 chunks=`[1,1,128,128]`，无损 codec；保存 mask；不存全部重叠窗口 |
| Q2 | IoTDB-FS GRIB typed 查询＋训练组装＋共享解码 | 精确版本与 snapshot；有界结果；所有服务/客户端成本计入 |
| Q0，消融 | Q2 关闭解码复用 | 保持索引、查询、模型和结果表示不变 |
| F3，诊断 | F1 reader 从普通 IoTDB-FS 文件读取 | 区分存储/FUSE与 typed 查询服务；不替代 F1 |

Z1 在计时前转好；同时报告转换和占用，以首样本、首 epoch、稳态及多个实验的生命周期成本比较。允许以独立校准 trace 公平调优 chunk/codec，冻结配置后再跑正式 trace。一个 Zarr 可以服务所有视图，不能默认每视图保存一份副本。公开 WeatherBench2 Zarr 可用于补充检查，但不能替换由**同一 GRIB**转换的 Z1：下载批次、变量、网格或重网格算子可能不同。[WeatherBench2 数据说明](https://weatherbench2.readthedocs.io/en/latest/data-guide.html)

本地索引能力参考 [cfgrib](https://github.com/ecmwf/cfgrib)，性能基线可采用直接 ecCodes offset reader，不能因 heterogeneous levels 导致 xarray 打开困难就退化成每次全文件扫描。

EG01 最小纵向切片先跑 F1/Q2；结论发布前补 Z1。EG02 必须有 F2；只胜过 F1 私有缓存不能声称共享机制是 IoTDB-FS 独占能力。

## 5. 阶段和验收门

| 单元 | 工作与通过条件 | 输出 |
| --- | --- | --- |
| EG00 | 环境隔离；D0 下载；160 场的键、网格、单位、时间、hash 审计；训练环境 import/forward/backward | 环境锁、CDS 请求、源 manifest、模型配置/hash |
| EG01a | 原字节摄取/读回一致；F1/Q2/Z1 消费相同样本；跨 single/pressure 文件组装；split 无泄漏 | 数值/mask/坐标/样本 ID 验证报告 |
| EG01b | D1，V0 loader-only＋50-step 训练＋完整训练 split 供给 epoch，至少 3 次配对重复 | 原始 trace、阶段 profile、冷暖/初始化分离、loss 轨迹 |
| EG02 | J=1/2/4；F1/F2/Z1/Q2 同预算；独立 seed；先 replay，再有资源时多 GPU 训练 | 吞吐、尾延迟、公平性、共享命中、真实内存/I/O |
| EG03 | append、修订、daemon 重启、checkpoint resume 与原版本保留 | 更新成本、样本身份/恢复一致性、旧新版本 provenance |
| EG04，可选 | D2，多季节/跨年、较长训练；区域源对照或较大模型 | 预测质量与长期生命周期成本 |

数值门：同版本 ecCodes 解码与同 cast/变换实现要求 finite 值逐元素一致、mask 与 NaN 位置一致；若引入不同库/运算路径，先报告最大绝对和相对误差，冻结合理公差后独立标注，不能自动放宽测试。固定 CPU 数值检查后，确定性 GPU 配置下对齐初始权重、batch trace 与 loss；后续性能运行若不能 bitwise 一致，须明确数值公差及其依据。

无缺失的 D0 共 6 个合法 init_time，每区域 96 个样本；用流式比较覆盖全部 patch、20 通道和两种区域，避免把全部窗口放内存。D1 比较完整样本 ID 清单并分层抽样数值（所有通道、split 边界、patch 边缘），性能运行记录实际 fetch，不把 Trainer/DataLoader 预取算成丢失样本。

同配置各后端使用相同随机 trace，重复轮次轮换后端顺序，保留全部运行及异常，不靠删除极值制造提升。单月随机 epoch 不保证重复 decode 接近零；实际命中由 trace 与预算决定。

## 6. 资源预算和 profile

单场 Float64 值为 8,305,920 B（约 7.92 MiB）；默认 20 通道、3 时刻涉及最多 60 场，值约 475.3 MiB，而一个 patch 输出值只有 3.75 MiB。batch=2 最多涉及 120 个不重复场；mask、坐标、临时解码与编码另计。因此先预留组装和 in-flight 空间，不能把 cache budget 当总内存。

初始总 decoded cache sweep 为 1／4／16 GiB，主配置 4 GiB；条目数不得先于字节限制产生意外驱逐。F1 的 J 份缓存总量为同一个预算；Q2、F2 为共享总预算。Zarr 缓存、OS page cache、IoTDB 自身缓存和客户端同样记录，比较总资源时按 PSS/共享页核算，不能累加 RSS 认定 N 倍内存。

固定 CPU 核配额、解码/read 并发与 BLAS 线程，起步单读取链路，2／4 并行作为独立消融。首先采用相同每批请求边界，额外预取单独报告。1 GiB 无法容纳某在途配置时减小双方并发而不是悄悄放宽一侧预算。

缓存状态区分：

- setup：索引／摄取／转换／snapshot／统计构建均单列；还报告进程启动至首个可训练 batch。
- cold-decoded：清应用解码缓存，记录 OS/IoTDB 状态；不称为冷磁盘。
- warm：同一服务再次运行相同 trace，记录真实命中，不预设全命中。
- 真冷存储实验另排维护窗口；不在共享服务器上无条件 drop_caches。

每个 batch 使用 correlation ID 串起：客户端请求 → 授权/元数据 → 消息定位 → 编码字节读取 → ecCodes 解码 → 空间裁剪/组装/标准化 → IPC/结果发布 → 客户端转换 → H2D → forward/backward。父子阶段不重复加总；并发解码累计 CPU 时间不是墙钟关键路径。

必报实际消息/range/call/字节、解码次数/时间、独立与共享命中、重复解码/驱逐、pinned/in-flight、IPC 大小与 copy、page faults、磁盘与网络实际字节、进程组 CPU/PSS、GPU 利用率与数据等待、batch p50/p95/p99。EG02 同时报每 view 的 slowdown、等待和取消后的资源回收。不能只报告数据库返回字节代表磁盘 I/O。

## 7. 更新与恢复协议

EG03 不要求等待真实 ERA5 修订：

1. **追加真实时次**：同一冻结 D1，先发布截至 01-30 的 S0，再纳入 01-31 为 S1。按原下载分组构建各自文件，不修改月文件内部 message 来迎合窗口。旧 snapshot 样本集合不变，新 snapshot 纳入新增合法样本。
2. **受控修订**：在独立测试 namespace 中，用 ecCodes 重新编码一条消息中的声明修改，产生 V2；保存修改坐标、输入/输出 hash 与值差。只称为修订模拟，不称为真实 ERA5T 校正。S0 精确读取 V1，S1 读取 V2；受影响样本数与 provenance 可验证。
3. **恢复**：V1 的固定 trace 训练 50 steps 对照，另运行 25 steps→保存→发布 V2→重启查询 daemon→用 checkpoint 中的内容 snapshot/view 重新注册→继续 25 steps。保存模型/优化器、CPU/CUDA RNG、sampler epoch/permutation/已消费游标、统计 artifact、训练配置、源版本和 snapshot/view hash。进程内 route/prepared ID 不能写作持久身份。
4. **失败边界**：旧版本仍保留时恢复应与未中断 V1 运行对齐；旧版本缺失、hash 不符或权限不足时须显式失败，不退回 latest。源版本已固定不等于模型 RNG 已恢复，两者都测试。

F1/F2/Z1 也保留版本 manifest 与不可变源/数组；允许它们正确恢复。比较版本准备、重建范围、额外物理存储和操作步骤，不能把一个只覆盖写文件的简陋基线当作唯一对手。若日后采集到真实 ERA5T/ERA5 两版，再增加真实案例；ERA5T 不保证与最终版本数值不同。[ECMWF ERA5 数据说明](https://confluence.ecmwf.int/spaces/CKB/pages/76414402/ERA5%2Bdata%2Bdocumentation)

## 8. 当前实现差距与执行顺序

已有 [GRIB typed-query 验收](../reports/grib-query-migration-acceptance.md)涵盖消息选择、裁剪、插值、精确版本和 provenance；ERA5 D1 的 Python harness 已用它完成逐样本精确版本查询与训练，但还没有 GRIB 原生训练窗口服务。当前 Arrow DatasetSnapshot/coverage/window assembler 在 `query/training/arrow`，不能直接把 GRIB 当作其输入。

需要依次补齐：

1. 独立 ERA5 目录与样本 oracle，本地 F1 和小模型；核验 valid time、层次、网格和 GRIB1/2。
2. GRIB 训练绑定与 coverage，将消息域映射为 `[T,C,Y,X]`。保持基础 query core、GRIB adapter 与训练 adapter 分层；不要扩展 Arrow 专有结构来容纳全部气象语义。
3. 复用已验证 immutable snapshot/view、稳定身份、授权与预算思想；GRIB 当前独立解码缓存接入训练共享边界时核验 namespace、版本、single-flight、租约、取消与公平性。64 MiB 默认缓存不直接用于主实验。
4. 有界 tensor 输出与客户端：测现有 typed 网格 IPC 转 dense 的实际成本，再决定训练 IPC 表示；不逐像素重复大字符串元数据。输出 limit 必须先做 shape/字节预检，不能末尾才失败。
5. 加 profile 后跑 EG01；补 F2/Z1 和多视图调度再跑 EG02；最后接入版本恢复 EG03。

本文件固定实验语义，不假定尚未评审的实现接口。具体公共 snapshot API、GRIB 缓存接入方式及 tensor IPC schema 在实现阶段形成可审阅设计。服务器资源准备可以先进行，不必等这些 API 全部实现。
