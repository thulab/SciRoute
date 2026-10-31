# 公开训练基线：HF Trainer、多变量时序模型与数据集

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-09-30 新增独立扩展：[ERA5 区域预测 GRIB 基线](era5-grib-training-baseline-plan.md)与[服务器资源准备清单](../knowledge/era5-grib-training-resources-20260930.md)。按单实验正确性与供给 → 2／4独立视图 → 更新与恢复推进；当前为计划，尚未运行，不替代本文 Arrow 已有结果。

核查日期：2026-09-27，首轮实验范围修订：2026-09-29。公开资料和接口已核查；[PB00 复验与 PB01 数据正确性阶段验收](../reports/public-training-pb00-pb01-acceptance-20260929.md)已完成。首轮 A/B 接线与挂载验收（服务器归档：`docs/public-training-ab-wiring-20260929.md`）及其 [50-step 训练 smoke](../reports/public-training-ab-long50-20260929.md)已验证公开数据的 IoTDB-FS typed 查询和两模型训练路径，但**完整训练与稳态性能尚未验收**。本文是 [Arrow 代码实施计划](arrow-query-implementation-plan.md) A00/A13 和上位设计 E1/E5–E8 的补充。

2026-09-30 追加：[Electricity 缓存扩容＋1/2/4 路并行消融](../reports/public-training-cache-parallel-20260930.md)已完成两轮冷/暖 50-step 运行。32 次 B 与 4 次 A 对照均通过轨迹/loss 校验；4 路冷读相对小缓存串行对照降低约 41% 墙钟。仍采用标准 RandomSampler，未构造连续窗口；完整 epoch 和多实验稳态验收继续单独进行。

## 1. 主线选择与证据边界

FS适用性按 [文件布局、有限采集与训练采样数学模型](../knowledge/fs-query-workload-mathematical-model.md) 评估：单次采集占比α、窗口message/变量选择比例、实际采集/解码粒度、采样复用距离、缓存预算与跨文件窗口比例分别计量；不从模型名称或“大文件”直接推断收益。PB00的trace应提供该模型的参数，PB04据此核验与优化原生reader相比的回本条件。

公开实验采用 **HF `transformers.Trainer`＋公开数据集＋开源多变量预测模型**。TimeBench/TimerLathe 保留为可选内部回归，不能成为下载、转换、训练、正确性 oracle 或论文主结果复现的必要依赖；现有远端环境记录保留为历史证据。

### 首轮目标：标准采样下的文件系统 / IoTDB-FS A/B

首轮只比较两条公开数据路径：**A** 为 `/data_01` 上本地文件系统中的原始 Arrow IPC stream，使用一次建立 message/row 索引、按需选择 List offsets/值并有界缓存的公开原生 reader（R1）；**B** 为把**同一 stream 原样摄取**到隔离 IoTDB-FS namespace 后，通过 typed window 查询提供相同训练样本（Q2）。两者固定源文件 SHA-256、逻辑样本、split、仅训练集拟合的标准化、通道顺序、模型与训练参数。R0 内存和 R2 memmap 仍为强参考；R4 plain FUSE 是区分存储与 typed 查询开销的诊断项，不能代替 A 路径。先用 ETTm1 做端到端正确性和耗时，再用 Electricity 370 通道观察更高数据供给负载。

训练采用安装版本 `transformers.Trainer` 的单进程 map-style 默认采样：`group_by_length=False` 时训练为 `RandomSampler`，评估为顺序采样。窗口清单按固定 split、`L/H` 和默认 `S=1` 生成；每个后端在相同 seed 下记录实际访问的 `sample_id` 序列并逐项比较，不对随机顺序做局部性重排，也不强制多个实验同步随机种子。首个核查配置为 `L=96,H=24,S=1`；这是本项目固定的首轮协议，不称为两模型的唯一官方超参数。保持相同 DataLoader worker 数、预取、batch、初始化、优化器、精度和 GPU 分配；先单 worker/单 GPU 核对，再增加 worker 与多实验。若两条路径的样本序列或 tensor/mask 不一致，停止性能比较。

**首轮不人工制造跨文件连续窗口。** 当前 ETTm1/Electricity 各是一份 IPC stream，按其真实 record-batch/row 分段读取；跨段窗口仍需按相同逻辑序列正确组装，但不能把它报告为跨文件收益。跨物理文件窗口保留为 A08/N06 的功能回归以及以后由真实数据布局产生的独立实验，不作为本轮 A/B 的前置条件或性能论据。首轮先报告 A/B 的 setup、loader-only 和 Trainer 端到端时间、样本吞吐、p50/p95 数据等待、I/O、解码、IPC、峰值内存与验证质量；同时区分冷启动和暖运行。不得仅凭小 fixture 缓存命中或合成边界宣称训练加速。

建议首批组合如下。模型和存储方案是两个独立维度：每个组合固定模型、样本和训练配置后，才比较不同读取后端。

| 组合 | 训练方式 | 主要用途 | 接入成本 / 优先级 |
| --- | --- | --- | --- |
| HF Trainer＋PatchTST＋ETTm1 | 随机初始化、全输入通道预测 | 小型正确性、训练/评估/恢复闭环，Arrow 嵌套窗口与内存 oracle 等价 | 低；第一个纵向切片 |
| HF Trainer＋PatchTST＋UCI Electricity 原始370通道版 | 从头训练；另设通道子集视图 | 重叠窗口、明确连续分片、多实验共享数据 | 低至中；首批主实验 |
| HF Trainer＋PatchTSMixer `mix_channel`＋上述两种数据 | 从头训练、显式通道混合 | 补充联合多变量建模；比较轻模型下数据供给开销 | 低至中；首批第二模型 |
| HF Trainer＋PatchTST/PatchTSMixer＋PEMS-SF | 独立日样本内短期预测 | 963通道、嵌套行选择、投影、短窗共享 | 中；补充，必须标明从分类数据构造预测任务 |
| HF Trainer＋PatchTST＋LargeST SD/CA | 先固定通道子集，再扩展规模 | 真实较大数据、跨年/跨文件窗口和预算敏感性 | 中至高；下载/清理/内存验收后开展 |
| HF Trainer＋IBM TTM-r2＋Electricity/ETTm1 | 公开权重微调 | 补充预训练模型的训练消费路径 | 中；可选，不阻塞 Arrow 交付 |

前两模型在 Transformers 中已有实现和公开 Trainer recipe，不必为首批实验移植另一套训练框架。[PatchTST recipe](https://huggingface.co/blog/patchtst)、[PatchTSMixer recipe](https://huggingface.co/blog/patchtsmixer)。本项目需要新增的是公开的数据/查询 adapter 和实验驱动；“有公开 recipe”不代表上述全部组合已经跑通。

## 2. 模型与训练接口

| 模型 | 已核实接口和语义 | 实验约束 |
| --- | --- | --- |
| `PatchTSTForPrediction` | 多变量输入；默认 `channel_attention=False`，各通道独立处理并共享权重。输入 `past_values`，目标 `future_values`，历史有效位参数 `past_observed_mask`。 | 可证明多变量窗口消费，不能将默认配置描述为显式学习通道关系。主实验从头训练；固定 context、horizon、patch 长度和步长。[官方接口](https://huggingface.co/docs/transformers/model_doc/patchtst) |
| `PatchTSMixerForPrediction` | `common_channel` 与 `mix_channel` 模式；后者混合通道。历史有效位参数名为 `observed_mask`。 | 第二模型显式设置 `mode="mix_channel"`。`prediction_channel_indices` 模式仍要求 `future_values` 包含全部输入通道，模型内部选择预测通道。[官方接口](https://huggingface.co/docs/transformers/model_doc/patchtsmixer) |
| `TinyTimeMixerForPrediction` | IBM notebook 直接用 `transformers.Trainer` 微调；`past_observed_mask`/`future_observed_mask`，可配置 decoder 通道混合。 | 仅选公开 `ibm-granite/granite-timeseries-ttm-r2`；固定满足 L/H 的分支及 commit、微调策略和预处理。模型卡要求外部逐通道标准化；不能把零样本推理吞吐称为训练收益。[官方 notebook](https://github.com/ibm-granite/granite-tsfm/blob/main/notebooks/hfdemo/ttm_getting_started.ipynb)、[模型卡](https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2)、[源码](https://github.com/ibm-granite/granite-tsfm/blob/main/tsfm_public/models/tinytimemixer/modeling_tinytimemixer.py) |

统一 adapter 拟定契约：

- 存储为 `target: list<list<float32>>`，每条序列 `[C,T]`；模型 batch 为 `past_values: [B,L,C_view]`、`future_values: [B,H,C_view]`。在 collator 中显式打包/转置，分别记录拷贝、连续化、归一化、pin-memory 和 H2D 开销，不承诺 tensor 零拷贝。
- 首批全输入通道预测。视图选择通道子集后，保持固定顺序并设置 `num_input_channels=C_view`；不把不同 C 的 checkpoint 自动混用。模型参数和 loss 随视图配置固定，同一视图各后端完全相同。
- 显式配置 `label_names=["future_values"]`；adapter 按模型映射 mask 参数名。metadata 由 collator 消费，不能直接传给 `forward`。若 Dataset 返回窗口描述符，设置 `remove_unused_columns=False` 并由 collator 仅输出模型支持的字段；若返回标准 tensor 字段，则验证默认列处理即可。
- 评估显式提取 `prediction_outputs`，不将 hidden states、loc/scale 等输出误当预测；报告相同尺度的 MSE/MAE。历史 mask 不自动保证目标 loss 忽略缺失值：首批只选完整目标窗口，缺失目标的统一 masked loss 单列验收，不静默补零进入 loss。
- 各后端共用 map-style Dataset 的窗口 ID 清单、sampler 和 collator。Trainer 接受 PyTorch Dataset，不要求先通过 HF Datasets `.map` 物化全部重叠窗口。[Trainer API](https://huggingface.co/docs/transformers/en/main_classes/trainer)
- 数据投影、窗口组装、标准化是读取 adapter 的职责；模型、优化器、调度器、AMP、梯度累积和 Trainer 主循环保持公开实现。首版 Python 到查询服务的传输方式在 A10 确定，需有界批量调用；不得每个样本启动 CLI 子进程，或为每个 worker 新建无法共享的服务。

Transformers 代码采用 Apache-2.0，首批随机初始化不依赖第三方 checkpoint；granite-tsfm 代码和上述 TTM-r2 模型卡也标注 Apache-2.0，其他 research 权重仓库须另查。[Transformers LICENSE](https://github.com/huggingface/transformers/blob/v4.57.1/LICENSE)、[granite-tsfm](https://github.com/ibm-granite/granite-tsfm)

版本冻结安排在 PB00。旧博客的无版本安装命令不能直接当锁文件：核查时 granite-tsfm `main` 的依赖声明为 Python `>=3.11,<3.14`、Transformers `>=4.57.6,<6`、Torch `>=2.10,<2.12`；这是当时源码约束，**不是已验证可用组合**。PatchTST/PatchTSMixer 最小 harness 可只依赖 Transformers；使用官方 `ForecastDFDataset` 或 TTM 时再引入相应 tsfm 版本。[依赖声明](https://github.com/ibm-granite/granite-tsfm/blob/main/pyproject.toml)

## 3. 数据集清单与适用范围

下表 float32 数值体积是按 shape 计算的估计，不含时间、mask、offset、索引、模型和工作内存，也不等于实际下载大小。

| 数据集 / 官方入口 | 结构、体量与许可 | 使用方式及限制 |
| --- | --- | --- |
| [ETT-small / ETTm1](https://github.com/zhouhaoyi/ETDataset) | 本次原文件审计为69,680时间点×7数值通道、15分钟；float32 值约1.86 MiB。[LICENSE](https://github.com/zhouhaoyi/ETDataset/blob/main/LICENSE) 为 CC BY-ND 4.0。 | 第一个训练闭环。发布下载/转换代码和 manifest，默认本地生成处理结果；不默认发布处理后的数据副本，必要格式技术修改与其他数据加工分别审查。README 统计与文件有差异，实际 shape 以 hash 对应文件为准。 |
| [UCI ElectricityLoadDiagrams20112014](https://archive.ics.uci.edu/dataset/321/electricityloaddiagrams20112014) | 原始370用户、140,256时间点、15分钟、2011–2014；float32 约198 MiB；CC BY 4.0。 | 主实验。保留全部通道或显式声明子集；未启用用户的零值和夏令时规则需要保留来源语义，不能一律把0当缺失。默认 step 轴，未解决当地时间歧义前不生成伪 UTC 连续轴。 |
| [UCI PEMS-SF](https://archive.ics.uci.edu/dataset/204/pems%2Bsf) | 440独立日样本×963传感器×144个10分钟步；float32 约232.8 MiB；CC BY 4.0。 | 一顶层 row 对应一天，天然适合两层 list。原任务是日分类，预测协议需另命名；默认 `L+G+H≤144`。原顺序已打乱且剔除了部分日期，禁止按文件/行顺序跨日续接。 |
| [UCI Beijing Multi-Site Air Quality](https://archive.ics.uci.edu/dataset/501/beijing%2Bmulti%2Bsite%2Bair%2Bquality%2Bdata) | 12站，共420,768站点行，小时频率；6污染物＋6气象量，其中风向为类别；CC BY 4.0。 | 可选缺失值/多实体验收：先选11个数值量，按站点独立序列；NA 保留 mask。不同站不得当作时间续片。很小，不作为规模证据。 |
| [LargeST](https://github.com/liuxu77/LargeST) | CA/GLA/GBA/SD 为8600/3834/2352/716传感器；2017–2021、5分钟、525,888时间点；CA 单float32值矩阵约16.85 GiB、SD约1.40 GiB。[论文 Table 1](https://arxiv.org/pdf/2306.08259)。数据 CC BY-NC 4.0，代码 MIT。 | 真实规模扩展，作者提供按年H5与清理脚本，下载托管 Kaggle；本次未验证整套下载。传感器作通道，固定清理版本、时间覆盖和 mask。先用SD/通道子集训练，CA全通道先做 loader-only，测量内存后再决定模型 batch。 |
| [MPI Jena Weather](https://weather.bgc-jena.mpg.de/weather_data.html) | [TensorFlow 教程](https://www.tensorflow.org/tutorials/structured_data/time_series) 使用2009–2016、14特征、10分钟版本，另有下采样/异常值处理。 | 候选气象补充，尚未核实原始数据独立许可，不作为首批可分发转换集；不得与其他论文21维 Weather 混同。 |

常见小时级321通道 `ECL/electricity` 是另一份处理版，可从 [LSTNet 作者数据仓库](https://github.com/laiguokun/multivariate-time-series-data) 追溯；它与原始UCI 370通道版必须使用不同 `dataset_id`、hash、预处理/切分说明。官方模型 recipe 的 ECL 训练结果不能直接等同本计划的 UCI raw 结果。

ETTm1 的本次审计记录：来源 [官方原文件](https://raw.githubusercontent.com/zhouhaoyi/ETDataset/main/ETT-small/ETTm1.csv)，10,360,719 bytes，SHA256 `6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e`；时间为2016-07-01 00:00至2018-06-26 19:45，列为 HUFL/HULL/MUFL/MULL/LUFL/LULL/OT。复现时重新校验，不将可变 `main` URL 当永久版本。

## 4. 公开数据到 Arrow/TsFile 的可追溯转换

这些数据多数原生为 CSV/TXT/H5，**并非已经发布为本项目的 IPC stream**。拟提供独立公开转换器，不导入 TimeBench/TimerLathe：

1. 下载并核对原文件，固定 `dataset_id/source_url/source_revision/source_sha256/license`；审计 shape、时间、通道顺序、实体边界、缺失和重复。将 parser/清理版本写入 manifest。
2. 生成同一份逻辑基础数据：float32 值、独立 mask、原时间标签/step 轴、entity/channel IDs。各后端使用相同数值和 mask；缺失不等于0，NaN 与 NULL 规则显式化。只在训练区间拟合标准化参数，将 transform artifact 独立版本化，避免修改共享 canonical fragments。
3. 写入 Arrow IPC **stream** 的 `target: list<list<float32>>`，外层通道、内层时间连续；顶层 row 是 `entity × segment`。通道顺序、全局 step 覆盖、可选时间绑定和明确 continuation 存在 sidecar。独立日/实体不声明 continuation。
4. 首轮保持公开转换器生成的每数据集单个 IPC stream，不为了 A/B 制造额外文件边界。E6 在后续真实多文件数据布局中再检查显式 continuation；如做受控分片压力测试，须单列为布局变体，不能充当首轮真实负载或收益证据。分片仍须遵守 List int32 offsets 和内存预算。
5. 同一生成器输出 NumPy/memmap reference 和后续 TsFile table 输入：实体、时间、measurement 对齐；准确保存 float32 和 mask，明确 TsFile 时间精度。TsFile 实现阶段再验证原生 reader＋公开 Dataset adapter；HF Trainer 没有被本计划假设为原生支持 TsFile。
6. 在划分后的目标区间上生成轻量 window manifest：`window_id/entity_id/input_range/target_range/channels/split`。不预生成全部 tensor 窗口。明确是否允许验证/测试上下文引用此前历史；目标不得跨 split，填补和标准化不得偷看未来。

拟定初始协议：ETTm1 可采用常见12/4/4个月的30日索引约定（12/16/20×30×24×4为边界），明示尾部未用记录；UCI raw Electricity 可采用2011–2012训练、2013验证、2014测试，标为本项目协议；PEMS-SF 保留官方 train/test 日集合，从训练日固定抽取验证日，日内预测，不能声称是连续时间外推。这些参数写入配置，运行前固定，各后端一致。

## 5. 必须保留的公开强读取基线

HF Trainer 负责训练，HF Datasets 是可选的数据容器，二者不能混称为存储优化。HF Datasets 已使用 Arrow memory mapping，应保留其正常能力，不以逐窗口从流头扫描的低效实现代表原生 Arrow。[HF Datasets Arrow 文档](https://huggingface.co/docs/datasets/about_arrow)

| ID | 读取/存储方案 | 用途与公平条件 |
| --- | --- | --- |
| R0 | 官方 recipe 的 pandas/`ForecastDFDataset` 内存基础序列＋按需窗口 | 小数据强参考；数据可驻留内存时完整保留。计入加载/预处理/RSS，不假设其逐窗重复解码；对齐本项目窗口清单。 |
| R1 | 本地 Arrow mmap，公开 stream message/row 索引＋两层 offsets 选择＋有界解压缓存 | 同格式主要基线；初始化扫描一次计 setup，warm 查询不重新从头扫描；禁止先把完整 target 转 Python list 再切片。可辅测标准 HF Datasets 行访问路径，但不能替代优化后的 R1。 |
| R2 | 连续 float32 NumPy/memmap 基础序列、按需切窗 | 允许改变布局的强工程替代方案；记录一次性转换、mask/索引和额外存储。无需重复存储重叠窗口。 |
| R3 | 预物化全部训练窗口 | 明确时间/空间换取吞吐；记录每个视图的生成时间、磁盘体积和视图变化重建成本，不只报告 warm epoch。 |
| R4 | 同 R1 reader 读取 IoTDB-FS 普通文件（plain FUSE） | 隔离存储路径与 typed window 优化。使用相同原文件/压缩和请求集合。 |
| R5（TsFile阶段） | 上游 TsFile 原生 table reader＋公开 Dataset adapter | 按时间和measurement查询，保留上游索引/缓存能力，与TsFile typed-window路径比较；依赖版本、encoding、精度和缺失语义先验收，不阻塞Arrow。 |
| Q0/Q1/Q2 | IoTDB-FS typed windows：共享缓存关闭 / 仅编码缓存 / canonical decoded fragments 共享 | 核心消融；Q2 增加按需局部跨文件组装与多视图，共享运行时、预算和复制成本均计入。 |

R1 的公开 reader 是待实现实验组件，不能写为已经存在的库能力。它可复用 Arrow 公共 API，但不得暗中依赖 IoTDB-FS 服务缓存或私有 TimeBench 代码。`pyarrow.ipc.open_stream` 全量重建仍可用于小规模正确性 oracle，不能作为唯一性能对手。

R5 可基于 [Apache TsFile C++ table/query 接口](https://tsfile.apache.org/UserGuide/latest/QuickStart/InterfaceDefinition/InterfaceDefinition-CPP.html) 做薄绑定，具体API和有界缓存随S2依赖spike验证；训练仍用同一公开HF Trainer。另保留上位设计E2的 IoTDB `load tsfile`＋等价SQL作为数据库查询替代方案，单列导入、布局改变和窗口组装成本。跨Arrow/TsFile的比较使用同一逻辑数据，但分别报告物理压缩/格式差异；不能把格式转换收益全部归给查询系统。

多实验 E7 至少比较“各进程私有解码缓存”和“一份共享解码缓存”，**总**缓存预算一致；进一步提供本地公开共享内存/服务缓存的 R1-sharing 对照，或将缺失该对照明确列为限制。R2 的只读 mmap 可天然共享 OS page cache，不能把其每进程 RSS 相加当作物理内存重复。只有击败私有缓存不能证明共享是文件系统独有优势。

所有运行记录逻辑缓存、pinned/in-flight、客户端/服务端驻留与共享内存、OS page cache 和 IoTDB 服务资源；控制相同机器/存储介质，或将拓扑差异独立报告。远端网络读与本地 mmap 的差异不能归因于窗口规划本身。native page faults、QuerySource 返回字节、网络/磁盘字节分别计量。

## 6. 工作负载、指标与结论范围

### 6.1 采样核查与FS适用性（2026-09-28补充）

**模型支持滑动窗口，不等于训练顺序具有局部性。** 核查 Transformers `v4.57.1` 的标准 map-style 路径：未启用 length grouping 时训练返回 `RandomSampler`，再由 Accelerate 处理；常规单进程评估返回 `SequentialSampler`。最终冻结版本和分布式配置须重新检查。[Trainer 源码](https://github.com/huggingface/transformers/blob/v4.57.1/src/transformers/trainer.py)。IBM `ForecastDFDataset` 则用 `time_id=index*stride` 定义窗口，默认 stride=1，并从内存 dataframe 截取历史/目标。因此它有重叠的样本集合，但原生训练会打乱访问次序；不是本身就提供跨物理文件查询的存储后端。[Dataset 源码](https://github.com/ibm-granite/granite-tsfm/blob/main/tsfm_public/toolkit/dataset.py)

PatchTST、PatchTSMixer 和使用此数据路径的 TTM，均不能仅凭模型名称被判定比用户描述的 Timer4 随机采样更适合FS。保留四类独立工作负载：

| 采样/执行方式 | 定位 | 需要证明的内容 |
| --- | --- | --- |
| 原生随机窗口顺序 | 必须保留的主训练基线 | 缓存小、活跃序列多时可能几乎无复用；保持每个job的sample/batch顺序、随机种子和样本集合，不以顺序训练替换它。 |
| 顺序滑窗评估/滚动回测 | 自然具有局部性的独立场景 | 解码片段在相邻窗口间复用；结果标明评估/回测，不计为训练加速。默认评估仅在窗口索引按实体/时间排列时产生这种局部性。 |
| 多个独立随机训练实验 | 核心共享假设，待trace证实 | 同一物理片段在缓存有效驻留期间跨job再被请求，或发生并发请求合并；不能为了提高命中强制各job相同seed或同步样本。 |
| 分块随机/局部打乱 | 可选采样策略扩展 | 改变了训练样本次序；所有后端使用相同策略，单独验证收敛/质量，不能把收益全部归因于FS。 |

在固定batch/有界预取范围内，可以对物理读取去重、合并并恢复原sample顺序；此优化不需要改变训练的随机次序，但只有请求实际命中相同物理片段时才有效。prefetch/in-flight内存和等待必须计入，禁止无限lookahead制造复用。

跨文件窗口是否存在由**逻辑样本范围与真实物理文件边界**决定，与 shuffle 无关；shuffle 只改变何时访问。首轮单 stream A/B 不含跨文件正例，不能以内部 record-batch/row 边界冒充。以后遇到真实连续文件时，A/B 两侧必须使用同一逻辑窗口；若文件代表独立序列，不能强行续接。

对无gap、均匀起点、等长分片P、总样本跨度W=L+H且W≤P的理想布局，忽略整个序列两端后，跨界样本比例约为 `(W-1)/P`。该估算是布局推导，不是实测性能；报告真实跨界样本比例以及它们对整轮耗时的贡献。不能只挑边界窗口或不合理缩小文件后声称普遍训练收益。

**PB00增加采样适用性门：** 先收集原生随机读取trace，记录window ID、entity、channel/range、物理解码单元、job/worker和访问次序；给出片段字节复用距离、相同预算下的字节命中/重解码率、跨界窗口比例和数据等待占比。未获得这些证据前，不把“滑窗重叠”“多实验”“换成PatchTST”当作缓存收益证明。若随机训练无收益，应保留该结果，将已证实价值限定到局部范围读取、跨界样本完整性、评估或其他实际有效工作负载；数据完全驻留内存时继续保留R0/R2强对照。

### 6.2 执行矩阵和指标

- **E1/E5：** ETTm1/Electricity 从 `L=96/336/512`、`H=24/96`、`S=1/16/L` 开始，先固定一种模型配置再扫存储参数；模型 patch 长度与 L 合法匹配。PEMS-SF 另用 `L=48/96,H=12,S=1/12`。小规模窗口逐项比值/mask/顺序，完整数据按 manifest hash 和分层抽样审计。
- **E6（首轮之后）：** 有真实多文件连续序列时核对跨文件窗口；另列受控布局变体才改变文件/record batch/压缩 buffer 边界。PEMS-SF 用来验收“独立行不得续接”，不充当连续跨文件正例。
- **E7：** 1/2/4独立 Trainer jobs，共享 snapshot，但使用不同 L/H、通道子集和 train-fitted transform。先 loader-only 隔离共享读取；训练时固定每job GPU/CPU分配，报告吞吐与公平性，避免把GPU争用当读取收益。不同 view 在同一 job 内不任意改变模型输入维数。
- **E8：** 固定 sampler seed 的顺序/打乱访问、worker数和有界预取； sweep 总缓存预算、压缩/未压缩，拆分冷启动/暖索引/暖解码/暖OS缓存。任何重排需要保持相同训练样本顺序，或作为另一策略实验。
- 同时报告 loader-only samples/s、batch p50/p95、数据等待、解码次数/字节/时间、有效载荷/offset/index读量、copy与H2D、总内存，以及 Trainer epoch/step wall time、GPU利用率、MSE/MAE。固定步数的吞吐实验与到相同验证质量的训练实验分别报告；初始化/种子/精度/优化器/批量/梯度累积保持一致。
- 发布 setup、转换/摄取/建索引、首epoch、后续epoch和多视图总成本；官方内存基线在数据能放进内存时可能更快，结果如实保留。对不满足预算的配置报告 OOM/不适用及原因，不换成刻意低效实现。

ETTm1、Electricity、PEMS-SF 主要证明语义和复用机制，通常不足以证明 out-of-core 收益。LargeST 也必须报告实际工作集与可用 RAM，16.85 GiB 在大内存服务器上仍可能完全缓存。复制/重复序列放大只能标为受控压力测试；需要真实工作集、预算和系统I/O证据后，才做规模结论。模型计算占主导时不能从 loader-only 加速外推出同等端到端训练加速。

## 7. 并入实施计划的交付单元

| 单元 | 接入原计划 | 待交付与完成条件 |
| --- | --- | --- |
| PB00：公开依赖和样本冻结 | A00 | 固定 Python/Torch/CUDA/Transformers/Accelerate/Datasets/PyArrow、可选 granite-tsfm commit/model revision；ETTm1＋Electricity source manifest、转换 schema 和公开 oracle。import → 单batch forward/backward → Trainer train/eval/save/resume 后生成实测 lock。 |
| PB01：可追溯转换与窗口清单 | A03/A06/A07 | 独立下载/转换脚本、split/normalization artifacts、stream/NumPy匹配、单 stream 内 continuation 和确定性 window manifest；无私有 import。真实跨文件正例不阻塞首轮 A/B。 |
| PB02：强公开后端 | A04/A05/A08/A09 | 优先完成本地 R1 与 IoTDB-FS Q2 的同源 A/B，并保留 R0/R2 强参考；R3/R4、私有/共享缓存对照及读取计数作为机制与成本分解。全部消费相同 window IDs，验证值/mask/顺序。 |
| PB03：Trainer 接入 | A10/A11 | 共用 PyTorch Dataset/collator 与标准 Trainer 采样，记录并对齐实际 sample ID trace；长驻服务有界批量桥接、PatchTST/PatchTSMixer 两份配置；标签/预测输出/mask、worker重连、取消和预算验收。FUSE 路径另计 IPC 编码/解码开销。 |
| PB04：实验与发布 | A13 | 先发布单进程、单 GPU 的文件系统/IoTDB-FS A/B 正确性与基本性能；再扩展 E1/E4/E5/E7/E8 的公开配置、原始计数和训练结果。E6 与 1/2/4 实验、压缩和缓存消融单列后续阶段，失败/无收益配置照录。 |

2026-09-29 已运行 [PB00 复验与 PB01 数据正确性阶段验收](../reports/public-training-pb00-pb01-acceptance-20260929.md)：源文件、环境、两模型最小训练闭环和 Arrow/NumPy 全量等价通过。其后已生成 PB01 split/normalization/window manifest，并完成 本地 Arrow 与 IoTDB-FS 的两步 A/B 接线（服务器归档：`docs/public-training-ab-wiring-20260929.md`）。真实跨文件正例仍留待其独立阶段；两步 smoke 不代替 PB04 完整性能验收。

建议放在 `tests/benchmarks/arrow_training/`：`prepare_public_data.py`、`public_backends.py`、`train_hf.py`、`configs/`、`manifests/`、环境锁与 README；实际文件命名/桥接 API 在实施前收敛。大数据和checkpoint不直接提交；复现包提供来源、hash、许可、命令及预期审计结果。后续 TsFile 沿用同一模型、样本与视图协议，只替换物理 reader，不复制训练框架。

首轮 A/B 验收门：新环境无需 TimeBench、TimerLathe 或内部路径，即可核验公开源、转换单 stream、冻结 split/normalization/window manifest，以 Trainer 的原生随机训练/顺序评估采样分别从本地文件系统和 IoTDB-FS typed 路径训练与评估两种模型；每对运行的 sample ID、输入/目标/mask 与预测尺度一致，报告相同预算下的基本性能和质量。未运行的 R3/R4、跨文件、多实验和缓存消融明确标为后续，不借首轮结果替其背书。


### 2026-09-30：准备对象复用

基于37.27 MB声明审计，实现一次性 prepared 对象与共享通道表；后续窗口请求仅传 ID 和坐标，消费后显式释放结果 route。接口及资源边界见 [prepared 协议](../contracts/arrow-prepared-training-registration.md)，同配置 Electricity 50-step 三组消融见 [实验报告](../reports/public-training-prepared-registration-20260930.md)。首个 minibatch 包含初次 prepare，采样器与数据版本保持不变。


### 2026-09-30：客户端批量转换

客户端按role/channel跨样本批量转换，默认batch，sample作为显式消融对照；IPC与服务端不变。两模型冷暖各两次重复，暖缓存训练均值7.925→6.562秒。见 配对消融与验证（服务器归档：`docs/public-training-client-conversion-20260930.md`）。
