# Arrow 查询、训练窗口与迁移：代码实施计划

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

日期：2026-09-27  
代码基线：`feat/arrow`，`41751b2267137133b2001c1cde24b49ff45aec72`  
状态：实施计划；本文不表示代码、依赖或验收已完成。

**已确认的实际数据基线：** 本轮支持 Arrow IPC **stream** 中的 `list<float32>` 和 `list<list<float32>>`。TimeBench 的多变量 `target` 是后者：一条顶层逻辑行包含多个连续存储的叶子序列。物理模型只记录 List 层数、每层子索引及叶子位置；`channel`、`step`、`time` 是经过单独探测/验证后才能附加的语义。此次用户确认取代原 B1 的 IPC file＋标量列假设，也将之前排除的单层 List 纳入范围。

远端环境核查（服务器归档：`docs/arrow-experiment-environment-baseline.md`）还发现了 `list<float>` 的其他 shard；这些单层 List 现在按其真实结构读取，不隐式包装成双层或猜测“单通道”语义。远端 IoTDB-FS-C 是无 `.git` 的旧副本，先同步到可追溯源码快照，再在独立目录构建；本次没有重新检查远端文件。

**公开实验基线更新：** TimeBench/TimerLathe 属于未公开训练框架，只作可选内部回归；公开交付不依赖其数据、代码或加载优化。采用 HF Trainer＋PatchTST/PatchTSMixer＋ETTm1/UCI Electricity，PEMS-SF 为独立行补充，LargeST/TTM 为扩展。完整候选、来源、许可、转换协议和强读取对照见 [公开训练基线计划](public-training-baseline-plan.md)。公开 CSV/TXT/H5 经独立转换器生成上述 `[channel][step]` IPC stream，不改变已确认的 Arrow 核心输入设计。

上位设计：[Arrow & TsFile typed query implementation plan](IoTDB-FS-C Arrow & TsFile on the typed query model — implementation plan.md)。本计划将其中 Arrow 部分拆成可独立审阅的实现单元。文件名和新增接口是拟定名称，代码实施时遵守 [AGENTS.md](../../AGENTS.md)；不添加兼容层，Git 写操作由用户负责。

## 1. 本轮交付范围

本轮先完成 Arrow，同时保留后续 TsFile 可以接入的格式中立边界。训练窗口是本轮核心要求，不推迟到 TsFile 之后。

- **M1：Arrow 查询基础闭环。** IPC stream 内容验证与消息索引、B/L/Sigma 摄取、精确序列行选择、`target[channel][start:end]` 通道投影与行内区间、映射后标量谓词；通过真实 QuerySource 范围读返回有明确 step/channel 语义的 typed 结果。顶层行筛选和序列内部窗口分开表示。
- **M2：Arrow 训练闭环。** 多变量重叠窗口按需生成、解码片段复用、跨文件局部组装、多个实验共享数据并输出不同视图；原生 C++ cursor 先完成。
- **M3：生产入口与迁移。** 同一常驻运行时提供有界查询/窗口虚拟文件，修复 FUSE 生命周期，迁移旧 Arrow 版本，完成 live 验收和实验记录。

暂不实现 IPC file reader、单层与双层 List 之间的自动变形、三层以上的任意递归 nested 类型或 TsFile decoder、v3/tree/table 选择，也不加入任意跨格式 join、分布式缓存、GPU tensor 零拷贝和完整训练框架。格式中立接口用内存 fake provider 验证，避免为一个尚未实现的 TsFile adapter 添加空壳。

## 2. 实施基线与决策门

用户已确定 Arrow 优先、训练优化目标、IPC stream 输入、单双层 List 结构，以及 A02 先绑定精确 source/version＋物理 batch/row、物理模型与语义完全解耦。下表区分这些确认事实与仍待确认的**接口/策略建议**，不把此前所有 D1–D19 自动视为批准。

| 门 | 建议基线 | 影响单元 / 原决策 |
| --- | --- | --- |
| B1：格式与类型（已确认） | IPC stream，物理目标 `list<float32>` 或 `list<list<float32>>`；读取必要标量/指定辅助 metadata。List 的 int32 offsets 先实现，LargeList/FixedSizeList/其他 float 位宽不悄悄混用。LZ4_FRAME/ZSTD 与字典依赖按 A00 识别、验证；不支持的必需 encoding 明确失败。 | A00/A03/A04；D3/D5 格式/目标类型已收敛，D14 错误策略仍需明确 |
| B2：物理模型与语义 | A02 先固定精确 source/version、record batch、row、native field 和匿名 List 子路径。单层 List 的子路径为空，双层 List 的子路径是一个 ordinal；叶子位置仍是物理位置。A07 另行探测/验证各层含义，再绑定 series/channel/step/time；不得从层数或字段名直接推断。 | A02/A04/A07；D4/D9/D18 |
| B3：共享边界 | 独立、格式中立的 fragment-provider seam，复用 Arrow decoder；常驻运行时持有共享 cache。普通查询和训练窗口使用同一 decoder/cache。QueryBatch 最终有界拷贝。 | A05/A06/A08；D17/D18 |
| B4：训练语义 | 首先按序列内 sample-count L/S/H/G 生成窗口；有验证的 start/cadence 或 timestamp 数据才支持绝对时间。缺失/ragged 时明确 RejectWindow 或 Mask，不静默补零/缩短通道；重复 series/step/channel 冲突报错。 | A07–A09；D15/D16 |
| B5：分片范围 | 跨 shard 可以批量请求不同序列，不能自动续接它们。跨文件单窗口必须有显式相同 series id、分段起点、连续性和 channel 映射；先时间分段，再显式变量分段 join。 | A07/A08；D16 |
| B6：序列覆盖索引 | 摄取时 L 索引 stream messages、递归 buffers、outer/inner offsets 与必要 validity；DatasetSpec/sidecar 绑定逻辑行、channel labels、序列位置/时间和可选续接片段。首次准备只解析必要 offsets/metadata/时间数据，不解码所有 target values。物理 L 与数据集语义 sidecar 各司其职。 | A03/A07；D12/D16 |
| B7：资源 | 全过程分别计量 planning I/O、实际 payload I/O、resident/pinned/inflight/输出内存。共享 producer 用服务预算，消费者另有逻辑需求和结果上限；不存在“首个请求替所有实验承担预算”的隐式规则。 | A02/A05/A06；D12/D17 |
| B8：入口 | 原生 API 优先；FUSE 使用新 typed 路由和 IPC stream，训练文件仅表示有界 sample group。stat 前物化/cache 与 open 的 snapshot 关联机制需要单独定稿。 | A10/A11；D1/D2/D19 |
| B9：元数据 | Arrow adapter 先拥有有限内建 Sigma；C1 支持已声明标量字段的缺失 NULL。训练变量选择依据完整 schema/coverage，不依赖 capped 集合。typed membership 和全局配置语法迁移后置，不作为 Arrow 主线前提。 | A02/A03/A07；D13 |
| B10：迁移写入者 | 建议首版离线迁移：停止挂载写入进程，由一个迁移进程独占该 namespace。若要求在线，则管理命令必须交给现有 owning daemon 执行，不能另起第二 writer。 | A12；现有 backend 部署约束 |

关键限制：B6 的 stream 消息/嵌套物理索引与 dataset 语义 coverage 是两层不同索引；均绑定精确版本，不能重复承担同一来源不明的时间索引。sidecar 存放位置、版本、校验、原子发布和失效策略仍需确定。建立 List offsets 索引涉及 body 中的 offset/validity buffers，不能称为只读 message metadata；该成本与 target 数值解码分开计量。

## 3. 模块与接口边界

所有公共头文件位于 `include/iotdbfs/`，实现位于 `src/`。以下清单是新增/修改范围，不要求每个概念机械拆成一个类。

| 模块 | 拟新增文件 | 责任 |
| --- | --- | --- |
| Arrow 摄取与布局 | `ArrowLayout.h/.cpp`、`ArrowFormatAdapter.h/.cpp` | stream 消息识别与边界验证、完整 native schema、递归 offsets/validity、物理 fragment 位置、有限 Sigma。 |
| Arrow 查询与解码 | `ArrowFormatQueryAdapter.h/.cpp`、`ArrowFragmentProvider.h/.cpp`、`ArrowRangeReader.h/.cpp` | 序列绑定后的 typed schema/capabilities、channel/step 切片、实际范围读取与 child float 片段解码。Arrow 类型只出现在实现内部。 |
| 物理数组定位与查询模型 | `formats/arrow/ArrowQuerySubject.h`、`formats/arrow/ArrowQueryModel.h/.cpp`、Arrow layout reader | 精确 source/version＋batch/row/field、匿名 List 路径、List 解析和 schema/capabilities；2026-10-01 从 `query/core/ArrayQueryModel` 迁入，与 GRIB 模型并列。 |
| 序列语义绑定 | `ArrowSequenceBinding.h/.cpp` | 在物理定位之上探测 dataset logical row、变量、位置/时间语义；不改变底层数组地址模型。 |
| 公共 fragment 模型 | `DecodedFragment.h`、`FragmentProvider.h` | 不可变 fragment key、字段与行/时间位置、有效性、字节估算、只读句柄和依赖枚举。 |
| 共享执行 | `QueryRuntime.h/.cpp`、`DecodedFragmentCache.h/.cpp` | provider 生命周期、single-flight、受控并发、lease、预算、取消和 metrics。 |
| 数据集与覆盖 | `TrainingQueryModel.h/.cpp`、`DatasetSnapshot.h/.cpp`、`DatasetCoverageIndex.h/.cpp` | 显式 schema/entity/time 映射、精确版本、coverage sidecar、sample/split 定义。 |
| 窗口与视图 | `WindowPlanner.h/.cpp`、`WindowAssembler.h/.cpp`、`TrainingQueryModule.h/.cpp` | 按需 anchor、需求并集、跨文件对齐、input/target、独立实验变换和 WindowCursor。 |
| 对外编码与入口 | `ArrowResultEncoder.h/.cpp`、`TypedQueryFrontend.h/.cpp`、`TypedResultStore.h/.cpp` | QueryBatch/WindowBatch 编码、路径解析、有界物化、结果句柄。 |
| 重摄取与迁移 | `ReingestionService.h/.cpp`；管理命令入口名称在 A12 定稿 | old-version reader 到新版本摄取、namespace publication、迁移日志与恢复。 |

修改现有 `FormatAdapter.h` / `Ingestion.cpp` / GRIB adapter 和测试替身；修改 `QueryPlanning.h/.cpp`、`QueryModule.cpp` 中明确列出的验证/预算入口；修改 `main.cpp` 的运行时构造与 FUSE dispatch。不将训练参数塞进现有 GRIB interpolation 字段。

接口要点：

1. `FormatAdapter::begin_ingestion(const FormatSource&)` 取代无参入口，更新全部调用者和实现；不保留旧入口转接层。source 回调只在同步 ingest 生命周期有效。
2. `FragmentProvider` 负责“覆盖需求 → stable fragment identities/dependencies”和“有授权、有预算的 source → immutable decoded fragment”。独立于路径解析和实验变换；普通 query adapter 与训练模块共同依赖它。
3. `QueryRuntime` 注入 adapter/provider/cache，不能每个 open 创建一个私有 cache。服务 namespace 参与 key，防止不同后端的相同 path/version 混用。
4. `TrainingQueryModule` 提供 prepare(snapshot/spec)、请求一组样本和 WindowCursor。数据读取发生在需求到达后；构造快照可扫描 catalog/索引，不读取全部 value payload。
5. WindowBatch 明确样本轴、时间轴、input/target 变量顺序、mask、sample id、view id 和分片 provenance。首版允许最终数组拷贝，不承诺修改 LogicalArray 后的零拷贝。
6. 普通 QueryBatch 继续按最终投影 schema 严格比较；序列视图先将目标行的 channels 映射为标量字段。变量分片训练使用分字段/通道子计划，在 assembler 验证组合 schema，不能把 `{x,y}` 原样交给缺少所需通道的源。
7. A02 将匿名物理 `PhysicalArraySubject` 放入 `QueryFragment`，要求精确 source/version，并提供字段验证前的 capabilities 入口，使 batch/row/field/List 路径在字段验证前可用；A04 审阅后入口统一为 `capabilities(context, fragment)`，同一个 `QueryPlanningContext` 向 prepare 传递精确 source、请求 limits 和已解析 layout。A03/A04 负责从 native schema/offsets 验证形状并给出位置视图；A07 才绑定语义。不得把行号藏进 SQL 字符串或假冒 list 为 Binary。raw nested 结果的通用读取不属于此次 QueryBatch 扩展目标。

### 3.1 单双层 List 的非语义地址模型

物理契约只使用 `field_id`、`record_batch_index`、`row_index`、`child_indices` 和叶子区间。`list<float32>` 的 `child_indices=[]`；`list<list<float32>>` 的一个被选中子列表使用 `child_indices=[c]`，多个子列表可按请求顺序列为 `[2]`,`[0]`。这里只把 `c` 当 ordinal，不宣称它是变量。两种形状共享 `ArrayListIndex::list_span(level,index)` 的按需接口，解析结果均为叶子 float 的绝对区间及 null List 状态；null 与有效空列表不同。实际 IPC buffer/bitmap 读取、压缩和 leaf value validity 属于 A03/A04，不在物理坐标中预先解码。

物理路径条数不得超过 `QueryLimits.max_ranges`，并且在去重前检查上限。若 row 本身为 null，路径解析短路并返回 `null_list_level=0`；若选中的内层 List 为 null，则返回 `null_list_level=1`。null 父 List 所覆盖的 child buffer 内容没有可观察语义，因此不检查该父节点以下的 child ordinal；只有有效父 List 的子索引越界才报 `InvalidQuery`。有效空列表保留 `valid=true`、`begin=end` 且无 null 层级。叶子 float 自身的 null 仍由后续 validity 读取区分。精确 source/version 若确实不存在报 `SourceNotFound`；若存在但被文件 metadata 或格式条件筛除，则返回空选择，不把筛除误报为不存在。该空选择目前尚无声明式结果 schema，A02 的空结果 schema 任务仍待完成。

单层 List 直接用 row r 对应的 offsets `I[r], I[r+1]` 定位叶子；双层 List 额外用外层 offsets 选择子列表。只有经过语义探测后，以下双层示例才可解释为 `[channel][step]`：

对于某个 record batch 的 `target: list<list<float32>>`，用 O 表示外层 offsets，I 表示内层 offsets，V 表示叶子 float values。以下下标是经校验/rebase 后相对于对应序列化 child buffer 的下标：

```text
row r 的 channel 数：             C_r = O[r+1] - O[r]
row r 的 channel c 对应 inner 行： j = O[r] + c
该 channel 的 values 区间：        [I[j], I[j+1])
该 channel 的序列长度：             T_rc = I[j+1] - I[j]
请求 step [s,e) 对应 values：       [I[j]+s, I[j]+e)
未压缩 float32 字节范围：           values_base + 4*(I[j]+s)，长度 4*(e-s)
```

约束为 `0 <= c < C_r`、`0 <= s <= e <= T_rc`，所有算术检查溢出。`values_base` 是真实未压缩叶子 buffer 的 payload 起点，绝不是 List offsets buffer 起点。IPC 带 compression 但该 buffer 使用 uncompressed sentinel 时，还需正确跳过对应 prefix；压缩 buffer 不能套用上述原始字节公式。Dictionary/extension/非 float32 变体不套用此公式。

按三个层级联合判断 validity：序列行 r、channel 所对应 inner 行 j、各叶子值；null list 与空 list、null channel 与零长度 channel、null float 与 NaN 分别保留语义。`n_channels`、`length`、labels 必须与 offsets 实际结构核对，不能替代边界校验。变量连续不意味着所有变量等长，也不意味着所有行拥有相同标签。

物理读与缓存分两种路径：

- **未压缩叶子 buffer：** 用 offsets 定位所需 channel 的时间子区间，按与窗口无关的固定 child-range tile 边界规范化缓存 key。只读取所需 tiles/validity 与索引；tile 的额外读计入放大。不能用精确窗口边界作 key 导致重叠不复用，也不能强制缓存完整 `(batch,target)`。
- **压缩叶子 buffer：** 一个物理 buffer 可能覆盖多个 channels，甚至多个序列行。首次请求需要完整相关 buffer 解压时，以该物理 buffer 标识 single-flight/cache；offset/validity buffers 独立判断压缩。之后不同 channels/windows 共享解压结果。tile 层引用必须与父 buffer 联合计费，不假称仅选一个 channel 就只读该 channel 的压缩字节。

本轮同时量化 cold-read amplification、decompression amplification 和 warm reuse。超预算的不可分压缩 buffer 明确报错，不为通过测试偷偷重写 B/rechunk；可选重编码派生数据集是另一个未来设计。

### 3.2 序列选择与 stream 索引

查询语义为 `dataset/snapshot → logical series row → channels → step window → view`。HF dataset 的 shard 顺序来自固定的 `state.json`/数据集 manifest；有 `_indices` 映射时，必须显式支持其 logical-to-physical 映射或拒绝该数据集，不按文件名排序猜测。物理身份始终保留 source/version/batch/row；重复 item_id 不能直接合并为同一连续序列。

摄取按 IPC framing 顺序扫描 Schema/DictionaryBatch/RecordBatch，跨 consume_chunk 边界维护有限状态，建立可随机定位的 L。continuation indicator 不是可靠独有魔数，必须验证完整 Schema message 和类型；stream 不要求 ARROW1/footer。合法 schema-only stream、完整消息边界 EOF/显式 EOS、截断元数据/body 和 EOS 后垃圾的规则均需测试；不支持的 legacy framing、拼接 streams 明确报错。字典辅助字段若被使用，L/decoder 必须重建对应 batch 的 dictionary epoch、replacement/delta 依赖，不能读取“最后一个字典”套用全部 batch。

L 保存 schema、消息位置、body lengths、递归 field nodes/buffer spans、offset 索引及 validity 所在位置；对于已解析 offsets，可存经过校验的分页目录供行/通道寻址，不必把所有 numeric values 复制到 L。索引仍有明确预算：现有 FormatIngestionArtifacts.layout 是完整 vector，超过内存/layout 限制要失败或在另行批准的接口修改中流式持久化，不能虚称无界可扩展。

训练结果从所选行映射为 channel Measure＋step Dimension；无时间信息时只支持位置窗口，有经过验证的 start/freq 或显式 timestamps 才提供 time。字符串 freq 的 calendar 语义不可当作固定 ns 时长猜测。原始 stream 普通 read byte-identical；此逻辑视图不改写 B，也不要求完整递归 List LogicalArray 成为所有格式的公共类型。

## 4. 可审阅实现单元

每个 Axx 完成后独立验收；勾选框随实测进度更新。必要时可以拆 PR，但本任务不执行 Git 写操作。

### A00 — 固定依赖、基线和 Arrow I/O spike

依赖：无。范围：CMake、`ci/dependencies.lock.json`、`ci/install_dependencies.py`、`ci/activate_dependencies.py`、`ci/tests/test_dependencies.py`，新增可重复 fixture generator/spike。

- [x] 记录现有 build/test 命令、发现的测试数、平台/toolchain 与工作树状态；先确认当前依赖是否可用。
- [x] 选择并锁定兼容本项目 C++17/toolchain 的 Arrow release/commit、checksum、IPC/codec 选项、license/NOTICE；不凭最新版本号直接升级编译标准。
- [x] 执行公开基线 PB00：固定 ETTm1/UCI Electricity 原文件 hash、channel 布局、独立转换器与 pyarrow/NumPy oracle；冻结 schema、compression、batch/row/series 长度。生成 matching IPC stream fixtures：多行、多 batch、两层 list、三层 validity、ragged、压缩/未压缩，保留 hash 和适用的 HF metadata；单层 list 不充当多变量验收。TimeBench/TimerLathe 仅另列内部回归。
- [x] 验证 PatchTST/PatchTSMixer 的 HF Trainer 单 batch forward/backward、train/eval/save/resume，再锁定实际 Python/模型/数据依赖。PB01–PB03 与 A03/A06–A11 接合；不把官方 recipe 的可用性当作本机运行成功。
- [x] 用 recording range reader 及独立 message 索引模拟 L 定位后直接读取选定 row/channel/step，而不是每次从 stream 头顺序解码；比较 batch reader 与 child-buffer range 路径。分别测未压缩 tile 和压缩整 buffer 的读取/解压放大、offsets 成本与字典状态。真实 L 接入属于 A03/A04。
- [x] 扩展真实 installer/verification；生产与 `unit_tests` 独立 source/link 清单都加入依赖。配置阶段依旧需要现有 FUSE/ecCodes/IoTDB SDK，不能声称纯 Arrow target 已摆脱这些依赖。

完成条件：能读取公开数据独立转换生成的两层 list stream，窗口值/mask 与公开 pyarrow/NumPy oracle 一致，有完整 message/offset/value range trace；锁文件和安装行为一致，公开环境不依赖内部框架。依赖版本以 spike 结果为准。

A00 实测记录见 [Arrow 依赖、公开数据与范围读取基线](../reports/arrow-query-spikes.md)。当前主机已验证 Arrow C++ 实建/安装、项目生产与单元测试链接、PB00 oracle 和物理 range trace；完整 CI 依赖前缀仍须在具备 gcc-11 与已核验自定义 SDK ZIP 的 runner 上运行，不能将本机 g++13 手工前缀称为完整 CI 安装器验收。

### A01 — 打通 stream 增量索引与稳定 source 访问

依赖：接口 B1；可以与 A00 的依赖调查独立进行。范围：`FormatAdapter.h`、`Ingestion.cpp`、`GribFormatAdapter.cpp`、全部 session fake。

- [x] 将已有 FormatSource 传入 session；验证越界、短读、读取失败和回调有效期。
- [x] 保留 consume_chunk 顺序，允许消息头/metadata/offsets 跨 chunk；不复制 B 或整个 batch body 到第二份完整内存 buffer。finish 可从原稳定 source 有界读取 offset/validity 等依赖；不是为了不存在的 stream footer。真实 Arrow FlatBuffer 解析留给 A03。
- [x] 保持当前实际执行顺序：复制 B/consume → finish 导出 artifacts → seal B → 写 L → Sigma/identity → publication callback → publish；失败时 abort，禁止读取未发布 backend 当作 source。
- [x] 回归 GRIB ingestion、pass-through、任一步失败的存储版本不可见性与重试边界。

完成条件：stream fake 在任意 chunk 切分下得到相同 L，finish 所需的 source 范围读受约束；GRIB 与普通字节摄取测试通过。

A01 实测（2026-09-27）：`begin_ingestion(const FormatSource&)` 绑定同一受检原 source，探测/复制/session 读取均检查范围和精确长度；探测短读不再静默降级为 pass-through。IPC-shaped stream fake 在 1、2、3、4、5、7、8、11、64 字节 chunk 下生成相同 layout，并在 `finish()` 从原 source 读取两个 offset；测试验证未发布阶段、回调失效后的安全拒绝、每个存储阶段失败后 abort 与同 key 重试。真实 GRIB2 样本和普通字节摄取通过。隔离服务器生产与单测目标编译通过，`ctest -L unit` 为 662/662。这里验证的是存储版本的不可见性；生产 `main.cpp` 的 namespace inode callback 在 blob publish 前执行，若 publish 后续失败，其独立的 inode 补偿/可见性问题仍需在生产迁移阶段解决，不能由本单元测试推断已解决。A02 的 general query planning I/O 预算问题亦未在 A01 修改。

A01 模糊测试修复（2026-09-27）：格式探测时一个候选的越界读取只使该候选不匹配，后续格式继续探测；即使候选在读取失败后声称匹配也不选中。真实 source 短读/失败仍使整次摄取失败，选中格式在 session/finish 阶段越界仍是 AdapterFailure。新增 0 字节、3 字节、16 字节及“前一候选越界、后一候选识别/真实读取失败”回归。隔离 Linux 生产目标编译通过，`ctest -L unit` 为 670/670。

### A02 — 补齐 Arrow 必需的 query core 契约

依赖：B2/B7/B9。范围：`QueryPlanning.h/.cpp`、`QueryModule.cpp`，必要的新 metadata schema registry 与统计结构。

- [ ] 允许声明支持运算符但 pushdown=None；未知/不支持运算仍报错。Candidate 的 residual 和 hidden fields 由真实结果测试验证。
- [ ] 增加已声明 scalar Sigma schema 的来源，区分 UnknownField、known-but-absent typed NULL、同名类型冲突和 empty catalog；不把缺失可选统计当作“值不存在”的证据。
- [x] 增加非语义 `PhysicalArraySubject` 的字段验证前入口：精确 source/version、batch/row/field、匿名 List 路径；`capabilities(source, fragment)` 可在报告字段前读取绑定。`ArrayQueryModel` 已验证单层/双层 Float32 List 的路径、offset 范围、null/empty 和越界。此项只完成物理契约，不声称已完成 Arrow native layout 探测或语义查询。
- [ ] A03/A04 按 native shape 验证 List 层数、field、row 和每条路径，再暴露位置标量视图；A07 单独探测/绑定变量与时间语义。其他 known unsupported native fields 在 UnknownField 前给出 UnsupportedCapability，不把核心 nested target 排除或伪装 Binary。
- [ ] capabilities/prepare 的实际读通过规划预算包装；execute 的预算继续约束请求。共享运行时的物理预算与消费者预算接口在 A05 实现，不保留捕获 planning source 的隐式通路。
- [ ] 保持 strict projected schema；检查未投影额外列成功。定义空结果/空源 schema 的处理，训练视图可从显式声明 schema 生成有类型空结果。
- [ ] 输出 query/phase/source/field 错误与 metrics；不顺带修改 SQL membership、TsFile table selector 或所有 GRIB frontend。

完成条件：no-stats residual、NULL/NOT/OR/IN、schema mismatch、planning I/O 超限、空结果均有语义测试；全部原有测试通过。

A02 物理模型验收（2026-09-27）：新增 `ArrayQueryModel`，统一用匿名 List 子路径和叶子区间解析一层/两层 Float32 List；`PhysicalArraySubject` 只包含 batch/row/native field/路径，精确 source/version 由请求 selector 固定。`FormatQueryAdapter::capabilities(source, fragment)` 在字段校验前收到绑定，GRIB 对不适用的物理 subject 显式拒绝。隔离 Linux 构建目录 `public-arrow-baseline/a02-physical-array-20260927/build` 中 `unit_tests` 与 `iotdb_fs` 编译通过，`ctest -L unit` 为 668/668。**A02 其余 Sigma schema、no-stats residual、planning I/O 预算与 Arrow native adapter 仍未完成；此记录只证明非语义数据模型与 core 入口。**

A02 边界回归（2026-09-27）：精确版本存在但被 metadata 谓词或格式选择排除时，物理查询返回空选择；版本确实不存在时保持 `SourceNotFound`，包括目录已经下推谓词并返回零版本的情况。路径数量在去重前受 `max_ranges` 约束。null List 短路时报告 `null_list_level`（外层 0、内层 1），有效父 List 的坏子索引仍报错。隔离 Linux 生产与单测目标编译通过，`ctest -L unit` 为 673/673。

### A03 — Arrow layout、摄取和身份注册

依赖：A00/A01，B1/B6。范围：ArrowLayout、ArrowFormatAdapter、FormatRegistry 注册、内建 Sigma/xattrs、单元测试。

- [x] 验证首个 Schema message/framing，再增量验证消息链、长度、结束规则；不用 ARROW1/footer 探测 stream，不把单个 ff ff ff ff 当充分证据。已识别后损坏不静默降级；探测 I/O 失败明确传播。
- [x] 实现 `IOTDBFS-ARROW-LAYOUT/1`，明确 container=stream，保存 schema、batch 顶层行范围、messages/body spans、递归 field nodes/buffers、实际存在的一/两层 offsets 索引及对应 validity 定位、压缩/字典 epochs 和不支持字段清单。
- [x] native schema 同时识别 List/Float32 与 List/List/Float32，保留真实层数；通过按需 `ArrayListIndex` 为 A02 物理路径提供 offset/validity，不把单层补成双层。
- [x] 所有长度/偏移做溢出与 source-size 校验；约束解析记录数与内存，区分 CorruptLayout/UnsupportedLayoutVersion/CorruptPayload。
- [x] 仅输出有限内建 Sigma；完整 columns/schema 放 L。可选统计不完整时保留候选，不作精确排除。不要对 UInt64 extrema 做有损转换。
- [x] 基础索引解析 message metadata 及必要 offsets/validity buffers，不解码所有 target float values；压缩 offsets 自身可能需要解压，应单独计费。channel labels/start/freq/逻辑行映射建立按 B6/A07 另计成本；value stats 不作为定位窗口前提。
- [x] 与生产注册同步引入 Arrow adapter；内容识别不依赖扩展名。普通读取仍返回原 B。

完成条件：错后缀、rename、合法 schema-only stream、多 rows/batches、跨 chunk framing、截断 message/body、坏 offsets/layout、publication 故障注入通过；原 stream B hash 不变。

A03 验收（2026-09-27）：`ArrowFormatAdapter` 已加入生产 registry，在 GRIB 前按内容识别；新增有界二进制 `ArrowLayout` 和 native metadata 交叉校验，保留真实 List/LargeList 层级、null provenance、LZ4/ZSTD 标记及 dictionary delta/replacement 依赖。只生成 container/layout_version/record_batches/rows/fields 五项 Sigma 与 xattr。隔离服务器 `public-arrow-baseline/a03-arrow-layout-20260927/build` 的生产目标通过，单测 **708/708**；新增两模块及其 **35** 项测试通过 ASan/UBSan。6 个 PyArrow PB00 fixtures、ETTm1、UCI Electricity 共 **8** 个输入与独立 Arrow reader 校验一致，原文件 SHA-256 与 PB00 manifest 一致。ETTm1 的 L 为 9,348 B、附加索引读 328 B；207,613,480 B 的 Electricity stream 的 L 为 67,960 B、附加索引读 26,784 B；两者无 float 值重读。摄取本身仍须完整复制 B，不能将附加索引读当作总摄取 I/O。普通 B 保持以字节相等的存储测试验证；rename 路径已核对其复制 format_id/layout 的现有实现，并通过改名后 xattr 身份测试，本轮未启动生产 FUSE 挂载测试。

布局、预算及边界见 [Arrow layout v1](../contracts/arrow-layout-format.md)，实测明细见 A03 oracle report（服务器归档：`docs/arrow-a03-validation-20260927.json`）。当前完整 offsets/父 List bitmap 索引受内存上限约束；大索引分页、float buffer 解码、typed query、语义绑定和窗口共享仍属后续单元。

### A04 — Arrow typed 查询和可复用 decoder

依赖：A00/A02/A03。范围：ArrowFormatQueryAdapter、ArrowRangeReader、ArrowFragmentProvider。

- [x] 先绑定精确 batch/row/native field/匿名 List 路径，分别完成单层与双层叶子区间读取及位置切片；探测并验证语义后才提供 channel projection、step/time slice 与映射后标量谓词。top-level metadata/row 筛选是另一步。没有 raw List 比较的隐式 any/all；time slice 仅在验证时间绑定后声明。
- [x] required channels 包含谓词/目标所需隐藏变量及 offsets/validity 依赖，按 3.1 公式定位。若 channels=[2,0]，读取可排序合并但输出须保留 [2,0]；coalescing over-read 计量。
- [x] 通过 QuerySource 读取实际存在的 1 或 2 层 offsets、所选 child float ranges 和对应的 2 或 3 层 bitmap（List offsets/validity 复用 A03 在 L 中保存的已验证索引，叶值与叶 bitmap 按需读 B）；验证 offset 单调/边界/溢出、null/empty/NaN、ragged、bitmap 非字节对齐。压缩 values 以真实 buffer 为解压单位，不能将相邻叶子序列误当为独立压缩块。
- [x] canonical decode 不应用窗口边界或实验变换；render/filter 阶段完成精确边界与 residual。保留同一 decoder 供 A05–A08 使用。
- [x] plan 与精确 source/layout/schema 绑定；错误版本 StalePlan、source/read 错误和取消正确传播。

完成条件：真实 nested stream 的 row/channel/step 查询对照通过；未压缩只读取指定 child tiles 加索引依赖，压缩场景报告真实整 buffer 成本。达到 M1，不能用扁平标量 fixtures 代替。

实现与宽表/null/ragged 契约见 [arrow-typed-query.md](../contracts/arrow-typed-query.md)。2026-09-27：服务器隔离环境完成 A04；739/739 unit tests、59/59 ASan/UBSan 相关测试、8 个公开 nested IPC 输入 native Arrow oracle 对照通过。公开 ETTm1 与 Electricity 的 `[3,17)`、子路径 `[2,0]` 查询各读取 32 KiB payload（两个固定 tile）；LZ4/ZSTD 每个物理 float/validity buffer 各解压一次并复用。复用目前限于 source-bound provider，公共跨请求 cache 留给 A05。详细验证记录见 arrow-a04-validation-20260927.json（服务器归档：`docs/arrow-a04-validation-20260927.json`）。

A04 后续审阅修复：`QueryPlanningContext` 使 capabilities/prepare 共享单次规划的已解析 L，execute 仍独立校验；未投影 index 时保留连续 Rows domain；子路径上限跟随请求。最新完整回归 750/750 通过，包含 >32 MiB 合法 layout 的默认预算查询，详见上述验证报告的后续记录。

### A05 — 公共 decoded fragment cache 与预算

依赖：A02/A04，B3/B7。范围：DecodedFragment、FragmentProvider、DecodedFragmentCache、QueryRuntime。

- [x] key 包含 storage namespace/exact source/version/schema/layout/decode identity；未压缩以叶子 buffer path＋固定 child tile range 为单位，压缩以物理 buffer id/range 为单位，不能以整个 `(batch,target)` 为必需唯一单位。包含解码 options/dependencies，不包含 window/view/experiment id 或整组 channels。
- [x] 返回 immutable shared handle/lease；offset 索引、validity、序列元数据和叶子片段按依赖共享。两个 channel 集合复用公共时间 tiles；若 channels 同属一个压缩 buffer，不同 channels 也应共享该解压结果而不重复解压。
- [x] 同 key 并发 miss 一个 producer、多 waiter；只发布完整片段，失败可重试；producer 拥有 source 能力及预算生命周期。
- [x] 内存计量分清 cache-owned、外部 pinned、inflight decode、临时压缩 buffer；同一 allocation 不重复计费，移出 LRU 不等于释放 pinned 内存。
- [x] 分配/解压前 reserve；无可淘汰空间时 backpressure，超大不可分片单位明确失败；等待和取消不死锁。
- [x] 服务物理 I/O/decode 计一次，各请求记录 logical demand/output；某 waiter 取消/超预算只影响自己，有其他 waiter 时不终止 producer。
- [x] metrics：hit/miss/wait、physical decode count、decoded/retained/pinned/inflight bytes、eviction/redecode、read/copy bytes。

完成条件：可控 fake decoder 验证并发、取消、失败、生命周期与硬预算；真实 Arrow resident 场景相同 key 只解码一次。随后普通查询和训练查询都接入此 cache。

2026-09-27：A05 完成。公共 `QueryRuntime` / `DecodedFragmentCache` 已接入 Arrow，采用用户确认的服务预算＋独立请求预算与 backend 实例 namespace；共享生产者持有独立 source 能力。服务器隔离环境通过 **780/780 unit tests、131/131 ASan/UBSan、94/94 TSan、8/8 native Arrow oracle**。ETTm1/Electricity 的重叠视图不增加 payload I/O/解码，LZ4/ZSTD 跨通道复用同一完整物理片段。TSan 首次因地址映射冲突无法启动，使用仅测试进程的 `setarch x86_64 -R` 后通过。28 个源码/构建/测试文件与服务器 SHA-256 一致。接口、预算范围与取消竞态见 [公共缓存契约](../contracts/arrow-decoded-fragment-cache.md)，完整日志位置、指标和 hashes 见 A05 验证报告（服务器归档：`docs/arrow-a05-validation-20260927.json`）。在该次 A05 验证时，A06–A08 尚未实现。

2026-09-28 驱逐修复：200 B 预算中先放入 10 个 15 B 片段，再请求 60 B 时，旧实现驱逐全部 10 个；现在每驱逐一个片段就先在锁外实际释放并更新计费，再判断是否仍需驱逐，并发 worker 等待释放完成。新增回归证明仅驱逐 1 个、其余片段保持可命中。修复后通过 **781/781 unit tests、132/132 ASan/UBSan、95/95 TSan**；证据见 驱逐修复报告（服务器归档：`docs/arrow-a05-eviction-fix-20260928.json`）。

2026-09-28 条目上限驱逐顺序修复：当最旧片段仍有 lease、另有未持有片段时，原实现先驱逐 pinned 片段，使下一次查询重复解码。现在先按 LRU 驱逐未持有片段；只有全部候选都被持有时，才驱逐 pinned 片段以允许加载继续。新增通用缓存与真实 Arrow layout 回归，修复后通过 **784/784 unit tests、135/135 ASan/UBSan、98/98 TSan**，见 条目驱逐修复报告（服务器归档：`docs/arrow-a05-pinned-eviction-fix-20260928.json`）。

2026-09-28 取消后同 key 重试修复：原实现中最后一个等待者取消后，已取消的任务仍占据 key 索引；新请求只能等待旧加载器退出，可能提前超时。现在最后等待者离开时立即移除该任务的 key 索引，旧 worker 完成时按条目身份清理，不会误删新的同 key 结果。回归测试覆盖旧加载器忽略取消、返回有效片段的情况；通过 **785/785 unit tests、136/136 ASan/UBSan、99/99 TSan**，见 取消后重试修复报告（服务器归档：`docs/arrow-a05-cancelled-key-fix-20260928.json`）。

2026-09-28 共享生产者 deadline 修复：原实现将建条目请求的截止时间施加给后加入的等待者；内存背压期间，首个请求到期会让后加入的请求提前失败。现在每个等待者保留自己的截止时间，worker 在等待内存时使用当前等待者中最晚的截止时间，并在等待者离开时重算。回归覆盖首个等待者超时后释放内存、后加入请求仍成功的情况；通过 **786/786 unit tests、137/137 ASan/UBSan、100/100 TSan**，见 等待期限修复报告（服务器归档：`docs/arrow-a05-waiter-deadline-fix-20260928.json`）。

### A06 — 训练结果模型与窗口语义

依赖：A02、B4/B3；模型工作可在 A05 同期进行。范围：TrainingQueryModel、WindowBatch/WindowCursor。

- [x] 定义 DatasetSpec、TrainingViewSpec、WindowRequest、WindowDescriptor、WindowBatch 与错误/限制；不借用 GRIB interpolation。
- [x] anchor a 为 target 起点：input `[a-G-L,a-G)`、target `[a,a+H)`，stride S；L/S 必须为正，G/H 非负，全部算术检查。
- [x] 显式 row/series 身份、target path、channel ordinal/labels 顺序、step/time 绑定、split、mask/reject/ragged policy、sample ids、epoch/shard/seed；无 start 的数据使用 step，不能编造时间或把 duration 当 sample count。
- [x] 区分源筛选、value validity、window eligibility 与删除时间行；常规 residual 不能无意压缩训练时间轴。
- [x] WindowBatch 携带 input/target values/masks/times 与 per-window source/fragment spans；bounded copy 是明确成本。

完成条件：不用 Arrow 即可在手工时间序列上验证边界、stride、horizon、split、空/末尾窗口和确定性枚举。

2026-09-28：A06 格式无关模型与懒窗口游标已实现，手工序列与完整回归通过 **796/796 unit tests**；新模型定向通过 **10/10 ASan/UBSan、10/10 TSan**。语义和边界见 [训练查询模型契约](../contracts/arrow-training-query-model.md)，构建、日志与源码 hash 见 A06 验证报告（服务器归档：`docs/arrow-a06-validation-20260928.json`）。本阶段只生成描述符并验证手工组装的有界 WindowBatch；A07 的版本快照/覆盖索引、A08 的实际值读取与跨文件窗口组装尚未接入。

### A07 — DatasetSnapshot 与 Arrow 时间覆盖索引

依赖：A03/A04/A06，B6。范围：DatasetSnapshot、DatasetCoverageIndex、准备索引的 API/工具。

- [x] 按明确 source selector 固定 path/version，保存 schema/entity/time binding 和 fingerprint；训练 epoch 不重复 prepare(latest)。版本被删除后定义 SourceNotFound，而非误切到新版本。
- [x] 明确底层数据保留边界：现有 read handle 仅固定 manifest，不提供防删除 lease。首版训练数据在使用期间禁止管理性删除/rename，或实现显式 retention；不承诺 cache miss 时被删除的 snapshot 仍可完整重放。
- [ ] 固定 HF state.json shard 顺序及必要 logical-to-physical row 映射，建立 series → file/version/batch/row → channel ordinal → step interval coverage；解析 labels/n_channels/length 与 O/I 核对，不把 label 相同视为相同序列。只读取必要索引/metadata/时间数据，不扫全部 target values。
- [x] coverage sidecar 绑定完整版本集合、字段语义、索引协议和 checksum，临时写完后原子发布；缺失/损坏时显式准备重建，不能每个窗口悄悄重扫数据。
- [x] 只有显式声明同 series 的分段起点/覆盖范围才建立跨文件 continuation；普通 shard 相邻行默认独立。变量分区 mapping 指定原 channel 身份；有时间信息才验证时间连续性，无时间信息用明确全局 step 起点，不能靠文件顺序推断。
- [ ] 所选 channels 的类型/单位/实际长度及标签映射预校验；核对可选 timestamp 数组与 start/freq，非固定频率按声明 calendar 规则或仅允许 step 查询；null series/channel/value 与缺失 channel 分别处理。

2026-09-28：已加入 `DatasetSnapshot`/`DatasetCoverageIndex`，固定 catalog 的精确版本和 HF shard 顺序，支持调用方显式逻辑行映射、跨文件 step coverage、变量分片原 channel ordinal、O/I 长度及时间连续性校验、null/missing List 标记、视图 channel 预校验与带 checksum 的原子 sidecar。后续回归修复了同一 series 某个时间段漏报 channel 的问题：每段的变量分片声明合并后必须覆盖完整 channel identity 集合；真正缺失须声明 `physical_ordinal = null`。针对可变 `DatasetSnapshot`，索引准备和视图预校验现在重算指纹，修改 step/channel/shard/manifest 后返回 `StalePlan`。sidecar 加载要求每个声明的 row/channel 恰好有一个 span，旧协议及有效 checksum 但不完整的 sidecar 均重建。HF state JSON 读取器支持标准转义并拒绝尾逗号；非空 `_indices_data_files` 在映射尚未验证前明确拒绝。sidecar 写失败只使缓存失效，发布文件为 `0644`。后续 sample ID 回归发现进程随机值及存储 namespace 混入内容指纹，已将它们移到独立的运行时身份与 sidecar 绑定；内容指纹现可跨进程复现，sidecar 协议升为 `/3`，source 复核仍限制 backend 实例。准备与 lookup 均不读取 target values；最初 A07 验证时完整 **819/819 unit tests** 通过。当前语义绑定由调用方/数据集清单提供；尚未实现任意 Arrow 辅助标量列的自动提取，以及存储中 timestamp 数组对 start/freq 的逐值核对。契约见 [A07 快照与覆盖索引](../contracts/arrow-dataset-snapshot.md)。

完成条件：跨文件覆盖查找正确，打开少量窗口不读取无关文件 value payload；新版本发布不影响 snapshot，重新准备产生新 fingerprint。

### A08 — 重叠窗口和跨文件按需组装

依赖：A05/A06/A07。范围：WindowPlanner、WindowAssembler、TrainingQueryModule。

- [x] 按请求生成 anchors/descriptors；只规划当前 minibatch 加有限 lookahead，不枚举全 epoch 的全部窗口对象。
- [x] 每 series/channel 合并真正重叠的序列内 step 需求，经 offsets 映射到 child tiles/压缩 buffers 去重；不能把分散区间扩大成一个含巨大空洞的包围区间。
- [x] 按 demand 懒打开 source 与 fragment。一个 List row 本身不会自动续到下一 batch；只有 A07 的显式 continuation 才允许跨 row/file 窗口，不把不同序列接起来。原 CombinedQueryCursor 的逐源耗尽不直接承担这项工作。
- [x] 先实现同 series/channel mapping 的 step/time 续接分段，再实现显式变量分片的 exact series/step 或 series/time join；缺失位置以 mask/reject 处理，不能 inner join 丢掉 null 位置。
- [x] 输入和目标各自组装、验证 split；输出与 oracle 一致，并携带所有贡献片段 provenance。
- [x] 及时释放 fragment leases；并行 fetch 可以重排物理执行，但返回顺序必须符合 sample/shuffle/shard 契约。

完成条件：TW-01/02/03/07/08/09 通过；cache 足够且不淘汰时，每个必要物理解压单位/未压缩 tile 只加载一次。跨文件续接用具有明确信息的 fixture 验证；真实 shard 的独立序列有禁止误拼接的反例。

2026-09-28：加入 snapshot/index 驱动的 `WindowPlanner`、`WindowAssembler` 与 `TrainingQueryModule`。规划器保留 step 空洞和非连续可用区间，按 split/stride/worker shard 惰性枚举，并以 `max_candidate_windows` 限定扫描；组装器只打开本次窗口触及的 shard，按 List offsets 映射叶子位置。最多 8 个候选的 lookahead 对物理 tile 内的重叠需求取并集，远隔需求保持分离；内存压力下回退到逐窗口读取。真实 source/batch/row/tile provenance、mask、时间数组及跨 shard 请求预算已接入。定向 **17/17**、完整 **836/836** 单元测试通过；新增回归证明未使用的已删除 shard 不会阻断其他窗口，触及时仍返回 `SourceNotFound`。ASan/UBSan 定向验证见 A08 验证记录（服务器归档：`docs/arrow-a08-validation-20260928.json`），契约见 [A08 按需窗口组装](../contracts/arrow-window-assembly.md)。

分页回归修复：`execute` 现通过 `WindowBatch.continuation()` 返回 anchor range 的 series 位置、下一 anchor 与全局合格窗口序号；调用方在下一次 `WindowRequest.continuation` 中传回即可跨 batch 上限继续。游标绑定稳定快照指纹、完整视图与选择条件，分片序号跨页延续；显式坐标模式不使用此游标。跨 series、worker shard、解码后拒绝窗口及超过 4096 条的 epoch 已加入回归测试。

多 worker 候选预算回归修复：分页扫描预算按 `max_candidate_windows × shard_count` 计算，避免一个 worker 的额度被其他 worker 的合格窗口消耗。触及扫描上限时返回已组装的部分 batch 和下一候选游标，不再丢弃样本。已加入 32 worker 填满 4096 条 batch 及解码拒绝导致部分 batch 的回归。

解码拒绝预算回归修复：lookahead 预取不再预扣请求级逻辑 payload/range 预算；实际窗口即使命中预取或缓存也按逻辑需求计数，遇到 null leaf 的 `RejectWindow` 时立即退回该窗口的计数。一次性 layout 读取及共享 runtime 的实际 I/O/解码消耗仍保留。流式与 lookahead 路径分别有紧预算回归。

2026-09-29 训练取数修订：上述 A08 初版的逐 tile 获取及默认 256 个 lookahead tile 阈值已由有界训练合批替代；普通 Arrow typed 与 GRIB 查询路径不变。训练组装器将一个窗口组的规范 tile 需求合并，通过共享 cache 批量认领缺失项，按精确 source/version 合并相接或重叠的物理范围，并在一次请求中读取多个不连续范围；超出条目/内存预算时按窗口及 tile 波次执行。片段身份、decoder 和 cache 仍与 typed 查询共用。接口、取消/预算、失败原子性和当前验收见[训练批量范围设计](../knowledge/arrow-training-batched-range-design.md)。

### A09 — 多实验视图与调度隔离

依赖：A08。范围：TrainingQueryModule、QueryRuntime、实验视图操作。

- [x] 同 runtime 创建多个 view：不同输入/目标变量、L/S/H/G、变量顺序；相同基础片段共享。
- [x] 提供明确确定性的 view 变换入口；首批证明 identity 与使用给定训练统计的标准化，统计 artifact/hash 进入 view identity。
- [x] 所有变换使用 private output，不修改 decoded base；未指定的 fill/resample/随机增强不猜测执行。
- [x] fair queue、每实验 in-flight/输出上限；一方取消不影响另一方；bounded prefetch 与 demand-only 模式可对照。
- [x] 保持给定 sample 顺序/worker shard；任何 locality 优化只调整内部 fetch 顺序。

完成条件：TW-04/05/06/10 与实验顺序交换测试通过；达到 M2。复用证据必须是 decode 次数和 CPU/内存数据，不只是 IPC 结果缓存命中。

2026-09-28：A09 原生 C++ 路径已实现，契约见 [多实验视图与调度隔离](../contracts/arrow-multi-experiment-views.md)。`experiment_id` 仅绑定调度；sample ID 和分页摘要绑定可跨进程复用的完整视图语义摘要，含统计 artifact 名称及其内容。共享 runtime 在同一 snapshot 上执行标准化与 identity 两个不同变量顺序/窗口形状的视图后，物理解码次数为 **5→5**；该小 fixture 的两视图进程 CPU 记录为 **2,781 clock ticks**、runtime 峰值计费内存 **152,119 B**（只作可复现的工作量记录，不推断规模性能）。调度轮转、每实验预算、取消隔离、预取/逐窗口对照和实验顺序交换测试通过；服务器正常构建完整通过，完整 CTest **870/870**（含 unit **864/864**），A09 定向 ASan/UBSan **42/42**。公开训练器与大数据性能实验仍属于 A13，不由此小 fixture 外推。

### A10 — 有界 IPC 编码与可测试前端

依赖：A04/A06/A09，B8。范围：ArrowResultEncoder、TypedQueryFrontend、TypedResultStore。

- [x] QueryBatch 编码为 IPC stream；固定 logical id/role/unit/timezone/domain/provenance 的 metadata 协议。
- [x] WindowBatch 采用已确认的固定 flat schema：每行一个 sample/role/step，Float32? 值列按视图顺序排列，role 区分不适用与数据缺失，schema metadata 保存形状/变量顺序，首行 descriptor 保存完整 provenance。
- [x] 编码累计 bytes 上限、单 result/cache 总量、失败清理、空结果与 schema-only 流；不完整结果不可见。
- [x] parser 对普通 typed query 和训练 sample-group 请求明确区分；不可变注册项以进程内 ID 引用，路径使用 `/.iotdbfs/query/v1/...`。
- [x] 不依赖 live FUSE 的 parser/encoder/result-handle 与服务级跨文件测试覆盖重复参数、escaping、版本冲突、偏移读取/EOF。

完成条件：pyarrow IPC 解码后的值、mask、schema、shape 和 provenance 可重建并与原生 cursor 对照；没有 whole-epoch 物化。

A10 实测（2026-09-29）：协议与生命周期详见 [A10 IPC 与可测试前端](../contracts/arrow-a10-ipc-frontend.md)。隔离服务器 Arrow 21.0.0 下，生产目标编译、A10 定向 **32/32**、完整单元测试 **896/896**；独立 PyArrow oracle 对 QueryBatch 与 identity/standardize WindowBatch stream 的值、mask、schema、shape、训练统计和 provenance 对照通过。体积审阅后，每个样本只在首行写 `sample_id`；固定单通道 `L=512/H=96` 测试从 121,344 B 降至 33,488 B。变换元数据审阅后，window IPC 协议升级为 `/3`，显式携带变换类型、统计 artifact 和每个唯一通道的 mean/stddev。结果上限审阅后，服务编码上限取其配置与 store 静态单结果/驻留上限的最小值；超限在编码阶段报错而非完整编码后发布失败。route registry 审阅后，注册项增加可配置的闲置 TTL（默认 5 分钟），成功解析续期、下一次注册表操作清理到期项；已解析请求可继续执行。A10 尚未挂入 live FUSE；stat/open/read/release 与权限检查留在 A11。

### A11 — FUSE 运行时接线

依赖：A10；stat/open 协议 B8 需定稿。范围：`main.cpp`、相关 frontend/result-store 代码、集成测试。

- [x] 构造一个长生命周期 QueryRuntime，注册 Arrow ingestion/query/fragment providers 和 published catalog，按 namespace 保持实例一致。
- [x] typed route 优先 dispatch，补齐 access/getattr/open/read/release 与必要的 handle attrs；read 首先看 handle，不重新解析 latest。
- [x] 定义 stat 准备结果与 open 的 snapshot/cache 关联、TTL、并发 open、结果淘汰和总预算；训练视图始终引用显式 snapshot，不能因 stat/open 之间新版本发布切换数据。
- [x] 普通查询与有界训练 sample-group 路由只读；每个 source 检查权限；正确映射错误 errno，记录 QueryStatus 详情。
- [x] 新路径挂接已有功能，不重写 legacy GRIB 字节协议；rename/unlink 后已物化 handle 可读，未物化 snapshot 失效行为明确。

完成条件：AQ-12、TW 的 mount 路径场景、stat-before-open/seek/concurrent-open/close 清理 live 验收通过。多个独立客户端经同一 mount 发起请求能证明共享 decode；不同 mount 不宣称共享。

A11 运行时、socket 协议、权限与 stat/open 语义详见 [FUSE typed 查询运行时](../contracts/arrow-a11-fuse-runtime.md)。隔离服务器完成生产构建、901/901 单元测试、ASan/UBSan 定向测试及真实挂载的 Arrow typed/window IPC smoke。两个独立客户端注册相同 Arrow 物理查询后，cache hit 从 1 增至 4、decode 保持 1；窗口查询又将 hit 增至 6、decode 保持 1。

### A12 — 旧 Arrow 数据迁移服务与操作流程

依赖：A03/A04/A11，namespace publication 接口审阅。范围：ReingestionService、管理入口、迁移报告和集成测试。

- [x] inventory 只读枚举最新候选，并单列历史版本；固定旧版本、大小、stream schema、单双层 target 结构、compression 与候选 hash。IPC file 等 Arrow 变体单独报告；已有 `format_id=arrow` 的版本核对 layout/container profile，不按后缀迁移。
- [x] 本机 mount 与离线 CLI 共用 namespace 排他文件锁；迁移操作流程要求停止其他主机的写入者。`IoTDBStructuredBlobBackend` 仍无跨主机 CAS，不能将本机锁当作分布式互斥。
- [x] 将旧 published read handle 包装为 repeatable FormatSource，经 IngestionModule 原字节重摄取为新版本并生成 stream/nested L，不转换容器、不 flatten/rechunk B、不修改旧 manifest/L/Sigma。
- [x] 不使用 `copy_structured_blob_version()`；运行手册禁止源版本删除/rename，read handle 本身不能保证底层字节保留。
- [x] 新版本通过未发布 inode 的 POSIX/metadata 写入和 storage.publish 发布；版本分配在持锁状态下推进，已存在 key 被后端拒绝。离线服务不复用 FUSE inode cache；挂载在迁移期间不能运行。
- [x] 每文件摄取前与发布前重验源版本、latest 与源 hash；发现变化报冲突，不覆盖并发更新。
- [x] 持久 journal 在写入前记录精确目标 key；resume 重开源/目标精确版本、核对 hash、identity、layout 和 inode，不信任历史成功标记。
- [x] 发布后重读目标 B 核对大小与 SHA-256；失败目标不自动进入训练 snapshot，旧版本保留。
- [x] 运行手册要求迁移后显式准备新的 DatasetSnapshot/coverage；现有实验固定旧 snapshot，不自动切换。

操作顺序：inventory/dry-run → 小 fixture 与少量实际文件试迁移 → old/new 精确版本和普通读取校验 → 原生查询 → 训练窗口 → 分批迁移 → 保存报告。不要把同一路径的普通重写当成天然安全的版本迁移；必须先核实其 truncate/write 行为。

恢复边界：迁移失败不删除源或修改旧版本；目的版本若已发布但验收失败，报告并隔离于新训练 snapshot。将 latest 切回旧版本不是现有 API 的既定能力，不能在 runbook 中承诺无实现的自动 rollback。

完成条件：AQ-01/02/09/11/16 在 live 环境通过；中断/重复执行/并发写冲突有记录；旧 exact version 仍为 pass-through，新版本有 arrow-ipc/L/Sigma。

A12 服务与操作入口已实现为 `iotdb_fs arrow-migrate inventory|run|resume --report DIR [--path /exact/path]`，详见 [迁移运行手册](../knowledge/arrow-data-migration.md)。隔离 IoTDB 数据库验收：合成双层 List fixture 从旧 pass-through 迁移后，源/目标 SHA-256 相同；模拟目的版本已发布但完成日志缺失，resume 重验同一目标；真实 FUSE typed 查询与训练窗口 IPC 可读取新版本，挂载持有锁时并发迁移命令被拒绝。另对 TimeBench `post_train/extracted_test_inputs_arrow/saugeen_W_short/data-00000-of-00001.arrow`（13,648 字节）完成试迁移，源/目标 SHA-256 为 `17f1b221c9b255c8d655b6e471e03c44d9532d899997d50ec0ef959be5159242`；FUSE 单层 List typed 值与 PyArrow 原文件 oracle 一致。完整单元测试 **928/928** 通过。试验报告分别位于服务器 `a12-migration-jxrzibly`（双层 fixture）和 `a12-migration-wlrwf7tj`（真实样本）目录。批量旧库迁移、跨主机停写证据及 AQ 全项验收仍未执行。

### A13 — 包装、实验与最终验收

依赖：M1/M2、A11/A12。范围：release scripts、support matrix、AQ/TW 记录、benchmark drivers。

首轮实验顺序按 [公开基线修订](public-training-baseline-plan.md)调整：以 Trainer 默认随机训练采样和顺序评估采样，先做 `/data_01` 本地 Arrow reader（R1）对 IoTDB-FS typed windows（Q2）的同源、同样本 A/B；ETTm1 正确性闭环后进入 Electricity。保持实际 sample ID trace、split、训练统计、模型种子和资源预算一致。首轮不人为切文件制造跨文件连续窗口；A08/N06 跨文件能力继续作为独立功能回归，真实多文件负载出现后再进入性能矩阵。

2026-09-29 接线进度：独立挂载与两数据摄取、PB01 窗口产物、全通道窗口核对、两模型四组单 GPU 两步 A/B smoke（服务器归档：`docs/public-training-ab-wiring-20260929.md`） 已完成。四组的样本访问序列和训练/验证 loss 一致；Electricity 的查询注册在短程 B 侧耗时中占主导。此为接线验收，不勾选下列完整训练和性能项目。

随后 [50-step 随机训练 smoke](../reports/public-training-ab-long50-20260929.md) 四组也全部完成，A/B 各组 102 个实际读取的训练窗口及 loss 一致。Electricity 的 B 侧 51 次查询注册累计约 143 秒，占训练墙钟的 96%–97%；在消除该开销并完成稳态测量前，不能将 A13 的基本性能门标为通过。

分段计时（服务器归档：`docs/public-training-stage-profile-20260929.md`）进一步将 Electricity 51 次训练请求的服务端 142.034 s 分为 payload 范围读取 121.915 s（35,671 次）、训练准备 4.711 s 和 IPC 编码 0.672 s。该诊断支持优先实现下列训练专用优化；模型规模变化能改变 I/O 等待占比，但不会自行减少当前每个缺失 tile 一条 SQL 的调用数。

- [x] 验证部署版 IoTDB 能在**一个真实请求**中返回多个不连续 `READ_OBJECT` 范围，并按列顺序和长度校验响应；首版单请求限制 64 个范围、8 MiB payload。
- [x] 在 `QuerySource`/storage 与共享 cache 增加有界多范围传输、按 key 批量认领和单飞加载基础能力；普通 Arrow typed/GRIB 查询继续走现有单片段接口。
- [x] 在 Arrow **训练 adapter** 中对窗口组的缺失 tile 分波次：精确合并重叠/连续范围，对不连续范围用多范围请求；沿用相同 decoder、规范 key、服务预算和独立请求预算，不改变采样/返回顺序。
- [ ] 用 recording source、native Arrow oracle 和真实 IoTDB 分别验证值/mask/provenance、并发/取消/失败、内存上界、SQL 次数/范围数/字节数；在同配置公开训练 A/B 中单列 cache 容量、多范围读取和范围合并的收益。

[Electricity 专项 50-step A/B](../reports/public-training-electricity-tilebatch-20260929.md)已复验 PatchTST 和 PatchTSMixer：A/B 样本轨迹、训练/验证 loss 一致；B 训练墙钟约 24.3–24.5 s，此前约 148 s。PatchTST 的 51 次训练查询 payload 调用从 35,671 降为 571，但解码次数和返回字节只各降低约 5%；标准随机 sampler 与默认 1,024 条目 cache 仍限制跨请求 tile 复用。完整 CTest **952/952** 通过。上列最后一项的独立 cache 容量/多范围/范围合并消融、完整 epoch 与稳态性能尚未完成，A13 基本性能门仍保持未勾选。

2026-09-30：[缓存容量与 2/4 路并行的两轮 50-step 消融](../reports/public-training-cache-parallel-20260930.md)完成。挂载缓存扩至 16,384 条目，仍保留 1 GiB 字节预算；训练完整驻留 lookahead 支持独立 provider 的有界并行，普通 typed 查询路径不变。32 次 B 运行均通过 A/B 轨迹和 loss 校验。冷缓存 4 路的 PatchTST/PatchTSMixer 均值为 14.478/14.431 s，比本次小缓存串行对照降低 41.1%/41.6%；扩容后冷读仅 6,660 次 tile 解码，暖读为零 I/O。955 项 C++ 测试和 8 项 profile 汇总测试通过。缓存容量消融已完成；完整 epoch、DDP、稳态性能及多范围/范围合并的进一步独立消融仍不视为完成。

- [ ] 更新真实 Ubuntu/macOS/Kylin 构建路径；Kylin 直接 g++ source list 和静态 codec 依赖不能遗漏。未执行平台明确标为未验证。
- [ ] 运行全部 Arrow AQ-01–AQ-18、TW-01–TW-10；不要求 TsFile TQ 才交付 Arrow。
- [ ] 先执行 E1/E5 的标准采样文件系统/IoTDB-FS A/B 并报告基本性能；随后执行 E4/E7/E8、同 total memory 的 native reader/cache baseline 和 cache-off/encoded-only/decoded-sharing 消融。E6 跨文件实验单列后续真实布局验收。
- [ ] 完成 [公开基线 PB00–PB04](public-training-baseline-plan.md)：HF Trainer＋PatchTST/PatchTSMixer，ETTm1＋UCI Electricity；公开 R0 内存、R1 优化 stream reader、R2 memmap、R3 预物化成本与 R4 plain FUSE 对照。PEMS-SF/LargeST/TTM 分别记录补充/扩展状态，不与首批完成混同。
- [ ] 同 window manifest、split、channel 顺序、train-fitted transform、sampler seed 和模型初始化，核对 `[C,T]` 到 `[B,L,C]` 打包与模型 mask/loss；报告 loader-only 和 Trainer 端到端结果，分开 copy/H2D、GPU争用和读取等待。多实验除私有缓存外提供公开共享缓存对照，或明确证据限制。
- [ ] 记录 cold/warm、unique/actual decode units、planning/READ_OBJECT bytes、copy bytes、峰值内存、first-window/steady-state latency 和 samples/s；不能用 k/N 推断真实训练收益。
- [ ] 更新 user manual、CONTEXT、support matrix；论文/audit 只写本轮实际完成的 Arrow 范围，TsFile 继续标为未实现。

完成条件：M3 完成，原始结果、fixture manifest、可重复命令和限制一起交付。

## 5. 依赖顺序与首次编码切片

```text
A00 dependency/I/O ─┬─ A03 ingestion/layout ─ A04 query/decoder ─ A05 shared fragments ─┐
A01 ingestion seam ┘                         ↑                                       │
A02 core semantics/budgets ──────────────────┘                                       │
A06 window contracts ─ A07 snapshot/coverage ─────────────────────────────────────────┤
                                                                                     ↓
                                                        A08 windows ─ A09 experiments
                                                                                     ↓
                                           A10 IPC/frontend ─ A11 FUSE ─ A12 migration
                                                                                     ↓
                                                                    A13 live/evidence
```

图中仅画主干；每个 Axx 的“依赖”条目为完整约束。可独立并行的工作是 A00 与 A01、A05 与 A06，以及已固定接口后的测试 fixture/包装工作；不安排多个 agent 同时修改 QueryPlanning/QueryModule/main。

**第一次编码只取 A00 → A01 → A02 的必要部分 → A03 → A04 的最小纵向切片：** 先用同一物理接口验证一个 `list<float32>` 行的空子路径及叶子范围，再验证 uncompressed IPC stream、两个 record batches、多条独立 `list<list<float32>>` row；选择其中一行、匿名子路径 `[[2],[0]]`、叶子位置 `[3,11)`。用确定值验证结果/顺序；语义探测通过后再称它们为 channel 与 step。测试配置足够小的 tile 并使 fixture 跨多个 tiles，证明只读所需 tiles 而非整个 target。接着验证 null masks 与谓词。随后立即接 A05–A09：同一序列重叠窗口与两视图共享，再加入压缩整 buffer 路径和明确 continuation fixture。不得用 IPC file/标量列的成功替代这条纵向切片。

TsFile 开始前应稳定 FragmentProvider、DatasetSnapshot、WindowBatch 和预算测试；届时替换物理 provider，不复制窗口/实验调度器。当前无需等待 TsFile S2、table 决策或 tsfile C++ 依赖构建。

## 6. 测试与交付文件

拟新增单元测试：`test_ArrowLayout.cpp`、`test_ArrowFormatAdapter.cpp`、`test_ArrowFormatQueryAdapter.cpp`、`test_ArrowRangeReader.cpp`、`test_ArrowSequenceBinding.cpp`、`test_DecodedFragmentCache.cpp`、`test_TrainingQueryModel.cpp`、`test_DatasetSnapshot.cpp`、`test_DatasetCoverageIndex.cpp`、`test_WindowPlanner.cpp`、`test_WindowAssembler.cpp`、`test_TrainingQueryModule.cpp`、`test_ArrowResultEncoder.cpp`、`test_TypedQueryFrontend.cpp`、`test_ReingestionService.cpp`。优先测试值/错误/副作用/物理计数，避免只镜像实现的数据结构测试。

测试数据：固定 seed 的小 fixtures 存在 `tests/fixtures/arrow/`，用 IPC stream writer 分别生成一层与两层 List schema，generator 及版本/hash manifest 随附；真实公开样本由 PB01 转换 ETTm1/Electricity，数据文件分发遵守各来源许可，不提交大文件。内部 TimeBench shard 可额外回归，不作为公开测试前提。覆盖 compressed/uncompressed、完整消息边界 EOF/EOS、任意传输 chunk 切分、多 batch/rows、各层 offsets/bitmap 非字节对齐、空/null/ragged、子路径顺序、独立 row、显式 continuation、HF shard/indices 映射、字典 epoch 和 timestamp 溢出。

专项验收必须加入 A03–A09/A13：

| 用例 | 验证内容 |
| --- | --- |
| N01：单双层定位 | 单层空子路径、双层非首 row/子列表、非零叶子位置；核对 offsets/values 范围、长度与请求顺序；不要只测 offsets 从零开始的首元素。 |
| N02：nullable/ragged | parent/inner/leaf NULL、NaN、空 list、不同通道长度/行数、坏 offset、错误 n_channels/labels，按声明错误或 mask。 |
| N03：stream 随机访问 | 用已摄取 L 直接查询后续 batch，不从 stream 头扫描；message/body 截断、EOS/EOF 和 dictionary 依赖失败可诊断。 |
| N04：冷读边界 | 未压缩读取必要 child tiles＋offset/validity；压缩 buffer 覆盖多 channels/rows 时完整解压并报告放大，不声称 k/N。 |
| N05：复用粒度 | 不同重叠窗口共享稳定 tiles；同压缩 buffer 的不同 channels single-flight；固定预算下无整 batch 隐式缓存/父子计费遗漏。 |
| N06：跨文件语义 | 普通 shard 独立 rows 不允许续接；人工或真实有声明的 same-series continuation 返回正确边界窗口/provenance。 |
| N07：时间与行映射 | 无 start 只做 step；labels/ordinals 与 HF state/_indices 映射一致，未支持映射明确拒绝。 |
| N08：迁移与强基线 | stream B hash 原样；与公开 R1 mmap/索引/两层 offsets/有界解压缓存比较相同 rows/channels/windows，并保留 R0 内存和 R2 memmap 强对照，不能只比 whole-file scan。无需 TimerLathe 即可复现。 |
| N09：公开训练等价 | 两个模型共用 window manifest；验证 tensor/目标/mask/channel 顺序、loss 与评估预测提取，固定短训练和恢复成功；不同后端不改变 split/归一化/样本顺序。 |

E1 改为“固定 series row 的 channel 投影 × 行内时间窗口”，用 `pyarrow.ipc.open_stream` 重建小规模完整 oracle；性能对手采用公开基线 R1 的 mmap＋一次建索引＋offset slicing＋有界解压缓存，另保留官方内存和连续 memmap 基线。TimerLathe 的选择/缓存路径只作可选内部补充。分别记录首次 message-index 构建和 warm L 使用，frame/offset 解码、叶子解压、值拷贝、IPC 编码；native mmap/page faults 与 IoTDB 返回字节不是同一计量层，不能直接混作磁盘 I/O。

首轮 E1/E5 同时运行 loader-only 和 HF Trainer，具体数据/模型/窗口扫参采用 [公开训练基线计划](public-training-baseline-plan.md)；ETT/Electricity/PEMS-SF 的小数据结果不能直接支撑 out-of-core 结论。E6 跨文件仅在真实布局出现后单列；PEMS-SF 独立日不得拼接，LargeST 需先完成下载与工作集审计。Trainer 使用薄 Dataset/collator，属于实验接入，不扩张为自研完整训练框架。

记录文件：

- `docs/reports/arrow-query-spikes.md`：A00/S4 结果和最终依赖/API 决策。
- `docs/arrow-query-acceptance.md`：Arrow AQ/TW 逐行状态、平台、源码版本和证据路径。
- `docs/knowledge/arrow-data-migration.md`：A12 的真实管理入口、dry-run/run/resume 命令和报告字段。
- `tests/benchmarks/arrow_training/`：E1/E4/E5–E8 的 workload/oracle/metrics 脚本及说明。
- `docs/deliverables/public-training-baseline-plan.md`：公开模型/数据来源及 PB00–PB04；benchmark 目录增加公开转换器、Dataset/collator、Trainer driver、配置、manifest 和实际环境锁，复现不依赖 TimeBench/TimerLathe。

依赖准备完成后，现有 CMake/CTest 入口的拟执行命令（仓库根目录；本计划编写时未执行）：

```bash
cmake -S . -B build-arrow -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug -DENABLE_FUSE_INTEGRATION_TESTS=OFF
cmake --build build-arrow --target unit_tests iotdb_fs -j4
ctest --test-dir build-arrow -L unit --output-on-failure
```

live 测试必须使用隔离测试 namespace/挂载点，按 `tests/integration/README.md` 的环境检查执行，再加入 Arrow 专用目标；不要运行会触及已有用户数据的未隔离测试。依赖安装器及 codec/link 检查使用其实际注册测试，不把“新增 lock entry”当作构建成功。

CI Python 修改后执行 `python3 -m unittest discover -s ci/tests -p 'test_*.py' -v`。Coverage/sanitizer/release 使用现有 `ci/run_tests.py` 对应模式；live 测试显式设置独立 `IOTDB_FS_TEST_DB` 并通过 `iotdb_fs_preflight`。这些都是未来实施的验证要求，本次计划编写未运行。

## 7. 完成标准和范围控制

M1 不等于本轮全部完成；只有 M2 的训练复用和 M3 的生产迁移证据都通过，才声称完成 Arrow 查询与迁移。缓存命中但反复解码、只拼接文件而未按时间对齐、只做整 epoch 物化、只在独立进程内私有复用，都不能替代目标验收。

实现过程中每次设计收敛更新本计划与上位设计的决定状态；每次验收填写实际结果。A00 已锁定 Arrow 21.0.0；FUSE path/Window IPC schema 见 A10/A11 文档，A12 管理入口见 [迁移运行手册](../knowledge/arrow-data-migration.md)。尚未完成的生产批量迁移与 AQ 全项验收仍按各阶段完成条件判断。
