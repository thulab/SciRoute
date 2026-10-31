# 未提交优化总览与查验索引

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

记录日期：2026-10-11。工作目录：`IoTDB-FS-C`；当前 HEAD：`0a62f6e4835c556cbba9dec0a7a6668cb6d412fb`。本文盘点该 HEAD 之后**当前工作树中尚未提交**的优化，依据源码 diff、各轮冻结二进制消融和原始结果整理。没有新增性能实验，也没有执行 git 写操作。

配套快照清单 JSON（服务器归档：`docs/uncommitted-optimization-summary-20261011.json`）保存当前文件状态、SHA-256、分类和证据文件 hash。源码行号是本次快照的位置，后续编辑后应以函数名和 hash 查验。

## 阅读与计时约定

这里各轮 A/B 的 A、B 默认是**IoTDB-FS 的修复前／后**；普通 FS 对照另行标明。每一行使用该轮自己的对照，不能把不同轮的中位数相减、降幅连乘，或拼接为一条累计加速曲线。

- **typed 服务端完整请求**：注册、catalog、授权、执行、IPC 编码及发布；不含 socket 帧 I/O 与 profile 写出。**typed 完整客户端消费**逐请求合计控制调用、FUSE stat 和完整 IPC 消费，再取分位数。具体 reader 与 TTL 以单轮报告为准。
- **dataset 目录客户端完整请求**：控制调用、授权、目录发现和响应；只返回精确 source/version＋dataset 身份，**不读取温度 payload、不编码取值 IPC**。
- **完整发布**：原驱动逐文件 `shutil.copyfile` 的耗时之和，含创建、FUSE 暂存、close/flush 同步、摄取、layout/catalog/index 构建和发布；不含 mount、父目录创建、源 SHA、后置查询／完整 SHA 校验。
- 所列查询主结果通常为暖态 **p50，单位 ms**，发布为 **秒**。OS／IoTDB 缓存未清空；“解析缓存冷”不等于磁盘冷。
- 阶段计时可能嵌套。COUNT 已含两条 SQL，SDK insert 已含于 Tablet API，session acquire 已含于 SQL／Tablet；**不能重复相加**。固定请求集合耗时之和不是整个实验进程 wall time。

目录模型中，`N` 是授权后冻结的候选源数，`K` 是时间筛选后需要目录检查的源数，`J` 是最终返回的 dataset 数；它们不能混用。GEOS N=16 时间命中通常 K=J=1；无时间条件的 `/T` 属性命中通常 K=J=16。

## 优化地图

| ID | 当前实现 | 直接减少的工作 | 作用范围／条件 |
| --- | --- | --- | --- |
| Q01 | published catalog selector SQL 下推 | 无关版本行传输、metadata JSON 解析、持锁工作 | 公共 source 发现；有选择条件时 |
| Q02 | catalog 行复用源权限 | 再读相同精确 source inode | 即时注册授权；证据身份与新鲜度有效 |
| Q03 | 祖先目录批量授权＋exact catalog 合并 | 逐祖先 SQL；有限路径扫描 | 公共授权；exact 小集合可合为一条 SQL |
| Q04 | 成功发布后预填权限缓存 | 首次 FUSE 消费重复授权 SQL | `attr_timeout>0` 且检查／发布仍在原 TTL 内 |
| Q05 | dataset discovery 贯通权限证据 | 多文件发现的逐源 inode 重读 | 全候选先授权，不能用筛选隐藏拒绝 |
| Q06 | manifest 有界批量重验 | 逐候选 manifest SQL 与重复 metadata 解析 | 每次请求仍重验全部 N 个精确版本 |
| Q07 | 两表 COUNT 与属性筛选合批 | 逐源 count/count/filter SQL | K 个候选按精确键分批 |
| Q08 | marker 合批 | 逐源目录 size SQL | 先验证全部 marker 和总预算，再读取对象 |
| Q09 | 缺失目录 OBJECT 合批 | 逐源 READ_OBJECT SQL | key／SQL／返回字节三重有界 |
| Q10 | 已验证不可变目录解析缓存 | 热请求 OBJECT 读取与反序列化 | 单 backend、精确版本、完整身份、LRU |
| Q11 | 整个谓词退化 TRUE 时删除属性 JOIN | 无用 attribute 扫描／关联 | residual-only 谓词；最终语义仍精确求值 |
| P01 | payload 生命周期内合并 manifest 重读 | `replace_manifest` 的第二次 SELECT | 一次 fresh 校验＋原块写入＋frontier 提交 |
| P02 | 有界 frontier checkpoint 合批 | 每 transfer 的 SELECT／INSERT | 默认 8 MiB／最多 64 块；不合并 OBJECT |
| P03 | 删除 backend 写入前冗余 vector 复制 | payload／layout 显式输入副本 | SDK 内部编码与必要所有权复制仍保留 |
| D01 | 查询、授权与发布分层 profile | 可定位但本身不省 SQL | 诊断能力；不作为独立加速项 |
| D02 | 可配置 1／2／4／8 MiB transfer | 支持传输粒度消融 | **较大块未显示收益，默认仍为 1 MiB** |

## 公共 catalog 与授权链路

### Q01：将源选择条件下推到 published catalog SQL

**原理与位置。** 原先 `load_published_manifests()` 读取 namespace 全部已发布文件版本，随后在内存中筛选。现在把 `SourceSelector` 传入 refreshing loader，由 [IoTDBPublishedCatalogSql.h](../../include/iotdbfs/IoTDBPublishedCatalogSql.h) 的 `compile_published_catalog_sql`（约 507 行）编译 exact/subtree 路径、exact/between 版本和格式条件；[backend](../../src/IoTDBStructuredBlobBackend.cpp) 的 selector overload `load_published_manifests(selector)`（约 507 行）进入 `load_published_snapshot`（约 518 行），仅消费匹配行；约 502 行的无参数 wrapper 仍为完整 inventory。[QueryPlanning.cpp](../../src/query/core/QueryPlanning.cpp) 的 refreshing catalog 仍执行最终选择。

**效果。** 89 个已发布版本的试点中，GRIB 返回／解析行数 **89→8**，HDF5 **89→1**。catalog p50 为 **12.578→5.324／12.406→4.573 ms**；typed 服务端完整请求为 **35.330→26.791／29.365→20.835 ms**（GRIB／HDF5）。通用元数据 SQL 次数仍为 4；收益来自每次调用处理更少数据。报告与消融入口（服务器归档：`docs/catalog-sql-pushdown-ab-20261010.md`）。

**边界。** `select_published_snapshot` 保留排序、去重、latest 和 source limits；格式先于 latest，文件 metadata 仍 residual。没有 SQL LIMIT 隐藏候选。无参数 inventory 仍能全量扫描；全 namespace latest 尚未变成数据库端每路径 latest 聚合。

### Q02：从 catalog 行检查源文件权限

**原理与位置。** catalog SQL 一并返回 `mode/uid/gid/is_dir`，生成请求级 `PublishedVersionSnapshot`／`SourcePermissionSnapshot`。定义位于 [QueryPlanning.h](../../include/iotdbfs/query/core/QueryPlanning.h)（约 78 行），selection 过滤位于 `RefreshingPublishedVersionCatalog::scan`（约 227 行），校验 helper 位于 [CatalogSourceAuthorization.h](../../include/iotdbfs/query/frontend/CatalogSourceAuthorization.h)。[main.cpp](../../src/main.cpp) 的 `authorize_typed_sources_as`（约 1384 行）使用本次精确身份的权限证据，省去相同 inode 的第二次读取。

**效果。** typed 注册元数据 SQL **4→3**，源 inode 授权 SQL **1→0**。授权 p50 GRIB **13.116→9.378 ms**、HDF5 **13.063→9.523 ms**；完整服务端 **22.820→19.631／17.204→14.262 ms**。报告（服务器归档：`docs/catalog-permission-reuse-ab-20261010.md`）。

**边界。** 权限不是内容 revision；不把 mode 写入不可变 manifest 代替活权限。证据必须绑定 backend namespace、精确 key、format、revision、大小、metadata generation 和观测时间；缺失、无效、过期则重新读取。chmod/chown 等在修改 SQL 前推进 generation，授权前后检查变化，只重试一次，持续变更返回 StalePlan。owner、主组、补充组和源 R_OK 均保留；父目录 X_OK 独立检查。该轮早期报告称 discovery 尚未复用，**当前已由 Q05 贯通**。

### Q03：父目录授权合批与 exact catalog 合并

**原理与位置。** 收集所有源的祖先、规范化去重，`load_latest_inode_permissions` 用 `path IN (...)` 批量读取最新已发布目录。backend 的 `load_published_snapshot(selector, include_ancestors)` 对小型、不与祖先重叠的 exact 路径集合，把源 catalog 与祖先证据合在一条 SQL；源版本／格式条件只施加到源分支。SQL helper、backend 和 `authorize_typed_sources_as` 共同接线。

**效果。** 两祖先路径试点中，注册通用元数据 SQL 从 **3→2（仅 bulk）→1（exact 合并）**。GRIB 服务端 **16.179→12.765→9.763 ms**；HDF5 **11.301→8.038→5.522 ms**。首次 OR 合并没有充分剪枝；最终增加外层有限 path IN 后，计划扫描由 **85→3 个 inode 路径 device**。三臂报告（服务器归档：`docs/catalog-parent-batching-ab-20261010.md`）。

**边界。** 每波最多 128 路径／64 KiB SQL，总路径上限 65,536；exact 合并集合也有上限。subtree、源与祖先重叠或超限采用普通 catalog＋bulk 祖先读取。latest 目录不存在、损坏或不是目录即拒绝，不能找旧目录放行；祖先行不计入文件候选或 metadata JSON。85→3 是特定计划的扫描范围，不是文件版本数或普遍降幅。

### Q04：成功发布后预填 FUSE 权限缓存

**原理与位置。** socket 和 FUSE 补充组统一排序、去重并去掉主 gid。[TypedAuthorizationCache.h](../../include/iotdbfs/query/frontend/TypedAuthorizationCache.h)（`normalized_authorization_groups` 约 15 行、`prefill` 约 120 行）按 uid/gid、规范化组、注册 ID、结果 SHA 和路由种类存允许决定。[QueryRegistrationSocket.cpp](../../src/query/frontend/QueryRegistrationSocket.cpp) 的注册／发布流程（约 1372 行）在检查前捕获 observation，成功发布精确结果后提交；main 的 publication observer（约 3519 行）校验 generation 并预填。

**效果。** TTL=1 时首次完整消费阶段的权限 SQL **2→0**。GRIB 消费 **9.066→0.906 ms**、HDF5 **8.778→0.722 ms**；客户端完整查询 **18.821→10.910／14.721→6.872 ms**。注册仍为 1 条 SQL，服务端注册本身没有测得提速。TTL=0 时保持禁用，没有该收益。报告（服务器归档：`docs/authorization-prefill-ab-20261010.md`）。

**边界。** 最多 1,024 项、单键 4,096 B，TTL ≤ attr_timeout 且 ≤1 秒；从授权检查前起算，不因慢执行／发布续期。失败、generation 改变、已过期或禁用均不填；后续先验证 route／result 身份，错误 version 不可绕过。真实 `/proc` 与 `fuse_getgroups` 组键一致已验收。同挂载修改即时失效；没有新增跨主机失效协议。

## 跨文件 dataset 目录发现

### Q05：把权限证据贯通到 discovery

**原理与位置。** backend 原先把包含权限的 snapshot 降成只含 manifest 的内存 catalog，socket 回调再传 nullptr，丢失 Q02／Q03 成果。现在 `discover_datasets`（backend 约 1253 行）使用 refreshing catalog；[DatasetCatalogQuery.h](../../include/iotdbfs/query/core/DatasetCatalogQuery.h) 的 `DatasetCatalogAuthorizer` 接收精确 `CatalogSelection`；参考实现 `discover_datasets_reference` 和 socket dataset_catalog 回调（约 1480 行）均透传证据。

**效果。** N=16 时间命中客户端 **153.267→87.924 ms**，完整 SQL **39→22**；时间未命中 **118.085→55.461 ms、34→17 SQL**；仅属性 **616.449→549.535 ms、114→97 SQL**。报告（服务器归档：`docs/dataset-discovery-permission-ab-20261010.md`）。

**边界。** 所有 N 个候选源在时间／属性筛选之前授权，时间未命中、属性无结果都不能掩盖权限拒绝。保留原 generation、新鲜度、补充组和祖先规则。这是格式中立授权链路，不是 GRIB/HDF5 Adapter 解码修改。

### Q06：有界批量 fresh manifest 重验

**原理与位置。** `revalidate_dataset_manifests_unlocked`（backend 约 1187 行）在授权后、时间筛选前，对冻结的精确 path/version 集合一次或若干批 fresh SELECT。`compile_manifest_revalidation_sql_batches` 只读身份所需的 format、大小、phase、published、revision，确认后复用初始 catalog 已解析 metadata。

**效果。** N=16 时间命中 **85.342→44.948 ms、22→7 SQL**；未命中 **54.976→12.653 ms、17→2 SQL**。仅属性 **550.127→500.926 ms、97→82 SQL**；其中 N=8 仅属性未测得时延改善。报告（服务器归档：`docs/dataset-manifest-revalidation-ab-20261010.md`）。

**边界。** 每批 128 精确键／64 KiB SQL，总键上限 65,536；外层 path IN 负责剪枝，内层保留 path/version 配对，不能构造成笛卡尔组合。缺失、重复、错误身份、未发布或读取失败保留 StalePlan。时间未命中的源仍重验，不换新 latest；没有跨请求可变 manifest 缓存。多 SQL 不是数据库事务，仍遵守单 writer 边界。

### Q07：完整性 COUNT 与属性筛选合批

**原理与位置。** `compile_dataset_catalog_index_sql_batches`（SQL helper 约 394 行）为精确键批次生成两张表的 GROUP BY `(path,time)` 实际 COUNT，以及跨源属性 JOIN。backend 在候选准备后（约 1470 行）按批执行和验证，消除逐源 count/count/filter。

**效果。** N=16 无时间条件的精确属性发现 **514.829→221.254 ms、82→37 SQL**；N=4／8 降幅为 38.9%／49.9%。时间命中仅 K=1，不减少该部分 SQL，没有相同加速。报告（服务器归档：`docs/dataset-index-batching-ab-20261010.md`）。

**边界。** 每批 128 键／64 KiB 完整 SQL；复杂谓词或长路径提前拆批，单键无法容纳则明确失败。两表 COUNT 仍检查全部声明索引，不能只数筛选结果；返回匹配仍由原始属性契约和精确 evaluator 验证。每源对象大小与读取的 2K SQL 留给 Q08／Q09，而不是本项已消除。

### Q08：目录 marker 合批，消除逐源 size

**原理与位置。** `compile_dataset_catalog_marker_sql_batches`（约 180 行）一次读取 size、protocol、layout digest/protocol、完整性和声明计数；backend discovery（约 1344 行）先完成全部 marker 身份和总逻辑字节预算检查，再开始对象读取。实际 marker size 用于预算，不再序列化 artifact 估算需求。

**效果。** N=16 仅属性 **228.999→178.998 ms、37→22 SQL**；目录准备阶段 **116.474→70.925 ms**。报告（服务器归档：`docs/dataset-marker-batching-ab-20261010.md`）。

**边界。** 每批 128 键／64 KiB SQL，额外行检测及完整身份验证保留。marker 缺失／异常、对象短读保留明确错误；marker 声明计数不代替 Q07 的实际 COUNT。发布生命周期的单源目录读取仍是独立路径，本项不优化发布。

### Q09／Q10：缺失 OBJECT 合批与解析结果复用

**原理与位置。** `compile_dataset_catalog_object_sql_batches`（约 255 行）生成 `READ_OBJECT(data,0,size)`，每批最多 128 键、64 KiB SQL、16 MiB 返回对象。backend（约 1389–1438 行）按已验证 marker 身份查缓存，仅合批读取 misses、检查精确长度、反序列化并验证。新 [IoTDBDatasetCatalogCache.h](../../include/iotdbfs/IoTDBDatasetCatalogCache.h) 保存 `shared_ptr<const DatasetCatalogArtifact>`，身份包括 backend namespace、精确 key、manifest revision／format／大小及 marker 全部相关字段。

**效果。** 同轮三臂“逐源读取／仅合批／合批＋解析复用”，N=16 仅属性客户端 **182.738／130.091／118.439 ms**，SQL **22／7／6**；目录准备 **69.843／20.830／6.175 ms**。逻辑需求都为 **234,576 B**，实际对象读取 **234,576／234,576／0 B**。N=16 首次解析冷请求为 **192.544／132.707／143.840 ms**，仅三样本，不能用热缓存结论代替首次成本。三臂报告（服务器归档：`docs/dataset-object-batching-ab-20261010.md`）。

**边界。** 缓存由单 backend 拥有，保守 resident 预算 64 MiB、65,536 项，LRU；在途 shared_ptr 持有内存不包含于该 resident 上限。inode、manifest、catalog、metadata 修改以及路径／子树删除在首次修改 SQL 前失效；不同 backend 隔离，旧精确版本独立。命中也收逻辑字节预算；权限、fresh manifest／marker、请求 schema、实际两表 COUNT、最终求值每次仍执行。没有启用持久 sidecar、跨进程缓存或取消完整性验证。129 源冷／热分别 **13／11 SQL**，不要外推成所有规模固定 7／6。

### Q11：SQL TRUE 回退移除无用属性 JOIN

**原理与位置。** [DatasetCatalogQuery.cpp](../../src/query/core/DatasetCatalogQuery.cpp) 的 `compile_dataset_predicate_sql`（约 562 行）保持“任一不能安全精确下推的条件使整棵谓词退化 TRUE”的原策略，同时清空**仅用于生成 SQL JOIN 的**属性依赖。字符串排序不依赖数据库 collation；原表达式仍用于 schema／完整性验证与最后 residual 精确求值。

**效果。** N=16 `/T`＋`standard_name < 'b'`，无时间筛选、K=J=16：客户端 **121.953→110.271 ms（9.6%）**；排序未命中 **120.417→111.297 ms（7.6%）**。命中属性筛选阶段 **24.700→15.173 ms**，两次 COUNT **73.768→75.220 ms**，SQL 仍为 6。可精确下推的等值 SQL 前后完全一致；其端到端波动不计为本项收益。[报告](../reports/dataset-unused-join-ab-20261011.md)。

**边界。** 没有删除 inode JOIN／COUNT，没有做部分 AND 下推。已证明不存在的属性按 typed NULL 和三值逻辑；不完整、不可读取或错误类型仍失败。12 条 EXPLAIN 仅 11 条完整，旧 N=16 AND 超 1 MiB 输出上限；这是逻辑计划，不混用早期 SQL-only EXPLAIN ANALYZE 的物理指标。

**避免“命中”混淆。** Q06 的 **85.342→44.948 ms** 是时间剪枝到 K=1；本项的 **121.953→110.271 ms** 是无时间筛选 K=16。当前同轮时间命中控制为 **44.354→44.114 ms**。本项修复后 15 个正式样本中，属性排序命中最短 **100.121 ms**；时间命中最短 **38.284 ms**，均非 p50 或稳定下界。

## 完整发布链路

### P01：一次生命周期操作内消除 manifest 第二次读取

**原理与位置。** [StructuredBlobStorage.cpp](../../src/StructuredBlobStorage.cpp) 的 `plan_payload_write`（约 30 行）统一校验 key、PayloadOpen／未发布、无 gap、范围／溢出和 revision；scalar `write_payload_range` 与 batch 共用 `write_payload_and_advance`。IoTDB backend 覆写（约 767 行）在同一 `mutation_mu_` 内一次 fresh load、写原 OBJECT 块，最后 `persist_manifest_unlocked` 直接 INSERT frontier，不再通过 `replace_manifest` 再 SELECT 一次。

**效果。** 同轮旧版→新版单块 checkpoint：完整发布 **142.165→115.910 s**。6,820 个原 1 MiB 块的 manifest SELECT **13,640→6,820**，frontier INSERT 和 payload OBJECT Tablet 都仍为 6,820 次。[P01／P02 三臂报告](../reports/hdf5-geos-publication-batching-20261010.md)。

**边界。** 这是 fresh 读后同锁执行，不是跨请求缓存可变 manifest；其他 seal/layout/description/publish 的生命周期 CAS 未普遍改写。输入参数在写任何 artifact 前校验，保持重试／abort 和版本发布语义。

### P02：有界 checkpoint 合批，保持原 transfer 与格式 consume

**原理与位置。** [Ingestion.cpp](../../src/Ingestion.cpp)（约 160 行）移动积累 chunks，最多 8 MiB／64 块，`write_payload_batch` 一次生命周期提交后仍按原 transfer 边界、原顺序 `consume_chunk`，尾批在 finish／seal 前完成。[Ingestion.h](../../include/iotdbfs/Ingestion.h) 增加配置，挂载接线位于 main 约 1324 行。

**效果。** 同轮 P01 单块→默认 8 MiB checkpoint：发布 **115.910→102.616 s**；相对旧 142.165 s 观察下降 27.82%。manifest SELECT／frontier INSERT **各 6,820→863**；payload OBJECT Tablet **仍为 6,820**。三臂 payload SQL **46.327／27.281／6.127 s**。这消除的是状态检查／更新往返，而非把 8 个 OBJECT 合成一次传输。

**边界。** 默认 transfer=1 MiB、checkpoint=8 MiB；批次内保留原 OBJECT offsets。明确块失败不提交 frontier，但可能留下不可见 artifact；fresh 重试覆盖或 abort 清理。**回执丢失可能已推进 frontier**，不能声称任何失败都不改变状态。Published 仍在完整 payload/layout/metadata 后。批次持锁期间其他操作等待，本轮没有证明并发发布／查询不会受影响。

### P03：删除 payload／layout backend 显式输入复制

**原理与位置。** backend `write_payload_range`（约 803 行）和 `write_layout`（约 829 行）删除 `auto copy=bytes/layout`，直接将调用方 const vector 传入 SDK `addValue(OBJECT)`；SDK 在调用内编码并拥有内容，不保留该 borrowed 引用。必要所有权移动、SDK 编码与网络序列化复制均保留。

**效果。** 同 `/3` profile 的 copy／nocopy 单轮：完整发布 **98.707→79.407 s**。对照中明确输入复制 **3.828 s**；payload OBJECT 编码 **11.384→4.571 s**、SDK insert **32.470→25.582 s** 也有变化。**不能把总 19.300 s 差额全归于删除复制**，两臂固定顺序，仅一轮，allocator/cache 和数据库状态有波动。报告（服务器归档：`docs/hdf5-geos-publication-construct-20261010.md`）。

**边界。** 未改 SDK、OBJECT 编码协议或布局；仍为 1 MiB transfer／8 MiB checkpoint。读取端 SDK string→owning vector 的复制是所有权转换，本次没有删除；SDK 请求构造、序列化、网络与服务器时间尚未完全分离。

## 诊断与没有获得收益的实验

### D01：可归属、可校验的分层计时

新增 [PublicationProfile.h](../../include/iotdbfs/PublicationProfile.h)，在 main 的 empty_create/content_commit root（约 1973／2060／2585 行）、Ingestion 阶段、backend fetch／OBJECT 构造及 [IoTDBClient.cpp](../../src/IoTDBClient.cpp) 的 query/nonquery/close/Tablet/session acquire/SDK insert 中接线。线程 context 将延后 close 归回原 stage；成功与异常一次性计数，诊断关闭时不采样时钟／构造 detail；诊断失败不改变查询／发布行为。

- `IOTDBFS_PUBLICATION_PROFILE_JSONL`：显式启用发布 root JSONL，当前 `/3`；包括 `tablet_build`、`object_encode`、源读取、Adapter consume、mutation lock wait 和嵌套 SDK／pool。
- `IOTDBFS_QUERY_PROFILE_JSONL`：既有查询 sink 新增 catalog、fresh revalidation、marker/object、COUNT/filter、cache、授权复用与 SQL/row/byte 计数，不是重新发查询来计时。
- `IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL`：可选 FUSE 消费授权诊断，核对 uid/gid/groups、允许／拒绝和 cache miss/hit；启用时其开销属于客户端消费。

profile-only GEOS 主轮完整发布 **135.092 s**，root SQL API **52.659 s（39.0%）**，Tablet **32.492 s**；HDF5 finish/index **2.256 s**。这支持优先优化 payload manifest 往返，不能称 profile 本身加速，也不是数据库 CPU 或真实 RPC 数。发布计时报告（服务器归档：`docs/hdf5-geos-publication-profile-20261010.md`）。`query_fetch` 是 hasNext/next 方法次数，不等于 SQL 次数；`sdk_insert` 是综合 wall time，不是纯序列化。

### D02：transfer 大小参数与 2／4／8 MiB 负结果

[resources/config.properties](../../resources/config.properties) 与 main 配置解析／校验（约 818／896 行）增加 `writer.transfer_chunk_bytes`，严格正整数字节、≤8 MiB，须容纳于 `writer.db_request_max_bytes` 和 checkpoint。`writer.manifest_batch_max_bytes` 默认 8 MiB，也必须合法；非法配置明确拒绝，不静默 clamp。该参数同时改变 generic ingestion 的 source read、OBJECT transfer 和 Adapter consume 边界，不是只调网络 packet；legacy GRIB streamer 内部 chunk 和普通读 chunk 不改。

相同 binary、16 个原件、相同 8 MiB checkpoint，固定顺序 1／2／4／8 MiB，发布 **84.733／108.249／106.891／117.271 s**；OBJECT Tablet **6,820／3,413／1,712／863**，checkpoint **863**、SQL **2,430** 均不变。**请求更少没有带来 wall time 改善，生产默认保持 1 MiB**。大块的 OBJECT 编码约 16–18 s，1 MiB 为 4.870 s；没有实测确认 allocator 根因或任意 SDK／长路径的 frame 安全。[完整负结果报告](../reports/hdf5-geos-publication-transfer-20261010.md)。

## 累计证据、最新状态与未做的事情

### 同条件累计复验到哪里

[累计复验](../reports/hdf5-geos-cumulative-20261010.md)从精确 HEAD 独立构建，与**当时 Q01–Q10 生产版**交错运行同一 144 请求集合：累计客户端耗时 **14.568→4.870 s（66.6%）**，控制请求 SQL **3,093→585**。N=16 时间命中 **153.493→41.533 ms、40→6 SQL**；仅属性 **621.515→127.032 ms、115→7 SQL**。其中仅属性每臂仅三个首次／部分预热样本，不能与充分预热 15 样本的 118.439／110.271 ms 混合。

另有 TTL=1 暖态 typed 对照：GRIB 完整客户端 **41.663→9.688 ms**、HDF5 **38.415→5.636 ms**，注册元数据 SQL **4→1**。这是公共入口优化，不是证明 HDF5 比 GRIB 快；两格式工作量不同。

这轮累计证据**早于 P01–P03、D02 和 Q11**，不能标成“当前全部未提交修改的最终累计 A/B”。历史完整发布 **136.751／147.763／133.167 s** 是不同轮参照，不与 P01–P03 消融串成一条因果下降链。

### 当前目录 SQL 模型

```text
初始 published catalog／权限证据
→ 对全部 N 授权
→ fresh manifest 批量重验
→ 时间剪枝得到 K
→ fresh marker 合批＋逻辑预算检查
→ 解析缓存命中复用／miss OBJECT 合批读取与验证
→ 原请求 schema／属性可用性校验
→ 两表完整分组 COUNT＋属性候选 SQL
→ 精确 residual 求值／有序结果
```

成功目录发现内部的 SQL 模型为：`1 + Bmanifest(N) + Bmarker(K) + Bobject(misses) + 3×Bindex(K)`。只有 bounded exact 请求已从初始 catalog 复用完整祖先／源授权证据时，它才等于完整 profile 的元数据 SQL；subtree 或证据回退可能额外执行授权 SQL。短路径、N>0 且 manifest 单批时，K=0 跳过 marker/object/index，为 2 SQL；空选择 N=0 为 1 SQL，129 源时间未命中为 3 SQL。K=16、各阶段单批且解析全热为 6 SQL，解析冷通常 7 SQL。Q11 减少 JOIN 工作而不减少 SQL 次数。latest 仍最终选择，N=16 注册沿用 exact 路径/latest，并逐返回身份与冻结版本核对，不声称接口已支持每路径不同 exact-version 声明。

当前本轮属性排序命中两次 COUNT **约 75 ms**，仍是大项。inode JOIN 保留；长期完整性证据缓存、COUNT 合并／删除、SDK 内部编码重写、发布并行、metadata 发布合并、锁粒度重构都**未实现**。早期[接口审计](../knowledge/iotdb-interface-optimization-audit-20261010.md)和[SQL 计划审计](../knowledge/query-sql-plan-audit-20261010.md)包含待验证建议，不能全部当作当前优化。接口审计中“复制尚未移除／transfer 尚未可调”由 P03／D02 更新，其余建议未自动落地。

当前 dirty files 没有修改 `src/formats/grib`／`src/formats/hdf5` 的查询／decoder；先前已提交的 layout cache、语义绑定、chunk 组装、Arrow training 优化不属于这次工作树优化范围。已存在的 payload 多范围读取本轮仅增加 fetch 计时包装，不能算作新增合批功能；Q10 的目录解析缓存也不同于既有 decoded value cache。没有新增版本压缩、分布式运行或大规模候选证明。16 GEOS 文件只是试点；已有已解析 FS 时间索引仍可比数据库目录快几个数量级，这些优化不构成一般性的 FS 优势证明。

## 验证、复现与文件覆盖

最新本轮独立 Release **1,444／1,444 单元通过**；Q11 本地定向 **100／100**、ASan＋UBSan **100／100**、恢复旧行为 **4／4 预期失败**。早期各轮测试数是该轮快照，不相加成独立测试覆盖。Q04／Q05／Q10 还包含真实非 root 补充组、源／祖先权限、chmod、unlink、缓存失效与精确版本 IoTDB 探针；各报告保存细节。D02 本轮主程序改动通过配置探针和 live mount，不把之前的库测试冒称重新执行。

GRIB 控制返回 level=500 message BinaryView（IPC 67,648 B），HDF5 控制为 GEOS `/T` 单点（IPC 5,752 B）。各自前后 bytes／SHA 不变，后期公共注册 metadata 为 1 SQL；Q11 typed 控制耗时略慢，只有内容／调用一致性，不声称它们提速。P01–P03 验收完整 payload SHA、HDF5 值与 layout digest；P03／D02 另验 Arrow、GRIB 跨块 message、small text、empty。不能把单元通过等同于稳定性能保证。

| 当前工作树文件组 | 涉及优化／用途 | 查验方式 |
| --- | --- | --- |
| `IoTDBStructuredBlobBackend.{h,cpp}`、新 `IoTDBPublishedCatalogSql.h` | Q01–Q03、Q05–Q10、P01/P03；generation 和索引／缓存失效 | exact-key SQL、fresh 验证与 backend 探针 |
| 新 `IoTDBDatasetCatalogCache.h` | Q10 immutable parsed LRU | identity、budget、invalidations、129 源验收 |
| `QueryPlanning.{h,cpp}`、`DatasetCatalogQuery.{h,cpp}` | Q01/Q02/Q05/Q11 的格式中立契约 | selector、permission evidence、三值逻辑回归 |
| `main.cpp`、`QueryRegistrationSocket.{h,cpp}`、新授权 helper 两头文件 | Q02–Q05、D01/D02 接线 | non-root socket/FUSE、组键、generation、TTL |
| `StructuredBlobStorage.{h,cpp}`、`Ingestion.{h,cpp}` | P01/P02 生命周期与流式边界 | 故障／重试、exact EOF、顺序与完整 SHA |
| `IoTDBClient.cpp`、新 `PublicationProfile.h` | D01、P03 可测构造链 | 关闭诊断、deferred close、nested timers／失败记录 |
| `resources/config.properties` | D02、P02 默认与限制 | 1 MiB transfer、8 MiB checkpoint、拒绝非法值 |
| 两处 CMakeLists | 新探针／profile unit 构建 | probe 为 EXCLUDE_FROM_ALL；不改变格式布局 |
| `tests/unit/` 改动与新 profile 单测 | 上述代码契约保护 | 最新测试结果和每轮 RED/GREEN |
| `tests/benchmarks/hdf5_query/` 改动及未跟踪工具 | 独立消融构建、同入口计时、权限／catalog／payload／profile 验证 | 各单轮报告的 runner/build/validator 链接 |
| `docs/` 改动及未跟踪报告／JSON | 单轮证据、累计参照、审计与本总结 | 配套文件清单及 hash；不是新增算法 |

所有现有未提交／未跟踪文件的逐项覆盖见配套 JSON。原始日志常位于被忽略的 `docs/results/`，单轮 Markdown／JSON 保存服务器路径、source/binary/raw hash；后续归档需同时保留 raw 证据，不能只保留总体 p50。本次整理没有复制凭据或 mount.log，也没有重启挂载、改数据或提交代码。

复验应从某个明确证据轮的源码／二进制 hash、原件身份、配置、warmup 和 reader 开始，用新输出目录和隔离路径；前后按同轮冻结版本配对。要确认**所有最新修改叠加后的总收益**，仍需另做精确 HEAD 对当前完整快照的同负载累计复验，并分别统计发布、目录发现、typed 完整消费，不能用本文各行的独立比例代替。
