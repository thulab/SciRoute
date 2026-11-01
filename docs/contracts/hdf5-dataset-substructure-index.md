# HDF5 dataset 子结构索引

## 单元与职责

首版以一个 HDF5 dataset 对象为子结构单元。已有 `DatasetCatalogRecord`
保存 object ID、规范 dataset 路径、带完整性状态的 typed 属性；目录绑定
layout protocol 和 layout SHA-256。发现结果返回精确 source/version、revision、
object ID、dataset 路径和 layout 身份。数组查询继续由 HDF5 Adapter 经该路径和
layout 解析 shape、dtype 与科学坐标；文件内读取由原生 HDF5 或自建 chunk 索引
两种引擎执行，不复制、重排 payload，也不在查询核心加入 GEOS 或 dataset 专用结构。

本索引提供与 GRIB message 元数据索引相似的发现方式，不声称 dataset 是单个
连续字节区间或能够作为一个独立 HDF5 文件解码。首版不增加时间／压力层 slab
条目，也不改变现有 dataset 入口、科学绑定或 typed 输出契约。

## 发布封印

HDF5 layout `/5` 和 `iotdbfs-dataset-catalog/1` 的编码保持不变。新增 manifest
元数据：

| 键 | 类型 / 含义 |
|---|---|
| `__iotdbfs.dataset_index.protocol` | String：`iotdbfs-dataset-substructure-index/1` |
| `__iotdbfs.dataset_index.sha256` | String：64 位小写十六进制 SHA-256 |
| `__iotdbfs.dataset_index.bytes` | Int64：规范序列化目录字节数 |
| `__iotdbfs.dataset_index.records` | Int64：dataset 记录数 |
| `__iotdbfs.dataset_index.attributes` | Int64：全部记录属性数 |

摘要输入按顺序为：协议字符串、源 path、version、format ID、payload size、
layout size、目录字节数、规范序列化目录字节。整数为 8 字节小端；字符串为
8 字节小端长度后接字符串字节；version 按 UInt64 编码。路径使用存储层
`path_utils::norm` 的既有规则，不新增 `.` / `..` 或重复分隔符归一化。
目录字节自身包含 layout 身份、对象路径、属性值、类型和完整性状态。

phase、revision 和目录之外的 metadata 不进入摘要，允许正常发布与 POSIX
权限修改。它是内容和源身份的一致性校验，不是 MAC 或抵御有数据库管理员权限
的篡改认证。进程内 cache 另以 backend namespace 隔离，不向持久摘要加入随机数。

描述阶段从待写目录计算封印，一同写入 manifest；不能只复制封印而不提供目录。
摄取 outcome 将最终 metadata 传给 namespace preparation，避免旧 metadata 覆盖封印。
rename/copy 为目标 path/version 重新封印，并将新 metadata 更新到目标 inode/cache。

发布前验证已存目录与预期封印一致；IoTDB backend 还在写锁内核对完整 marker、
每一条 dataset entry 和 typed attribute。缺行、重复、额外行、对象关联错误、
类型或值替换均不得发布，不以 COUNT 相等代替内容验证。读取使用有界对象和
`expected+1` 哨兵行限制。验证失败保留不可见状态，可重试或 abort。

发布后 backend 禁止改写 payload、layout、catalog、内容 metadata 或 manifest。
合法权限、时间和链接数更新保留；删除继续隐藏源、清理 artifact 并 tombstone
精确版本。约束仍要求受控单 writer，不以本机锁代替跨主机写入协调。

目录不完整、属性不可读取等状态原样封印，不将它们升级成“完整”。是否可查询
继续由目录完整性和请求属性的既有验证规则决定。

## 统一跨文件发现

两种正式 [HDF5 查询模式](hdf5-query-modes.md) 都使用发布封印的目录 artifact，
在有界解析缓存中执行 typed 属性谓词，不执行查询时二级表 COUNT 或属性 JOIN。
旧 `query.hdf5.catalog_mode` 已移除，`strict/substructure-index` 仅是历史发现策略。
发布前的二级表内容验证继续保留，不能将读分支整理理解为放宽发布校验。

source selection、授权、冻结 manifest 重验、marker 检查、精确版本绑定、time pruning、
请求逻辑字节预算、schema 检查和结果上限保持原样；按原 source 顺序及 object ID
顺序返回。缓存命中不能免除授权或请求预算。即使时间不命中，已选 source 也必须
具有有效封印；无封印明确报 `LayoutUnavailable`，需要显式重摄取。

冷 miss 读取目录、检查 marker 和封印后才可入缓存。温热 hit 不重读对象、不重复
SHA-256，不执行 payload/layout 读取。cache key 包括 namespace、精确版本、
manifest revision、format、payload/layout sizes、完整 marker、封印摘要。
缓存既有 64 MiB 字节预算与 65,536 项保护上限保持不变。

查询不依赖规范化二级 SQL 表的查询时状态。发布后的直接 SQL 改动不属于正常
写入协议，不能要求同时省略二级表读取并检测其所有损坏。目录对象的冷读取受
SHA-256 保护；同身份的已缓存对象不会每次读取原数据库验证。

## Owner 下跨请求的已验证状态复用

HDF5语义索引仍位于HDF5 Adapter，复合键有序存储／范围查找由公共`ImmutableEqualityIndex`提供；完整候选、head键、费用与proof生命周期复用backend机制。显式启用 owner 时，两种正式 HDF5 模式的六属性等值 AND 均可复用
已冻结候选域、完整属性索引及成功验证证明，不改持久格式。
首版字段为 `shortName`、`typeOfLevel`（UTF8）和 `level`、`step`、
`validityDate`、`validityTime`（Int64）。其它表达式、时间条件、文件谓词或
不满足完整标量类型约束的整个候选域继续使用既有通用路径。缺失属性的 NULL、
不可读／不支持类型及全候选错误检查保持原语义。

域按 backend owner 实例、namespace/incarnation、完整 head、路径／版本／
dataset 范围及 source 上限隔离。证明还绑定该域和规范化 uid/gid/补充组。
任何提交令新查询重新选取候选；旧计划仍绑定原版本。命中证明只省略重复的
元数据验证，每次 execute、发布或新 open 仍经过真实 owner 准入；Frozen、
head 冲突、权限撤销、删除和新文件不能由缓存绕过。

GRIB／HDF5共用的热池合计限制为 64 MiB、16 个候选域和 64 个主体证明，包含索引、费用轨迹、
管理信息、构建中状态及淘汰后仍被请求／执行持有的对象。它不替代既有其它缓存
预算，也不表示进程总 RSS 限制为 64 MiB。超限使用既有有界路径；已经冻结候选
后的失败不能重新选择 Latest。相同键构建合并，索引和证明不可变共享，不保存
ROI 或查询答案。每请求仍按原身份的 max/delta 逻辑费用计入原请求预算，
保留选择、授权、manifest、catalog 的顺序和错误优先级；不能将费用重放称为
与候选数无关的 O(1) 工作。

热域的选择／manifest／catalog 费用及主体证明费用以不可变批次共享。无活动
轨迹收集器时，一次批量重放只锁账本一次，每个身份一次定位，再按原字典序
计入 max/delta；同一账本已经完整成功重放的同一批次可跳过重复展开。不同批次
仍逐身份核对，不直接累加整批总额。有任意活动收集器时沿用逐项
“账本→内层收集器→外层收集器”，包括新建收集器对已收费证据的观察。
首次失败保留成功前缀与原错误，失败批次不记录为成功，耗尽状态始终粘性。

每请求账本固定最多强持 256 个成功批次，防止对象地址复用导致误命中；满额仅
停止新增这项复用。批次持有原缓存 reservation，淘汰后仍被请求持有时继续
占用原池额度，新增批次对象及管理开销也预留。缓存分配失败退出缓存构建，
不新增请求失败。它不跨请求免账，也不省略 owner 准入；首次重放仍随费用项数增长。

新增 `hot_query.domain_hits`、`domain_builds`、`index_lookups`、`proof_hits`、
`proof_builds` 计数；实际 owner 准入继续记录为 `version_context.admissions`。
热查询与首次构建必须分别计时，完整查询验收见
跨请求热缓存实验 (local archive: `docs/reports/hdf5-route3.md`)。

## 操作与观测

配置示例：

```properties
# 默认引擎；两种模式都要求发布封印。
query.hdf5.mode=iotdb-indexed
```

重新摄取原始文件并发布新版本，不回填／修改已发布旧版本。现有按 layout 版本
选择候选的迁移 inventory 不会自动将 `/5` 无封印版本列为升级目标，不能依靠
切换配置补齐封印。历史 strict 数据可由原二进制读取；当前代码不提供旧发现分支。
切换文件内引擎不会隐式迁移数据，一次请求涉及任意无封印源即明确失败。
模式在 adapter 构造时固定，挂载配置变更需要新进程。

新增 profile counters：`dataset_substructure_index.selected_sources`、`cache_hits`、
`cache_misses`、`verified`、`predicate_records`、`matches`、`publication_validations`
（除首项完整拼写外，其余均加 `dataset_substructure_index.` 前缀）。原有
`dataset_catalog_*` SQL 和对象读取计数继续保留，便于直接检查新模式是否省略
COUNT/JOIN；下述性能报告保留各自的历史 strict 对照。`sql_queries` 不包含授权回调自行执行的 SQL，不能与 SDK 总调用数混用。

统一发现路径执行阶段使用请求内不可变的 `DatasetAttributePredicate`：
一次编译字段、类型约束和表达式 slot，每条记录只绑定一次原始 typed 属性。
先检查全部候选再输出，保留不可读属性、类型错误、NULL 及预算边界；不以短路或
输出上限跳过检查。新增 `predicate_compiles`、
`predicate_bindings` 计数也使用 `dataset_substructure_index.` 前缀。
预编译消融与经用户批准的计时外采集修复见
性能报告 (local archive: `docs/reports/hdf5-predicate-precompile-ablation-20261015.md`)。

非 owner 通用路径将授权后的精确 manifest 重验与时间候选的 marker 获取
合为带 INT64 类型标签的有界 `UNION ALL`：每批最多 128 源、整条 SQL 最多
64 KiB，两个分支各有 `expected+1` 哨兵。时间未命中的源仍重验 manifest，
无 marker 候选时只运行 manifest 分支。逻辑检查保持 manifest、封印、时间、
marker 的错误优先级；marker 错误延迟到 source／封印／时间检查之后返回。
合并查询无法建立 freshness 时返回 StalePlan，不隐式回退或省略校验。
owner 路径使用自己的冻结目录、准入和变更校验，不能据此推断每次执行相同 SQL。

新阶段 `dataset_catalog_verification_read` 覆盖合并编译、SQL 与 manifest 校验；
同名前缀的 counters 为 `calls`、`manifest_keys`、`marker_keys`、`manifest_rows`、
`marker_rows`。独立 manifest／marker counters 保留用于对应存储操作，
不能将它们为零解释为跳过检查。该 SQL 要求表模型支持有界 UNION ALL；
单分支与 UNION 使用同一 INT64 标签，避免常量投影的 INT32 类型差异。
N=128 合并读取消融 (local archive: `docs/reports/hdf5-manifest-marker-ablation-20261015.md`)记录固定
runner、完整客户端计时、GRIB 控制和已验证的错误边界。

功能探针的 SQL 次数不直接推导 wall-time 改善。冻结 N=128 runner 的实际计时、
发布额外成本及适用边界另见
性能验收报告 (local archive: `docs/reports/hdf5-substructure-index-n128-20261015.md`)；该实验未修改
冻结多文件 runner 或历史性能基线。
