# 查询完整性扫描与 JOIN 计划审计

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-10。本轮检查当前源码、重新统计历史请求级 profile，并在 `root@192.168.99.49` 的既有 GEOS 数据库执行独立只读 SQL 探针。**有改进空间，但增加 JOIN 条件、删除排序或更换 GROUP BY 并不能自动带来收益。优先处理退化谓词的无用 JOIN，并继续寻找保留完整性验证的低扇出聚合计划。**

没有修改生产查询代码、SDK、数据库、schema、挂载或服务器配置。SQL 诊断未经过 frontend/core、授权、IPC 和结果发布，因此不作为端到端 A/B 结果。

## 现有请求的主要成本

重新统计目录对象合批实验（服务器归档：`docs/dataset-object-batching-ab-20261010.md`）的修复后全热 profile，每格 15 个计时样本。以下为 p50，单位 ms；COUNT 阶段包含 entry 与 attribute 两条 SQL。

| 场景 | 客户端完整目录请求 | 服务端完整请求 | 两次 COUNT | 属性筛选 SQL | 元数据 SQL 数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 候选，仅属性 | 35.611 | 34.994 | 13.293 | 11.940 | 6 |
| 16 候选，仅属性 | 118.439 | 117.771 | 73.998 | 22.367 | 6 |
| 16 候选，时间命中 1 源 | 44.813 | 44.207 | 14.920 | 13.221 | 6 |
| 16 候选，时间未命中 | 13.378 | 12.859 | — | — | 2 |

16 候选全热属性发现的逐请求 `(COUNT + 属性筛选)/服务端完整时间` 占比中位数为 **83.69%**。它们是目录索引检查与查询计划成本，不是 HDF5 payload 解压或子数组复制。该场景连接获取累计 p50 仅 **1.330 μs**，零超时，不支持优先扩容连接池。

这组历史测量使用 `5b693418…` 二进制，不能称为最新 transfer 二进制 `c8659065…` 的复跑；查询 SQL builder 的历史／当前 SHA-256 相同，但 backend/client 已有其他修改。另一份累计报告的 16 候选仅属性结果为 127.032 ms、7 SQL，只含 3 个部分预热样本，不能与上述全热结果混合。

历史 `metadata_query_nanoseconds` 只计初始 `executeQuery`，含 session 获取，不含后续 fetch/close；没有独立 mutation 锁等待计时。阶段包含关系和中位数均不能相加解释全部差值。[独立复算记录](../results/query-sql-plan-audit-20261010/historical-profile-audit.json)保存原始文件 hash、样本数与统计边界。

## 扫描已经有界，问题在候选内部的扇出

当前 SQL 对每批精确 `(path, version)` 保留有限 `path IN` 和配对 OR，最多 128 个键、64 KiB SQL；完整性 COUNT 不因 `dataset_path` 或属性筛选缩小。实际 16 源查询每源具有 18 个 dataset、194 个属性：

| 完整性查询 | DeviceEntry Count | 两个扫描器输出行合计 | 原计划 |
| --- | ---: | ---: | --- |
| dataset entry COUNT | 288 | 288 | DeviceTableScan → Filter → 聚合 |
| attribute COUNT | 3,104 | 3,104 | DeviceTableScan → Filter → 聚合 |

原计划的 Filter 未剔除行，已经限定到这些候选，**不是重新扫描整个 namespace**。`path/object_id/attribute_name` 等 TAG 构成许多表设备条目；合批减少了 RPC 数，却仍留下大量小单元扫描与算子调用。原 attribute COUNT 的单次 EXPLAIN ANALYZE 中一个分区产生 1,576 行，扫描后 Filter 的 HasNext 调用达到 3,153 次。算子 CPU 是包含子算子的统计，不能逐项相加。

本轮相关 ANALYZE 均报告磁盘 I/O 为 0，属于热态观察；不是磁盘冷读，也不能将全部耗时归为磁盘或字节传输。规划还包含 schema/partition 获取、逻辑优化、分发、Exchange 等固定与扇出成本。

验证过的 SQL 改写边界：

- **补充 `time IN(本批所有版本)`**：保留精确配对 OR，确实新增扫描端 TimePredicate；不能以两个独立 IN 替代配对条件。当前 entry/attribute 读取行数没有下降，较多历史版本时是否有收益仍需专门验证。
- **`GROUP BY path` 加 `max(time)`**：仅在一条 path 对应一个精确版本的批次内等价；当前 16 源仍有扫描后 Filter，没有获得 AggregationTableScan，不能只改分组就宣称实现聚合下推。同 path 多版本必须保留逐版本分组或采用另外经过验证的计划。
- **单源、固定版本、按 path 聚合诊断**：能出现 AggregationTableScan，但工作量只有 1 源，不能拿其耗时预测 16 源收益，也不建议退回逐源 SQL 循环。

补充控制进一步区分原因：单源保留 `max(time)` 仍能出现 AggregationTableScan；16 源去掉 `max(time)`、只返回 path 与 count，仍是 DeviceTableScan＋相关 OR Filter，新增 time IN 后也一样。因此当前证据指向多源精确配对的残余过滤，而不是 `max(time)` 本身阻断单源下推。该控制只有 EXPLAIN，不声称其执行速度。

两次实际 COUNT 当前用于检测索引整组丢失、子集外缺行、额外或错误计数组，marker 中的预期数量与解析缓存不能代替它们。将 entry 与 attribute 直接 JOIN 后 COUNT 会产生行乘积；若合并检查，必须先独立聚合、保留零行／缺失组检测，再关联结果。仅减少一次 RPC 也不保证减少底层扫描。

## JOIN 的实际计划与冗余

原单属性查询为 entry JOIN inode，再 LEFT JOIN `standard_name`。等值谓词拒绝 NULL 时，优化器将该 LEFT JOIN 转为 INNER JOIN；缺失属性／NULL 查询不能照此手工改写。

| 诊断 SQL | 规划设备条目 | Fragment instances | 结构观察 |
| --- | ---: | ---: | --- |
| 原单属性查询 | 544 | 11 | entry 288＋inode 16＋属性 240 |
| 每个 JOIN ON 重复精确源条件 | 544 | 11 | 没有新增设备剪枝，计划更复杂 |
| 移除 inode JOIN | 528 | 7 | 少一个 JOIN 与 inode 扫描，仅诊断 |
| `TRUE` 仍保留属性 LEFT JOIN | 544 | 11 | 仍扫描 240 个属性设备条目 |
| 同一 `TRUE` 查询移除无用属性 JOIN | 304 | 7 | 只留下 entry 与 inode |
| 两属性查询 | 832 | 15 | 同一 attribute 表被两个别名分别扫描 |

**不优先重复写各表精确条件。** 原右表已有优化器推导的范围 Filter，显式 ON 改写没有减少设备或读取行。新增 time IN 能把 inode 原来的 32 行历史输出减为 16 行，但当前负载未改变最终候选；这与减少所有 JOIN 成本是不同结论。

原单属性 ANALYZE 的逻辑优化耗时为 13.091 ms，schema 获取为 2.469 ms；一轮计划中的小排序实际 sortCost 只有数微秒。它们来自独立插桩运行，不能换算为重复 SELECT 的精确占比，但提示应优先缩短表达式／关联计划，而不是先优化客户端对象排序。多属性确实重复扫描同一表，后续可单独验证一次有界属性扫描加条件聚合／投影的计划；需先确认服务器支持能力，并保留类型列、缺失／NULL、重复行检测和最终精确求值，不能直接以拼接字符串替代 typed 属性。

**退化为 `TRUE` 的属性 JOIN 可以成为独立修复项。** [谓词编译器](../../src/query/core/DatasetCatalogQuery.cpp:467)对部分不能精确下推的条件改用 SQL `TRUE`，但仍返回全部 `attributes`，后续继续构造无用 LEFT JOIN。这类请求本来就由最终 C++ evaluator 精确判断，应只移除 SQL 实际不使用的关联，仍执行全部候选的属性类型／完整性校验和 residual 求值。当前真实 `/T` 样本的 TRUE 有／无属性 JOIN 返回相同 16 个精确对象；这不是允许忽略真实谓词的依据，也未覆盖所有 NULL 或损坏索引输入。

**inode JOIN 存在重复验证，但不能仅凭静态结果删除。** 当前发现流程已批量 fresh 重验发布版本，并在后续索引查询期间持有 backend mutation 锁；移除关联的诊断保留最终对象集合并减少执行片段。生产修改仍需明确其依赖的单 writer 边界，覆盖授权后删除、重命名、替换、新 latest 等回归，保留 fresh manifest 重验与权限证据。多个 SQL 不是数据库级事务，不扩展为任意外部 SQL 写入时的保证。

**删 ORDER BY 没有消除全部排序。** C++ 最终结果确实重新组织源和对象顺序，但 MergeSortJoin 仍要求输入排序；诊断还出现原 StreamSort 被普通 Sort 替代。不能从 SQL 文本少一个 ORDER BY 推导排序成本归零。

## 独立 SQL 的交错重复计时

先执行 20 个不同 SQL 的 EXPLAIN、EXPLAIN ANALYZE 和真实结果查询，再对 7 条关键语句各执行 5 次 SELECT，按轮旋转／反转顺序。另补 3 条区分 MAX 与相关过滤的 EXPLAIN；合计 **98 次只读请求**。全部 55 次实际 SELECT 和 20 次 ANALYZE 成功；23 次 EXPLAIN 中两条双属性计划超出探针 1 MiB 展示上限，失败原始记录保留，其对应 ANALYZE 与 SELECT 成功。

本表每条 5 个样本、单 campaign、已有 OS／IoTDB 缓存未清空，单位 ms：

| SQL 诊断 | p50 | 最小—最大 | 判断 |
| --- | ---: | ---: | --- |
| 原 attribute COUNT | 61.694 | 52.064—87.884 | 主要成本仍在完整性扫描 |
| COUNT 增加 time IN | 63.066 | 51.245—70.852 | 当前负载没有测得收益 |
| 原单属性筛选 | 26.137 | 23.667—27.214 | 参照 |
| 各 JOIN 显式精确范围＋time IN | 23.920 | 22.361—25.945 | 设备数不变，两个改写合用，不能单独归因 |
| 单属性筛选移除 inode JOIN | 21.137 | 20.221—23.606 | 少一次关联，约少 5.0 ms，须验证生产一致性边界 |
| TRUE，仍有属性 LEFT JOIN | 26.633 | 25.329—41.865 | residual-only 诊断参照 |
| 同一 TRUE，移除属性 JOIN | 17.061 | 15.504—29.730 | p50 少 9.572 ms，约 35.9%，支持优先修复 |

计时含 SDK execute、fetch、逐行 JSON 构造和 close；排除 session.open 与 stdout 输出。不经过 IoTDB-FS runtime/session pool，不能直接与历史完整请求的阶段相减。ANALYZE 是另一轮带插桩执行，不将其各 CPU／fragment wall 相加解释上述 SELECT elapsed。

结果核对按**对应变体与其 oracle**进行：16 源 COUNT 比较完整 `(path,time,count)`，属性查询比较精确 `(path,time,object_id)`，单源聚合只与该源的计数比较。不是宣称 20 条不同查询全部互等。TRUE 与真实属性谓词在当前 `/T` 样本碰巧选择相同对象；生产使用仍须保留 residual 判断。这里没有修改／删除索引验证损坏场景，也没有重新跑权限或 GRIB/HDF5 端到端控制，因此这些诊断不能作为生产修复验收。

## 公共路径与 GRIB 的关系

以上 COUNT／dataset 属性 JOIN 属于跨文件 dataset 目录发现，当前主要由 HDF5 使用；普通 GRIB/HDF5 typed 取值查询不执行这两条 COUNT。公共 published catalog、精确 manifest、权限访问前几轮已合批，不能把本次 dataset SQL 的潜在收益归到所有 typed 查询。

旧 GRIB cross-file／semantic-grep 另有 [inode＋set-member INNER JOIN](../../include/iotdbfs/MetadataSetIndex.h:59)。其 `field_id/member_type/member_value` 均为 TAG，谓词含具体成员值；HDF5 的属性值 `svalue/fvalue/ivalue` 是 FIELD。两者剪枝结构不同，不能直接套用 GRIB 的选择性结论。本轮没有运行该旧 GRIB JOIN 的消融。

公共 catalog 的 `latest` 仍先读匹配路径／格式的多个版本，再在 C++ 选最新。未来版本量很大时，可独立验证数据库端先选每 path 的最大版本再获取完整行，保留“格式筛选先于 latest、metadata 谓词晚于 latest”以及祖先目录最新权限规则。当前没有证据表明再加一个聚合 JOIN 在小版本量下更快。

## 后续优先级

1. 移除 residual-only／TRUE 情况的无用属性 JOIN；保留校验、精确 evaluator、缺失／NULL、对象及行数上限。先作为独立变体测试。
2. 围绕 COUNT 的设备扇出和精确版本相关条件设计真正有存储端聚合的批量计划；保持实际检查，不返回逐源 RPC，也不以 marker 缓存替代检查。
3. 单独验证发布状态 inode JOIN 的冗余及去除边界；不能混入 COUNT 改写，避免无法归因。
4. 在多历史版本数据上验证额外 time IN 和 latest 选择计划；不优先扩大属性 ON 表达式或普遍删除排序。

任何生产修复都应重新跑完整目录端到端 A/B，并保持 GRIB/HDF5 同入口 typed 控制、精确对象与 IPC 正确性、授权及损坏索引回归。本轮证据支持优化查询结构，尚不支持承诺端到端降幅或相对本地 FS 的优势。

## 证据与复现边界

- 服务器数据库 `geostime20261007`，冻结源来自 `geos-time-20261007/ingest-scale-16/pilot.json`，没有创建 fixture、重新摄取或改变数据。远端证据目录：`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-query-sql-plan-20261010/`。
- [SQL 与精确身份](../results/geos-query-sql-plan-20261010/statements.json)、[原始计划／查询结果](../results/geos-query-sql-plan-20261010/raw.jsonl)、[重复计时](../results/geos-query-sql-plan-20261010/repeat-raw.jsonl)、[补充计划](../results/geos-query-sql-plan-20261010/extra-raw.jsonl)。这些文件位于忽略的 `docs/results/`，远端也保留副本；提交用摘要（服务器归档：`docs/query-sql-plan-audit-20261010.json`）保存数值与 hash。
- 独立探针直接使用已安装 TableSession SDK，无 Connector 初始化／ensure_schema；只允许 SELECT/EXPLAIN，单条 SQL ≤64 KiB、查询 timeout 30 s、输出行 ≤2,048、展示预算 1 MiB，每次进程最多 96 请求、驱动 timeout 240 s。本轮分初始、重复、补充三次调用累计 98 请求，不是一次超过上限的进程。凭据沿用既有环境 helper，未写入证据。
- 审阅的 SQL builder、backend、client、predicate compiler 与 GRIB JOIN 源码在本轮前后 SHA-256 一致。只新增本审计报告与证据，无需生产构建或重复完整单元测试。
