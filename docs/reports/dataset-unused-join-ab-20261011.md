# 移除 residual-only 谓词的无用属性 JOIN

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-11。针对[SQL 计划审计](../knowledge/query-sql-plan-audit-20261010.md)确认的问题，本轮只修复整个谓词退化为 SQL `TRUE` 后仍关联属性表的行为。没有改写 COUNT、inode JOIN、latest 选择、索引协议或缓存边界。

## 修复与保留的契约

[谓词编译器](../../src/query/core/DatasetCatalogQuery.cpp:562)完成整棵表达式编译后，若某个条件不能安全精确下推，就沿用原策略把整个 SQL 谓词替换为 `TRUE`，同时清空 SQL 属性依赖列表。SQL builder 因而不再生成无用 `LEFT JOIN blob_dataset_attribute`。这不是将查询语义替换为恒真：原始谓词仍在最后由 C++ 精确求值。

[接口注释](../../include/iotdbfs/query/core/DatasetCatalogQuery.h:60)明确返回的属性名仅描述 SQL JOIN 依赖，不是原始表达式的类型检查或 residual 依赖。所有消费点已复核：列表仅用于构造属性 JOIN。

- 原请求谓词仍用于候选对象的 schema／属性完整性校验，不能通过先选出空结果隐藏不可读取或类型错误。
- 已证明缺失的属性仍按 typed NULL 和三值逻辑处理；目录不完整或属性不可有界读取仍返回原错误。
- entry／attribute 两张完整源索引的实际 COUNT、精确 path/version 配对、inode 发布状态 JOIN、源和祖先权限、fresh manifest／marker 重验均保留。
- 等值、IN、NULL 等可精确下推的表达式保持原属性 JOIN；没有混入部分 AND 下推或其他谓词改写。
- 输出仍经过原精确 evaluator、对象身份验证、数量和字节上限，保持原顺序。

## 回归证据

新增六项测试并增强既有排序谓词测试，覆盖四类 UTF-8 排序、AND／OR／NOT 全表达式退化、精确谓词保留 JOIN、同路径多版本与两张完整 COUNT、residual 真假／NULL，以及不可读取／不完整／类型不匹配的错误。SQL 联测直接验证生成查询没有 LEFT JOIN，同时仍包含发布状态 JOIN 和完整性检查。

独立新构建的 **100 项定向测试全部通过**；同样 **100 项 ASan＋UBSan 测试全部通过**。将临时 compiler 副本恢复旧行为后，四项新回归全部按预期失败，包括实际生成 SQL 仍含 LEFT JOIN 的断言。未恢复或修改生产文件来制造失败。

服务器修复版 Release 构建及 **1,444 项完整单元测试全部通过**，无 disabled/error/failure。[完整结果](../results/dataset-unused-join-ab-20261011/server-evidence/unit-tests.json)和日志均保存。

[本地验证摘要](../results/dataset-unused-join-ab-20261011/local-tests/verification-summary.json)保存次数和源码 hash；[通过结果](../results/dataset-unused-join-ab-20261011/local-tests/green.json)、[sanitizer 结果](../results/dataset-unused-join-ab-20261011/local-tests/asan-green.json)、[旧行为失败结果](../results/dataset-unused-join-ab-20261011/local-tests/red.json)保存逐项证据。

## 服务器对照方法

新隔离目录 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-dataset-fallback-join-20261011/`，使用既有 GEOS 16 时次和 typed 控制文件，不重新摄取。修复版与[旧行为对照版](../../tests/benchmarks/hdf5_query/build_dataset_fallback_join_ablation.py)共享全部其他对象文件；对照只重编译恢复属性依赖的 `DatasetCatalogQuery.cpp`，在复制的 archive 内替换一个对象再重链。旧实验 source/build 不被覆盖。

[配对 runner](../../tests/benchmarks/hdf5_query/dataset_fallback_join_ab.py)对 N=1／16 各做三组独立挂载前后交错，每格预热后五次计时。真实 `standard_name` UTF-8 排序触发退化，分别产生命中／未命中；保留精确等值、时间命中／未命中控制。每格 15 个计时样本。

计时外从原始 HDF5 枚举 dataset 属性作为独立 oracle，验证排序真假、NOT／AND／OR、无 dataset_path 的多对象结果及缺失名称 NULL；检查 path/version/dataset 和跨版本冻结 object_id／revision／layout 身份。每次请求仍核对全源重验、fresh marker、解析缓存命中、两次 COUNT 和 SQL 调用数。GRIB/HDF5 typed 控制通过同一注册、FUSE stat 与完整 IPC 消费入口。

N=1 目录请求使用精确版本；N=16 沿用按精确路径选择 latest，并逐对象与冻结 pilot 的版本校验，不宣称注册协议已经支持逐路径不同 exact-version 声明。GRIB typed 控制同样沿用 latest，HDF5 typed 控制使用 exact version。实验期间没有摄取或修改这些源。

目录 TTL=0，attr/entry timeout=0；OS／IoTDB 缓存未清空，不能称磁盘冷读。编译、原始属性 oracle、FS 时间索引构建、挂载、prime、正确性请求和 warmup 均排除计时。目录客户端时间包含控制请求、授权、发现和响应，不含温度 payload；typed 客户端时间另包含 stat 和完整 IPC 消费。各阶段为嵌套计时，不将阶段中位数相加。

## 完整目录请求的结果

共 **480 次目录请求，其中 300 次计时**；每个版本／N／场景 15 个样本。下表客户端 p50，单位 ms。降幅只表示本轮观测，不将未改 SQL 的控制变化归于本修复。

| 候选 N | 场景 | 前→后，ms | 观测降幅 | warm 元数据 SQL 前→后 |
| ---: | --- | --- | ---: | --- |
| 1 | 排序 residual 命中 | 37.991 → **33.699** | 11.3% | 6 → 6 |
| 1 | 排序 residual 未命中 | 38.332 → **32.725** | 14.6% | 6 → 6 |
| 1 | 精确等值控制 | 38.537 → 37.801 | 1.9% | 6 → 6 |
| 1 | 时间命中控制 | 37.498 → 38.016 | -1.4% | 6 → 6 |
| 1 | 时间未命中控制 | 7.773 → 8.244 | -6.0% | 2 → 2 |
| 16 | 排序 residual 命中 | 121.953 → **110.271** | 9.6% | 6 → 6 |
| 16 | 排序 residual 未命中 | 120.417 → **111.297** | 7.6% | 6 → 6 |
| 16 | 精确等值控制 | 121.898 → 115.366 | 5.4% | 6 → 6 |
| 16 | 时间命中控制 | 44.354 → 44.114 | 0.5% | 6 → 6 |
| 16 | 时间未命中控制 | 13.176 → 12.295 | 6.7% | 2 → 2 |

直接受影响的属性筛选阶段 p50 如下，包含 SQL 提交和结果处理；完整性 COUNT 同时列出，防止把收益误解为省略验证。

| N／场景 | 属性筛选阶段前→后，ms | 两次完整性 COUNT 前→后，ms |
| --- | --- | --- |
| 1／排序命中 | **13.438 → 8.503** | 14.447 → 14.044 |
| 1／排序未命中 | **12.827 → 8.092** | 14.078 → 13.726 |
| 16／排序命中 | **24.700 → 15.173** | 73.768 → 75.220 |
| 16／排序未命中 | **24.833 → 15.826** | 74.564 → 75.383 |
| 16／精确等值控制 | 22.498 → 22.323 | 78.844 → 75.130 |

N=16 排序命中的属性筛选少约 **9.53 ms（38.6%）**，但两次 COUNT 没有变少，耗时还略增。端到端节省约 11.68 ms 不能全部归到一个算子：其他阶段及环境也有波动。精确等值的属性 SQL 完全不变，筛选阶段基本不变，完整请求却下降 5.4%；因此不将这 5.4% 计为修复收益。

三对挂载的排序命中客户端中位数均改善：

| N | 三对前→后，ms |
| ---: | --- |
| 1 | 51.736→34.932；37.876→32.739；37.161→33.549 |
| 16 | 124.921→113.201；122.241→109.448；117.994→106.254 |

第一对 N=1 原版等值及时间命中也在约 51–53 ms，比后两对明显慢；全部原始样本保留，未因较慢而剔除。三组交错和控制有助于识别波动，仍不是固定降幅或统计显著性的承诺。本轮未测索引构建或相对普通 FS 的速度。

## 语义、计划与 typed 控制验收

- 16 源的原生 oracle 共 288 个 dataset，其中 48 个没有 standard_name。每个挂载组都验证了全部对象、真实字符串排序真假、组合逻辑和缺失名称 NULL；未知且已证明不存在的名称及其 NOT 均返回零对象。N=1 无 dataset_path 的排序可以命中 `/T` 与 `/lev`，并非只检查一个温度对象。
- 480 次目录请求全部通过原生身份及冻结版本／对象／layout 对照；暖态仍为 6 SQL，两张实际 COUNT 各一次；时间未命中仍为 2 SQL，先对全部冻结源重验。目录字节预算与解析缓存命中数前后相同。
- [生产编译器生成的 SQL](../results/dataset-unused-join-ab-20261011/server-evidence/plans/generated-sql.json)确认排序和含排序的 AND 两种 fallback 的属性 JOIN 被移除，inode JOIN 保留；COUNT 字符串前后完全相同，精确等值 filter 字符串也完全相同。12 条 EXPLAIN 中 11 条完整，旧版 N=16 AND 的展示超出探针 1 MiB 上限，明确标不完整；不把截断部分当成完整计划证据。
- GRIB/HDF5 typed 共 72 次请求，其中 60 次计时，每格 15 样本。每种格式前后 IPC SHA-256、字节数完全相同，元数据 SQL 均为 1。性能结果如下；这些代码没有使用本次 fallback 修改，后版稍慢，不宣称其获得提速或证明完全没有性能波动。

| typed 控制 | 服务端前→后，ms | 完整客户端消费前→后，ms | IPC 字节 | 元数据 SQL |
| --- | --- | --- | ---: | ---: |
| GRIB | 9.171 → 10.011 | 10.238 → 11.169 | 67,648 | 1 → 1 |
| HDF5 | 4.912 → 5.326 | 5.789 → 6.264 | 5,752 | 1 → 1 |

GRIB IPC SHA-256 为 `4b9bb802a6829f0808ed1e4bf1247b1c9752b822eaf9e11f13b7560e4608a27b`；HDF5 为 `de75704449c8e71b0e4b1d39c0e4ced2493f26f8f3a6a051ca47f6c7cbee2fb4`。

## 证据与结论

修复确实消除了无意义的属性扫描／JOIN，实际 residual 查询的筛选阶段节省约 4.7–9.5 ms；N=16 完整请求本轮下降约 7.6–9.6%。公共完整性检查仍约 74–75 ms，是该场景更大的剩余成本；这项修复不会普遍加速所有属性或 typed 查询。

[完整请求报告](../results/dataset-unused-join-ab-20261011/server-evidence/ab/report.json)、[验证摘要](../results/dataset-unused-join-ab-20261011/server-evidence/verification-summary.json)、[对照构建信息](../results/dataset-unused-join-ab-20261011/server-evidence/ablation-r2/build-report.json)、[清理核对](../results/dataset-unused-join-ab-20261011/server-evidence/cleanup-verification.json)保存原始结果。提交用摘要 JSON（服务器归档：`docs/dataset-unused-join-ab-20261011.json`）记录 hash、数值与统计边界。原始证据位于忽略的 `docs/results/`，服务器也保留副本。

452 个快照文件 SHA 一致，最终测试文件另行核验；所有临时实验挂载和短路径链接已清理。没有改写旧实验 source/build，没有重新摄取数据或更改持久化协议，没有执行 git 写操作。
