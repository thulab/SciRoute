# HDF5 / GRIB 查询成本结构复核（2026-10-14）

本次只复核当前源码、冻结 runner 的既有记录及既有 SQL 计划，不运行新的绝对时间实验，不修改查询实现、数据集或性能验收基线。

## 结论与适用范围

当前 N=128、K=2、D=4、depth=2 的温热“属性发现＋完整命中单元取值”，HDF5 的主要成本落在 **dataset 目录发现的完整性检查与属性 SQL 路径**。证据支持 SQL 工作分配和执行计划为主要问题，暂不支持“HDF5 子数组查询引擎不如 GRIB message”这个解释。

两份完整当前版本记录中，逐次计算占比再取中位数：

| HDF5 指标 | 固定复验 | 辅助当前版本复验 |
|---|---:|---:|
| 发现阶段 / 客户端完整查询 | 96.17% | 94.00% |
| 取值阶段 / 客户端完整查询 | 3.83% | 6.00% |
| 完整性 COUNT＋属性筛选 / 服务端发现阶段 | 74.97% | 78.16% |
| SDK executeQuery 累计 wall / 服务端完整查询 | 89.15% | 91.80% |

这些比例用于定位当前工作量，不能作为优化收益或历史 A 的替代。SDK executeQuery wall 包含会话租用、请求提交和首次结果等待，不等于纯数据库 CPU；后续结果消费另有成本。嵌套计时不能相加，独立组件的中位数也不能相加来重建整体中位数。

## SQL 工作分配

两份记录的 12 个温热观察分别呈现相同次数：

| 阶段 / 操作 | HDF5 | GRIB |
|---|---:|---:|
| 发现：已发布源目录、源权限字段 | 1 | 1 |
| 发现：合批父目录权限 | 1 | 1 |
| 发现：冻结 manifest 重验 | 1 | 0 |
| 发现：dataset catalog marker | 1 | 0 |
| 发现：dataset entry 完整性 COUNT | 1 | 0 |
| 发现：dataset attribute 完整性 COUNT | 1 | 0 |
| 发现：dataset 属性谓词筛选 | 1 | 0 |
| **发现合计** | **7** | **2** |
| 取值请求的 catalog / 精确路径父目录查询 | 2 | 1 |
| **完整查询合计** | **9** | **3** |
| socket 注册请求（不等于 SQL 次数） | 3 | 2 |

`dataset_catalog_sql_queries=6` 没有包含外部授权回调的父目录 SQL；发现阶段实际 `iotdb_delta.query_calls=7`，不能混用两者。

额外六次 SQL 中，五次来自 HDF5 目录发现契约，另一次来自 runner 的取值请求分组。两份温热记录均无 layout / payload 读取；解析缓存已命中，当前不是因反复下载完整 layout 或数据 chunk 而变慢。

### 格式目录路径不同

GRIB 使用精确版本绑定的已解析 message 索引，在 C++ 中对候选 message 求值。当前 HDF5 虽已缓存解析后的 dataset 目录和属性，仍需重新确认 manifest、marker，检查二级表行数，再通过 SQL 属性 JOIN 返回候选，最后在 C++ 中复核返回候选的谓词。

因此这里不是“同一种 SQL 在 HDF5 下执行更慢”：GRIB 没有执行对应的属性 EAV JOIN / COUNT。两种 Adapter 将相似发现职责放在了不同层次。这是当前索引与一致性策略的差异，不是 HDF5 格式必然要求的 SQL 数量。

完整性检查目前用于严格、失败关闭地检测二级索引缺失和版本不一致，不能仅凭性能归因删除。特别是本轮没有恢复已回退的成功计数复用，也没有弱化权限或损坏检测。

### 取值分组不同

冻结 runner 的两个 HDF5 命中均为 `/data/u0000`，并非 dataset 名称不同。runner 按 `(dataset, path, version)` 分组，各发一个 Exact 查询；GRIB 将两个路径合为一个 Latest 查询，再校验结果的精确版本与冻结 oracle 一致。

这导致 HDF5 多一次注册和 catalog SQL，不是物理 chunk 布局强制要求。若要消除分组差异，需保持精确版本语义并单独确定请求契约；不能直接改用 Latest 并声称原实验不变。本次取值阶段约占 HDF5 总时间 4%–6%，该差异不是主要瓶颈。

## SQL 执行计划的额外工作

HDF5 二级索引采用 dataset entry 与规范化 attribute 表：

- entry 的 tags 为 `path, object_id`，版本放在 `time`；N=128、D=4 对应 512 个 dataset device。
- attribute 的 tags 再包含 `attribute_name`；本负载六个属性对应 3,072 个 attribute device。
- 完整性 COUNT 使用 `path IN (...)` 加 128 组精确 `(path,time)` OR 条件，按 `path,time` 聚合。
- 属性筛选生成一个 inode JOIN 和六个属性 LEFT JOIN，匹配 typed 值并排序。

既有 COUNT 的 EXPLAIN ANALYZE 确认分别枚举 512 / 3,072 个 DeviceEntry，各使用三个 Fragment Instance。属性计数计划的 Filter / Project CPU 明显高于 Scan；计划报告的 metadata Disk IO Time 为零。这里没有磁盘字节计数字段，不能将其表述为“零字节”。叶子片段 ready queue 较小，协调片段 blocked queue 主要等待下游计算，不能解读为网络断连或遗留查询。

这支持瓶颈集中在 **device/tag 组合处理、精确版本过滤、聚合及属性关系求值**。长 OR 条件是合理候选原因，但现有计划不能单独证明所有 CPU 都来自 OR；也不能仅凭六个逻辑 JOIN 断言物理扫描量增加六倍。现有计划没有完整分解属性筛选 JOIN 的各算子成本。

源码定位：

- [backend 目录重验与 marker / parsed cache](../../src/IoTDBStructuredBlobBackend.cpp:1307)
- [两类完整性 COUNT](../../src/IoTDBStructuredBlobBackend.cpp:1474)
- [属性筛选与最终复核](../../src/IoTDBStructuredBlobBackend.cpp:1500)
- [精确版本条件及 COUNT / JOIN 编译](../../include/iotdbfs/IoTDBPublishedCatalogSql.h:379)
- [GRIB 缓存索引上的 message 谓词](../../src/formats/grib/GribFormatQueryAdapter.cpp:720)

## 查询引擎、其他成本与计数限制

本负载读取完整命中单元，没有空间 slice。每个 HDF5 命中 dataset 为 `[1,1,64,64]`，chunk 为 `[1,1,32,32]`，gzip 1＋shuffle；GRIB 为完整 64×64、16-bit grid_simple message。K=2 均返回 8,192 个值。温热时两侧均不再读取 payload。当前证据不能判定真实空间子集或冷启动时 HDF5 chunk 剪枝、解压、数组组装是否劣于 GRIB；两侧冷路径编码也不同。

GRIB LogicalValues 的原生解码使用独立 GribDecodedFieldCache，不计入公共 runtime 的 `decode_count`。既有冷记录可出现 GRIB payload 读取而公共 decode_count 仍为零。因此不能以公共计数直接比较两种格式的全部解码工作。本次 GRIB 温热复用判断依据无新增 payload I/O，并结合 cache miss loader 才调用 `decode_native_field` 的代码路径；每次结果裁剪 / 输出复制仍需执行。

其他已确认但尚未证明为主因的工作：

| 项目 | 判断 / 边界 |
|---|---|
| SQL 编译器逐键复制 batch、反复构造完整语句 | 当前源码存在，随候选数增长增加本地工作；已有 profile 不支持其为主要 wall 来源 |
| backend `mutation_mu_` 覆盖完整性检查和筛选 SQL | 使该阶段串行，也可能阻塞并发请求；单客户端记录不能量化并发影响 |
| 注册、IPC、数组输出复制 | 仍有成本，但 HDF5 取值阶段比例较小，无法解释主要发现差距 |
| 公共 source catalog、父目录授权 | 两侧均有相同类别工作，既有阶段记录未显示其为主要 HDF5 特有差距 |
| 主机后台校验、合成任务、数据库物理状态 | 可改变绝对 wall，但没有暂停配对证据可量化净影响；不能覆盖稳定的 SQL 数量和路径差异 |

原生解码路径见 [GRIB cache / loader](../../src/formats/grib/GribFormatQueryAdapter.cpp:832)；chunk 组装见 [HDF5 fragment provider](../../src/formats/hdf5/Hdf5FragmentProvider.cpp:657)。

## 后续定位顺序

首先核对 HDF5 目录阶段的索引一致性职责与 GRIB 内存 message 索引职责，区分哪些检查确实必须每次 SQL 执行；其次针对现有完整性 COUNT 与属性 JOIN 检查候选剪枝、精确版本表达和聚合算子；再处理客户端编译及取值分组。需要始终保留严格模式和当前正确性边界。

暂不以重做 chunk / 派生布局、解码向量化或更换工作负载作为本次瓶颈解释。那些方向需要独立的冷路径或真实子集证据，不能由当前温热全单元结果推出收益。

## 证据与复现边界

- 生产源码：`36d1118fa48e3f25922ff28fae9eb38ef66625ed`。
- 冻结历史 runner SHA-256：`36d5517d587bd19265804484c7fa96af88dd9f07f464acca366a381a40fbaa99`。
- 参数：N=128、K=2、D=4、depth=2、seed=20261013、tree_width=4；每份完整报告每个格式 / 工作负载为三轮×五次，剔除每轮第一次后 12 个温热观察。
- 固定 A 不变：GRIB 480.361781 ms；HDF5 480.385748 ms。首次当前 B 的 HDF5 未通过结论不变。
- 固定复验 report SHA-256：`65f7ab0747949e40c31a8e7962f4f388c46e7f391f47ba9127c1021b886cbfb9`。
- 辅助当前 report SHA-256：`d52f3acd6c4fc4778e245e719a25ebed14f0cce83e90bd239e8eb22af15c8c9a`。

原始报告、冻结 runner、数据库计划位于 `experiments/multi_file/results/fixed-historical-reproduction-20261014/`，分别为 `report.json`、`diagnostic-current/report.json`、`small_runner.py`、`database-state/queries.json`。本次离线衍生比例及 SQL 分配保存在该目录的 `cost-structure-analysis.json`；未增加计时 runner。未完成的 `diagnostic-old/report.json` 不用于中位数分析，辅助历史二进制复验也不充当新的 A。
