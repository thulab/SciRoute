# HDF5 子结构索引：冻结 N=128 查询实验

后续仅针对重复谓词准备的优化与 GRIB 控制结果，见
[预编译属性谓词消融](hdf5-predicate-precompile-ablation-20261015.md)。本报告数值
保留为其固定最好值来源，不以之后的临时旧二进制复测替换。

## Material Passport

- 类型：代码实验；执行及核验日期：2026-10-15。
- 状态：VERIFIED，限定于本报告的冻结 workload、正确性 oracle 和计时口径。
- 证据：服务器原始报告、逐请求 profile、发布 profile、配置和二进制哈希；数值来自实际运行。
- 研究问题：发布封印的 dataset 子结构索引是否增加成本，以及 HDF5 与 GRIB 的查询时间差距能否缩至 4× 内。

## 结论与固定 A

本轮达到 **4× 内**的目标。温热“发现＋完整命中单元取值”客户端中位数为
HDF5 **121.07 ms**、同轮 GRIB **48.18 ms**，比值 **2.51×**。
相同当前二进制、相同精确版本的 strict 对照为 540.82 ms，切换索引后降低
77.6%（4.47× 加速）。strict 是模式消融对照，不替代历史 A。

| 格式 | 固定历史 A：客户端温热中位数 | 本轮索引模式 B | B / A | 历史 A 验收 |
|---|---:|---:|---:|---|
| HDF5 | 480.385748 ms | 121.067539 ms | 0.2520 | 通过，降低 74.8% |
| GRIB | 480.361781 ms | 48.182437 ms | 0.1003 | 通过 |

GRIB 的历史 A 来自较早实现，不能将其与本轮 HDF5 相除来判定 4×。
上述 2.51× 使用**本轮同时期、同模式运行中的 GRIB**。HDF5 索引配置不改变
GRIB 的实现；本轮 strict/index 两次运行的 GRIB 中位数仅相差约 1.2%，
不能把 GRIB 相对历史 A 的改善归功于 HDF5 索引。

## 实验边界与身份

沿用原 `small_runner.py`，SHA-256 为
`36d5517d587bd19265804484c7fa96af88dd9f07f464acca366a381a40fbaa99`。
67 个冻结依赖文件全部哈希匹配，没有修改 runner、native reader、谓词、查询
分组、几何、缓存预算或在线计时范围，也没有重新生成数据。

| 控制项 | 本轮设置 |
|---|---|
| 数据 | 原有受控 HDF5 / GRIB，N=128、K=2、D=4、depth=2、seed=20261013、tree_width=4 |
| HDF5 | 每 dataset `[1,1,64,64]`，chunk `[1,1,32,32]`，gzip level 1＋shuffle；原始总字节 10,889,216 |
| GRIB | 每 message 完整 64×64 网格，grid_simple 16 bit；原始总字节 4,285,952 |
| 取值 | K=2，共 8,192 个 Float32 输出值；不增加空间切片 |
| 组别 | FS 原生扫描、FS SQLite 持久索引、IoTDB-FS；各含属性发现、发现＋完整取值 |
| 重复 | 每模式 3 campaigns × 5 repeats，共 180 条观测；排除每轮 repeat 0，每格 12 个温热观测 |
| 在线计时 | 原客户端 `total_ns`：注册、发现、取值、mmap/IPC 和 owned array copy；oracle/hash/profile 收集在时钟外 |
| 原生依赖 | HDF5 1.14.4、ecCodes 2.34.1；动态插件关闭 |
| 数据库 | 实验实例 `127.0.0.1:33667`，`h09probe1790939889dbfc1f`；新独立路径前缀 |

无封印旧版本不能启用新模式，因此以相同原始字节重新发布新版本；未修改原历史
版本。strict/index 查询**同一批新发布精确版本**。FS 文件也按原始哈希校验，
其持久索引通过原 native reader 在新位置重建：HDF5 67.54 ms、GRIB 400.14 ms，
均在在线计时之外。

冻结 runner 没有模式参数。本轮用启动脚本在 exec daemon 前向原配置追加唯一一项
`query.hdf5.catalog_mode`，分别为 `strict` 和 `substructure-index`；启动不计入
查询时钟。两组执行同一真实二进制，SHA-256 为
`17db8db693156438c1d84327a03d3fa62c5d5e157641d4b3b4e0a52e42b20bb5`。
runner 报告中的 daemon hash 是该启动脚本的身份，真实二进制身份另存于 environment、
contract 和每次 launch receipt，不能混读。14 次完整查询挂载的实际配置和二进制
哈希均已核验。

保留原分组差异：HDF5 的两个命中 dataset 使用两个 Exact 取值注册；GRIB 使用
一个包含两条源路径的 Latest 取值注册并核对输出精确版本。这是冻结 runner 的
既有口径，不声称它是格式的必然要求。

## 查询结果

以下全部是温热**客户端**中位数。SQL 数来自 SDK 总计数，包含实际授权查询，
没有用不含授权的 `DatasetCatalogResult.sql_queries` 替代。

| 模式 | 负载 | HDF5 | GRIB | HDF5 / GRIB | HDF5 / GRIB SQL 次数 |
|---|---|---:|---:|---:|---:|
| strict | 仅属性发现 | 500.23 ms | 43.44 ms | 11.51× | 7 / 2 |
| strict | 发现＋完整取值 | 540.82 ms | 48.77 ms | 11.09× | 9 / 3 |
| substructure-index | 仅属性发现 | 102.34 ms | 41.76 ms | 2.45× | 4 / 2 |
| substructure-index | 发现＋完整取值 | 121.07 ms | 48.18 ms | **2.51×** | **6 / 3** |

三轮各自的温热“发现＋完整取值”中位数如下，未择优选择轮次：

| 模式 | campaign | HDF5 | GRIB | 比值 |
|---|---:|---:|---:|---:|
| strict | 0 | 536.33 ms | 49.32 ms | 10.87× |
| strict | 1 | 527.65 ms | 49.03 ms | 10.76× |
| strict | 2 | 572.98 ms | 48.29 ms | 11.87× |
| substructure-index | 0 | 122.03 ms | 48.18 ms | 2.53× |
| substructure-index | 1 | 121.47 ms | 49.31 ms | 2.46× |
| substructure-index | 2 | 116.93 ms | 47.06 ms | 2.48× |

索引组 HDF5 的 12 个温热完整查询在 110.56–134.96 ms 之间；按相同
`(campaign, repeat)` 配对的 HDF5/GRIB 比值为 2.24–2.80×，全部低于 4×。
这是描述性结果，没有进行显著性检验或声称对其他规模的统计保证。

相同索引运行中的 FS 对照仍保留如下，不能据格式间差距缩小而宣称已超过 FS：

| 发现＋完整取值 | FS 原生扫描 | FS 等价持久索引 | IoTDB-FS |
|---|---:|---:|---:|
| HDF5 | 35.52 ms | 0.652 ms | 121.07 ms |
| GRIB | 332.78 ms | 1.028 ms | 48.18 ms |

FS 表中保留原 native 时钟，worker 往返另外记录；FS 索引构建成本不在该表内。
这些结果与原 runner 的 FS/IoTDB 计时边界一致，没有人为禁用 OS 缓存。

## 改善来自哪里，是否增加了成本

温热发现中，128 个源目录全部缓存命中，遍历 512 条 dataset 记录，返回 2 条
命中；目录逻辑字节为 227,072。新模式没有完整性 COUNT、属性筛选 SQL 或目录
对象重读，payload/layout 读取均为零。strict 相同请求仍有两次 COUNT 和一次
属性筛选 SQL，因此总 SQL 从 7 降为 4；完整取值从 9 降为 6。

仅属性发现的服务器阶段中位数如下。阶段存在嵌套，**不得相加**为总时间：

| 阶段 | strict | substructure-index |
|---|---:|---:|
| 发现服务端总时间 | 499.45 ms | 101.54 ms |
| 二级索引完整性检查 | 245.34 ms | 不执行 |
| SQL 属性筛选 | 134.67 ms | 不执行 |
| C++ 子结构属性谓词 | — | 14.94 ms |
| 精确 manifest 批量重验 | 27.75 ms | 27.45 ms |
| 目录 marker 批量读取 | 29.32 ms | 29.14 ms |
| 目录准备 | 37.63 ms | 37.40 ms |

“发现＋完整取值”请求中，HDF5 两次取值调用合计客户端时间的中位数为
16.03 → 15.87 ms，基本保持；主要变化在发现链路。完整查询的 SDK query-submit
wall 中位数由 483.19 降为 77.28 ms，它包含客户端等待等开销，不等同于数据库 CPU。
温热完整查询两组都没有新增 payload/layout I/O，不能把本轮收益解释成 chunk 解码优化。

索引不是零成本：

1. **发布前验证增加成本。** 相同语料以旧二进制和当前二进制依次发布一次：

   | 完整发布 | 旧版 | 当前版 | 增量 |
   |---|---:|---:|---:|
   | HDF5 128 文件 | 55.45 s | 60.52 s | +5.07 s，+9.2% |
   | GRIB 128 文件 | 39.02 s | 43.30 s | +4.28 s，+11.0% |

   HDF5 `content_commit/publish` 每文件查询由 4 增为 8，核验封印及完整 dataset /
   attribute 行。包含空文件创建和内容提交的全部发布 profile 中，HDF5 查询总数
   为 3,072 → 4,352，query-submit wall 为 8.76 → 13.07 s。当前更改还包含通用
   生命周期保护和稀疏 typed 列修复，GRIB 也受部分保护影响；加上只有一轮且旧版先执行，
   不能把整个 5.07 s 归因于 SHA 或 HDF5 索引，也不能将这组发布实验与历史 GEOS
   136.751 s 基线混用。

2. **冷目录需要验证。** 每个新 daemon 的第一次 HDF5 属性发现均验证 128 个封印。
   三次客户端时间为 175.35 / 165.83 / 161.16 ms，中位数 165.83 ms；strict 为
   508.60 / 542.95 / 543.11 ms，中位数 542.95 ms。冷发现 SQL 为 5 对 8，
   并未为封印增加额外逐源 SQL。冷暖差还包含对象读取和解析，不能作为独立 SHA 耗时。
   这里“冷”只表示新 daemon 的目录缓存，不表示冷磁盘。

3. **温热仍有索引维护边界。** 新模式仍读源 catalog、精确 manifest、marker，并
   检查父目录授权；温热发现四次 SQL 中，三次属于目录路径、一次属于父目录授权。
   另有约 14.94 ms 的内存属性筛选。没有绕过权限、版本身份或预算来取得本轮数字。

新增持久数据主要是 manifest 中的发布封印；本轮复用已有 dataset catalog，
没有复制 payload 或新建 step-major/slab 派生数据。默认配置仍为 strict；性能
收益需要显式启用 substructure-index，且所有参与源必须具备有效封印。

## 与 GRIB 的子结构思想是否相似

两者现在都将文件内可查询单元的身份和属性前置到摄取／发布阶段，绑定精确源版本，
查询时复用已解析索引、在内存筛选少数子结构，再通过 Adapter 读取命中数据。
本轮从约 11× 降至 2.5×，支持此前的主要差距来自逐查询二级表完整性检查和
属性 SQL 执行，不能据此认定 HDF5 的子数组引擎必然慢一个数量级。

单元仍有区别：GRIB message 通常是可独立解码的编码块；此处 HDF5 单元是 dataset
对象，取值还依赖 layout、datatype/filter 和多个 chunk。该 workload 每文件四个
dataset、每个命中文件一个完整命中 dataset；它没有验证单 dataset 内的压力层／
空间块选择性，也未验证真实大规模气象文件、其他 N/D 或冷 I/O 下的普适收益。

## 运行记录和复核入口

运行过程保留了两次准备／检查失败：第二组 FS 目标目录复用被 `checked_copy`
拒绝，以及复制的 SQLite 索引因原绝对根路径被 native reader 拒绝。前者未开始
查询，后者未产生 IoTDB 查询样本；改为独立 FS 目录并通过原 reader 重建索引。
失败日志和空／不完整报告均保留，没有修改 runner、删除慢观测或重复发布择优。
最终两份报告各 180 条观测全部通过正确性 oracle。

strict 先于 index 执行，模式顺序没有随机化；每模式内部保留原三组轮转顺序。
服务器仍有其他任务，本轮不声称独占主机或冷存储性能。两组 GRIB 控制相近，
但不能消除所有运行顺序和共享环境因素。实验结束后，14 个完整查询临时挂载均已
卸载，自己的 daemon 已退出；未重启数据库，也未停止长期基线挂载。

服务器原始证据：

```text
/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/multi_file/work/hdf5-substructure-n128-20261015/
```

本地副本位于 `experiments/multi_file/results/hdf5-substructure-n128-20261015/`
（按现有策略忽略 git）。`contract.json`、`frozen-dependencies-verification.json`、
`execution-receipt.json` 保存身份；两份 `*-complete-results/report.json` 保存完整
观测及 manifest；`analysis.json` 保存聚合及发布阶段计数；`publication.json` 与
原始 publication JSONL 保存发布成本。启动／准备脚本和原始失败记录保留在服务器。
实现契约见 [HDF5 dataset 子结构索引](../contracts/hdf5-dataset-substructure-index.md)。
