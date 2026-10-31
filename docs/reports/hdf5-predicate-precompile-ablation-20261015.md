# HDF5 属性谓词预编译消融（2026-10-15）

## Material Passport

- 类型：代码优化与实验；状态：VERIFIED，限定于本文测试与 N=128 workload。
- 证据：完整客户端观测、逐请求 profile、源码／二进制／runner 身份和测试输出。
- 用户确定的最好值 A：**121.067539 ms**；本轮旧二进制复测不替代该值。

## 改动与语义边界

新增不可变、请求内复用的 `DatasetAttributePredicate`，一次编译属性字段、类型约束和表达式结构，将叶子绑定到整数 slot。每条记录在栈数组中绑定原始 typed 属性引用，不再构造 LogicalValue map、LogicalSchema 或重复解析字段名称。记录指针不离开求值调用，计划可复用于不同值、实际类型及缺失状态的记录。通用请求边界验证仍保留；“一次编译”指执行阶段计划，不是删除请求入口的语法检查。

生产接线只改变 `substructure-index`：先检查并求值全部候选，再输出匹配。临时位图为每个目录记录保存一位，保留原目录检查顺序与 object ID 输出顺序。AND/OR 短路或输出上限不能隐藏不可读属性与类型错误。NULL 三值逻辑、IN、精确 UInt64 比较、不隐式转换类型及复杂度限制保持不变。strict 使用原 schema／求值器，作为独立差分参照。

三个生产文件为 `include/iotdbfs/query/core/DatasetCatalogQuery.h`、`src/query/core/DatasetCatalogQuery.cpp`、`src/IoTDBStructuredBlobBackend.cpp`；另有两个测试文件。本轮不修改 SQL、授权、发布封印、layout、payload、缓存预算或数据布局。

## 固定口径与采集修复

沿用[上一轮 N=128 实验](hdf5-substructure-index-n128-20261015.md)的 N=128、K=2、D=4、depth=2、seed=20261013、tree_width=4，复用同一批带封印精确版本、原始文件、FS 文件和 SQLite 索引，没有重新生成、发布或构建索引。三组仍为 FS 原生扫描、FS 持久索引和 IoTDB-FS，各含发现、发现＋完整取值。每个二进制 3 campaigns × 5 repeats，共 180 条观测；排除每轮 repeat 0，每格使用全部 12 个温热观测的中位数。

主指标为原客户端 `total_ns`，包含注册、发现、完整取值、mmap/IPC 与 owned copy；oracle/hash/profile 收集在时钟外。两组均启用 substructure-index，采用相同配置和 writer lock；GRIB 实现未改动。

首次旧二进制运行因异步 profile 尚未落盘停止，只留下 63/180 条观测；服务端随后写出的相关 profile 均成功。该不完整尝试保留在 `A-current-results/`，不计算正式中位数，B 当时未开始。

用户随后明确批准：仅应用仓库已有的、**停止计时后有界等待 profile** 的修复，对旧、新二进制统一完整重跑。原冻结副本未改写。AST 核验确认所有其他函数、`profile_call` 计时区域及采集后的返回逻辑一致；其余 66 个依赖文件未变。等待不重发查询，不加入 `total_ns`，但可能改变少量请求间隔；不能声称采集程序仍字节相同。

| 身份 | SHA-256 |
|---|---|
| 原冻结 runner | `36d5517d587bd19265804484c7fa96af88dd9f07f464acca366a381a40fbaa99` |
| 批准的采集修复 runner | `be728432fc2b01f810af167a5d07308211a50cf980a478a0dfcf4e229ce65155` |
| A-current 旧 daemon | `17db8db693156438c1d84327a03d3fa62c5d5e157641d4b3b4e0a52e42b20bb5` |
| B-compiled 新 daemon | `15c51d0904adfc0192c8c4629b3d2de98512b55976a17c5fd171fe1e87053132` |

隔离构建的 460 个源码文件全部校验；相对基线只改变上述三个生产文件和两个测试文件。原 native reader、HDF5 1.14.4、ecCodes 2.34.1 及查询参数保持不变。14 次完整实验挂载的真实二进制和配置另存 receipt，不将启动脚本哈希冒充 daemon 哈希。

## 主结果与 GRIB 控制

下表为“发现＋完整命中单元取值”的客户端温热中位数。比值统一使用两种格式中位数相除。

| 组别 | HDF5 | GRIB | HDF5 / GRIB |
|---|---:|---:|---:|
| 固定最好值来源，上一轮 | **121.067539 ms** | 48.182437 ms | 2.5127× |
| A-current，本轮旧二进制 | 150.059525 ms | 61.013365 ms | 2.4595× |
| B-compiled，本轮预编译 | **118.981456 ms** | 57.712605 ms | **2.0616×** |

- B 比固定最好值低 **2.086083 ms / 1.72%**，数值上通过本轮最好值门槛。
- 同环境旧／新对照中，HDF5 降低 **20.71%**，GRIB 降低 **5.41%**。
- 同环境 HDF5/GRIB 比值降低 **16.18%**；相对上一轮比值降低 **17.95%**。

本轮旧二进制相对上一轮的 HDF5、GRIB 均变慢约 24%–27%，说明跨轮有明显环境波动。因此不能将 1.72% 的小差值单独视为稳定或统计显著收益；更有针对性的证据是同环境比值、减少的重复工作及阶段 profile。没有将 150.06 ms 重新定义为最好值，也未根据慢样本重跑择优。

| campaign | HDF5 A-current | HDF5 B-compiled | GRIB A-current | GRIB B-compiled | 比值 A → B |
|---|---:|---:|---:|---:|---:|
| 0 | 156.32 ms | 111.38 ms | 58.38 ms | 60.02 ms | 2.68 → 1.86× |
| 1 | 151.12 ms | 118.98 ms | 63.06 ms | 56.57 ms | 2.40 → 2.10× |
| 2 | 141.48 ms | 120.70 ms | 62.70 ms | 61.11 ms | 2.26 → 1.98× |

B 的 HDF5 温热完整查询范围为 101.16–139.72 ms。按 `(campaign, repeat)` 配对的 HDF5/GRIB 比值为 1.14–2.41×，中位数 2.0462×；它不替代主表的 2.0616×。A、B 中 GRIB 超过 100 ms 的慢观测均保留，没有筛除异常值。

仅属性发现的客户端中位数：HDF5 120.59 → 92.39 ms，GRIB 48.73 → 49.30 ms；比值从 2.47 降至 1.87×。HDF5 完整查询中，两次取值调用合计中位数为 19.60 → 18.90 ms，主要收益在发现阶段。

## 阶段归因与检查没有减少的证据

下表来自仅属性发现的温热请求，均为服务端阶段中位数。阶段有嵌套，不得相加为端到端总时间。

| 阶段 | A-current | B-compiled |
|---|---:|---:|
| 发现服务端总时间 | 119.77 ms | 91.62 ms |
| 目录准备 | 42.48 ms | 32.10 ms |
| 属性谓词阶段 | **17.66 ms** | **0.34 ms** |
| 精确 manifest 重验 | 28.39 ms | 29.24 ms |
| marker 读取 | 31.35 ms | 31.52 ms |
| published catalog 扫描 | 19.95 ms | 19.77 ms |

原准备中的 schema 检查现在归入预编译求值阶段。因此另按请求先将“目录准备＋谓词”两段求和，再取中位数，防止仅凭阶段迁移判断收益：仅发现为 **59.90 → 32.41 ms**；完整查询中的发现调用为 **62.47 → 30.88 ms**。新谓词阶段包含编译、全部候选绑定／类型检查／求值与结果输出，没有只计更小范围。

两组温热检查均满足：

- 选中 128 源、检查 512 个候选、返回 2 命中；逻辑目录字节为 227,072。
- 128 个目录缓存全部命中，没有目录对象重读、COUNT 或属性筛选 SQL。
- 实际 SDK SQL：仅发现 4 次、发现＋完整取值 6 次，没有减少授权或重验。
- 完整 HDF5 查询均为 8 个 decoded fragment 命中、32,768 B 值复制，没有新增 payload/layout I/O 或解码。
- B 每次发现恰好 `predicate_compiles=1`、`predicate_bindings=512`、`predicate_evaluations=512`，没有跳过后续候选。

## 验证与适用范围

本地 43 项聚焦测试及同范围 ASan/UBSan 通过；包含 5 项新增测试，覆盖多类型、缺失／不可读记录与原求值器的差分比较、计划复用、错误优先级、短路检查及复杂度限制。服务器按完整相关测试套件展开后共 **594 项通过**。模块依赖边界和 `git diff --check` 通过。两份正式报告各 180 条观测全部通过原 oracle，哈希和各格中位数另行重算核验。

计时结束后运行真实 IoTDB `dataset_catalog_probe`，**1,777 项断言通过**，包含新增的每请求一次编译／两记录绑定计数检查，以及 strict 对照、129 源批量边界、类型／缺失／不可读属性、授权、损坏和限额用例。该功能探针不作为性能数据；使用实验库内独立临时前缀，`cleanup=true`，自身 fixture 已清理。全部 14 个正式实验临时挂载已卸载，未替换长期挂载或重启数据库。

本轮没有实现 manifest＋marker 合并、跨文件 SQL 改写或持久倒排索引。谓词阶段已小于 1 ms，剩余成本更集中在 catalog、manifest、marker 与授权。HDF5 仍未超过本轮 FS 对照：完整取值的 FS 原生扫描为 43.85 → 41.05 ms，FS 持久索引为 0.899 → 0.852 ms；GRIB 对应为 359.44 → 376.32 ms、1.231 → 1.237 ms。

旧、新组顺序执行，服务器共享、非独占；GRIB 控制和阶段归因支持优化存在，但不能消除所有时间变化，没有进行统计显著性检验。没有更换数据集或 workload。

服务器证据目录：

```text
/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/multi_file/work/hdf5-predicate-compiled-20261015/
```

`A-current-complete-results/report.json`、`B-compiled-complete-results/report.json` 是正式报告；首次不完整尝试在 `A-current-results/`。`contract.json` 保存固定最好值与真实二进制；`runner-verification.json`、`approved-runner.patch` 保存采集修复与用户批准边界；`source-hashes.json`、`source-verification.json`、`execution-receipt.json` 保存构建及启动身份。本地原始结果副本在 `experiments/multi_file/results/hdf5-predicate-compiled-20261015/`，按现有策略忽略 git。

`unit-tests.xml`、本地 sanitizer XML 与 `functional-probe.log` 保存测试证据。功能探针内部 `sql_queries` 不包含其无 SQL 授权回调，不能将其温热 3 次与正式 FUSE 请求的总 SQL 6 次混用。
