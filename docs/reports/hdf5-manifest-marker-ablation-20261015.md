# HDF5 精确 manifest 与 marker 合并读取消融（2026-10-15）

## Material Passport

- 类型：代码优化与固定 workload 消融；状态：VERIFIED，限定于本文环境、测试与 N=128 负载。
- 固定最好值 A：**118.981456 ms**，来自[预编译谓词实验](hdf5-predicate-precompile-ablation-20261015.md)。本轮旧二进制复测不替代该门槛。
- 结论：B 为 **106.811351 ms**，通过固定最好值门槛，保留实现；HDF5/GRIB 比值降至 **1.8607×**。这是描述性实测结果，不作统计显著性或所有负载的保证。

## 实现与检查边界

只对显式 `substructure-index` 模式，将授权后的精确 manifest 重验和 dataset catalog marker 获取合为一次带标签的 `UNION ALL` SQL 提交。两个分支各自具有有限 `path IN`、精确 `(path,time)` 对和 `LIMIT expected+1`，不通过笛卡尔 path/version 组合或 JOIN 展开返回结果。每批最多 128 源，整条 SQL 最多 64 KiB，输入仍受 65,536 键上限约束；按长度增量编译，完成一批才构造完整 SQL。

所有冻结源均重验 manifest，包括时间未命中的源；只有时间候选进入 marker 分支。没有 marker 候选时只执行 manifest 分支。逻辑检查顺序仍为授权、精确 manifest、全部源的封印、时间证据、marker、目录／谓词。marker 错误先暂存，不能盖过 source stale、缺失封印或时间证据错误；没有通过检查的行不会进入解析目录缓存或结果。合并 SQL 执行失败不能建立 freshness，返回 StalePlan。

复用既有 manifest 与 marker 校验器，继续检查精确版本、revision、format、payload/layout sizes、发布阶段、marker 协议／digest／完整性／记录数、缺失与重复行。授权、逻辑目录字节预算、解析缓存身份、发布封印和返回值契约保留。默认 `strict` 仍使用原来的两个读取与二级索引审计，作为独立功能对照。布局与持久协议没有升级。

生产改动位于：

- `include/iotdbfs/IoTDBPublishedCatalogSql.h`：共享键验证与合并 SQL 编译器。
- `include/iotdbfs/IoTDBStructuredBlobBackend.h`：私有验证结果及延迟 marker 错误。
- `src/IoTDBStructuredBlobBackend.cpp`：组合行解析、freshness 校验、时间候选投影与模式接线。

## 固定实验口径

沿用原多文件 runner、原 N=128/K=2/D=4/depth=2、seed=20261013、tree_width=4；HDF5 为 Float32、shape `[1,1,64,64]`、chunk `[1,1,32,32]`、gzip 1＋shuffle；GRIB 为 64×64、grid_simple/16 bit。复用同一批发布封印的精确版本、原始文件及 FS SQLite 索引，没有重新生成、发布或构建索引。

每个真实二进制均跑 3 campaigns × 5 repeats、三组 × 两阶段 × 两格式，共 **180 条完整观测**；每格按原规则排除 repeat 0，取全部 12 个温热观测中位数。主指标是 IoTDB-FS 客户端 `total_ns`，包括注册、发现、匹配单元完整取值、IPC/mmap 和 owned copy。oracle/hash/profile 收集在时钟外。仍采用已获用户批准的、计时结束后有界等待 profile 的 runner，SHA-256 为：

```text
be728432fc2b01f810af167a5d07308211a50cf980a478a0dfcf4e229ce65155
```

原 native reader SHA-256 为 `49f7f7127e6a7c73bab304f9ec230f46e540ebf26b3d3e40fed05b3a6e398864`。HDF5 1.14.4、ecCodes 2.34.1、独立实验 IoTDB 端口 33667 和原 writer lock 均沿用既有配置。HDF5 继续为两个命中 dataset 分别发起精确取值，GRIB 继续用一个请求读取两个命中源；不通过改变这项原有差异改善结果。

| 二进制 | SHA-256 |
|---|---|
| A-current，原预编译谓词实现 | `15c51d0904adfc0192c8c4629b3d2de98512b55976a17c5fd171fe1e87053132` |
| B-combined，合并验证读取 | `e1460750fbee36070786801355d8e241137a69a239fa2940f34ede90f12e7db1` |

隔离源码快照 460 文件逐项核验，相对 A 只改变三个生产文件和两个测试文件。14 次正式挂载均核对真实 daemon 路径、二进制哈希和 substructure-index 配置；launcher 哈希与真实二进制哈希分别记录。GRIB 源码未改动。

## 主结果与 GRIB 控制

“发现＋完整命中单元取值”的客户端温热中位数：

| 组别 | HDF5 | GRIB | HDF5 / GRIB |
|---|---:|---:|---:|
| 固定最好值来源，上轮 | **118.981456 ms** | 57.712605 ms | 2.0616× |
| A-current，本轮旧二进制 | 117.256772 ms | 57.200012 ms | 2.0499× |
| B-combined，本轮合并读取 | **106.811351 ms** | 57.404444 ms | **1.8607×** |

B 比固定最好值低 **12.170105 ms / 10.23%**，比本轮旧二进制低 **10.445421 ms / 8.91%**。同期 GRIB 增加 **0.36%**；HDF5/GRIB 比值相对同期 A 降低 **9.23%**，相对固定最好值来源降低 **9.75%**。保留本轮改动，106.811351 ms 可作为后续同口径的最好实测值；不能把它作为变化环境中的性能保证。

| campaign | HDF5 A-current | HDF5 B-combined | GRIB A-current | GRIB B-combined |
|---|---:|---:|---:|---:|
| 0 | 102.03 ms | 107.64 ms | 54.38 ms | 57.41 ms |
| 1 | 127.36 ms | 111.42 ms | 56.22 ms | 55.62 ms |
| 2 | 116.97 ms | 98.42 ms | 58.38 ms | 57.40 ms |

第 0 轮的 HDF5 B 比 A 慢；没有声称每轮均改善，也没有删除该轮或重跑择优。A 的完整查询温热范围为 91.59–150.17 ms，B 为 96.66–119.62 ms。按 `(campaign,repeat)` 配对的比值中位数为 2.1092 → 1.9048×，与主表“两个中位数相除”属于不同统计量。

仅属性发现的客户端温热中位数为 HDF5 **97.913094 → 86.155797 ms**，GRIB **47.559405 → 48.011077 ms**，比值 **2.0588 → 1.7945×**。两次 HDF5 取值调用合计中位数为 **18.56 → 18.01 ms**，收益主要发生在发现路径。

FS 原生扫描和持久索引两组完整保留于原始报告，没有纳入本轮最好值门槛。冻结 runner 的完整取值 `total_ns` 中位数分别为：HDF5 扫描 40.92 → 40.29 ms、索引 0.923 → 0.779 ms；GRIB 扫描 341.79 → 351.20 ms、索引 1.140 → 1.115 ms。FS 这些值是原 native reader 的计时区，另有 `worker_client_ns`；不把它们冒充与 IoTDB socket/IPC 完全相同的入口成本。结果没有证明 HDF5 IoTDB-FS 超过 FS 原生索引。

## SQL 与阶段归因

| 实际 SDK SQL 次数，温热 | A-current | B-combined |
|---|---:|---:|
| HDF5 仅属性发现，含授权 | 4 | **3** |
| HDF5 发现＋完整取值，含授权 | 6 | **5** |
| GRIB 仅属性发现 | 2 | 2 |
| GRIB 发现＋完整取值 | 3 | 3 |

两组所有温热 HDF5 发现均选中 128 源、检查 512 个候选、输出 2 命中；目录逻辑字节都是 227,072，128 个解析目录缓存全命中，没有目录对象、layout 或 payload 重读。每次仍编译一次谓词、绑定并求值 512 条记录；源权限证据复用 128 项，上级目录检查仍覆盖 24 条路径。B 的一次 combined 调用实际返回 **128 manifest 行＋128 marker 行**，不是跳过其中一类检查。

仅属性发现的服务端阶段如下，均为每请求阶段中位数：

| 阶段 | A-current | B-combined |
|---|---:|---:|
| 发现服务端总时间 | 97.09 ms | 85.31 ms |
| published catalog scan | 19.25 ms | 20.20 ms |
| manifest 与 marker 读取合计 | **62.69 ms** | **54.25 ms** |
| 属性谓词 | 0.308 ms | 0.314 ms |

A 的合计是先按请求相加两段、再取中位数，未直接相加两个中位数。完整查询中的发现调用，相同合计为 **63.95 → 54.41 ms**。新 timer 覆盖 SQL 编译、执行、行解析和 manifest 校验；marker 的后续校验与目录准备仍计入总发现时间。

原 `dataset_catalog_prepare` 包含 marker SQL，B 的合并读取移到了该 timer 之前；因此 prepare 的 **30.85 → 1.25 ms** 不能独立解释为额外优化。阶段互相嵌套，不能累加它们推导端到端结果。两个版本的完整取值均为 8 个 decoded fragment 命中、32,768 B 复制、0 次新解码，没有用更多数据缓存换取本轮收益。

## 验证、失败记录与运行边界

- 本地 66 项相关测试及同范围 ASan/UBSan 通过；新增 4 项 SQL 编译测试覆盖精确键、独立哨兵、128/129、转义长路径、64 KiB、无 marker、非法输入和子集约束。
- 服务器最终候选 **604 项单元测试通过**；真实 IoTDB 功能探针 **1,800 项断言通过**，`cleanup=true`。strict 对照、129 源、时间剪枝、缺失／损坏 marker、封印、授权、预算、缓存隔离、精确版本和删除后 stale 均验收。
- 两份正式报告各 180 条观测全部通过冻结 oracle；原始数据、源哈希、真实二进制 receipt 和每格中位数另行核算。模块依赖检查与 `git diff --check` 通过。
- 本轮 14 个临时挂载均已卸载；既有三处长期挂载及实验 IoTDB 的 DN/CN 进程仍在，没有重启数据库。

正式计时之前，首次编排器使用系统 Python，因缺少 PyArrow 在导入 runner 时退出；观测为 0、实验挂载为 0。失败记录保留。用户明确批准后，改用既有 venv 启动编排器，没有修改 runner 或计时区。

首次新增功能探针还发现：此服务器将单分支 `CAST(0 AS INT32)` 常量投影返回为 SDK INT64，而 UNION 返回 INT32，导致时间全部未命中时误报 StalePlan。已将两分支内部标签统一为 INT64，并分别通过最小只读 SDK 检查与完整探针；失败测试和诊断日志仍保留。两个问题均在正式 A/B 观测开始之前处理，没有混用不完整或旧候选的计时。

本轮为单机共享服务器、旧／新顺序运行的 warm N=128 消融，不是独占环境或随机化交叉试验。GRIB 稳定和减少的真实 SQL 支持该优化的针对性，但不足以消除所有环境影响；没有进行显著性检验。没有在本轮测量发布成本、cold-I/O 收益或更大 N 的扩展性。新模式要求表模型引擎支持这里使用的有界 UNION ALL／INT64 投影；本轮在现有 TimechoDB 2.0.11-SNAPSHOT 环境验证，strict 路径独立保留。

服务器证据目录：

```text
/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/multi_file/work/hdf5-manifest-marker-20261015/
```

`A-current-complete-results/report.json`、`B-combined-complete-results/report.json` 为正式报告；`contract.json` 固定最好值与真实二进制；`analysis.json`、`execution-receipt.json`、`source-hashes.json`、`unit-tests.xml` 和 `functional-probe.log` 保存核验证据。`startup-import-failure*`、`startup-retry-approval.json`、`int32-tag-attempt-*` 与 `diagnostic-int64.log` 保留前置失败、用户确认与修复过程。原始结果包 SHA-256 为 `c5d4ffc1d3ce973f579c96f2bf3340952a80f426ebc227e49536ccb893a45774`。

本地副本位于被忽略的 `experiments/multi_file/results/hdf5-manifest-marker-20261015/`；Git 只保留本手工报告、契约更新及代码／测试。
