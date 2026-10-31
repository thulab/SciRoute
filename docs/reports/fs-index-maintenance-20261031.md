# FS-index 增量维护实现与 M0 验收

日期：2026-10-31。结论：**P0–P3 已实现，M0 功能门禁通过。** 本轮没有执行 M1–M3 性能实验，也没有更改论文的历史数值或现有数据库服务。

## 交付能力

- `fs_index/maintenance.py` 提供 Writer/Reader；`include/fs_index.h` 提供 v2 C ABI。
- SQLite schema v2 保存完整 source 身份、稳定 source ID、科学 units、latest、head 与幂等 operation。打开维护目录不再依赖外部 catalog JSON。
- 单 writer 独占锁；新增不可变版本执行复制/hash、文件同步、无覆盖命名、目录同步、单事务发布。版本前置条件与 operation 请求身份均验证。
- 新发布只提取新增文件，不重新提取旧源，不替换整个 SQLite 文件。indexed 发布还验证每个单元可由当前 profile 读取，完整成本记入发布。
- Reader 在每次选择时按稳定 ID 增量刷新来源映射；index 选择与刷新共用短目录快照。scan 在绑定源后释放目录读事务。旧 locator 保持精确来源，不切换到新版本。
- `indexed=False` 提供 native FS 的 source/head 发布目录；不提前构建 unit 索引，支持后续三路径生命周期对照。
- writer 重开清理 staging，保留并报告 orphan；重试可核验并复用未登记文件；确认丢失不重复发布。
- `status()`、发布 metrics 与 `checkpoint()` 提供阶段成本、空间、刷新和 WAL 收尾计量。并发 reader 空间统计明确标为非原子近似值。

新维护实现 include 到原翻译单元，共用原 inventory/select/read；原 v1 函数代码和 `native.py` 保留，原七项功能断言继续执行。新库具有独立摘要，不能替代旧实验冻结二进制的身份。

接口与边界见 [组件说明](../../fs_index/README.md) 和 [v2 契约](../contracts/fs-index-maintenance-v2.md)；设计与后续实验见 [维护计划](../deliverables/fs-index-incremental-maintenance-plan.md)。

## 最终验证

最终验证目录：

```text
root@192.168.99.49
/data_01/zzy/iotdb-fs-exp/fs-index-maintenance-20261031/run-002
```

| 项目 | 结果 |
| --- | --- |
| 原生旧接口回归 | 7/7 通过，无跳过 |
| 维护 M0 | 13/13 通过，无跳过 |
| 发布故障 | GRIB/HDF5 × file_sync、rename、directory_sync、before_commit、after_commit，共 10 个 SIGKILL 子场景通过 |
| 崩溃后首次访问 | Reader 先打开并读取合法提交状态，随后 Writer 恢复/重试，通过 |
| 并发发布与查询 | 两格式 × index/scan 共 4 个独立进程子场景通过；含 retained reader、旧 locator 与并发 status |
| 增量门禁 | 新增文件提取数为 1，旧 unit 行不变，SQLite inode 不变，查询继续使用 predicate 索引 |
| 幂等与冲突 | 相同请求重放稳定结果，异内容/版本请求冲突，expected_version 与 ordinal 约束有效 |
| 异常恢复 | SQL 事务失败回滚；首次 prepare 失败返回错误且可恢复；查询源失效后 checkpoint 不被遗留快照阻塞 |
| 能力边界 | 拒绝非 4096、非 Float32、非有限 HDF5，及外部链接、VDS、外部 raw storage |
| 生产构建 | 独立 `FS_INDEX_BUILD_TESTS=OFF` Release 库，GRIB/HDF5 发布—选择—owned output—checkpoint 通过 |
| 测试钩子隔离 | Release 的 test_faults_enabled=false；设置测试 kill 环境变量仍正常完成发布 |
| 服务隔离 | 验证前后 Java / iotdb_fs 进程 PID 与启动时间一致；未启动/重启/重配服务 |

功能检查绑定 CPU 2、3，数值库线程上限为 1；未使用 2 GiB cgroup，因此这里不是“两核/2 GiB 正式性能”证据。未运行 IoTDB-FS 全仓构建/全系统集成测试；实际构建目标是独立 fs_index_native。

环境采用已有依赖：GCC 13.3.0、thread-safe HDF5 1.14.4、SQLite 3.45.1、Boost 1.83.0、OpenSSL Crypto 3.0.13；原生 status 同时记录 ecCodes API 与 SQLite 版本。依赖路径保存在 ldd-release.txt。保留原 `fclose` 模板属性编译警告，未为警告修改旧核心。

最终库 SHA-256：

```text
测试库（含故障注入）
4a4b01326eda2bbf00b1619b4dd29291734837e9cda16ab44c30435ffbf8cd04
生产 Release 库
5475722e71f14620cf05960ec8b8cd07948f1d2e0d260978d49e76e66295e990
```

## 同步顺序证据

生产库在 strace 下完成两格式的发布。`logs/sync-trace.log` 中：

| 格式 | staging 文件 fsync | 无覆盖 renameat2 | objects 目录 fsync | 后续 SQLite WAL fdatasync |
| --- | ---: | ---: | ---: | ---: |
| GRIB | 16 | 17 | 18 | 19 |
| HDF5 | 33 | 34 | 35 | 36 |

各系统调用返回 0。显式 checkpoint 无 busy，结束时 WAL 字节为 0。该证据支持已执行路径的调用顺序，SIGKILL 支持进程故障恢复；两者均不证明硬件掉电安全、NFS 持久性或分布式事务。

同步跟踪 SHA-256：`2da461c6a054d1d2aef991731fce4e165780c9e4ba8be7995bfd4b8cb7a67111`。

## Opus 复审与修复

本机 Claude CLI 实际返回模型 `claude-opus-5-5`、`is_error=false`。只读输入包括共享核心、维护实现、Python 边界与 M0 测试；没有向其开放工具。其静态结论未发现阻断发布协议的问题，并指出两处非阻塞缺陷：

1. 首次 prepare 失败会在 queries 中留下空指针，异常清理可能段错误。已在 reset_queries 判空；新增独立子进程测试注入 prepare 失败，确认 NativeError、修复后的重查询与 checkpoint。
2. Reader 的 status 枚举文件时可能遇到并发 rename，且 SQL 快照与目录空间不原子。已处理 ENOENT 并显式标记 space_snapshot_atomic=false；并发测试覆盖 status，精确空间报告限定为 writer 串行静止快照。

同时补验 scan 并发与 Reader 先行恢复，并显式关闭测试中的 Python SQLite 连接。初轮发现的 SQLite 错误字符串求值顺序问题已修正；不将初轮失败记录删除或计入最终通过数。

Opus 是静态审查，不是执行验证；最终修复由 run-002 的 M0 与 Release 验证覆盖。原始输入/输出摘要：

```text
review/input.txt
4df4a584113e1e1ab17ca05db778fd6731d476e5e4d963645b5b17c90f87296b
review/result.json
a9ad70fbf6fd127637d63d05de039b0ccd8b52da7f138526f978e96387b714ba
```

## 证据归档与下一步

本地生成材料全部位于忽略目录 `build/fs-index-maintenance-20261031/`：最终源码清单 revision-04.json、最终源码包、final-validation.json、final-logs/、review/。远端 run-001 保留初轮失败及复验，run-002 保留最终构建/测试/Release/同步日志、库和小样例；未覆盖此前实验语料或库。

本轮只修改组件与必要手工文档，未提交或推送 Git。维护能力仍限定于单 writer、不可变版本、两格式固定 profile；无删除/GC、Arrow、多 writer 或分布式发布。source-only 与 indexed 的准入阶段不同，正式对比必须使用共同支持的输入。

下一阶段可以基于这套 API 实施 M1：同一新协议下重新测 native FS、增量 FS-index 与 IoTDB-FS 的初始化＋发布＋查询＋收尾，不能把新维护结果与旧两路径生命周期数字直接拼表。M2/M3 的并发性能和超缓存边界仍未测量。
