# Owner 不可变对象 reader pool

2026-10-24。基于 `04db6383f5587c58a90d80f87d195ffadb654ea3` 实现；本轮没有提交或推送 Git，没有替换现有挂载，也没有改动数据库配置、原科学语料或 namespace head。

## 实现与语义

`IoTDBVersionStore::get_object/get_objects` 不再持有控制路径的全局 mutex。它们从有界池取得独占 reader，完整执行 SQL、拉取结果、关闭结果集、检查对象长度／摘要和完整批次，再通过最终取消检查后归还。失败默认丢弃连接，关闭完成后才释放容量；当前操作不重试。对象顺序仍由请求位置重建，缺失、重复、错类型、错长度和摘要错误继续拒绝。

head、schema、能力检查、writer、未确定写入状态及 reconcile 保持原有控制连接与串行保护。owner 的冻结域、精确版本、执行租约和准入流程未修改。连接池不创建 worker，继续在原调用线程执行，因此 QueryStorageReadControl、预算、取消、metadata 和 execution lease 的线程局部上下文没有切换或丢失。统计计数改为独立锁，避免并发读取数据竞争。

新参数：

```properties
storage.version_directory.reader_pool_size=4
```

默认 4，允许 1–256；显式配置要求 owner 模式。空值、0、负数、非整数及超限均拒绝。池满等待最长 5 秒，每 10 ms 检查查询取消／截止时间；建连和同步 SQL 仍有各自 SDK 超时，并不承诺总操作在 5 秒内结束。对象连接按需建立，原有两条控制连接另计。legacy 的 `iotdb.pool_size/IOTDB_POOL_SIZE` 不控制此池。

`Stats` 新增 reader pool 的 capacity、active、peak、waits、acquire timeouts、created 和 discarded。peak 是池生命周期内的并发租约峰值，不能当成某条 SQL 的服务端 CPU 并行度。

## 正确性验证

| 检查 | 结果 |
| --- | --- |
| Linux Release 产品、全量单测及只读探针构建 | 通过 |
| Linux 全量单元测试 | 1,923 / 1,923；无失败、跳过或禁用 |
| 新增 pool 测试 | 10 / 10；含 1/2/4 独占、容量、超时、取消、factory 失败、discard、move 和延迟析构 |
| 新增真实 QueryRuntime 回归 | 3 / 3；池饱和时最后 waiter 取消、共享 producer 的 survivor、预算错误 sticky 与失败后新请求 |
| 产品启动配置检查 | 12 / 12；含默认／1／2／4／256、非法数值、非 owner 显式设置；在连接数据库前退出 |
| macOS owner／payload／cache／scheduler 定向回归 | 147 / 147 |
| macOS ASan/UBSan | 新增 13 / 13，无报告 |
| macOS ThreadSanitizer | 新增 13 / 13，无报告 |
| 模块依赖、差异检查、SDK 头文件语法检查 | 通过 |

macOS 首次扩大回归中，3 项既有 FileJournal 测试因默认临时路径含符号链接而被安全路径校验拒绝；保留首轮失败记录。设置 `TMPDIR=/private/tmp` 后整组 147 项通过，没有为此修改产品源码。

真实数据库探针以独立 SDK SELECT 取得固定排序的原始对象作为 oracle，然后检查 scalar、乱序 batch、缺失对象、错误 incarnation、重复请求、错误长度、取消前不发 SQL、失败后可继续读取，以及测量输出的完整字节。所有成功读取继续执行生产摘要校验。未人为断开原数据库连接，也未声称测试覆盖真实网络 ACK 丢失。

## 1/2/4 路对比的协议

这是不可变对象读取机制实验，不是 GRIB/HDF5/Arrow 的端到端查询验收，不进入 FM1 主矩阵或论文的完整查询加速结论。

- 复用已有封印的 GRIB N8/S1 与 HDF5 N8/S1 namespace；不重新发布语料。
- 固定 4 个调用线程、32 次 get_objects 操作；只改变 reader 上限 1／2／4。
- 每请求 1 个或最多 8 个对象；排序后固定同一对象集合和请求序列，比较相同总字节与 SQL 次数。
- 3 个 block，pool 顺序分别为 1/2/4、2/4/1、4/1/2；每个 format × batch × pool 收集 15 个样本，共 180 条。
- 每次进程单独进行正确性检查及连接预热；计时从同时释放调用线程开始，到全部返回对象成为客户端 owned buffers 为止。独立 oracle 与输出对照在计时外；生产摘要校验在计时内。
- 连接预热用有界 barrier 确保目标 reader 已建立；测量期间必须零新建连接、零 acquisition timeout。每次核对前后 head，以及跨样本的 namespace/incarnation/head/oracle 身份。
- 客户端及其线程固定 CPU 2/3；原 DataNode 保留既有共享资源和设置。未将数据库纳入 FM1 两核 cgroup，未清空 OS/DB 缓存；只能称 retained-server/OS warm 条件。负载均值随原始行保存，没有按速度筛除样本。

运行入口：[owner_reader_pool.py](../../experiments/cross_version/owner_reader_pool.py)；真实读取入口：[owner_reader_pool_probe.cpp](../../tests/benchmarks/versioning/owner_reader_pool_probe.cpp)。烟测 6 条单独保留，不纳入正式统计。

## 结果

正式 180/180 条完成，每格 15 个样本；全部结果通过完整字节对照与 head 一致性检查。各配置的 SQL 次数／返回字节相同，测量期间没有新建连接或获取超时，没有丢弃失败样本或按速度补采。

下表单位为毫秒，每格为 32 次操作完成的中位墙钟；括号为相对同组 1 reader 的中位耗时比。不是单次对象查询延迟，也不是相对修改前产品完整查询的加速比。

| 对象来源 | 每请求对象数 | 总读取 MiB | 1 reader | 2 readers | 4 readers |
| --- | ---: | ---: | ---: | ---: | ---: |
| GRIB N8/S1 | 1 | 18.29 | 347.80 | 187.59（1.85×） | 136.70（2.54×） |
| GRIB N8/S1 | 8 | 153.63 | 1,965.55 | 1,045.89（1.88×） | 881.28（2.23×） |
| HDF5 N8/S1 | 1 | 23.06 | 368.23 | 200.05（1.84×） | 148.96（2.47×） |
| HDF5 N8/S1 | 8 | 184.46 | 2,325.28 | 1,256.12（1.85×） | 1,058.43（2.20×） |

4 readers 的对象读取吞吐约 133.83／174.33／154.79／174.28 MiB/s（按表顺序）。本负载支持保留默认 4，但没有证明它是所有查询、缓存状态或资源配额下的最优值。增加到 4 以上尚未测量。

正式时间为北京时间 2026-10-24 20:33:53 至 20:38:14。原 DataNode／ConfigNode／trusted daemon PID 仍存活，15 个实现／验证源码文件与本地摘要一致，依赖库及探针二进制摘要均归档。

结论限定为：已有 4 个独立读取任务时，owner 对象访问不再被单 reader 串行化，1→2→4 reader 的读取阶段中位耗时持续下降。此次没有增加格式调度任务，单调用者不会仅凭池容量自动并行；未测端到端查询、native HDF5 全局锁、Arrow 训练或首次建连成本。

## 证据

服务器独立目录：

`root@192.168.99.49:/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/owner-reader-pool-20261024/`

包含 `source/`、源码包及补充包、`build.log`、`unit-tests.xml/log`、`smoke/`、`performance/plan.json`、逐次 stdout/stderr、`measurements.jsonl` 和最终摘要。私有连接参数只在内存继承，没有导出到该目录。

本地同名 `experiments/cross_version/work/owner-reader-pool-20261024/` 保存源码绑定、差异、单元测试 XML/log、本地 sanitizer／定向回归结果及性能证据副本。远端连接曾短时中断，正式采集在连接恢复且构建／烟测通过后启动；连接失败不计作性能样本。

下载后独立重算全部 12 格中位数，核对 180 条结果身份、每格 15 条、相同工作量、零测量期新连接、源码／二进制绑定及全量测试 XML，均通过。核验收据为该本地目录的 `verification.json`，原始行在 `evidence/performance/measurements.jsonl`。

- 白名单证据包 SHA-256：`22778cd1b6bf9d9d40dd496bf58ddd78b68dfb2ae0941fe0ac4ec171ec19a7b7`
- 正式原始行 SHA-256：`a595ee72b11d225f94b0ed149853a7f9eb3b6d39d3499b9b501bf49ae4873bb9`
- 正式摘要 SHA-256：`f8db2539030217b316b732ad352378a20cea050c1494f47b26571dc4034e188f`
