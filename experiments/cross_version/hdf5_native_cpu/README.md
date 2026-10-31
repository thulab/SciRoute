# HDF5 native CPU 优化独立 A/B

本轮比较三项 HDF5 native CPU 优化。A为已接受的 `f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087` 构建；B使用本轮实际 binary 与标准 build-binding 固定身份。产品源码差异必须恰好为 `src/formats/hdf5/Hdf5SourceDriver.cpp` 和 `src/formats/hdf5/Hdf5FormatQueryAdapter.cpp`。测试变化单独记录；不包含未采纳的 GRIB render、公共候选复用或批量解析候选。

| tier | 独立固定 case | 四轮配置顺序 | 每配置／臂 | 总条数 |
| --- | --- | --- | --- | --- |
| n8 | HDF5 N8/V1/C8/D4/K2/S0/Latest | AB、BA、BA、AB | 5暖身＋10正式 | 240 |
| n128 | HDF5 N128/V1/C128/D4/K2/S0/Latest | AB、BA、BA、AB | 5暖身＋10正式 | 240 |

总480条，320正式、160暖身。每个配置使用独立 mount 和 FS-index 上下文，两臂交替。先完成 N8 与全部服务／资源恢复门槛，再由根任务明确批准启动同一 A/B 的 N128。失败保留整次记录，不自动重试、补采、追加轮次或按性能择优。

两个配置均使用 `native-hdf5`、受管文件系统中的精确版本、`native_cache=true`、客户端 `batch=True`。复用原 collection-r2 的 corpus/case/oracle、完整文件、SQLite 索引、发布版本与已验收的位置绑定，不生成数据、不发布、不补绑定、不重建索引。FS-index 缓存参数保持不变。

完整查询计时覆盖发现、取值和客户端拥有的 Float32/value-mask 交付；profile/oracle 在计时外。每轮先计算同配置两臂完整查询中位数的 R，以及 Rd、Rq，再计算 B/A；同时报告两臂原始完整 T 的 B/A。保留各轮方向与所有慢值，不以 R 的下降代替完整查询加速，不将嵌套 profile 时钟相加作为独占耗时。

资源沿用 balanced root v2：guest vCPU2/3 的有效／独占 cpuset、共享两核 CPU 配额、query 组2GiB，原 DataNode 堆和 direct-memory 配置、账号及许可证。此设置不证明物理核独占、频率固定、宿主无竞争或共享缓存／内存／IO隔离。保存完整环境、每查询及周期实际资源、源码闭包、启动和恢复证据；资源包络包含计时外校验，不能解释为查询独占 CPU。原始记录留服务器，本地只保存派生摘要和 SHA 清单。

入口参数为 `--repo --work --binary --baseline-binary --build-binding --baseline-build-binding --library --inspector --source-collection --current-collection --tier n8|n128`。A/B 编译器内容、Release flags、Arrow/HDF5/SDK 配置必须一致，两份 source_files 完整核验；focused/full XML 均不得包含失败、跳过或未完成项。B的两份XML均须包含下列 `Hdf5SourceDriverTest` 测试，各唯一且 run/completed：

- `NativeSessionWarmHitRetainsHandlesAndChunkWithoutSourceReads`（包含三次温读）。
- `NativeSessionRetainsNewLogicalDemandAcrossWarmReturns`。
- `NativeSessionPreleaseCancellationLeavesCachedEntryReusable`。
- `NativeSessionMutableCancellationPreservesColdAndWarmPollSequence`。

`experiments/fixed_matrix/hdf5_native_ab.py` 为新建共享执行引擎，`hdf5_native_cpu.py` 只提供本轮不可变声明；旧 `batch_parse.py`、旧测试和历史证据保持原样。worker使用本轮显式模块名，不替换旧模块全局配置。

先在独立 work 执行 `--prepare-only`，再用同 work 执行 `--verify-prepared`；两次均为只读零查询，不改服务或资源。正式运行使用新 work。N128必须额外提供本轮完整240条、同A/B且全部恢复通过的 `--gate-report <N8/report.json>`。部署、编译和采集由根任务单独批准；本目录创建不构成启动授权。
