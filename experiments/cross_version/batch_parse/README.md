# HDF5 批量子请求解析独立 A/B

本轮只验收批量子请求解析改动。A为已接受的`f1b896…`构建，B从本轮完整、真实的`build-binding.json`及实际binary固定SHA；不使用占位SHA，不恢复上一轮公共层候选或GRIB render C。产品source差异必须仅`QueryRegistrationSocket.cpp`，测试源码另记；编译器、Release flags、Arrow/HDF5和SDK配置保持一致。

| tier | 独立固定case | campaign顺序 | 每配置/臂 | 总条数 |
| --- | --- | --- | --- | --- |
| n8 | HDF5 N8/V1/C8/D4/K2/S0/Latest | AB、BA、BA、AB | 5暖身＋10正式 | 240 |
| n128 | HDF5 N128/V1/C128/D4/K2/S0/Latest | AB、BA、BA、AB | 5暖身＋10正式 | 240 |

总480条，320正式、160暖身。N8完整成功且原服务/资源恢复后，才可显式启动相同A/B的N128；无自动重试、补采或规模扩展。每配置使用独立mount和FS-index上下文，两臂交替；保留每个慢值和失败。按每轮同块两臂中位数形成R/Rd/Rq，再逐轮报告B/A及原始IoTDB-FS/FS-index T，不择优或混入旧预热协议数据。

两个配置均显式`native-hdf5`、受管FS精确版本、`native_cache=true`、客户端batch=True；原FS-index SQLite与文件/dataset/chunk缓存设置不变。完整owned时钟沿用现有客户端，覆盖发现、批量取值和Float32/mask交付；oracle/profile在计时外。批量帧`registration_attempts=3`（外层1次＋两个item各1次），发现阶段1次，完整查询共4次；`typed_batch_items=2`。不要求或解释上一轮borrowed新计数。

复用原collection-r2 corpus/case/oracle、原始/FS副本、SQLite索引、发布版本，以及已验收的HDF5位置receipt/active journal/受管sha.blob。只读核验，不生成、发布、补绑定或重建索引。原历史source report整体complete标志不作新运行恢复证明。

资源为现有balanced root v2：exclusive/effective CPU2/3、共享2核quota、query 2GiB、原DataNode heap/direct与原账号/许可证；每查询实际资源、完整环境、执行闭包和恢复证据留档。原始查询和资源样本留服务器，本地仅同步派生摘要及SHA清单。

`run.sh`仅使用既有session-20261012 venv。参数为`--repo --work --binary --baseline-binary --build-binding --baseline-build-binding --library --inspector --source-collection --current-collection --tier n8|n128`；两份build-binding均采用标准source_root/source_files/binary/unit/focused/all-XML证据，不增加兼容schema。B的focused/full XML均须包含四个新增SocketFixture安全测试，各唯一且run/completed，所有测试无跳过、失败或未完成。

先在独立work执行`--prepare-only`，再对同work执行`--verify-prepared`，均为零查询且不改变服务/资源。正式采集必须使用新work，并由根任务明确授权。N128另传本轮已完成的`--gate-report <N8/report.json>`；本协议完整240条、同A/B与全部恢复门槛缺一不可。连接恢复和根任务批准前不得SSH、部署、编译或采集。
