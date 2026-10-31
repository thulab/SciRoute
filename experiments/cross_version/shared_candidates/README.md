# 公共层候选复用 A/B

仅复用FM1四个既有独立case和语料；不生成、发布、补绑定或重建索引。A为已接受的`f1b896…`，B为本轮`d52c9b…`，204个产品文件中仅批准的6个公共层文件不同；不恢复GRIB render候选。

| tier | 格式/规模 | campaign | 查询数（含暖身） |
| --- | --- | --- | --- |
| n8 | GRIB N8/V1/D4/K2/S0/Latest | 0 | 48 |
| n8 | HDF5 N8/V1/D4/K2/S0/Latest | 0/1/2 | 144 |
| n128 | 独立GRIB N128/V1/D4/K2/S0/Latest | 0/1/2 | 144 |
| n128 | 独立HDF5 N128/V1/D4/K2/S0/Latest | 0/1/2 | 144 |

共480条。每配置独立mount与FS-index上下文，复用原SQLite索引；每臂2次暖身、10次正式，两臂交替。配置顺序仅按campaign为AB/BA/AB，不按case另翻转。N8两格式192条完整成功并恢复后，方可显式启动N128两格式288条；不自动串联或重试。

GRIB两配置均使用`format_catalog`及原逐命中typed取值；HDF5两配置均显式`native-hdf5`、受管FS精确版本、native_cache=true和batch=True。FS-index保持8文件/8 dataset、2MiB metadata、256KiB raw chunk缓存。完整t0/d/t1/q/t2至owned Float32/mask；oracle和profile在计时外。统计保留全部原件，先形成同块R/Rd/Rq，再按campaign求B/A，旁列两臂完整T；不使用历史最小值替代A。

资源沿用exclusive CPU2/3、可均衡root partition、共享2核总quota、query2GiB、原DataNode heap/direct及自动恢复。source proof记录当前执行闭包，核验固定corpus/case/oracle、实际原始/FS/受管HDF5内容、索引/版本/head/journal和已有位置receipt。A/B构建缓存中的Release、compiler/flags及Arrow/HDF5配置必须一致；缺失可选键显式记null。B完整1833单测及8个新增项须无失败/禁用/跳过，不伪造focused门槛。

执行入口为`run.sh`，必须使用既有session-20261012 venv（wrapper已设置PATH）。所有模式显式提供`--repo --work --binary --baseline-binary --build-binding --baseline-build-binding --library --inspector --source-collection --current-collection --tier n8|n128`。`--build-binding`是B现有verification.json，A为原标准build-binding.json；B source-manifest/source/完整XML及unit binary从其固定build目录核验。

先对独立work执行`--prepare-only`，再对同work执行`--verify-prepared`；均无服务/资源改动或科学查询。正式采集使用另一个新work，不带上述两开关。N128还须`--gate-report <本轮N8/report.json>`。根任务统一预检并批准科学启动。

B机制资格：GRIB发现borrowed=C、constructed=0，exact取值borrowed=1且授权membership build/entries=0；HDF typed_batch顶层borrowed=K、typed_batch_items=K及membership build/entries=0。不要求items[]有counters；A缺新计数不解释为零工作。
