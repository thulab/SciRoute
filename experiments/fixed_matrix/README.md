> FS-index implementation and build instructions moved to [the main component](../../fs_index/README.md). Commands below describe frozen campaigns and their original accepted binaries.

# FM1 固定实验框架

本目录是后续正式多文件／跨版本性能采集的共同入口。历史 runner 和结果保留追溯，不进入 FM1 的统计或基线。

- 设计及用户确认：[固定矩阵方案](../../docs/deliverables/fixed-experiment-matrix-plan.md)。
- 已预注册：[飞书 4.3 原表](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg#doxcn5FH2k0iTUtMKq2Ch9b9gmg)，revision 638；执行前收据位于服务器 `work/fixed-matrix-20261021/registration-receipt.json`。

结果展示遵循2026-10-22用户要求：初始历史基线保留；优化列仅记同case相对指定历史参照的ΔR与主线决定，备注仅服务器目录。完整原始时钟、每轮FS-index、A组、资源及验收证据仍须存服务器。本次历史参照为“10.21基线”，按未舍入R计算；历史变化不替代同轮A/B及完整T验收，后续不得择取其他case或历史最低值充当参照。
- 唯一机器清单：`catalog.json`，SHA256 `41eff9c7aa824ed62df6c025531c74e731a94e75270f3e936e7b71934bcab47d`。首轮32组；18扩展仅登记，本轮不执行。CLI不接受任意N/V/C/S/ROI覆写。
- 每组绑定完整语料、查询、运行三层身份。N512/C128属于N512，不能写成独立N128。首次成功封印后重复使用同一发布和case，优化只新建run；新增组须先确认和登记。

| 模块 | 职责 |
| --- | --- |
| `data.py` | 固定种子、完整独立版本文件、所有unit的原生属性和值／mask oracle；拒绝覆盖／续写已有目录 |
| `native/`、`native.py` | 独立ecCodes/HDF5原生基线与通用SQLite全历史索引；保留真实SQL和EXPLAIN |
| `client.py` | 三臂统一t0→发现→t1→客户端owned Float32/mask→t2；独立校验和profile在钟外 |
| `telemetry.py` | 只读环境身份、每秒主机／进程／cgroup资源原始采样，记录采样器自身开销 |
| `run.py` | 原服务保护、两核／query2GiB门禁、先全部发布再三轮矩阵、故障恢复、完整性验收 |
| `analysis.py` | 严格验收3456条记录；每campaign中位T/d/q与R/Rd/Rq，描述性跨campaign汇总 |
| `hdf5_filesystem.py` | 复用已封印的7份HDF5语料／16首轮case，显式补文件位置，FS原生通路三轮1,728次；接续已验收的当前journal，不重发科学数据 |
| `hdf5_cache_batch.py` | 既有N8／独立N128／V8相同内容选择性All，原生会话缓存×批量typed的2×2消融；先N8单轮96条，正式三轮864条；只复用当前journal／文件绑定，不发布或重建索引 |
| `hdf5_cache_batch_package.py` | 实验脚本139份依赖闭包核验；只部署本轮四个新增Python文件，既有依赖及归档work只读 |

FM1首轮封印采集中的HDF5固定为当时的 `native-hdf5` 数据库VFD路线；GRIB固定当时的跨文件及文件内索引，首轮产品二进制未重建。这是该批历史run的协议。当前源码的同名native模式已改为数据库目录＋受管FS完整文件，后续必须补绑定、记录新实现/后端与准备成本并建立新run，不能改写首轮记录或冒充同引擎重复。固定catalog/corpus/case保持不变。FS-scan／FS-index分别保留有界原生上下文，均输出客户端owned数组，不缓存答案。双方内部缓存机制不宣称相同。

所有语料准备、FS复制/fsync、索引创建、数据库发布与读回验证、挂载启动均单列。正式时钟不含oracle/profile采集。查询资源envelope包含钟外审计，不冒称纯查询CPU。每campaign重新建daemon及native上下文；OS/DB缓存不清空，只称retained-process warm。

正式采集只能在已授权的49服务器、原数据库／账号／许可证上由控制器执行。需要Linux root、可用cgroup v2 isolated partition、现有运行时和固定依赖；入口参数是路径，不能改变矩阵：

```sh
PINNED_PYTHON -m experiments.fixed_matrix.run \
  --repo /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C \
  --work NEW_PRIVATE_COLLECTION_DIRECTORY \
  --library WORK/native-build/libfixed_native.so \
  --native-binding WORK/native-build/verification.json \
  --registration WORK/registration-receipt.json
```

启动前检查表格收据、产品/helper/链接库/源码摘要；同表N8功能门禁不进入性能汇总。语料全部封印后才开始3×32×3×12计时。完整查询满足T=d+q；逐查询检查实际CPU2/3、总quota两核、query叶2GiB及OOM，采样覆盖和原服务／资源恢复失败时禁止发布主结果。失败保留目录，不自动重跑、不排除慢样本、不宣布不完整矩阵中的“最佳组”。

私有配置、凭据、许可证、原服务备份和原始诊断日志留在服务器；外传仅白名单证据和无凭据摘要。未实际测量的单元保持待测，扩展保持未执行。

已封印 FM1 后，初始 `run` 发布入口拒绝再次发布。已完成的原架构整轮第三轮补采使用以下历史入口（HDF5 namespace补绑定后，不可再用原二进制／旧journal重跑该入口）：

```sh
PINNED_PYTHON -m experiments.fixed_matrix.replay \
  --repo REPO --work NEW_PRIVATE_QUERY_ONLY_DIRECTORY \
  --source-collection WORK/collection-r2 \
  --library WORK/native-build/libfixed_native.so \
  --native-binding WORK/native-build/verification.json \
  --registration WORK/registration-receipt.json
```

此入口严格复用原14份 corpus／32 case、原生索引与产品；保留完整 campaign 0/1，整轮替换 campaign 2。原第三轮72行和失败报告保留。新段 `queries.jsonl` 为1,152行，`accepted/`组合为3,456行；两段资源与服务恢复证据分别引用，不把中间停机间隔算作连续采样。必须脱离SSH、将监督进程stdout/stderr写入私有文件启动；不依据时延筛选样本或自动重试。

用户于2026-10-21授权补齐FS原生通路的HDF5实验，入口为：

```sh
PINNED_PYTHON -m experiments.fixed_matrix.hdf5_filesystem \
  --repo REPO --work NEW_PRIVATE_COLLECTION_DIRECTORY \
  --source-collection WORK/collection-r2 \
  --bound-source NATIVE_FILESYSTEM_WORK/gate-r2 \
  --binary NATIVE_FILESYSTEM_WORK/build/bin/iotdb_fs \
  --build-binding NATIVE_FILESYSTEM_WORK/reviewed-build-binding.json \
  --library WORK/native-build/libfixed_native.so \
  --inspector PINNED_HEAD_INSPECTOR
```

只接受完整16个HDF5首轮case，三campaign、每臂2预热＋10测量，共1,728次；18扩展和GRIB不执行。先验证全部原始语料、SQLite索引、oracle、当前绑定与产品SHA；已绑定的N8/N128从已验收的`owner-active`复制当前journal并沿用原受管文件根，其他5份从原归档journal复制独立当前journal、显式增加文件定位。以后须使用新收据中的当前journal，不能重新复制旧发布head。查询只读，补绑定不是重新发布科学数据。原72条小探针单独保留，不拼入正式三轮。`--prepare-only`只做离线门禁，不进入服务维护。采样、原服务／资源恢复和完整1728条门禁全部通过后才能填完整主表或标粗最佳组。

该首次FS通路复测已于2026-10-22完整验收，目录`work/hdf5-native-matrix-20261021/collection-r1/`，本地白名单副本为同work下`evidence/`。16组中位R为7.031–10.164，未显示优于FS-index；完整结果及真实启动成本见[HDF5统一报告](../../docs/reports/hdf5-route3.md#native-filesystem)。后续重复仍使用同一case与`current-locations.json`中的当前journal／受管根建立新run；不能直接再次调用首次补绑定流程，把5份已扩展namespace当作原发布head打开。

原生会话缓存×批量取值消融已于2026-10-22完成，`work/hdf5-cache-batch-20261022/collection-r1/`含864条正式记录，另`smoke-r1/`96条不拼入统计；本地`evidence/`含白名单副本及独立统计／资源验收。C0B0／C1B0／C0B1／C1B1同一产品，N8／独立N128合用R改善，V8缓存无稳定额外收益并保持关闭；绝对时间漂移及全部block范围见[统一报告](../../docs/reports/hdf5-route3.md#native-cache-batch)。复测必须建立新run并沿用同一case、当前journal／根与完整资源门禁，不重建或覆盖本轮目录。
