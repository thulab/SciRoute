# 数据库接口剩余优化空间审计

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-10。检查当前本地源码、服务器实际安装的 C++ SDK 头文件，以及既有 GEOS 发布／查询原始 profile。本轮未修改生产代码、SDK、数据库或挂载，也未运行新的性能实验。以下区分已有测量、代码事实与待验证方案。

仍有优化空间。manifest 合批后，完整发布的主要数据库接口成本转到逐块 OBJECT Tablet 传输及其数据构造；目录发现则仍受分组 COUNT 和属性 JOIN 影响。连接池已经复用长连接，不能把当前开销解释成每次建立数据库连接。

## 现有成本证据

使用 [三组发布报告](../reports/hdf5-geos-publication-batching-20261010.md) 的最新 8 MiB checkpoint 组，16 个文件共 7,142,773,610 B：

| 当前阶段或接口 | 耗时，s | 解释 |
| --- | ---: | --- |
| 完整发布 | 102.616 | 原 copyfile 计时口径 |
| 全部 SQL 接口 | 11.790 | 查询提交、结果访问、关闭、nonquery 合计 |
| 全部 Tablet 接口 | 33.811 | 包含 payload、EOF、layout 与目录索引 |
| 全部数据库接口合计 | 45.601 | 约占完整发布 44.44%；不是服务器 CPU 时间 |
| payload Tablet | 32.661 | 6,820 次同步调用，平均 4.789 ms／次 |
| payload SQL | 6.127 | 863 次 fresh manifest SELECT ＋863 次 frontier INSERT，含结果访问／关闭 |
| payload 未归属工作 | 23.792 | payload wall 减 SQL／Tablet；尚未拆分读源、构造、复制、锁等 |
| 暂存 fsync | 14.641 | 不属于数据库接口 |
| 格式索引构建 | 2.193 | 不属于数据库接口 |

payload Tablet 的总字节／接口时间约为 208.56 MiB/s，这只是串行客户端 API 吞吐，不是服务器磁盘或网络带宽。父阶段与分项嵌套，不能再次相加。单轮发布观察不能用来承诺后续提速幅度。

重新统计同组 `server-profile.jsonl` 中全部 65 个登记请求：连接获取 184 次、累计 48,693 ns（0.048693 ms）、零超时；查询提交累计 800.159 ms，连接 lease 历史峰值为 1。它们发生在发布后的登记／查询阶段，不能代表发布期间的连接等待，但明确不支持把该单客户端查询瓶颈归因于池容量。

## 1. payload 传输仍逐块串行，优先验证更大的有界请求

[write_payload_and_advance](../../src/IoTDBStructuredBlobBackend.cpp:761) 按批读取／更新一次 manifest，但内部仍按块调用 `write_payload_range`。后者每次构造单行 Tablet，再经 [insertTablet](../../src/IoTDBClient.cpp:299) 获取 lease 并同步等待 `TableSession::insert`。

因此 8 MiB checkpoint 不等于 8 MiB 数据传输。配置 `writer.db_request_max_bytes` 也不能直接突破挂载摄取当前 `min(1 MiB, configured limit)` 的限制，见 [ingestion 配置](../../src/main.cpp:1316)。

可做 1／2／4／8 MiB 连续 OBJECT 请求消融，仍保留原 Adapter consume_chunk 顺序与边界，并保持 checkpoint 字节／块数、在途内存及 RPC frame 上限。8 MiB 连续传输可把本负载的 payload Tablet 调用数从 6,820 降到逐文件共 863；这是调用次数推导，不是实测时间收益。更大请求可能增加复制峰值、尾延迟与重试成本，需要完整 SHA、失败重试和精确版本验收。

服务器安装的 `TableSession.h` 只暴露单 Tablet 的 `insert`。在头文件 SHA 与服务器一致的本地 SDK 源码中，其实现走 `insertRelationalTablet`。`Session.h` 虽有 `insertTablets(unordered_map<string, Tablet*>)`，本次审阅的对应实现走另一请求路径，没有设置关系表写入的 `writeToTable`／`columnCategories`，因此不能直接作为当前 table OBJECT 批量接口使用。此实现源码尚未与远端所链接库建立 SHA 对应关系，不概括其他 SDK 版本。

同一 `(path, version)` 多行分段也需要验证服务器的 OBJECT 更新／部分失败语义。本次审阅的本地 SDK 已排序检查只判断时间严格下降；重复时间行不会自动触发排序，但 mixed-time Tablet 的排序使用非 stable sort，不能据此保证等时间片段的顺序。检查到的客户端构造代码没有去重，不等于服务器保证全部同 key 片段写入。不能直接用重复 key 多行替换现有传输。

2／4 路并行同样是待验证方案。普通池扩容不会让现有同步循环自动并行；对同一 OBJECT 的不同偏移并发更新需先验证服务器实现及重试幂等性。失败后必须等待全部在途写停止，再 abort／重试，不能让晚到写入越过清理边界。可先验证跨独立对象或文件的有界并发，但目前全 backend 的 mutation 锁会限制该路径。

## 2. 数据构造存在已确认的冗余复制

[write_payload_range](../../src/IoTDBStructuredBlobBackend.cpp:795) 在调用 SDK 前执行 `auto copy = bytes`；layout 写入也先复制完整 vector。实际安装 SDK 的 OBJECT `Tablet::addValue` 参数是 `const vector<uint8_t>&`，因此这层复制并非满足 SDK 可变参数要求所必需。移除此处 payload 复制可少复制本次完整发布的 7.143 GB 数据，但节省的时间尚未测得。

服务器 SDK 的 inline 实现 `Session.h:310–355` 还执行：分配 `content.size()+9` 的 vector → 写 EOF／offset → 复制 content → 构造 string → 写 Tablet 的 string 值。本地 SDK 关系表插入源码还会分组并构造局部 Tablet，涉及已有 Tablet 值的复制；其与远端链接库的一致性尚未确认。因此，backend／OBJECT 编码发生在 client Tablet 计时前，而 SDK 请求构造发生在该计时内；23.792 s 与32.661 s 两个区间都可能包含复制，不能把后者全部视为 RPC 或纯传输。即使移除 backend 的副本，SDK 内部的 inline 编码复制仍存在。后续可在 SDK 中直接构造最终编码 string，减少临时 vector／string，并单独测量序列化和分组。

读取端 [read_payload_range](../../src/IoTDBStructuredBlobBackend.cpp:887)、[read_payload_ranges](../../src/IoTDBStructuredBlobBackend.cpp:910) 将 SDK string 复制为 owning vector，layout 同样如此。这是当前返回类型／生命周期的所有权转换，不能简单删除。若进一步改成共享 buffer，需要明确定义 SDK 结果销毁后的有效期及缓存内存记账，不宜和写入侧的明确冗余复制混为一谈。

payload 的 23.792 s 未归属工作含上述 backend／Tablet 构造，也含暂存读取和格式 consume 等。它比单块组增加 7.859 s，具体原因未定位，不能直接声称都是复制导致。

## 3. 其他生命周期仍重复读取 manifest，但收益规模较小

[seal_payload](../../src/StructuredBlobStorage.cpp:246)、`write_layout`、`write_metadata_and_format_identity`、`publish` 先 `load_for_write`，随后 [replace_manifest](../../src/IoTDBStructuredBlobBackend.cpp:718) 再 fresh 读取。可以沿用 payload 的方式，把校验、artifact 写入、状态更新合并为 backend 生命周期操作，保持同一锁及失败边界；不采用跨请求缓存的可变 manifest。

本轮 32 个发布 root（16 empty_create ＋16 content_commit）的 SELECT 次数为：seal 64、layout 64、description 64、publish 96。publish 包括内容版本的目录 size／OBJECT 重读，不能把全部 96 次都算作重复 manifest。

这四阶段的全部查询提交／结果访问／关闭分别仅 0.221、0.193、0.185、0.300 s；均包含真正必要的读取。合并重复 manifest 只能削减其中一部分，优先级低于 32.661 s 的 payload Tablet。description 的 SQL 2.401 s 中 nonquery 占 2.216 s，更值得检查重复元数据更新，而不是只减少 SELECT。

`write_metadata_and_format_identity` 经 `write_metadata` 写 inode.metadata，再经 manifest 更新写一次；`namespace_prepare` 的 `upsert_inode` 又写 metadata，并再次替换 set-member 索引。对于这条有独占生命周期的摄取路径，可考虑统一最终 metadata／namespace 准备，减少重复 INSERT、DELETE 与索引重建。必须保留独立 chmod／普通 inode 修改的索引一致性和发布前完整性，不能通过普遍跳过 DELETE 达到合批。

## 4. 目录发现的数据库执行计划仍值得优化

当前 [目录发现](../../src/IoTDBStructuredBlobBackend.cpp:1287) 已有批量 manifest 重验、marker 读取、缺失 OBJECT 合批、已验证解析结果复用。成功暖态请求在单批范围内仍执行：初始 catalog、manifest 重验、marker、两条分组 COUNT、属性筛选，共 6 条 SQL；缺失目录对象时多一批读取。

[累计 GEOS 实验](../reports/hdf5-geos-cumulative-20261010.md) 中 N=16 首次仅属性查询 p50=127.032 ms，分组完整性 COUNT 阶段 p50=70.150 ms、属性筛选 p50=23.722 ms。阶段中位数不能相加解释总体。本轮只重新检查源码，未重跑此查询实验。

[SQL 构造](../../include/iotdbfs/IoTDBPublishedCatalogSql.h:422) 对 dataset entry、attribute 分别 GROUP BY 计数，再执行 entry＋inode＋属性的 LEFT JOIN。进一步空间在：精确 path/version 条件是否落实为有效扫描剪枝，属性表是否重复扫描，JOIN 与聚合的执行顺序，以及能否在一个经验证的聚合计划内同时返回完整性和候选结果。

后续先做 EXPLAIN／EXPLAIN ANALYZE 与扫描行数、TsBlock、执行耗时测量，再选择合并 SQL、查询模板或索引结构。直接删除实际 COUNT 会改变当前可检测索引缺行的契约；仅凭解析缓存或 marker 计数不能替代它。实际聚合证据若要跨请求复用，也需另行定义写入／删除失效与绕过 backend 修改的支持边界。

## 5. 接口与并发管理的次级问题

| 发现 | 代码证据与判断 |
| --- | --- |
| 已复用连接 | [SessionPool](../../src/IoTDBClient.cpp:132) 初始化时创建 session 并 CREATE／USE；查询不会每次重新建连接或 USE。当前单客户端 pool wait 很小。 |
| 全 backend mutation 锁覆盖 I/O | payload 批次、catalog 读取／聚合与元数据修改持有 `mutation_mu_`。一个 8 MiB 写批会阻塞需要该锁的其他操作；需要并发试验测 lock wait，再决定 key 锁／namespace 协调方案，不能直接去掉一致性锁。 |
| 结果持有连接直到销毁 | [executeQuery](../../src/IoTDBClient.cpp:246) 将 lease 绑定到 dataset deleter。当前不少路径已有 `rows.reset()`；原则应是读取／拥有结果后尽早释放，避免结果对象跨嵌套查询或长时间 CPU 解析。不能在 SDK 仍可能 fetch 时提前归还连接。 |
| `use_prepared` 配置没有实际执行路径 | 配置被 Connector 传入 Client，但 Client 不读取它，仍执行字符串 SQL。开启该开关不会减少当前 SQL 解析成本。是否存在可用 table prepared API 需要独立能力确认，不应以开关名宣称已启用。 |
| disabled DEBUG 仍构造消息 | [query](../../src/IoTDBClient.cpp:253)／nonquery 先 redact SQL、拼接字符串再进入日志函数；[宏](../../include/iotdbfs/Logging.h:32) 没有提前检查阈值。可先检查日志级别再构造，收益待测，主要针对长 SQL／高调用量。 |
| 超时／fetch／RPC compression 使用固定值或默认值 | pool acquire 5 s，builder fetch、connect timeout、重试使用 SDK 默认值，`table.open(false)` 关闭 RPC 压缩。可暴露有界配置及 query timeout，主要改善并发与尾延迟；已压缩 HDF5 payload 或同机连接不一定受益于 RPC 压缩。 |
| 查询关闭不是当前主成本 | 发布阶段全部 query close 仅 0.130 s；SDK 是否实际发 close RPC 不能从方法次数推断。不要省略结果清理，换取潜在资源泄漏。 |

## 推荐的后续顺序

1. 先补分阶段计时：源读取、backend copy、Tablet 构造／OBJECT 编码、SDK 序列化／传输、mutation lock wait、pool wait；保留 API elapsed 作为外层指标。
2. 去除 backend 冗余复制、disabled DEBUG 字符串构造，以原 1 MiB 传输做独立 A/B。
3. 验证 2／4／8 MiB 传输，再测有界并行；不能将扩大请求与并行、SDK 编码重写同时混成一个无法归因的实验。
4. 合并非 payload 生命周期读取与重复 metadata 写入，保持故障恢复及发布顺序。
5. 对属性发现的 COUNT／JOIN 做数据库计划消融；并发实验再决定锁粒度和接口 deadline。

每次对公共接口的变更都应保持 GRIB／HDF5 同入口控制、完整 SHA／精确版本验证、故障重试测试。当前证据支持存在优化空间，不支持直接承诺总发布时间按 Tablet 调用数等比例下降。

## 证据来源

- [发布原始 profile](../results/hdf5-geos-publication-batching-20261010/batch/publication-profile.jsonl)、[登记原始 profile](../results/hdf5-geos-publication-batching-20261010/batch/run-01/server-profile.jsonl)、发布摘要（服务器归档：`docs/hdf5-geos-publication-batching-20261010.json`）。
- 目录对象合批报告（服务器归档：`docs/dataset-object-batching-ab-20261010.md`）、[累计查询报告](../reports/hdf5-geos-cumulative-20261010.md)。
- 服务器 SDK：`/data_01/zzy/iotdb-fs-exp/sdk/iotdb-session-cpp/include/`。本次检查的头文件 SHA-256：`Session.h=3011a61094094d0be931de7b0f0f3b5d118dbc4144258870cd2f7f9540c2c062`，`TableSession.h=db83b264341f8cff5ba0c45e48bf09b2a93636e5dc0133c7ee5a17ddcacc6559`，`AbstractSessionBuilder.h=d82feebbbf08afe1c8d5c179cdede30799ca8d55d760300e3fe36648b9dce162`。
- 本地同头文件 SDK 源码：[关系表分组](/Users/alan671/Documents/iotdb/iotdb-client/client-cpp/src/session/Session.cpp:1614)、[局部 Tablet 副本](/Users/alan671/Documents/iotdb/iotdb-client/client-cpp/src/session/Session.cpp:1694)、[另一多 Tablet 请求路径](/Users/alan671/Documents/iotdb/iotdb-client/client-cpp/src/session/Session.cpp:1800)、[TableSession 转发](/Users/alan671/Documents/iotdb/iotdb-client/client-cpp/src/session/TableSession.cpp:25)。`Session.cpp SHA256=ebb64659dd50c62fb7631c7dea1786be2df878e01776d13228a99a46d2721282`，`TableSession.cpp SHA256=9da22cb72997025ad96fe7e869021210580adfef7ad873d7f811bfb7be348766`；未将它们当作已验证的远端链接库构建来源。
