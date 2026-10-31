# V00 持久化写入口与原子性源码审计

审计日期：2026-10-15。代码基线：`46650fa9ea1aa3e033cf1ed23f6d46f614d62773` 的生产源码，工作树 HEAD `15b90820191e72bcc636b254021a07e9a3165e57`。本报告记录源码审计；独立恢复与现行语义测试的范围、结果见 [V00 验证报告](version-directory-v00-validation-20261015.md)。未执行 Git 写操作，未修改生产存储协议。

## 1. 已查明的权威边界

- `src/IoTDBConnector.cpp:19–29` 只是 `IoTDBClient` SQL/Tablet 薄封装，不提供事务、CAS 或 commit ID。
- `src/IoTDBClient.cpp:226–246,302–359` 每次请求获取独立池租约；RPC 成功返回 true，异常返回 false。false 无法区分未发送、部分执行和执行成功但确认丢失；多条 SQL 没有共同事务。
- `src/IoTDBStructuredBlobBackend.cpp:757–767` 的 revision 检查是实例内 `recursive_mutex` 下 SELECT → 比较 → INSERT；不是数据库 CAS。其它进程、同进程其它 backend 实例、直接 SQL 不受这把锁约束。
- `include/iotdbfs/IoTDBStructuredBlobBackend.h:51–55,137–142` generation 和 parsed cache 是 backend 实例本地状态；`src/StructuredBlobStorage.cpp:12–28` storage namespace 由进程内原子计数产生。它们不能用于持久 namespace 身份、跨进程变更证明或恢复 epoch。
- mount 在 `src/main.cpp:3675–3683`、迁移在 `src/migration/MigrationCli.cpp:43–49` 获取 `NamespaceWriterLock`。`src/NamespaceWriterLock.cpp:24–57,74–87` 是本机 private file + flock，锁名哈希 host:port/db，且 `IOTDBFS_NAMESPACE_LOCK` 可覆盖；既不阻止远端主机 writer，也不阻止换锁路径或数据库地址别名的进程。
- 没有发现独立定时后台 inode flush 线程。`InodeCache.cpp:18–20` 存储 flush interval、59/214 存 dirty_since，但本文件没有定时消费；实际 `flush`/`flush_all` 为同步显式入口。不能在文档里把配置名当成已实现后台线程。

## 2. 持久化写入口全清单

以下源码引用相对 `IoTDB-FS-C`。所有新模式入口要求先由唯一 owner 接纳操作；不能用以下旧 SQL 行作为与新 head 并列的真相。

| 入口及行号 | 调用方 / 写入对象 | 当前原子性及失败边界 | 新 owner 接入要求 |
|---|---|---|---|
| `IoTDBStructuredBlobBackend::ensure_schema`, `src/IoTDBStructuredBlobBackend.cpp:146–211` | mount/init；建 inode、payload、layout、metadata_set、version_identity、dataset_catalog、dataset_entry、dataset_attribute 八表 | 顺序 DDL，短路失败，可部分建表 | namespace 初始化独占；能力/协议不符拒绝启用，不自动升级旧 namespace |
| `upsert_inode`, `:255–320` | mkdir/root、chmod/chown/utimens、ingest prepare、copy prepare、cache hooks；写 inode POSIX 字段和 metadata set 索引 | mutation mutex；先 generation++，DELETE/重写 metadata_set，后 inode INSERT；可中间失败 | 权限/祖先/namespace 与同一 CatalogRoot 发布；派生 inode SQL 不再独立授权 |
| `create_manifest`, `:451–475` | `StructuredBlobStorage::begin` (`StructuredBlobStorage.cpp:206–215`) | 查询 tombstone 和 existing 后 INSERT 初始 inode，非 DB 原子 create-if-absent | owner 保留 exact identity，staging 不可见；tombstone 永不复用 |
| `write_payload_and_advance`, `:817–850` | `StructuredBlobStorage::write_payload_range/:217–224`, `write_payload_batch/:226–246`；ingestion/copy | 锁内重读 manifest，多个 payload Tablet 后一个 manifest frontier INSERT；失败可留不可见字节，ACK 丢失不确定 | staging writes 与提交对象封闭性检查；复核 commit ID 后重试，不发旧 context |
| `write_payload_range`, `:853–896` | backend 公共 API；`require_write_phase` 后 OBJECT 局部写 | 单 Tablet，不更新 manifest；仍可由 backend 调用者绕过 Storage orchestration | 仅 owner staging capability 可调用；不能暴露给只读客户端 |
| `append_payload_eof`, `:724–754` | manifest 从 PayloadOpen → PayloadSealed (`:794–798`) | 先 OBJECT EOF/size/sealed Tablet，再 inode phase INSERT | EOF/对象完整性必须在 head 前完成 |
| `replace_manifest`/`persist_manifest_unlocked`, `:757–815` | Storage 默认 payload 实现 `:61–76`；seal `:248–259`；layout `:262–280`；description `:283–340`；publish `:343–382` | expected revision 仅本实例保护；已 published 拒绝修改；发布阶段验证 catalog 后多字段单 inode INSERT | 只在 owner 下形成不可变版本绑定/提交；head 是唯一线性化点，不能只保留 published=true gate |
| `write_layout`, `:899–930` | Storage::write_layout；ingestion/copy | 一个 OBJECT Tablet 后独立 manifest 更新；空 layout 直接更新 manifest | 只写不可变 staging/content，完成后纳入引用闭包 |
| `write_metadata`, `:932–950`; `replace_metadata_set_members`, `:952–980` | Storage description；后者也被 upsert_inode 调用 | DELETE metadata_set → Tablet members → inode metadata INSERT → manifest INSERT；无跨表原子性 | metadata index 变成派生视图或不可变对象，所有可见性由 root 绑定 |
| `write_dataset_catalog`, `:1096–1235` | Storage::write_metadata_and_format_identity `:307–315` | 删 marker/entry/attribute 三表，写 entry Tablet、attribute Tablet，最后 marker OBJECT/fields Tablet；仅 LayoutStored 阶段允许；随后独立描述/发布 | catalog proof 在 head 前完整验证；marker-last 不是新 head 原子性替代品 |
| `erase`, `:1082–1094` | Storage abort `:385–395`、remove `:397–402` | SELECT revision 后委托 remove_path；锁仅实例内 | abort staging 与逻辑删除分开；删除先 root/tombstone，物理回收走 GC |
| `remove_path`, `:378–409` | main 删除包装；erase；probe | SELECT published exact/all → tombstone Tablet → DELETE inode → 六类 artifact DELETE；返回 hidden | owner 原子撤销、持续保留 tombstone、延迟回收；详见下方失败缺口 |
| `remove_tree`, `:411–449` | main cache erase_tree hook；probe cleanup | 枚举 path、退休 published keys；逐 path DELETE inode，成功隐藏全部后再清 artifacts；失败可只隐藏部分树 | 一个 root 提交完整子树删除；按冻结键集处理，不能以逐行删除冒充树事务 |
| `retire_published_versions_unlocked`, `:340–375` | remove_path/remove_tree | 查询全部 published 非目录 key → 一次多行 tombstone Tablet；无行数上限/多行原子保证 | 版本存活/tombstone 根纳入同次提交；有界分页/操作预算 |

**删除失败缺口（不得被注释掩盖）**：`remove_path :395–404` 即使 `DELETE inode` 返回 false 也继续删 artifacts。当前注释“先隐藏后回收”的保证只在 hidden 真成功时成立。若隐藏明确失败，可能留下可见 inode 指向缺失对象；若确认丢失，也无法确定。V00 应记录为 V01/V05 的接线前置问题，本文没有修补当前行为。`remove_tree` 在隐藏失败时立即返回，避免随后 artifact 回收，但仍非完整子树原子提交。

## 3. 主进程到四个重点函数的完整生产调用图

```text
upsert_inode
  main.cpp:1815 upsert_inode_db(i) -> upsert_inode(i, i.is_dir)
    :1840 InodeCache DbHooks.upsert
      InodeCache.cpp:222–245 flush / :248–274 flush_all
      main.cpp:1860–1882 insert_inode -> put(dirty=true) + flush
        :2563/2594 mkdir
        :3088/3108/3114 chmod
        :3117/3139/3146 chown
        :3149/3196/3203 utimens
        :3492–3502 mount root bootstrap
        :3595–3605 initialize_backend root bootstrap
      main.cpp:3576–3583 op_destroy -> flush_all (explicit shutdown writeback)
    :2069–2125 commit_write_session -> IngestionModule prepare callback :2112
      :2553 truncate; :3061–3072 flush/fsync; :3222 release
    :3441 rename directory target; :3458 directory compensation
  main.cpp:2196 copy_structured_blob_version -> upsert_inode(target,false)
  main.cpp:2644 op_create empty ingestion -> upsert_inode(i,false)
  migration/MigrationCli.cpp:74–75 ReingestionNamespace.prepare_inode
    migration/ReingestionService.cpp:193–209 ingest callback

remove_path
  main.cpp:1822–1824 delete_inode_db
    :1843–1845 InodeCache DbHooks.erase_one
      main.cpp:1916–1921 delete_inode_by_path
        :2662 failed create writer setup rollback
        :3235–3262 unlink (optional exact version)
        :3273–3301 rmdir
        :3453 rename source cleanup
    :3430 rename destination rollback
  backend.cpp:1091 erase
    StructuredBlobStorage.cpp:394 abort / :401 remove
    Ingestion.cpp:80 failure abort; main.cpp:2146 copy failure abort

remove_tree
  main.cpp:1826–1827 delete_inode_tree_db
    :1847–1848 InodeCache DbHooks.erase_tree
      InodeCache.cpp:85–107 erase_subtree (production main currently removes empty dir via erase_one)

replace_manifest
  StructuredBlobStorageBackend default write_payload_and_advance :76
  Storage::seal_payload :259
  Storage::write_layout :276,280
  Storage::write_metadata_and_format_identity :338
  Storage::publish :382
  IoTDB backend overrides payload advancement to persist_manifest_unlocked :850
```

`main.cpp:3307–3470` rename holds only `g_namespace_mu` in one daemon. It enumerates versions, copies/publishes each destination (`copy_structured_blob_version :2128–2208`, payload copy chunks 1 MiB at :2148), then deletes source paths. Rollback/compensation are best-effort additional writes, not a namespace transaction. Incoming path may rebase version timestamps if destination exists (`:3422–3425`); old path plans must be invalidated and new outer seals generated. New owner must prepare all bindings then switch one root. Ordinary `setxattr/removexattr :2378–2385` returns ENOTSUP and has no persistent mutation.

## 4. 独立 reader、迁移、测试与脚本入口

| 入口 | 当前行为 | 新模式要求 |
|---|---|---|
| `src/CrossFileClient.cpp:48–56,304` | 独立连接、独立 backend/storage，直接查读 IoTDB；没有 owner context | 查询开始及准入经 owner，禁止以本地 generation/TTL 证明没变 |
| `src/SemanticGrepClient.cpp:28–36,1074` | 同上，独立语义检索 reader | 同上 |
| `src/IoTDBStructuredBlobBackend.cpp:551–713,1543–1960` | catalog/snapshot/permission/discovery 读；同实例 mutex 和 generation；strict 默认 (`include/...Backend.h:27`) | root/page 一致读 + owner 准入；保留 strict 独立路径 |
| `src/main.cpp:3522–3561` registration socket | 当前 socket 进程内 backend；可获取权限 snapshot；含明确实验限定 frozen typed opt-in | owner 内执行、结果发布与授权撤销同边界；实验 frozen path 不视为生产持久 lease |
| `src/migration/MigrationCli.cpp:13–51,63–87,101–160` | `arrow-migrate/hdf5-migrate inventory/run/resume`；本地 writer lock；inventory 只读 DB但写 report；run/resume 用旧 namespace 读/prepare/publish | 新 namespace migration 通过 owner 或排他离线模式；不能与新 owner 并发直接写，不能隐式兼容回退 |
| `src/iotdb_fs_preflight.cpp:332–349` | 独立 IoTDBClient，创建/写入/删除探测表 | 仅隔离 probe table/db；无 root/业务表写权限；不能用探测成功推定 head 原子性 |
| `tests/benchmarks/hdf5_query/dataset_catalog_probe.cpp:90–91` | 独立 backend；prepare inode :164，删除 :403/:421/:787/:942，直接改 inode/catalog/index SQL :571/:588/:629/:774/:853/:894/:918/:921，immutable bypass :958/:975/:991/:1008/:1023，cleanup :39/:113/:1069 | 只允许隔离测试 namespace；直接 SQL corruption 是协议外故障注入，不得接生产新模式 |
| `tests/benchmarks/hdf5_query/h09_migration_fixture.cpp:126–155,197` | 独立 backend + prepare_inode + reingestion | 新模式仅 owner ingest 或隔离旧模式 fixture |
| `tests/benchmarks/arrow_training/a12_legacy_seed.cpp:75–120` | 独立旧版本 fixture writer | 同上 |
| `tests/integration/test_IoTDBClient.cpp:186,231,287,405–429` | 独立 SQL/Tablet 客户端，独立测试 DB；DDL/写/删除 | 保留隔离；不提供 owner 能力证明 |
| `tests/benchmarks/arrow_training/read_shape_probe.cpp:88–94`, `derived_range_bridge.cpp:191–194`, `arrow_query_paths_probe.cpp:435–436` | 直连 backend reader | 生产新模式必须经 owner；只读诊断可明确 strict |
| `tests/benchmarks/hdf5_query/publication_payload_verify.cpp:34–36`, `published_catalog_probe.cpp:47–82`, `geos_pilot_direct.cpp:202`, `h11_query_probe.cpp:425,792`, `sc_perf_direct.cpp:201` | 直连 reader（有本地/in-memory替代支路） | 同上，不借进程本地 namespace 作为持久身份 |
| `experiments/cross_version/publish.py:48–60,204–251` | 私有 FUSE mount 内 mkdir/write/fsync，新版本或重启审计；不直接写 DB | mount 由 owner 接入，独立 publisher 不以另一个本地 lock 绕过 owner |
| `tests/benchmarks/arrow_training/ingest_public_arrow.py:37–49` 与其它 smoke/perf 写文件脚本 | OS open/write/fsync/unlink/copy，经 FUSE 进入上述 main 操作 | 统一由挂载 owner 覆盖；不需各脚本另造提交逻辑 |
| `experiments/multi_file/manager.py` / `cross_version/corpus.py` | 本地 raw 文件/manifest 构建与共享，不直接改业务 DB | 不属于 head writer；其输出经 publisher 摄取 |

2026-10-19 目录整理：本节表中 `cross_version/publish.py`、`cross_version/corpus.py` 及行号均指审计当时的历史快照；当前活脚本分别位于 `cross_version/pilot/publish.py`、`cross_version/pilot/corpus.py`。历史快照与审计身份未改。

`tests/unit/**` 中对 replace_manifest 的实现均为内存/只读 fake，不能作为持久 IoTDB 原子性证据。`tests/integration/write_test_mysql.cpp` 的 SQL 是 MySQL 对照，不是 IoTDB namespace writer。

## 5. 现行协议与预算（不擅自选新协议常量）

| 项目 | 现行值与源码 |
|---|---|
| Storage phase | `StructuredBlobStorage.h:56–64` PayloadOpen/PayloadSealed/LayoutStored/Described/Published；现行 schema 无通用 NamespaceHead 协议 |
| GRIB layout | `IOTDBFS-GRIB-LAYOUT/1`，`include/iotdbfs/formats/grib/GribLayout.h:10`, `src/formats/grib/GribLayout.cpp:9–20` |
| HDF5 layout | `IOTDBFS-HDF5-LAYOUT/5`；定义 previous `/4`；`include/iotdbfs/formats/hdf5/Hdf5Layout.h:11–13,188`。不能原地把已发布 `/5` 重解释为共享协议 |
| Arrow layout | `IOTDBFS-ARROW-LAYOUT/1`，`src/formats/arrow/ArrowLayout.cpp:16–17` |
| Dataset catalog | `iotdbfs-dataset-catalog/1`，`include/iotdbfs/DatasetCatalog.h:27` |
| Dataset publication seal | `iotdbfs-dataset-substructure-index/1`，`include/iotdbfs/DatasetSubstructureIndex.h:9–14`；digest 包含精确 path/version，不是跨版本 content ID |
| typed metadata | serialized cap 65,536 B，`MetadataSerialization.h:13`；backend 多处显式同值 |
| payload write batch | 64 chunks、8 MiB，`StructuredBlobStorage.h:27–28`；ingestion 默认 1 MiB chunk + 8 MiB batch，`Ingestion.h:59–62`；标量写旧接口没有统一 8 MiB cap，不能声称所有 payload 写都有此上限 |
| payload read batch | 64 ranges、8 MiB，`StructuredBlobStorage.h:37–38`；backend 禁 empty/zero-length、uint64 overflow、short read (`backend.cpp:1009–1046`) |
| public QueryLimits | 1,024 sources；65,536 ranges；64 MiB layout；1 GiB payload；100,000,000 rows，`QueryPlanning.h:253–258` |
| dataset discovery | 1,024 sources/datasets、64 MiB aggregate catalog，`DatasetCatalogQuery.h:17–19` |
| single dataset catalog | 16 MiB，`DatasetCatalog.h:34`；object batch 16 MiB，`IoTDBPublishedCatalogSql.h:339` |
| permission/revalidation SQL | 总 65,536 paths/keys、每 SQL 128、每 SQL 64 KiB；`IoTDBPublishedCatalogSql.h:14–16,91–93` |
| HDF5 layout | 64 MiB layout、8 MiB directory、64 MiB reads、65,536 read calls、1,048,576 page scans、65,536 objects/pages、131,072 links、1,000,000 chunks、256 chunks/page；`Hdf5Layout.h:131–137` |
| HDF5 metadata | 64 KiB attr、2 MiB metadata、256 attrs/object、16,384 attrs、1,024 attr elements、4,096 scales、1,048,576 coord elements、8 MiB coords、4,096 string bytes、depth 256、1,024 filter params；`Hdf5Layout.h:140–147` |
| HDF5 range transport | 1/2/4 parallel reads；8 MiB/64 per call；65,536 transport ranges，`Hdf5RangeReader.h:9–13` |
| HDF5 fragment batch | 64 fragments / 8 MiB encoded；64 MiB buffer/decoded/encoded chunk；`Hdf5FragmentProvider.h:23–30` |
| decoded shared cache | 公共 memory 1 GiB、1,024 entries、64 inflight、4 worker threads、2,000 ms wait timeout、1 GiB/read load、65,536 ranges/load、4 work groups、2 requests/workload、512 MiB output/workload、1,024 queued groups；`DecodedFragmentCache.h:25–37` |

现行生产代码不存在 catalog COW page size/tree height/extent tree/locator bytes limits；新上限已经用户 D5 确认，见 [V00 契约](../contracts/version-directory-content-sharing-v1.md#6-d5-硬预算及边界)。这些是后续实现的硬约束，不是现有实现已经验证的容量。现行 scalar read 为零长度可成功；batch 的空集合/零长度禁止；短读由 Storage read handle/backend batch 失败。须保持二者区分。

## 6. 实际远端源码的单 BLOB head / WAL 证据

只读 checkout：`root@192.168.99.49:/data_01/zzy/iotdb-fs-exp/timechodb`，`git rev-parse HEAD` = `2e8852d5e98c877f75b84d8db0b0ce0fac3f92e6`。`TsFileProcessor.java`/`WALBuffer.java` 的定向 git status 为空。未把本地 `/Users/alan671/Documents/iotdb` 参考源码当作 release 相同构建；release 的提交前缀相同但 `git.dirty=true`，不能推定完全等同；实际 JAR 的关键 WAL 字节码已独立核验，详见 [验证报告](version-directory-v00-validation-20261015.md#51-实际发行包与-wal-证据)。

下列 Java 路径以 `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/` 为根：

1. `storageengine/dataregion/memtable/AbstractMemTable.java:247–280` 的 aligned row 入库显式忽略失败 measurement（`:259–267`）。因此不能从“同一次多字段 INSERT”推论所有字段总成功。head 用固定 table/time/tag 的唯一 BLOB FIELD；完整 envelope/protocol/namespace/incarnation/seq/commit/root/hash 全编码在该单值内，可避免多个 FIELD 各自部分成功组成混合 head。不是 OBJECT range 更新。
2. `utils/datastructure/AlignedTVList.java:468–499` 单行 `putAlignedValue` 为 synchronized，逐字段 addValue 完成后才 rowCount++；`:1150–1193` tablet 同为 synchronized。一个 BLOB 是一个 Binary 值；替换同 time/tag 值可作为单单元候选原语。这是实现结构证据，完整并发可见性仍以固定 build 双 reader 实验佐证，不把它称作通用 DB 多对象事务。
3. `storageengine/dataregion/wal/buffer/WALInfoEntry.java:60,69` 仅配置 WALMode.SYNC 时 listener `wait=true`；`wal/utils/listener/AbstractResultListener.java:69–83` 在 wait=true 时等结果，否则可返回 RUNNING。因此 ASYNC 不能用于 ACK 前持久保证。
4. `storageengine/dataregion/memtable/TsFileProcessor.java:357–358` row、`:676–677` tablet 先 walNode.log 再 waitForResult，失败抛错；成功后才 memtable 写入 (`:401–403`, `:726–729`)。
5. `storageengine/dataregion/wal/buffer/WALBuffer.java:654–673` 把 WAL buffer 写 OS；`:706–728` 调 `currentWALFileWriter.force()` 成功后 listener.succeed；`wal/io/LogWriter.java:201–208` 最终 `FileChannel.force(metaData)`。该链支持 SYNC 下 WAL 强制落盘先于成功执行返回，仍受 OS/设备 fsync 诚实性边界约束。
6. `storageengine/dataregion/wal/io/WALReader.java:66–99` 只有 `WALEntry.deserialize` 完整成功才暴露 nextEntry；EOF 中断条目或 IO 错误标记损坏并停止，不能返回半条 entry。提交前 crash、ACK 后 crash、lost ACK 恢复须用实际 release 实验补齐。
7. C++ SDK `iotdb-client/client-cpp/src/session/TableSession.cpp:25–26` → `Session.cpp:1689–1732` 单 tablet 构造 table request，一次 insertTablet RPC，`:1703` 校验 TSStatus。连接异常可能已经提交；多端点连接分支 `:1710–1716` 还可能重试。它不提供 CAS。`IoTDBClient` 返回 false 必须被新 owner 视为不确定，按 head 内 commit ID 对账；在对账完成前不得发放旧上下文。

单 BLOB head 使用前置条件：只存在唯一合法 owner；同一 key 只写该完整 cell；禁 NULL/部分 envelope/OBJECT 分片；先验证协议、长度、digest、namespace/incarnation；引用对象先持久可读；WAL=SYNC；owner 对确认丢失停止准入并读回同一个 key。并发、多节点故障、灾备回滚 incarnation/fencing 不由一次本机 flock 或本轮实验自动证明。
