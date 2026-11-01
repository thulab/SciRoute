# HDF5 两种正式查询模式

挂载配置仅提供两个 HDF5 执行模式，默认保持 `iotdb-indexed`：

```properties
query.hdf5.mode=iotdb-indexed
# 或 query.hdf5.mode=native-hdf5
```

| 模式 | 跨文件选择 | 文件内读取 |
|---|---|---|
| `native-hdf5` | IoTDB-FS 发布封印的 dataset 目录、属性／版本索引及文件定位 | 受管文件系统中的完整精确版本文件，HDF5 原生索引、过滤器和 `H5Dread` |
| `iotdb-indexed`（默认） | 相同目录与属性索引 | IoTDB-FS layout/chunk 索引、批量 payload 读取与解码 |

这两个值区分取值的数据通路与文件内执行引擎。权限、精确版本绑定、owner 准入、请求预算、跨文件谓词和 typed 输出契约共用；它们不是两套版本管理机制。`native-hdf5` 现要求 owner 模式和显式配置 `storage.native_file_root`。GRIB、Arrow 查询不受本开关影响。

## 原生数据通路

`native-hdf5` 使用 `Hdf5SourceDriver::open_local`。数据库完成跨文件属性谓词、版本、路径范围与 dataset 选择，并授予已授权精确版本的文件能力；文件内读值经只读本地 `pread` VFD 完成。VFD 仅作为范围／累计字节预算和取消的检查接点，不调用数据库 payload 读取、不查询 payload-map，也不经过自身 FUSE 挂载路径。文件内定位、过滤器与解码由 libhdf5 执行。它不是将整个文件先从数据库下载到内存后再读，也不是实验 raw 路径映射。

目录仍保留科学属性与版本谓词，并非退化为只按文件名查路径。`iotdb-indexed` 继续使用数据库 payload 和自建 chunk/layout 索引，默认模式不变。首版数据库 payload 仍用于 indexed/POSIX；完整文件副本的生成和空间成本需要单列。

### 文件位置、发布与补绑定

```properties
storage.version_directory.mode=owner
# 其余 owner/prefix 配置沿用既有 namespace
storage.native_file_root=/absolute/private/native-files
query.hdf5.mode=native-hdf5
```

文件根须属于 daemon 用户、权限0700、位于普通文件系统，路径各组件和文件不得为符号链接。共享内容以原始文件 SHA-256 命名并只读安装。先完成完整文件写入、校验、文件与目录 fsync，再由 owner 在同一次 head 事务中发布精确版本定位。失败留下的未引用文件仅作孤儿保留，不在查询时重建或自动 GC。

数据库定位协议 `iotdbfs-native-file-location/1` 存于 Current 的保留键域，绑定 namespace/incarnation、精确 `(path,version)`、descriptor/seal、payload root、大小、内容摘要和相对位置。namespace-record/3 与文件科学内容不改写；新增定位是显式的目录扩展。旧二进制不能打开含此扩展的 namespace，需使用当前二进制及更新的 owner journal，不提供忽略扩展的兼容读取。

既有版本必须由持有唯一 owner 的离线命令显式补绑定：

```sh
# IOTDB_CONFIG_FILE 指向含原实例、现有 owner/prefix 和 native_file_root 的配置。
iotdb_fs native-file-bind --path /science/file.h5 --version 1790000000000
iotdb_fs native-file-bind --subtree /science
```

补绑定从既有已验证精确版本流式生成完整文件，计为准备成本；不产生新的科学版本。命令逐版本输出结果收据。已有定位仅检查并复用，不替换丢失或被改变的文件。科学语料／case 不变，新的目录 head、准备工作和执行协议进入新 run；历史封印记录、原始时钟与 journal 证据不重写。

每次执行检查受管文件的类型、只读权限、大小、设备／inode 和变更时间；读取前后检查文件及其目录项，保留打开句柄贯穿执行。查询不逐次重算全文件 SHA；不可变性依赖受管根与唯一 owner，不能据此声称对同权限任意篡改具有密码学防护。无绑定、错误版本、文件缺失／替换／改变时明确失败，不回退数据库读取。

原生模式不要求 `index_complete`，不读取自建 chunk 索引页。它仍使用共享 layout 目录中的 dataset 描述与科学绑定信息，以保持现有 dtype、shape、CF/GPS 坐标、掩码、数值转换、投影和 IPC 语义。本次不改变摄取流程或持久格式，不删除已存 layout/chunk 索引；`native-hdf5` 不等同于“完全没有持久化 HDF5 元数据”。

只读、外部链接／外部存储与过滤器限制沿用 SourceDriver 契约。每个 source cursor 的元数据缓存（8 MiB）、转换缓冲、声明 chunk 与结果临时内存接受执行预算预检。此公式不包括原生库内部过滤器 scratch、trace vector 和 control 临时开销，不是全请求／进程 RSS 硬上限；核心会为选中源建立多个 cursor，不能将每 driver 8 MiB 当作全部源合计上限。共享 layout 缓存、owner 热缓存及被计划持有的对象也各有原有生命周期。`query.hdf5.read_parallelism` 对应 indexed chunk 批读，原生路径保留既有 HDF5 库锁。

## 状态、观测与升级

模式在 adapter／挂载进程创建时固定；prepared plan 绑定模式，不能交给另一种引擎执行。结果元数据 `hdf5.query_mode`／`hdf5.payload_backend` 和服务 profile `server_hdf5_execution.mode`／`payload_backend` 报告实际模式及 filesystem/database 通路。原生读取另记录 `hdf5.native.filesystem.opens/calls/bytes/read` 和既有 `hdf5.native.*`；验收还须检查数据库 payload/Store 计数为0，不能只凭模式名称判断。

两种模式共用 [dataset 子结构索引](hdf5-dataset-substructure-index.md) 和 owner 下的跨请求已验证状态复用。每次 execute、发布和新 open 仍经过 owner 准入，缓存不保存 ROI 或最终查询答案，命中仍计请求逻辑预算。

旧 `query.hdf5.catalog_mode=strict|substructure-index` 是历史跨文件发现策略，不是文件内引擎。该键现已移除，出现即明确报错，不提供别名；两种正式模式均使用发布封印目录。无封印旧数据需要显式重摄取，切换引擎不会隐式迁移。

`IOTDBFS_EXPERIMENT_FROZEN_TYPED` 启动环境变量与 `experiment_typed_bind/read/release` socket 请求也已移除，旧请求拒绝执行。普通 typed、Arrow prepared window、授权发布和结果生命周期继续保留。

2026-10-19及FM1首轮中的同名 native 模式使用旧数据库 VFD；这些历史结果不代表当前 FS 数据通路，见总报告 (local archive: `docs/reports/hdf5-route3.md`)。完整性能结论必须来自同一固定 case 的新 run，不能把历史较快 indexed 结果当作原生基线。实现边界及性能判据见数据通路方案 (local archive: `docs/deliverables/hdf5-native-filesystem-plan.md`)。

2026-10-22新增可选`query.hdf5.native_cache=true|false`，默认false；仅影响native原生会话复用，不改变默认查询模式。开启后同一挂载的Adapter共用最多8个file／8个dataset、64 MiB计费池，metadata每file上限2 MiB、raw chunk每dataset 256 KiB；关闭保留原8 MiB／raw 0的一次性通路。原生句柄仅在相同完整head、精确内容／文件身份和选择／slab工作流下复用。空闲池不保存请求授权／取消／账本；每次租用重新绑定，缓存命中仍校验文件并逐范围重放逻辑需求，实际VFD读取单列。该实现及三组2×2消融已完成验收，K2合用改善，K16缓存无稳定额外收益；默认false及原默认引擎保持。详见统一报告 (local archive: `docs/reports/hdf5-route3.md`)，此前无会话池的正式结果不改标为缓存结果。

新增格式中立`typed_batch`控制入口，最多64个已有typed请求，要求Exact路径和版本，返回稳定索引及逐项结果路径。整批共享累计预算，逐项保留执行／发布／新open准入，失败撤销本批结果路由和pin；正常有界内容缓存可按原策略回收不可达共享结果。首版顺序执行，每项Arrow输出与客户端owned数组契约不变，不等同于一个Arrow流或客户端直接H5Dread。

当前FS通路已完成原16个HDF5首轮case的正式三轮1,728条验收；先行N8/N128单轮72条不拼入正式统计。完整case、R/Rd/Rq、实际资源及钟外绑定／启动成本见总报告的新通路结果 (local archive: `docs/reports/hdf5-route3.md`)。后续使用`work/hdf5-native-matrix-20261021/collection-r1/current-locations.json`中的当前journal与受管根，原发布journal只读归档。

2026-10-26 的进程内 Native 读取优化保留原 `H5Dread`：chunked dataset 的 slab 切分在原 row／byte／输出预算内向下对齐 chunk 边界，非对齐 ROI 使用绝对 chunk 边界；边界无法在上限内容纳时仍按原受限大小切分。不会扩大 ROI、改变 C 顺序或增加单批内存额度；批边界与 IPC 分批数可变，不属于固定输出契约。连续布局及 Indexed 切分不变。受管 VFD 继续逐次检查原版本／文件能力、取消和实际读取累计范围／字节预算，无新增跨请求数据缓存。

物理值视图直接转移 `H5Dread` 返回的独占 Float32／Float64／Int32 数组，保留返回后的取消、schema／长度／ROI 校验；Int16→Int32 已由原生读取完成，不再执行重复的压缩／展开。科学值视图的原 packed 输入、缺失值掩码及 CF 转换保留。该优化减少重复读取及内存复制，不解除 libhdf5 库锁，也不增加查询或解码线程；原 Native 资源感知排队仍生效。profile 新增 `hdf5.native.chunk_aligned.cursors`、`hdf5.native.selected_chunk.visits`、`hdf5.native.output.moved.bytes` 和 `hdf5.native.output.copied.bytes`，chunk visits 为预检相交数，不能直接当作 VFD 物理读取数。

## 历史实验

2026-10-26 的 Native HDF5 有界进程后端已根据规模探针 (local archive: `docs/reports/hdf5-native-indexed-scale-probe-20261026.md`)撤回，当前 Native 查询使用进程内执行与既有资源感知排队。`query.hdf5.native_processes` 和专用 helper 不再属于当前配置／构建；旧配置包含该键（包括值0或空值）会在连接数据库及挂载前明确拒绝，需删除该键。当时的[进程池契约](hdf5-native-process-pool.md)、源码快照和实验结果保留用于复核。

`cross_version` 的 FS-scan、FS-index、catalog-native-local、catalog-native-fuse、typed 五个 arm 是基线与原型对照，不是五种产品模式；`multi_file` 的历史三 arm 结果也保持原口径。原始 runner、源码快照和结果不重写。历史 catalog strict 探针和 frozen-typed 用例从正式构建移到 归档索引 (local archive: `experiments/legacy/README.md`)。H01/H08 等阶段契约描述其当时的实现；本页定义当前模式边界。
