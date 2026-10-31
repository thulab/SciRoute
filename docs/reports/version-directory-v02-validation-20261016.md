# V02：不可变内容证明与独立版本封印验收

日期：2026-10-16。开发基线 `5bf4f03`。关联[实施计划](../deliverables/version-directory-content-sharing-plan.md)、[实现契约第 8 节](../contracts/version-directory-content-sharing-v1.md)。

## 1. 实现与启用边界

**V02 已完成。** 新增格式中立的 `ContentArtifacts`：FileDescriptor、FormatProof、VersionSeal，以及分页的 metadata/layout/catalog 内容 image。内容身份不包含外部路径、版本和权限；版本 seal 独立绑定 namespace/incarnation、path/version、描述和证明。既有原始布局/目录编码保留，只有精确字节相同才能共享；五个旧 exact-version envelope 字段移至外层 seal，其余 metadata 不随意裁剪。

用户已确认使用全新 V02 前缀。namespace-record/2 与 content-build/2 profile 随 genesis head 原子发布；V02 拒绝隐式升级 V01，旧前缀和原 journal 使用固定 V01 二进制。默认 strict、每版本独立 payload、既有账号、原服务及许可证保持。未创建新账号/ACL/数据库/实例，没有 payload 去重或 GC。

内容页为至多 60 KiB chunk，索引页至多 64 KiB/256 child、树高至多 8；范围读取只取命中 chunk，完整读取另校验 image 原始 SHA。完整 HDF5/GRIB/Arrow 布局在发布前校验，内容对象与 seal 写入读回后才提交 head；恢复逐版本深验证当前内容闭包。确认提交后清理旧 staging layout/catalog OBJECT 与 metadata 行，保留 payload、secondary dataset/attribute 行；清理失败单独计数，不能撤销已提交结果。

内容缓存按 namespace/incarnation、内容 ID、类型、规则及解码上下文复用。HDF5 directory/page 与 GRIB parsed index 使用可信布局身份；计划、权限、来源和解码数值仍按精确源隔离。相同 layout 不代表相同 payload。

## 2. 验证方式与错误顺序

独立源码/构建位于服务器 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/v02-verification-20261016/{source,build}`，复用已有 SDK、HDF5、Arrow 和 Python 环境，未覆盖 V01 固定构建。构建五个目标：iotdb_fs、unit_tests、v01_store_probe、v02_content_probe、v02_cache_probe。

最终本地与远端冻结源码/CMake/测试/探针共 **270 文件，0 差异**；实际运行的 daemon、两份 V02 probe 及 Store probe SHA 与构建文件逐字节一致。源码清单 SHA `44fb6877d9c5daede321bf6fef36a4466dcedf9a36cae539722f835e241e98f7`，最终测试 XML SHA `4121b2fa2762bea26cc2cfeeb923bd6ae12ab7ef7f40a8db50f728dd8b4b0781`。

| 二进制 | SHA-256 |
|---|---|
| iotdb_fs | `f61c283e16ff01eafcab6698714211b3b476c6380dc3d88d0e33bb41ae859bdc` |
| v02_content_probe | `73d09e05a758becf6c9e44cba248ca334db4ef7c5ffae85dd3b81c6d8c630c1c` |
| v02_cache_probe | `b601b4aa825cf597394092c5574bbdde44bd0a151c9f45a576126f5a17f0d65c` |

统一回归 **449 tests / 44 suites，0 failures**。新增测试覆盖内容/版本身份拆分、解析复用、LRU/逻辑预算、15 MiB 跨 256 chunk 索引边界的局部读取、缺失/损坏引用、错 namespace/path/format/长度、伪造规则/协议/catalog 尺寸、byte-pass 不得携带格式证据；同时覆盖 GRIB/HDF5 共享布局但不同 payload 的值缓存隔离。skeleton 物化测试确保 metadata 谓词与 adapter 收到真实 metadata，拒绝 stale 尺寸/revision。

审查修复了目录枚举过早读取 proof 的问题：inode/parent/source selection 只读取身份与权限；授权后物化所选 exact metadata，再校验内容和执行查询。未选中文件不会因内容 hydrate 被触碰。普通读取/virtual exact 缓存、新 typed 注册/execute 及授权缓存命中都检查当前内容；已打开物化 IPC 的 read/fstat 保持原语义。virtual getattr/xattr 沿用原入口权限规则，未另行改变。

`v02_content_probe` 全程只读核对 head/commit/profile、exact 树、控制引用、完整 image SHA、catalog 统计及外层 seal。`v02_cache_probe` 正常恢复并可重试已提交暂存清理，因此明确不是纯只读；报告区分恢复计数与后续热读计数。

## 3. 真实验收与观测

结果 `experiments/cross_version/results/v02-live-20261016.json` 为 `ok=true`；原始配置、日志与操作序列保留在同名 work，报告未记录凭据。主前缀 **`vd_v02_0b0c2f2098ee4406`**，最终 seq=107，namespace=`8a07ae75-95c1-4265-a365-ed6a90d587d4`，incarnation=`946f2702-b452-4eb2-8342-c0fd663300ef`，root=`6d14b33b847d6ceeea3fec1114820abcb66550a206f14f07fd469c231676f878`。

复用原 raw corpus，一个逻辑文件/格式/场景各 8 版，合计 32 科学版/128 typed 单元。首次、rename 后及崩溃恢复后分别核对全文件 SHA 与独立 native 数值/掩码/provenance；删除一个 GRIB 精确版本后，31 科学版/124 单元保持可读，删除版未复活。普通 create/O_TRUNC 产生的空版本另计。

| 验收 | 结果 |
|---|---|
| identical 组各格式 8 版 | 每组 descriptor/proof/metadata/layout 各 1 个，seal 恰 8 个；HDF5 catalog 也只有 1 个。 |
| single_unit 与 identical 合计各格式 16 版 | 本批数据布局、metadata 字节仍相同，仍复用同一组内容 ID；各版变化 payload 的数值 oracle 均独立通过。 |
| POSIX 修改 | chmod/chown/utimens 保留 descriptor、proof、metadata/layout/catalog 和 seal 引用。 |
| 整树 rename | 一次 head；共享内容 ID 不变，每个外层 seal 都按新路径重绑。 |
| 精确删除 | 一次 head；其它版本保留，跨原 DataNode SIGKILL/restart 后 tombstone 未复活。 |
| 挂载/协议门禁 | 同 prefix 第二 owner、重复 initialize 拒绝；V01 前缀拒绝升级且原 head/journal 摘要不变；恢复 ASYNC 后 owner 拒绝激活。 |
| 权限 | 9 项非 root oracle 通过，包括历史权限/当前祖先、补充组、EPERM、EBUSY、暖结果撤权、新注册/open 拒绝及已打开 IPC read/fstat。 |
| 实际 Store 故障包装 | 沿用 8 项真实持久化故障控制流，全部通过；进程内 wrapper 不冒充真实网络丢 ACK。 |

GRIB 延用原 message index，不新增它原本不存在的 dataset-catalog artifact，`catalog_id` 为空；其布局、metadata 与 parsed-index 共享已验证。descriptor/proof 是 metadata/layout 的证明身份，不是整个 payload 文件摘要。

冷 cache 探针恢复了 66 个文件版本（包括空版和普通文件），恢复阶段 767 次内容对象读取、300,237 个被核验编码字节，metadata/catalog/layout 校验计数为 66/16/66。随后两次遍历计数均保持不变，没有新增内容对象读取；对象缓存命中分别累计 144、288，head 不变，暂存清理失败 0。缓存自身计费 735,789 B/167 条目，无 eviction。这证明本轮已驻留内容的解析与页复用，不等同于整个查询零 SQL。

单次正确性运行的成本观测：

| 指标 | 观测 |
|---|---:|
| 初始化 mount（含 schema/genesis） | 2.636511 s |
| crash 后 fresh mount | 22.716229 s |
| 独立 cache probe 完整恢复 | 16.0695 s |
| HDF5 / GRIB 发布中位数（各 16 版） | 3.929230 / 2.898096 s |
| 整树 rename | 99.883718 s |
| daemon 初始/发布后/恢复读取后 RSS | 21,232 / 67,972 / 42,284 KiB |
| cache probe 当前/峰值 RSS | 19,922,944 / 93,044,736 B |
| 初始化 `_objects` | 12 行 / 1,222 B |
| 发布/rename/delete 后 `_objects` | 1,222 行 / 2,839,504 B |
| 权限 oracle 后最终 `_objects` | 1,365 行 / 3,113,882 B |

最终 catalog-page 占 2,916,730 B/808 行，目录 COW 写入开销占主导；其余包括 135 个 version-seal、5 个 descriptor/proof 及共享内容对象。旧根/旧 seal 仍在，未执行 GC。V01 与本轮的版本/操作数不同，不能直接据两次总量计算阶段收益。

## 4. 默认 strict 与原服务

同一 daemon 使用默认 strict、不设置任何 version_directory 配置，复验原 publication 的 **256 历史科学版/1,024 typed 单元**与两组 8 个 latest/属性谓词查询通过。复用了 `v01_strict_validation.py` 的检查器，新报告为 `results/v02-default-strict-20261016/publication.json`；实际新二进制 SHA 写入报告，原 publication 报告和绑定未改写。

维护仅操作被识别的原 DataNode；原 ConfigNode、节点身份、许可证保留，原配置 SHA 恢复为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`。结束时 DataNode PID 为 2070389（本轮观测值）。独立只读配置查询确认实际 applied 值为 **ASYNC / true**；采用已由 V00/V01 验证的 worker（SHA `2575ba1f3c5b151a00521bd94cc90fdc30e1f91d1fdcd0490a80f985102e8221`），post-restore 记录单独归档，不改写成功结果。首次误选旧版 worker 的配置命令失败仅为只读诊断，未触碰服务配置。新 owner 在原 ASYNC 配置下拒绝激活，没有降级继续运行；owner journal 0700/state 0600 保留。未来打开该 owner 仍须显式满足 SYNC/禁 partial insert 门禁。

## 5. 成本与后续范围

写入量以 `_objects.value` 的逻辑 BLOB 字节计，包含旧根、旧 seal、事务中间孤儿，不能当成唯一可达字节或数据库磁盘节省。payload 仍按版本存储，rename 延用逐版本复制；WAL/TsFile/OBJECT 分配、数据库与 OS 缓存不在此计数内。缓存持有量是内部计费估算，外部借用 shared_ptr、临时解析内存另计；RSS 为探针/daemon 观测，不含数据库。

冷恢复当前逐版本强制读取与解析，逻辑预算按重复引用累计，不能据热缓存复用宣称冷启动解析消除。发布保留原 catalog/secondary 校验，因此也不宣称写入端每份内容仅解析一次。仍存在有界 Exact 扫描、精确源/marker/secondary 重验和 SQL；完整 ReadContext、最终发布准入、统一跨调用预算/deadline、发现性能消融属于 V03，payload 映射共享、租约 GC、迁移分别属于 V04–V06。

固定 N=128 主指标 A 保持 HDF5 106.811351 ms、GRIB 57.404444 ms；本轮正确性 runner 的时间/内存观测不替代公平性能消融，不宣称压缩率收益。


机器可读结果见 [v02-acceptance-20261016.json](../../experiments/cross_version/results/v02-acceptance-20261016.json)，列出完整成功报告、测试、操作日志、源码/二进制绑定和恢复记录的远端路径与 SHA。所有 Git 写操作仍由用户执行，本轮未提交或执行 Git 写操作。
