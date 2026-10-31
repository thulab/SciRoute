# V00 现行版本语义与提交原语验证

日期：2026-10-15。关联：[冻结契约](../contracts/version-directory-content-sharing-v1.md)、[持久化写入口审计](version-directory-v00-write-audit-20261015.md)。

已完成版本目录相关 **155 项单元测试（94 + 61）**，全部通过；其中 5 项为本轮新增。另有 **9 项 live FUSE/socket 语义检查**通过。单元测试使用与本地生产源码指纹相同的冻结构建对象；live 检查使用已有实验 daemon，两者的构建身份分别记录。本文尚不据此宣称新版本目录协议、内容共享、GC 或 owner 已完成实现。

## 1. 固定源码与构建身份

服务器为 `root@192.168.99.49`。下文远端路径均以 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/` 为根。

单元测试基准为 `experiments/multi_file/work/hdf5-manifest-marker-20261015/{source,build}`。本地生产源文件及构建描述的 179 项 SHA-256 清单，与该冻结 `source` 逐项比较，**179 项相同、0 项差异**。清单保存于远端 `experiments/cross_version/work/v00-unit-20261015/v00-local-source-sha.json`，文件 SHA-256 为 `3fc26df7bb1c124a9ec8e00b771f087aba4987b23d4ab1d3b5aac1a4f4c53c08`。此核对覆盖清单中的生产源码/头文件和构建描述，不表示当前测试源码与旧快照完全相同。

本轮将 3 个修改后的测试文件复制到独立 work 目录，按原 `flags.make` 的 `-O3 -DNDEBUG -std=c++17`、宏和 include 重编译。相对依赖 `tests/support` 链接到冻结源对应目录；原 `link.txt` 仅替换这 3 个测试对象和输出位置。第 2 轮新增空版本选择测试时只重编译变化的 QueryPlanning 对象，其余两对象在源码 SHA 核对后复用。原基准 build 未被替换。

独立测试二进制为远端 `experiments/cross_version/work/v00-unit-20261015/unit_tests.independent`，SHA-256：

```text
fff1f9fd748c30814171f6a71981a36159118e8b12e19f80ad719c1a9b44bddb
```

3 个最终测试源码指纹如下，均与本地对应文件一致。

| 测试文件（相对 tests/unit） | SHA-256 |
|---|---|
| `query/core/test_QueryPlanning.cpp` | `d96936bc08ae474b46aea7b8e3a2d7d7ce51b5920f0ff37417124e6d221dd634` |
| `query/core/test_DatasetCatalogQuery.cpp` | `2ee14187526c652e3c36c6025c8b385602da058cc76862912cbe2af4dbb17fb5` |
| `test_StructuredBlobStorage.cpp` | `537ac7e6d47a7bfe5790a4f29f9083bdb9bf22a064a20542bb43d7989c207240` |

live 检查沿用 `experiments/multi_file/work/runner-20261013/environment.json`，环境文件 SHA-256 为 `cf9978c4e43ef6045eb1bfb8642f78bd26c760eec360a59b77c34a879e89944b`。其 daemon 为 `experiments/single_file/work/grib-binary-cache-20261012/build/bin/iotdb_fs`，SHA-256 为 `f4c4786a949474fc4129348546983f9e869417991eb10e16fad5ff7e667ba75e`。这是**已有实验二进制**；本文不将它称作最新生产构建，也不将上面的 179 项源指纹核对自动套用于该 daemon。

## 2. 单元测试：155 项通过

| 分组 | 数量 | 失败 / 错误 / 禁用 | 远端证据（相对 experiments/cross_version/work/v00-unit-20261015） |
|---|---:|---|---|
| 版本选择、查询规划、格式目录、Storage | 94 | 0 / 0 / 0 | `independent-status.json`、`independent-tests.log`、`independent-tests.xml` |
| namespace 锁、源/祖先授权、授权缓存、IPC store、manifest/目录重验 | 61 | 0 / 0 / 0 | `dependency-status.json`、`dependency-tests.log`、`dependency-tests.xml` |

`independent-status.json` SHA-256 为 `a9277f527a6d8a49041db4a416472e17fa8e22288b7253f325f8e2e7b299cb47`；`dependency-status.json` 为 `1b660eac829a66543bd067ad333b0574033d6f177f2df542a4f7d5dc6eac7c38`。JSON 保留实际 argv、源码/二进制指纹及结果计数；编译、链接日志也在同目录。首轮 93 项通过结果保留为 `r1-independent-*`，最终计数使用第 2 轮 94 项，不重复相加。

核心 filter：

```text
AdapterPlan.*:DatasetCatalog.*:DatasetCompiledPredicate.*:FormatQueryAdapter.*:PreparedQuery.*:PublishedVersionCatalog.*:QueryRequest.*:QuerySource.*:StructuredBlobStorage.*
```

依赖 filter：

```text
NamespaceWriterLock.*:CatalogSourceAuthorization.*:CatalogParentAuthorization.*:TypedAuthorizationCache.*:TypedResultStoreTest.*:ManifestRevalidation*:DatasetCatalogMarker*:DatasetCatalogVerification*:DatasetSubstructureIndex.*
```

新增 5 项冻结以下行为：

| 测试 | 断言 |
|---|---|
| `PublishedVersionCatalog.RemovingLatestRevealsSurvivingVersionWithoutRebindingExact` | 删除最新科学版本后，新的 Latest 选择存活旧版；已有选择保留原 exact 身份，已删 exact 不偷偷回退。 |
| `PublishedVersionCatalog.LatestAfterDeletionIncludesEmptyVersionsUnlessFormatFiltered` | generic Latest 可以选择存活空版本；HDF5 格式过滤后选择存活 HDF5 旧版。 |
| `PublishedVersionCatalog.HistoricalPermissionEvidenceKeepsExactInodeAndCurrentParents` | 历史源权限证据取 exact inode，祖先证据取当前目录；不混用最新源 inode。 |
| `DatasetCatalog.SourceCatalogFailurePrecedesAuthorizationAndArtifactErrors` | 源目录失败优先返回；授权回调和 artifact 读取均不执行。 |
| `StructuredBlobStorage.IdenticalVersionsKeepIndependentDeletionAndReadIdentity` | 同字节不同版本的删除、tombstone 和读取身份独立；删除一个版本不破坏另一个版本的 payload/layout。 |

最后一项使用内存 backend，删除后的旧 Storage reader 读取 payload 返回 NotFound；它不承诺已删除底层对象仍可读。已打开物化 IPC 的语义由下方 live 检查单独覆盖。错误顺序和锁测试也不构成 IoTDB 跨对象事务或多 owner 排他的证明。

## 3. live FUSE/socket：9 项通过

机器可读原始结果为远端 `experiments/cross_version/results/v00-permissions-20261015-r2.json`（本地忽略目录保留副本），`ok=true`，SHA-256 为 `319c627740a3b8fd91180702eae07255be33d9a3d9209b0db97ee195cc66e039`。远端同名结果位于 `experiments/cross_version/results/`；fixture、mount 日志及配置位于 `experiments/cross_version/work/v00-permissions-20261015-r2/`。

检查使用已有实验数据库 `h09probe1790939889dbfc1f`，仅操作随机 `/v00-semantic-*` 子树和 2,176 字节 HDF5 fixture；挂载设置 `attr_timeout=0,entry_timeout=0`。非 root 子进程身份为 UID/GID 65534，补充组为 `[40077]`；v1 为 root:40077 / 0640，v2 为 root:root / 0600。测试没有以 root 成功读取替代非特权授权验证。

| JSON checks 键 | 实测结果 |
|---|---|
| `historical_exact_inode_group_allowed` | 补充组可读取历史普通路径、已有 IPC，并可新注册/读取相同 exact 版本结果。 |
| `latest_different_inode_group_denied` | 最新版本独立 inode 无组读取权限；普通读取、IPC 新打开和注册均拒绝。 |
| `historical_uses_current_ancestor_search_permissions` | 当前祖先改为 0700 后，历史版本普通读取、IPC 新打开和注册均拒绝。 |
| `version_chmod_eperm` | 精确版本路径 chmod 返回 EPERM。 |
| `version_chown_eperm` | 精确版本路径 chown 返回 EPERM。 |
| `ordinary_open_unlink_ebusy` | 持有普通文件句柄时 unlink 返回 EBUSY。 |
| `opened_ipc_survives_revoke_new_open_denied` | 撤销祖先搜索权限后，已打开 IPC 仍可 read/fstat；同一结果的新 open 返回 EACCES。 |
| `delete_latest_respects_format_scope_exact_v2_absent` | 删除 v2 后 exact 路径 ENOENT；generic 与 HDF5 Latest 分别选择各自范围内最大存活版本。 |
| `historical_permission_still_valid_after_latest_delete` | 删除 v2 后 v1 的历史普通读取、IPC 读取与重新注册仍按 v1 权限成功。 |

fixture SHA-256 为 `0a70463d0a0f9b49a8954f80cd7e31d260143245c12fb6811172214aa14472c7`；两次科学内容写入字节相同，真实 HDF5 版本为 v1=`1792049870440`、v2=`1792049872572`。这验证了相同内容仍保留独立版本身份与权限。

## 4. 删除后的 Latest：首轮 oracle 修正

首轮 live 结果 `experiments/cross_version/results/v00-permissions-20261015.json` 为 `ok=false`，保留原始错误 `ValueError: deleting latest did not expose surviving v1`，并记录清理阶段 ENOTEMPTY。其 SHA-256 为 `9ae6d88b0a07f727374a7b72dc41ac762b5d40526c581d186ac2721d4204568f`。首轮不能算通过，也不能仅凭该错误判定生产选择器失败。

首轮预期遗漏了已有 daemon 在 create/O_TRUNC 流程中发布的空 byte-passthrough 版本。第 2 轮补充完整历史 inventory，区分 generic Latest、HDF5 Latest 和普通路径字节，并按所有 exact 版本清理自有子树；没有为通过该检查修改生产选择规则。

第 2 轮在删除 v2 后实测：

| 访问范围 | 选择 / 内容 |
|---|---|
| generic Latest，包含 byte-passthrough | `1792049872272`；format=null，payload=0；该版本位于 v1 与 v2 之间。 |
| HDF5 Latest，排除 byte-passthrough | `1792049870440`，即存活 HDF5 v1。 |
| 普通裸路径 `versions.h5` | 0 字节，SHA-256=`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`，与 generic Latest 对应。 |
| 精确 v2 路径 | ENOENT；全版本 inventory 中已不存在 v2。 |

因此契约必须保留“格式过滤在按 path 取 Latest 之前”的语义。不能把“删除最新 HDF5 后 HDF5 Latest 显露 v1”泛化成“普通路径一定回到 HDF5 v1”；也不能把空创建/截断快照当成科学格式版本计数。

## 5. 单 BLOB head、并发可见性与恢复

### 5.1 实际发行包与 WAL 证据

用户明确授权使用原数据库维护窗口，保留原许可证，不再尝试独立实例。实际服务是 `runtime/releases/timechodb-2.0.11-SNAPSHOT-bin`，DataNode RPC `127.0.0.1:33667`，ConfigNode 保持运行。仅创建随机 `v00_head_<16hex>` 测试数据库；没有改写现有业务表。每次信号前核对 DataNode 的 PID、启动 ticks、命令摘要和 cwd；在原目录使用原启动脚本/环境恢复。

`timechodb-server-2.0.11-SNAPSHOT.jar` 的 SHA-256 为 `763d9c7f7365090f7c9ff5fadee1212791c9f7f473c0f5f567d85aea97f301e8`；SDK `libiotdb_session.so.2.0.11-SNAPSHOT` 为 `ca8a4ac51ec57fe8523f74c978e21c14090f84a3c535d7c6bb8fa3fbdc4edea2`。完整 JAR/SDK/source 清单保存在 head 结果 JSON 的 identity 中。JAR 的 git.properties 为 `2e8852d`、`dirty=true`，与 checkout `2e8852d5e98c877f75b84d8db0b0ce0fac3f92e6` 的提交前缀一致，但不能宣称源码与发行包完全等同。

为消除这一证据缺口，直接对实际 JAR 执行 javap，核对以下正常成功路径：

- `WALInfoEntry` 仅 SYNC 设置 wait=true，经 `WALEntry` → `WALFlushListener` 传到 `AbstractResultListener`；后者在 RUNNING 时等待。
- `TsFileProcessor` 先 WAL.log → waitForResult，成功后才写入 memtable。
- `WALBuffer$SyncBufferTask` 正常路径先 force，之后才对等待的 listener 标记 succeed；force 异常不走成功通知路径。
- `WALWriter` 继承 `wal.io.LogWriter`，后者最终调用 `FileChannel.force`。

9 份字节码输出及摘要位于 `experiments/cross_version/work/v00-head-existing-20261015-r2/`，清单 `bytecode-wal-evidence.json` SHA-256 为 `a034caac8b49f8c6a8510039656064881663452d7d2ece3649f0f6cfe487f42b`。`release-build-binding.json` 记录上述对应关系和限制。源码中单 BLOB 的 Binary 单元、同步更新/行计数与 WAL 完整反序列化分析见 [写入口审计第 6 节](version-directory-v00-write-audit-20261015.md#6-实际远端源码的单-blob-head--wal-证据)。不是把多 FIELD、多个 Tablet 或 OBJECT 分片写入当作原子事务。

### 5.2 10 组恢复用例

结果 `experiments/cross_version/results/v00-head-existing-20261015-r2.json`：`complete=true`、`original_service_restored=true`，SHA-256 为 `bda562c0dcc211d83dccf9de58d5a0c974e7bbae20f844a60c8b5509e752dfe7`。同名 work 目录保存操作日志、原配置、编译/启动日志和执行脚本快照。

实验只替换固定 `(table=head, ns=n, time=0)` 的一个 BLOB FIELD。每个值包含协议标识、序号、序号相关填充及 SHA-256，读回检查完整长度、摘要和全部填充，避免只检查 sequence 漏掉混合代次。该 envelope 是实验 oracle，不冒充未来生产 NamespaceHead codec。

| 用例 | 次数/规模 | 实测结果 |
|---|---|---|
| 并发完整可见性 | 200 次更新，256/4,096 字节交替；两个 reader 分别 134/137 次观测 | 271 次完整值读取，0 撕裂/混合值，最终序号 200。 |
| 成功 ACK 后 SIGKILL，不手动 FLUSH | 201、202、203 三次 | 每次重启均恢复刚确认的完整值。 |
| 写对象但未切 head 后 SIGKILL | 65,536 字节对象 | 对象完整保留；head 仍为 203，未把 staging 当提交。 |
| 丢弃真实 Thrift 响应 | 提交 204 | 客户端未收到 ACK；独立 reader 已见 204；杀客户端及 DataNode 后重启仍为 204，按 head 身份对账。 |
| 请求已发送、未消费 ACK 时 SIGKILL | 205/206/207；延迟 0/1/10 ms | 三次均恢复此前完整 204，属于允许的旧值；没有半值。不能据此称这三次请求已提交。 |
| 对测试库 FLUSH 后 SIGKILL | 1 次 | 仍恢复完整 204。 |

本轮共 9 次故障 SIGKILL；DataNode 每次在原目录恢复，ConfigNode 未停止。全套使用追加 `wal_mode=SYNC`、`enable_partial_insert=false` 后重新启动的原服务，配置 SHA-256 为 `3ee99cc25356ce74e8847d9ad89707438d946257fc3e88184b619f7c4343958e`。每次启动/定向信号的 UTC 时间、PID、配置摘要均在 `operations.jsonl`。

### 5.3 运行时配置和最终服务恢复

补充短轮 `results/v00-head-effective-20261015-r3.json` 已通过，SHA-256 为 `82246495949f089374c4661d0cc223aa07625c66bdd1772ae5efa7a48af84c14`。它复用成功库 `v00_head_aaa2a839922741a8` 的既有 region，使用全新 tag `9ee5a66449f64f74`，保留全套测试原来的 `ns=n` head。`SHOW ALL CONFIGURATION ON 1` 在初始、SYNC 启动后、ACK/SIGKILL 恢复后及最终恢复后分别读取内存 applied 值/默认值，结果为 ASYNC/true → SYNC/false → SYNC/false → ASYNC/true。ACK 后完整 head 恢复为 1，短轮 `complete=true`、`original_service_restored=true`。全套 10 组与本短轮 1 组分别报告，不将短轮查询倒写为全套每次启动都实测了 effective 配置。

原配置字节的 SHA-256 为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`；全套结束已恢复该值并重启。原设置没有显式覆盖 WAL/partial insert，恢复后的 `SHOW ALL CONFIGURATION ON 1` 读取内存 applied properties，显示默认 ASYNC/true。该恢复后查询本身不能倒推成测试期间的运行时 SYNC 观测；上述短轮补齐了直接观察。最终 DataNode PID 为 `966492`（仅记录本轮结束时状态，后续启动可变）。

许可证、`.env` 和 `activation/system_info` 的初始/最终摘要逐项相同；未请求、生成或替换原服务证书。原许可证内容和数据库凭据没有写入结果，配置备份和私有日志保存在服务器 work。

## 6. 失败尝试与可溯源边界

- 早期隔离实例尝试因初始账号假设、随后因实例许可证身份不匹配而未进入 head 实验，结果为 complete=false；它们不计入原语验收。用户明确要求使用原实例后，脚本已移除独立实例/新激活分支，不再启动隔离实例。
- 原实例首轮 `results/v00-head-existing-20261015.json` 完成并发可见性后，在首次 SIGKILL 后遇到进程退出/端口释放竞态，脚本报告恢复失败。按原配置手动恢复服务，操作追加在该轮 `operations.jsonl` 的 `manual_restore_original_after_port_close_race`。该失败 JSON 原样保留；没有将它改写为成功。随后脚本增加端口释放等待、原子接管启动 PID、独立启动 session、信号清理及原配置 fsync，完整新轮结果见上节。
- 补充配置验证的前两轮新库写入返回 906。ConfigNode 日志确认原因是 CREATE_SCHEMA_REGION 先报 901：预计 direct buffer 分配 1,828,716,544 B 超过预算 1,825,361,100 B；不是只等心跳即可解决。第一轮已经观察到 effective SYNC/false 并恢复 ASYNC/true，但未执行新增 crash 用例；第二轮在停服前的 setup 失败，未改变原服务。证据 `work/v00-head-effective-20261015-r2/region-diagnostic.json` SHA-256=`fe5057867d15759166bb2daaf67213439cbbfecc798f5a7f71b4dc62f318bdb1`；当时 CN/DN 均 Running/ACTIVATED、既有 61 个 region 全部 Running。最终复用成功实验库和新 tag，不再增加 region、不扩大内存配置。
- 权限首轮 oracle 和清理问题见第 4 节；新增真实版本 inventory 后复验，不修改生产选择规则去迎合错误 oracle。

这些实验证明的是指定 build、单 owner 假设、单 BLOB 完整替换、SYNC WAL 条件下的进程崩溃行为。没有断电/设备缓存失信、多节点 failover、多 writer CAS 或灾备回滚测试；新协议 owner、引用闭包、fencing、incarnation 恢复、COW 树和 GC 仍由 V01–V06 实现与验收。原服务恢复 ASYNC 后，不能将 SYNC 条件下的保证套用到现行默认配置。

## 7. 原跨版本数据复验与 V00 结论

最终恢复原服务后，在新的独立 FUSE mount 对原前缀 `/cross-version-1b723c3e5e464290` 重新审计：**256 个历史科学版本全文件 SHA、1,024 个 typed 单元数值/掩码/provenance、32 路径 latest 与格式历史集合、8 组 latest/谓词查询及 raw 源指纹全部通过**。结果为 `results/v00-final-recovery-20261015/publication.json`，`complete=true`，SHA-256=`fb6b98f22888c50dfcf758e2cd92caf8d2dc5660059f76c0a3449b84e16d5bdb`。使用原 publication 记录的副本，不重发业务数据；验证挂载结束后卸载。

**V00 完成**：写入口清单、必要设计决策、协议与预算边界、现行语义回归和 head 候选原语证据均已交付。小型 [验收摘要](../../experiments/cross_version/results/v00-acceptance-20261015.json)汇总计数与远端证据摘要，详细失败与成功原始记录全部保留。D1–D5 已由用户确认，现行性能基线 A=106.811351 ms 和同期 GRIB=57.404444 ms 保持不变；本轮是正确性/可行性验证，没有性能收益结论。
