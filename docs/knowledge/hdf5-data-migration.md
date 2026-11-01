# H09：旧 HDF5 数据重摄取

> 2026-10-06 扩展：当前写入协议为 layout `/5`；新增 GEOS 压力轴证据，旧 `/4` 保留物理查询但不提供压力语义。下述 H03–H11 记录保留其当时版本；现行操作见 [dataset 发现与 CF 查询](../contracts/hdf5-dataset-discovery.md)及 GEOS 生产多时次验收 (local archive: `docs/reports/hdf5-geos-production-multitime.md`)。

状态（2026-10-02）：**H09 已完成小规模验收**。Linux 完整构建、1,254/1,254 CTest 与隔离 IoTDB/FUSE 迁移验证通过；本地公共 journal 15/15 ASan/UBSan、策略／服务 26/26 普通及 ASan/UBSan 通过。范围仅为本次隔离数据库，不代表现有生产数据已经迁移。

## 范围与不变量

用户确认首版只处理 **inventory 时每个路径的最新 byte pass-through 版本**。原始字节必须能够识别为 HDF5，并至少有一个符合当前 typed profile 的 dataset。迁移经 `IngestionModule` 创建新的 published version，生成 `format_id=hdf5`、layout `/3` 和格式摘要；源、新版本的完整 B SHA-256 必须相同。不会转换 HDF5、重分块、重编码、重写旧版本或删除历史数据。

已注册 HDF5 identity 且使用旧 layout `/1`、`/2` 的版本只报告为 `requires_reingestion`，**不属于本次自动迁移候选**；其普通原始字节读取保持不变，当前 typed 查询仍要求 `/3`。以后扩展旧 layout 升级需要另行确定范围。当前 `/3` 版本检查身份、目录、全部索引页和摘要后列为 `already_indexed`；不通过修改文件后缀或手动编辑 manifest 来触发升级。

公共实现位于 `migration/ReingestionService`、`MigrationCli`、`MigrationJournal`。`ArrowReingestionPolicy` 与 `Hdf5ReingestionPolicy` 各自识别、解释和验证格式，避免复制另一套 publication、journal 或 resume 状态机。仅安装 adapter 不会改变已经发布的 pass-through 身份。

## 停写与锁

迁移按离线、单写入者运行：停止目标 namespace 的本机挂载及所有其他主机写入者，并确保整个批次不会 rename/unlink 源路径。准备可恢复备份。持有 published read handle 不等于 backend 会为它保留被删除的源字节；本机文件锁也不是跨主机互斥或分布式 CAS。

FUSE daemon 和 CLI 共用 `NamespaceWriterLock`。推荐显式设置同一个绝对路径；父目录须存在且可写，锁文件必须由执行用户拥有、为单链接普通文件且不可被组或其他用户写入。未设置时，路径由配置中的 host、port、database 字符串派生到 `/tmp`。连接同一数据库的不同主机别名可能产生不同默认路径，因此不能用别名绕过停写或锁配置。

每个报告目录还持有 `.migration.lock`，防止不同 namespace 的命令同时改写同一份报告。这两个锁文件不会主动删除；锁随进程退出释放，不能通过删除仍被持有的锁文件恢复运行。

## 命令与批次操作

使用与目标挂载相同的配置。以下路径为占位符，应替换为本机的实际配置、锁和持久报告目录：

```sh
export IOTDB_CONFIG_FILE=/absolute/path/iotdb.properties
export IOTDBFS_NAMESPACE_LOCK=/absolute/path/shared/namespace.lock

./bin/iotdb_fs hdf5-migrate inventory \
  --report /absolute/path/hdf5-migration-batch01 \
  --path /science/observation.bin
./bin/iotdb_fs hdf5-migrate run \
  --report /absolute/path/hdf5-migration-batch01
./bin/iotdb_fs hdf5-migrate resume \
  --report /absolute/path/hdf5-migration-batch01
```

`--path` 是精确逻辑路径，不按后缀过滤。省略它会检查目标 namespace 的所有已发布路径；执行大批量迁移前应先审阅范围。`run`/`resume` 的可选 `--path` 只能筛选冻结 inventory 中已有的路径，不会重新枚举扩大范围。

1. 停写并确认配置、锁和备份。先对一个小文件执行 `inventory`；它不发布或修改数据库版本，但会读取实际 B、生成候选 layout 并计算 hash，不能视为廉价的文件名列表操作。
2. 审阅 `inventory.jsonl` 中的分类、dataset profile、部分可查询标记和错误，再执行 `run`。
3. 对每个 `verified` 项比较旧、新精确版本的普通读取、大小和 SHA-256；核对新版本 dataset 子数组与原生 HDF5 oracle。需要 GPS 等科学坐标时仍须显式绑定，不由迁移自动推断语义。
4. 出错或中断后保留整个报告目录，解决停写、连接或源冲突问题，再运行 `resume`。不要手动把状态改成成功，也不要重新执行 inventory 覆盖同一报告。
5. 小批验收通过后，再为后续已确认范围创建新的报告目录。原有查询或实验的精确版本不会自动改成新版本。

命令返回 `0` 表示对应操作完成，`1` 表示执行、验证或报告失败，`2` 表示参数错误，`3` 表示锁不可用。inventory 中的非候选项不会因为命令返回 `0` 就变成可迁移或可查询；仍须审阅各项 `kind` 和原因。

## Inventory 分类

| `kind` | 含义与行为 |
| --- | --- |
| `candidate` | 最新 pass-through，内容识别通过且至少一个 dataset 可查询；允许新版本重摄取。 |
| `historical_version` | 历史版本保留，列出身份和大小，不自动迁移，也不逐个全量读取。 |
| `already_indexed` | 已注册当前 HDF5 identity，layout `/3` 与摘要检查通过；不重新发布。具体 dataset 能力见 profile。 |
| `requires_reingestion` | 已注册旧 `/1`、`/2`；明确报告，首版不自动处理。 |
| `unsupported` | 能识别 HDF5，但当前 profile 无可查询 dataset；例如仅有不支持的 datatype、存储方式或过滤器。 |
| `other_format` | 已有其他已发布 format identity；不会凭内容猜测覆盖身份。 |
| `not_format` | 内容不属于可识别的 HDF5，而不是根据 `.h5`/`.hdf5` 后缀作判断。 |
| `error` | 读取、损坏、身份／索引验证或其他检查失败；不能当作可迁移候选。 |

`partially_queryable` 独立于迁移资格：例如文件有一个支持的 Float64 dataset 和一个不支持的字符串 dataset，仍可作为 candidate；迁移不会让后者变得可查询。`profile.datasets` 列出 object ID、路径、原生类型、元素大小、shape、存储方式、chunk shape、filters、索引完整性、已分配 chunk 数、可查询性与原因。hard-link 别名不产生另一份独立物理数据。

## 统一报告协议与恢复

Arrow 和 HDF5 使用同一套报告结构。旧 `A12I1`/`A12J1` 报告不被新 CLI 读取或升级；归档旧报告，使用新目录重新 inventory。旧报告所引用的源/目标版本不会因此被修改。

| 文件 | 用途 |
| --- | --- |
| `report.manifest` | `IOTDBFS-MIGRATION-REPORT/1`；绑定配置的 host、port、database、adapter 策略、目标 layout protocol 和冻结 inventory 的 SHA-256。 |
| `inventory.tsv` | `IOTDBFS-MIGRATION-INVENTORY/1`；机器读取的精确源 key、大小、分类、B SHA-256 和预期 layout SHA-256。 |
| `inventory.jsonl` | 人工审阅的完整分类、原因和格式 profile；不是执行输入。 |
| `journal.log` | `IOTDBFS-MIGRATION-JOURNAL/1`；追加记录每次精确目标 key、两个 B hash、预期 layout hash、策略、状态和错误。 |
| `results.jsonl` | 每个源版本的最近一次结果摘要；完整尝试历史仍在 journal 中。 |

`report.manifest` 在 inventory 文件完成并同步后最后写入。新 inventory 要求目录中不存在旧 manifest/inventory/journal/results。每次写入前的 `started` 事件必须完成文件及目录同步，之后才允许数据库摄取。报告读取和单次写入有界：单文件最多 64 MiB、机器记录单行最多 1 MiB；超限会明确失败。

`run`/`resume` 先核对 namespace、策略、layout 和 inventory digest，再解析 journal；只有最后一条没有换行结束的不完整追加可以截断修复，完整损坏行会报错。不同 endpoint/database、不同 adapter/layout 或被修改的 inventory 不会因路径和版本相同而复用报告。连接密码不写入报告；配置中的 host 别名变化也会触发绑定不匹配。

恢复分三种情况：

- **目标已经发布**：重新核验源/目标 B hash、由精确源重新生成的预期 layout、目标完整 layout、format identity、摘要和 namespace inode。验证通过后重记 `verified` 并复用原目标；失败保留为 `published_unverified`，不覆盖目标、不分配替代版本。
- **目标不存在或明确未发布**：可以重新尝试。未发布目标保留并记录原因，不把它伪装成已发布结果；新目标版本必须大于 journal 中所有已预留版本，即使系统时钟回退也不能重用旧预留 key。
- **backend 状态读取失败**：立即失败，不能把连接或读取错误当作目标不存在。重试仍须使用同一报告并重新检查。

摄取前和 publication 前都会检查精确源及 latest，发现变化报告 `conflict`。这些检查不能替代单写入者边界。若新版本已经发布但验证失败，当前服务不自动回滚 latest；先调查并排除该目标，再决定后续操作。不要删除源版本，也不要手动更改旧 manifest。

## 验证入口与当前证据

隔离脚本为 `tests/benchmarks/hdf5_query/h09_migration_smoke.py`，fixture 工具为构建目录中的 `bin/h09_migration_fixture`。脚本从指定的现有进程读取连接配置而不打印密码，为测试创建独立数据库和报告目录，不对该现有挂载的数据执行迁移：

```sh
/path/to/pyarrow-python tests/benchmarks/hdf5_query/h09_migration_smoke.py \
  --source-pid EXISTING_DAEMON_PID \
  --build /absolute/path/build \
  --reports-parent /absolute/path/isolated-h09-reports \
  --prepared /absolute/path/h08-prepared
```

`--prepared` 使用 H08 的小型压缩 fixture、GWOSC H1 原始文件和 NumPy oracle manifest。脚本包含 latest-only、历史保留、已索引／其他格式／旧 layout 分类、精确版本 hash、重复恢复、latest 冲突、FUSE 普通读取及 typed 查询。旧 `/1`、`/2` fixture 只修改 magic/version 用于**分类测试**，不代表重建了真实历史编码。发布后丢失最终事件的场景通过隔离报告模拟；另一场景实际 SIGKILL 在 durable `started` 之后、数据库摄取之前暂停的 fixture 子进程。两者均不代表主机断电或任意中间时刻崩溃的穷尽证明。

本次 live 使用独立数据库 `h09probe1790939889dbfc1f`，报告目录为 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa`；归档 [summary.json](../results/hdf5-h09-20261002/live-summary.json) 的 `ok=true`。

主批次 inventory 共 8 个版本：2 个候选、1 个历史版本、1 个当前已索引版本、2 个旧 layout 标记、1 个其他 format identity 和 1 个普通文本。只迁移两个候选：

| 文件 | B 字节数 | 旧精确版本 | 新精确版本 | 新 layout 字节数 |
| --- | ---: | ---: | ---: | ---: |
| 压缩 7×11 Float64 fixture | 4,473 | 1790939890276 | 1790939893866 | 1,222 |
| GWOSC H1 原始样本 | 1,040,592 | 1790939890516 | 1790939893149 | 9,550 |

两项均为 `verified`，源/目标完整 B SHA-256 相同。GWOSC 文件的 15 个 dataset 中仅 `/strain/Strain` 符合当前 typed profile，inventory 明确记录 `partially_queryable=true`；这次迁移没有扩大其余 dataset 的查询能力。

已保留的 live 证据包括：历史及旧精确版本 B 不变；重复 resume 目标一致；实际 SIGKILL 后未发布目标，恢复分配更大的新预留版本；模拟丢失发布后最终事件时恢复并复用原目标；latest 变化得到 `conflict` 且未发布迁移目标。挂载期间迁移锁竞争被拒绝；旧 pass-through 的子集查询明确拒绝；新压缩 fixture 的 40 值矩形子集和公开样本的 64 值子集通过 oracle 比对，包含多 batch domain 重建。FUSE 普通 latest/exact-version hash、typed stat/read/seek/EOF/mmap 和并发 open 均通过。

构建与回归证据：[Linux CTest 1,254/1,254](../results/hdf5-h09-20261002/ctest.log)、[公共 journal ASan/UBSan 15/15](../results/hdf5-h09-20261002/journal-asan.log)、[策略／服务普通 26/26](../results/hdf5-h09-20261002/native-policy-service.json)、[策略／服务 ASan/UBSan 26/26](../results/hdf5-h09-20261002/native-policy-service-asan.json) 和 [源码 SHA-256 清单](../results/hdf5-h09-20261002/source-sha256.json)。另有 [Arrow 合成双层 List live 回归](arrow-data-migration.md#统一实现的-arrow-回归2026-10-02) 通过，覆盖共用实现下原 typed／训练入口；[405 项部署源码对比](../results/hdf5-h09-20261002/source-verification.json) 无差异。这不是批量迁移、任意崩溃点、跨主机写入互斥或查询性能优势的验收。
