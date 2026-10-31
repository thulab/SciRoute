# A12：旧 Arrow 数据迁移

旧版本即使存有 Arrow IPC stream 字节，也可能是以普通 byte pass-through 形式摄取的。安装 Arrow adapter 不会改变已发布版本的格式身份。迁移从一个**精确的旧版本**重复读取原始字节，经正常摄取生命周期发布一个**新的文件版本**，生成 Arrow layout 和格式摘要；旧版本及其字节保持不变。迁移不会把 IPC stream 改写成 IPC file，也不会展开训练窗口。

## 运行边界

首版按离线、单写入者运行。先停止其他主机上写入同一 IoTDB namespace 的所有进程，并停止本机的 FUSE daemon 与其他写入进程，确认迁移期间不会删除或重命名源路径，保留数据库备份。挂载进程和迁移命令都会在整个运行期间持有同一个本机排他文件锁；未设置 `IOTDBFS_NAMESPACE_LOCK` 时，路径由 IoTDB host、port、database 派生到 `/tmp`。操作时建议两者都显式配置同一绝对路径，并确保该路径的父目录已存在且可写。本机文件锁不能约束其他主机。`IoTDBStructuredBlobBackend` 只提供进程内的变更互斥，不提供跨进程 CAS；迁移命令会在每个文件处理时重新核对源版本和 latest，发现变化便报告冲突，但这不能替代停写。

以只读 inventory/dry-run 开始。首版候选只包括 **latest** 为 byte pass-through 的版本，且原始字节是真实可解析的 IPC **stream**，存在可索引的 `list<float32>` 或 `list<list<float32>>` 字段；文件后缀不是判定依据。已是 Arrow 格式身份（当前 `format_id=arrow`）的 latest 要核对现存 layout 与容器 profile，不能盲目重摄取。历史旧版本、IPC file、Feather、其他 Arrow 变体和损坏数据应单列为未迁移项。inventory 报告应保存精确 path/version、大小、schema、目标 List 层数、compression 与源字节 hash，以便执行前复核。

管理入口使用与挂载相同的数据库配置。`inventory` 是 dry-run；`run` 处理报告中冻结的候选；中断后用 `resume` 重新核对已发布目的版本并处理尚未完成的候选。三者使用同一**报告目录**，`--path` 可把操作限定到一个精确文件路径：

```sh
export IOTDBFS_NAMESPACE_LOCK=/absolute/path/shared-by-this-host/namespace.lock
./bin/iotdb_fs arrow-migrate inventory --report /absolute/path/arrow-migration
./bin/iotdb_fs arrow-migrate inventory --report /absolute/path/one-file --path /dataset/part.arrow
./bin/iotdb_fs arrow-migrate run --report /absolute/path/one-file
./bin/iotdb_fs arrow-migrate resume --report /absolute/path/arrow-migration
```

报告目录应位于可写、持久的文件系统；命令出错后不要丢弃它。H09 整理后，Arrow 与 HDF5 共用 `MigrationCli`、`MigrationJournal` 和格式中立的 `ReingestionService`，格式检查保留在 `ArrowReingestionPolicy`。每个报告目录另持有 `.migration.lock`，避免不同 namespace 同时改写它。

当前必须使用新统一报告协议：`report.manifest`（`IOTDBFS-MIGRATION-REPORT/1`）绑定配置的 host、port、database、adapter 策略、目标 layout protocol 和冻结 inventory SHA-256；`inventory.tsv`（`IOTDBFS-MIGRATION-INVENTORY/1`）是执行输入，`inventory.jsonl` 保存人工审阅的 profile。manifest 最后提交。新 inventory 要求不存在已有 manifest、inventory、journal 或 results。旧 `A12I1`/`A12J1` 报告明确拒绝，不提供兼容读取或原地升级；保留旧报告并在新目录重新 inventory。

每个候选精确目标 key 在摄取前追加到 `journal.log`，同步文件及父目录后才允许数据库写入；`results.jsonl` 保存最近结果。`run`/`resume` 先验证报告 namespace、策略和 inventory digest，再读 journal，因而相同 path/version 不能让另一数据库误用这份报告。host 别名变化同样视为绑定变化；密码不写入报告。`--path` 只筛选冻结输入中的精确路径，不按文件后缀匹配，也不隐含迁移全部历史版本。单个报告文件最多 64 MiB，机器记录单行最多 1 MiB，超限明确报错。

迁移服务只创建新的 version，不删除或修改源 version。迁移期间操作员也不得删除或 rename 源路径：持有 published read handle 并不能保证底层字节不会因删除而保留。复制旧 artifacts 的 `copy_structured_blob_version()` 不是迁移；它不会重新识别 Arrow 格式。服务在摄取前和发布前校验源 hash，发布后重新读取目的 B 核对大小和 hash，并确认新 manifest 的 `format_id=arrow`、layout 可解析及 namespace inode 对应新 version。批次验收还应核对格式摘要。旧版本的精确查询仍保持 byte pass-through；新的精确版本才可进行 Arrow typed 查询。

## 批次操作

1. 停止目标 namespace 的写入，确认备份和数据库连接信息，运行只读 inventory，并归档报告。
2. 对固定小 fixture 和少量真实文件运行 dry-run；检查候选、跳过及不支持项，特别是误用 `.arrow` 后缀的非 stream 文件。
3. 对少量候选执行迁移；逐个核对旧、新精确版本的普通字节读取、大小与 SHA-256，以及新版本 layout 与 Arrow 原生查询结果。
4. 构造新的 `DatasetSnapshot` 和 coverage index，使用新精确版本验证多变量窗口、跨 shard 窗口和训练视图。旧实验继续使用其原 snapshot，不自动切换。
5. 分批迁移其余候选，保存每批报告与日志；只有验收通过的新版本才写入新的训练 snapshot。

`inventory.jsonl` 记录源 path/version、字节数、分类、原因、B SHA-256、预期 layout SHA-256 和 `partially_queryable`；Arrow 的 batch/row 数、字段名与原生类型、List 层数和 compression 现在位于 `profile` 对象。通用分类名使用 `unsupported`、`not_format`，替代旧报告中的 Arrow 专用分类名。历史版本仍标成 `historical_version`，只列身份和大小，不读取其全部内容或执行迁移。`results.jsonl` 增加预期 layout SHA-256，同时记录源/目标 version、字节数、两个 B hash、layout 大小与版本、adapter ID、状态和错误。

`journal.log` 使用 `IOTDBFS-MIGRATION-JOURNAL/1`，显式保存策略和目标 layout；不要手工编辑。完整损坏行会失败，只有通过全部身份校验后的末尾不完整追加可截断修复。恢复时重验源/目标 B hash、从源重新生成的 layout、完整目标 layout 与 namespace inode；已发布目标验证成功则复用原 key，验证失败列为 `published_unverified`，不创建替代版本。明确不存在或未发布的目标允许重试；未发布目标保留并记录原因，新版本大于全部历史预留版本，避免时钟回退导致重用。backend 错误不会当作目标不存在。

失败时不得删除源版本或修改旧 manifest。若新版本已发布但验收失败，报告该状态并将其排除在新训练 snapshot 之外。现有 API 没有保证能把 latest 自动切回旧版本，操作流程不承诺自动回滚。需要先处理冲突或验收失败，再继续后续批次。

## 验收

至少覆盖无后缀及误导性后缀、单层与双层 List、压缩与未压缩、多 batch、损坏/非 stream、已迁移版本、迁移中断与重复执行、并发 latest 变化。对每个成功项核对源版本保留、源/目标 B hash 相等、目标 Arrow IPC stream layout 和摘要有效，以及精确版本查询与训练窗口。最终 live 验收按实施计划中的 AQ-01/02/09/11/16 执行并归档证据。

## 已完成的隔离试迁移

以下是原 A12 实现的历史验收，不是新统一报告协议的复验。H09 公共 journal 本地 15/15 ASan/UBSan、统一实现 Linux 1,254/1,254 CTest 及 HDF5 隔离 live 验收通过；本轮 Arrow 合成双层 List 的 A12 live 重构回归也已通过，见下节。旧报告继续作为历史证据保留，但不能由新 CLI resume。HDF5 入口及公共恢复契约见 [H09 操作手册](hdf5-data-migration.md)。

在 `root@192.168.99.49` 的独立 `a12probe*` IoTDB 数据库验证了命令链路。合成双层 List fixture 使用空格式注册表先发布旧 pass-through 版本，再运行 inventory、run、resume；测试删除最终完成日志以模拟目标已发布时的中断，resume 重验同一目标。新版本经 FUSE typed 查询和训练窗口 IPC 读取，持锁挂载期间第二个迁移命令按预期被拒绝。报告位于 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/a05-decoded-cache-20260927/a12-migration-jxrzibly`。

一份真实 TimeBench `saugeen_W_short/data-00000-of-00001.arrow`（13,648 字节）按相同流程试迁移，源/目标 SHA-256 均为 `17f1b221c9b255c8d655b6e471e03c44d9532d899997d50ec0ef959be5159242`。FUSE 精确目标版本的单层 List typed 结果与 PyArrow 直接读取原文件一致；报告位于同一实验根目录的 `a12-migration-wlrwf7tj`。完整单元测试 **928/928** 通过。

这些试验不代表现有 TimeBench 数据库已批量迁移，也未覆盖跨主机停写、分批后新 DatasetSnapshot/coverage 的生产验收或计划中的 AQ 全项。

## 统一实现的 Arrow 回归（2026-10-02）

本轮使用 **合成双层 List fixture**，不是重新迁移上述真实 TimeBench 文件。独立数据库 `a12probe1790939941` 的报告目录为 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h09-20261002/a12-migration-2wug6co4`，归档 [Arrow live 日志](../results/hdf5-h09-20261002/arrow-live.log) 记录成功。源/目标 B SHA-256 均为 `aa97e71e09dee596d9e32cfb00a5a99ccfc8617e5fd1ec0b9e7f24007745e1d7`。

沿用 A12 live 脚本检查 inventory → run → resume、发布后缺失最终事件的恢复、旧版本保留、FUSE typed 查询、训练窗口及挂载时 writer lock 互斥，验证共用迁移实现仍支持原 Arrow 流程。它不扩大批量迁移范围，也不将历史 TimeBench 结果计作本次复验。Linux 源码／构建／测试文件与本地 [405 项 SHA-256 对比](../results/hdf5-h09-20261002/source-verification.json) 无差异；全量 CTest 证据见 [1,254/1,254 日志](../results/hdf5-h09-20261002/ctest.log)。
