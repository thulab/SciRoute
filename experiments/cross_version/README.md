# GRIB/HDF5 跨版本实验环境

> **正式性能采集统一使用 [FM1 固定矩阵](../fixed_matrix/README.md)**。首轮32组、18组仅预注册扩展；旧实验结果只保留追溯，不作为新基线或性能证据。后续优化复用同一已封印 corpus/case，仅新增 run；新增实验组先确认并登记。

> 当前正式代码仅提供 [`native-hdf5` / `iotdb-indexed`](../../docs/contracts/hdf5-query-modes.md)。本目录历史 runner、arm 和计时结果保留当时口径；含旧 catalog 设置或 frozen-typed 请求的 runner 必须配合其冻结二进制运行，不能直接用于当前二进制。归档说明见 [legacy](../legacy/README.md)。

当前 `native-hdf5` 为数据库目录／属性／版本索引＋受管FS完整文件读取，要求显式文件绑定。历史同名数据库VFD结果不能当作新通路结果。`hdf5_route3/native_filesystem/verify.py` 在原实例、两核／2GiB查询组内验收：默认只运行独立功能fixture，科学性能查询为0；显式 `--timing-probe` 则对已封印的独立 HDF5 N8/N128、V1/S0/Latest 两个 FM1 case 补绑定并采集72次查询（每arm预热2次、计时10次，单block）。不新增case、不重发科学语料、不重建FS-index；原journal保留为历史，补绑定后必须使用收据中的当前 `owner-active`。功能、准备成本、时钟、资源、恢复及失败记录分开保留，结果见[总报告](../../docs/reports/hdf5-route3.md#native-filesystem)。后续完整性能复测继续复用固定catalog/corpus/case，另建run。

后续全部16个HDF5首轮组的正式三轮1,728次已完成，入口为`experiments.fixed_matrix.hdf5_filesystem`，证据位于`work/hdf5-native-matrix-20261021/collection-r1/`；先行72条不拼入正式统计。各namespace接续使用该目录`current-locations.json`中的当前journal与受管根，原发布／先行探针journal只作档案，GRIB和18扩展保持原状态。

服务器：`root@192.168.99.49`，仓库根目录
`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C`。本目录准备跨版本压缩研究的输入、真实发布版本和独立数值 oracle。
现阶段运行既有存储协议，压缩／去重实现和性能对比留待后续实验。

## 目录与运行入口

脚本、测试、spec 与 native 辅助代码按实验归组，不再在顶层平铺：

| 子目录 | 实验及主要入口 |
| --- | --- |
| [pilot/](pilot/) | 初始 GRIB/HDF5 语料与发布：`corpus.py`、`publish.py`、`server.sh` |
| [version_directory/](version_directory/) | V00–V04 协议／权限／恢复验证及 N128 消融；共用的实例控制器也在本组 |
| [version_matrix/](version_matrix/) | N=8、V=1/2/4/8 单／跨版本矩阵及其 native worker |
| [large_file/](large_file/) | 大文件有利场景、GRIB cache-fit、payload 审计与 profile 汇总 |
| [hdf5_route3/](hdf5_route3/) | 选择性规模、选择器／成员消融、热缓存、正式两模式；[总报告](../../docs/reports/hdf5-route3.md) |
| [resource_limited/](resource_limited/) | 固定两核、查询组 2 GiB 的既有三场景复测；以同组 IoTDB-FS/FS-index 完整暖查询时间比值为主指标 |

每组的 `tests/`、`specs/`、`native/` 随对应实验归置（按需存在）。`work/` 与 `results/` 保留原位置；旧源码快照、manifest、冻结 helper 路径与历史 SHA 不随活脚本迁移。当前脚本只使用完整包名互相导入，不保留旧平面路径入口或短名别名；迁移前脚本及路径映射在 `work/layout-reorganization-20261019/`。

Python 命令从 **IoTDB-FS-C 仓库根目录**运行，模块名由相对路径去掉 `.py` 并将 `/` 换为 `.`：

```sh
python3 -m experiments.cross_version.hdf5_route3.hdf5_two_modes --help
python3 -m unittest discover -s experiments/cross_version -t . -p 'test_*.py'
```

离线测试仍使用各自原有依赖；完整语料测试需要 pinned HDF5 1.14.4 和 ecCodes，native ABI 测试另需 `ROUTE3_NATIVE_LIBRARY`。`pilot/server.sh` 会自行定位仓库根，CORPUS／REPORT／WORK 相对参数仍基于 `cross_version/`。当前活 helper 的字节守卫已绑定迁移后的脚本，历史冻结文件守卫保持原值；不混用两者。

## 实验变量与身份

新的语料、查询 case 和执行 run 分别绑定身份，见[实验身份契约](../../docs/contracts/experiment-identity.md)。N 只表示语料内逻辑文件总数；`C_paths` 是查询范围内候选逻辑路径数，`C_versions` 是版本选择后、属性谓词前的候选精确文件版本数，均不能写成 N。展示简写 C 指 `C_paths`。N512 中选择四个完整桶、128条候选路径统一展示为 **N512/B4/C128**；完整范围为 **N512/C512**（覆盖全部16桶）。这四桶按已知命中选取，不能当作独立 N128 或随机样本。既有真正独立生成／发布的 N128 仍写 N128。

`stored_V` 是计时阶段已存储的科学版本数，`selected_versions` 是该 case 选中的精确版本集合；空 create／truncate 历史另计。Latest、Exact、All、Between 必须保留实际选择规则、逐路径版本号或时间边界，不能用选择1版推断 stored_V=1。Latest 的 N512/B4/C128 对应128条候选路径与128个候选文件版本；若128条路径各存8版并选 All，则 `C_paths=128`、`C_versions=1024`，两项须分别记录。S 记录摘要绑定 inventory 中实际原编码文件版本的字节数及分布，不使用目标 MiB、解码大小或 ROI 字节数代替。oracle 分别报告 `K_files`、`K_file_versions`、`K_units`，历史 `K=2` 仅沿用该实验原定义，不能据此假定三者相等。

`work/`、`results/`、历史源码快照与原始计时行保持原字节；部分旧 runner 将候选128序列化为 `N=128, backing_N=512`，这里只修正文档展示，通过原报告的 source case／scope／manifest 映射为 N512/B4/C128，不把它登记成独立 N128。后续独立 N128 实验必须绑定真正的128文件语料，不复用此四桶子集冒充。

## 首批配置

用户确认的 pilot 使用现有多文件 `(N,K,D,depth)=(8,2,4,2)`，每路径 **8 个科学数据版本**，两种格式、两组，共 **256 个版本绑定、1,024 个单元**。

| 组 | v1 | v2–v8 | 不变量 |
|---|---|---|---|
| `identical` | 引用现有 raw 文件 | 始终引用同一个 raw 文件；每次在 IoTDB-FS 重新写入以生成真实版本 | 全文件 SHA、全部单元和数值相同 |
| `single_unit` | 引用现有 raw 文件 | 累积修改，每次仅一个 dataset/message，槽位 `(ordinal-2)%D` | 其余单元数值和 GRIB 编码不变；属性、形状、过滤器、网格与有效性掩码不变 |

HDF5 修改选中 dataset 首个 Float32 的低尾数位（与 `ordinal-1` 异或），确保在原随机位型数据上实际改变数值且保持有限值。
GRIB 对选中 message 的原生解码值进行 `-values + ordinal/4`，由 ecCodes 按原 `grid_simple`/16 bit 配置重新编码；其余 message 原始字节直接复制。
所有版本均用原生库重读并检查数值 SHA，保存实际 GRIB offset/length。修改率按 **单元数 1/4** 定义，不代表 25% 字节改变；HDF5 每次只修改一个元素。
语义查询及其 `K=2` 保持原多文件实验配置，日期标签仍为 `20261013`。

## 复用与存储

直接复用以下已有文件，不下载数据、不复制运行时或重新构建：

- raw v1：`../multi_file/work/small-query-20261013/{hdf5,grib}-data`；原 manifest 和文件 SHA 全量校验。
- Python：`../single_file/work/session-20261012/venv/bin/python`，h5py 3.12.1 / HDF5 1.14.4 / NumPy 2.1.3 / PyArrow 21.0.0。
- 守护进程与原生库：`../multi_file/work/runner-20261013/environment.json`，校验已有二进制 SHA 和 Python 运行时身份。
- IoTDB 实验库：`h09probe1790939889dbfc1f`；沿用多文件实验的 namespace writer lock。

`work/pilot-20261015/manifest.json` 是带 SHA 封印的版本矩阵。`blob.kind=base` 指向原多文件目录，`blob.kind=local` 指向本环境 `objects/`。
不创建指向原数据的硬链接或软链接；只有修改组的 v2–v8 保存完整新文件，共 112 个。
原数据是这个环境的外部依赖，后续应保留；任何源变动会在验证时拒绝。
新 raw 字节预算为 16 MiB，manifest 和目录元数据另计。逻辑版本字节数、实际新增 raw 字节数分别记录，不能将输入复用比例称为 IoTDB 压缩率。

IoTDB-FS 使用独立随机前缀 `/cross-version-<id>/{scenario}/{format}/raw/...`，每个稳定路径写 8 次。
raw v1 在准备阶段直接复用；后端中各组首次仍需各发布一份 v1，后续完整文件按当前协议发布。
原多文件路径保持可作为对照。凭据只从已授权守护进程读入内存，不写入报告或配置。
源进程默认 PID=1001546，若重启则用 `IOTDBFS_CONNECTION_SOURCE_PID=实际PID` 指向仍由当前用户持有的 `iotdb_fs`。

## 使用

```sh
cd /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version

# 已准备环境的离线校验
bash pilot/server.sh verify-raw

# 对既有真实版本重新挂载审计；读取原始字节及所有 typed 单元
bash pilot/server.sh verify

# 回归测试：在临时目录生成小型本地 fixture，不向 IoTDB 发布
bash pilot/server.sh test
```

重新准备一轮时必须使用新输出路径；发布报告也不可覆盖：

```sh
bash pilot/server.sh prepare work/another-corpus
bash pilot/server.sh publish work/another-corpus results/another/publication.json work/another-publisher
```

`pilot/server.sh` 启动 Python 时清除 `LD_LIBRARY_PATH`、`PYTHONPATH` 并设置 `HDF5_PLUGIN_PRELOAD=::`。
只有守护进程子进程加载原环境描述中的 C++ 库路径。独立挂载在操作结束时卸载，后端版本持久保留。
直接 Python 入口从仓库根目录使用 `python -m experiments.cross_version.pilot.corpus prepare|verify` 或 `python -m experiments.cross_version.pilot.publish publish|resume|verify`，参数见 `--help`。

准备器复用多文件 manager 的独占 writer lock、staging 与原子目录发布，拒绝覆盖既有输出；预算失败仅清理本次 staging。
后端发布逐次记录真实版本并原子更新报告；失败报告保留已知绑定，未记录的最后一次发布可能存在，不能假定失败等于未写入。
`resume` 显式续接未完成报告：先重读全部已记录的历史 SHA；每个路径最多接纳一个未记账、且恰与下个计划版本 SHA 相同的新版本。
历史不一致或存在多个未记账版本时拒绝继续；接纳时保留原真实版本号，不重写，未知原发布耗时记为 null。
`verify` 只审计完整记录。新一轮实验用新报告／前缀，不能通过覆盖报告丢失旧绑定。

## 版本与验收

`ordinal=1..8` 是本实验的逻辑序号，真正的版本号是服务器分配的 `row_time`，保存在 `publication.json` 的 `bindings` 中。
完整发布报告位于服务器 `results/pilot-20261015/publication.json`。
同一输入重复写入也必须生成严格递增的独立版本，不能用多个文件名模拟同路径历史。

创建路径时现行 FUSE 还会生成一个空文件初始版本，因此整个物理历史可能多一条 byte-passthrough 记录。
验收使用 GRIB/HDF5 adapter 过滤后要求每路径恰好 8 版；保留空记录，不修改现有协议。
历史完整字节通过 `.<stem>_<真实version>.<suffix>` 读取，typed 请求使用原路径加 `versions.kind=exact`。
`versions.kind=all` 验证全部科学版本集合，`latest` 必须选择第 8 版。

发布后关闭 publisher 守护进程，开启新的独立守护进程，检查：

- 全部 256 版的历史全文件 SHA 和精确目录绑定；全部 1,024 单元的 owned Float32 数值／掩码 SHA 与真实 path/version provenance。
- 32 个稳定路径的 `latest` 字节和版本号；按格式筛选的完整历史集合每路径恰为 8 版。
- 四个格式／组组合的普通 latest 子树查询各有 32 单元、8 路径，原六属性谓词各有 2 单元、2 路径。
- 发布前后原始基线、所有新增文件、每步修改范围和预算再次验证。

2026-10-15 首次服务器验收全部通过：10 项回归测试、256 个真实科学版本、1,024 个 typed 单元。
已发布前缀为 `/cross-version-1b723c3e5e464290`，运行结束后独立挂载已关闭，版本保留在实验库中。
新增输入文件 6,639,136 bytes（约 6.33 MiB），对应逻辑版本原文件总计 15,175,168 bytes（约 14.47 MiB）；磁盘块分配 6,881,280 bytes，另计目录／manifest。
首次发布因相对挂载路径的 IPC 检查中断，修正后通过精确历史 SHA 找回 1 个未记账版本，沿原前缀续接，没有重新发布已完成版本。

验收摘要见 [acceptance-20261015.json](results/acceptance-20261015.json)。raw、完整发布记录、挂载日志放在忽略的 `work/`、`results/`；代码、spec 与小型验收摘要可入库。
准备、哈希与审计会暖 OS 缓存；这里的时间只描述环境搭建，不是查询性能测量。

## V00：版本目录边界与 head 提交原语

设计冻结见 [V00 契约](../../docs/contracts/version-directory-content-sharing-v1.md)，写入口和验证证据见
[审计](../../docs/reports/version-directory-v00-write-audit-20261015.md)与
[验证报告](../../docs/reports/version-directory-v00-validation-20261015.md)。
`version_directory/specs/version-directory-v1.json` 保存用户确认的 D1–D5 和硬预算；它不是已启用的生产存储配置。

`version_directory/v00_permissions.py` 使用既有 runtime、writer lock 和独占随机子树，验证 exact 历史权限、当前祖先权限、
version chmod/chown、普通打开时 unlink、IPC 撤权以及删除最新后的格式筛选语义。示例：

```sh
cd /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: \
  experiments/single_file/work/session-20261012/venv/bin/python -m experiments.cross_version.version_directory.v00_permissions \
  --environment experiments/multi_file/work/runner-20261013/environment.json \
  --source-pid 1001546 \
  --writer-lock /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock \
  --work experiments/cross_version/work/v00-permissions-NEW --report experiments/cross_version/results/v00-permissions-NEW.json
```

`version_directory/v00_head_probe.py` 仅支持原实验服务 `runtime/current`、RPC 33667 和随机 `v00_head_<16hex>` 测试库。
它会停启原 DataNode，因此需先取得维护窗口授权，再传 `--allow-existing-restart`。
2026-10-15 本轮用户已明确授权原数据库停启；后续运行不能从此推定任意时刻都无其它实验。
脚本保留 ConfigNode、数据目录、许可证和节点身份，临时设 `WAL=SYNC`、`enable_partial_insert=false`，
结束时恢复原配置并启动原服务。既有 ASYNC 配置恢复后不具备本轮 SYNC 条件下的保证。
脚本不再创建独立数据库实例或请求新证书。

```sh
cd /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C
# DATANODE_PID 必须取 runtime/run/datanode.pid 的当前值，脚本会核对进程身份。
python3 -m experiments.cross_version.version_directory.v00_head_probe \
  --release /data_01/zzy/iotdb-fs-exp/runtime/releases/timechodb-2.0.11-SNAPSHOT-bin \
  --runtime /data_01/zzy/iotdb-fs-exp/runtime \
  --server-source /data_01/zzy/iotdb-fs-exp/timechodb \
  --sdk /data_01/zzy/iotdb-fs-exp/sdk/iotdb-session-cpp \
  --source-pid 1001546 --datanode-pid DATANODE_PID \
  --work experiments/cross_version/work/v00-head-NEW --output experiments/cross_version/results/v00-head-NEW.json \
  --reuse-probe-report experiments/cross_version/results/v00-head-existing-20261015-r2.json \
  --allow-existing-restart
```

每轮使用新 work/report 和随机 cell tag，保留测试库和失败记录供核查，不改已有业务表。
`--reuse-probe-report` 校验一份成功 V00 报告，复用其测试库/region，以新 tag 保留既有 head 证据。
本轮后续创建新测试库曾触及 SchemaRegion direct-buffer 预算，因此此服务器后续应使用上述复用方式；
不通过盲目重试写入或扩大内存配置掩盖资源不足。`--durability-smoke` 只运行带生效配置查询的一次 ACK/crash 用例。
`operations.jsonl` 记录 UTC 时间、配置摘要、进程身份、定向信号与启动命令；原配置保存在该轮 work，
凭据仅在内存/子进程环境中使用。脚本捕获异常和常规终止信号后尝试恢复；SIGKILL 或主机断电不能执行
Python finally，恢复时须核对该轮操作日志、原配置副本和原服务 PID 后使用原启动脚本。
head 测试只验证单 BLOB 候选原语，不能证明多 writer CAS、断电持久性或完整 V01 协议已经实现。

V00 已完成：155 项单测、9 项真实权限检查、10 组 head 用例与 1 组生效配置恢复短验通过；最终原数据 256 版/1,024 单元复验通过。
汇总见 [v00-acceptance-20261015.json](results/v00-acceptance-20261015.json)，全部原始证据按摘要中的远端路径与 SHA-256 查验。

## V01：单 owner 目录提交

新代码默认仍为 strict。显式 owner 模式使用原数据库中的独立 `vd_` 表前缀、固定 namespace 锁和
持久 `owner_directory`，所有实验共用原账号；不新建账号、不修改 ACL、不创建独立实例。
owner journal 是恢复数据，不能当临时锁文件删除或换位置。该模式要求受控单机入口、单个 loopback
DataNode、生效的 `WAL=SYNC` 与 `enable_partial_insert=false`；不满足则拒绝挂载。
它不提供同账号 raw SQL 隔离、跨机器 fencing、payload 去重或 GC。

```properties
storage.version_directory.mode=owner
storage.version_directory.table_prefix=vd_example_20261015
storage.version_directory.owner_directory=/absolute/private/persistent/owner
storage.version_directory.initialize=true
```

表前缀须为小写字母、数字和下划线；首次只允许空 namespace，随后必须改为 `initialize=false`。
权限、目录、发布、删除与 rename 以同一 head 发布；删除保留 tombstone 和旧 artifact，物理回收属于 V05。
head 结果不明时停止新准入，恢复必须同时核对数据库与原 journal，禁止删除 pending 后继续写。
具体字节协议与恢复边界见[契约](../../docs/contracts/version-directory-content-sharing-v1.md)。

`version_directory/v01_validation.py` 在原服务的维护窗口运行：先验证真实 Store 的故障包装，再以新构建 mount 运行
GRIB/HDF5 多版本、单 head 元数据修改/rename、staging 隔离、非 root 权限与原 DataNode SIGKILL 恢复。
脚本使用 V00 的原 DataNode 控制器，临时切换两项 durability 配置并在 finally 恢复；已有许可证、节点身份、
ConfigNode 和旧表保持原状。需提供当前 PID、既有 runtime 环境与独立构建的两个二进制，使用全新 work/report。
本次用户已授权原服务维护，后续运行仍须确认实际维护窗口。SIGKILL 脚本或主机断电无法执行 finally，
恢复应依据该轮 `control/operations.jsonl` 与原配置备份，不得启动隔离实例替代原服务。

```sh
cd /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: \
  experiments/single_file/work/session-20261012/venv/bin/python -m experiments.cross_version.version_directory.v01_validation \
  --environment experiments/multi_file/work/runner-20261013/environment.json \
  --input experiments/cross_version/work/pilot-20261015 --source-pid 1001546 --datanode-pid CURRENT_PID \
  --daemon experiments/cross_version/work/V01_BUILD/bin/iotdb_fs --probe experiments/cross_version/work/V01_BUILD/bin/v01_store_probe \
  --runtime /data_01/zzy/iotdb-fs-exp/runtime \
  --release /data_01/zzy/iotdb-fs-exp/runtime/releases/timechodb-2.0.11-SNAPSHOT-bin \
  --work experiments/cross_version/work/v01-live-NEW --report experiments/cross_version/results/v01-live-NEW.json
```

`v01_store_probe` 为 CMake 显式目标，支持 `head|stats|faults --prefix vd_name`；faults 另需新的
`--journal /absolute/private/path`。连接值从现有五个 `IOTDB_*` 环境变量读取，不打印凭据。
stats 计量目录对象 BLOB 字节，包含旧根/孤儿，不等于 IoTDB 物理磁盘或 payload 压缩率。
故障包装在真实 IoTDB 持久化之上模拟丢 ACK/未发送/写失败，不冒充真实网络故障；V00 的真实响应丢失证据单独保留。

V01 已完成：251 项相关单测、8 项 Store 故障包装、12 项目录/恢复和 9 项非 root 权限检查通过。
新模式验收 GRIB/HDF5 各 8 个科学版本；`version_directory/v01_strict_validation.py` 复制原 publication 报告，
用新 daemon 的默认 strict 模式复验原有 256 版/1,024 单元，不重发旧数据。
详情见 [V01 报告](../../docs/reports/version-directory-v01-validation-20261015.md)与
[小型验收摘要](results/v01-acceptance-20261015.json)。原服务已恢复 ASYNC/true，新模式仍受上述显式门禁约束。


## V02：内容对象与版本封印

V02 owner 使用全新表前缀与 `namespace-record/2`，在 genesis 原子绑定内容 profile；不升级 V01 前缀。
已有 V01 数据须使用固定 V01 二进制及原 journal 打开。默认 strict、原数据库/账号/许可证不变。
内容 metadata/layout/catalog 共享，版本权限、来源封印及 payload 独立；尚无 payload 去重或 GC。
布局保留原编码，分页 60 KiB；完整布局证明在发布前与恢复时验证，目录/页解析缓存按可信内容身份复用。

```sh
cmake --build BUILD --target iotdb_fs unit_tests v01_store_probe v02_content_probe v02_cache_probe -j 8
```

`version_directory/v02_validation.py` 复用 V01 的受控原 DataNode 维护流程，并以新前缀发布 identical/single_unit
两组、GRIB/HDF5 各 8 版共 32 个科学版本。除 V01 参数外还需提供：

```sh
--content-probe BUILD/bin/v02_content_probe --cache-probe BUILD/bin/v02_cache_probe
```

本轮 runner 固定检查现有 V01 验收前缀拒绝升级（head/journal 摘要保持），不尝试重建该前缀。
`v02_content_probe inspect --prefix vd_NAME` 全程只读，核对根、内容引用闭包、原始字节摘要及每版本封印。
`v02_cache_probe --prefix vd_NAME --journal /absolute/owner --repeats 2` 需先停止该 owner mount；
它正常执行完整恢复，并可能重试已提交文件的暂存清理，**不是纯只读工具**。输出解析/读/hash 字节计数、
布局身份分组、恢复耗时、缓存持有量与进程 RSS。连接值沿用现有 `IOTDB_*` 环境变量，凭据不写报告。

同内容版本共享 descriptor/proof/image，seal 按精确版本独立；chmod/chown/utimens 保留所有内容引用，
rename 重绑 seal，删除一版保留其它引用。布局相同的不同 payload 仍由各精确版本的独立数值 oracle 检查。
写入量计数包含旧根与孤儿；单次冷启动/热读观测不作为发现性能收益或物理压缩率。

V02 已完成：449 项回归、32 科学版/128 单元、8 项 Store 故障包装、9 项非 root 权限和崩溃恢复通过；
同一新二进制默认 strict 复验原 256 版/1,024 单元通过。完整证据与成本见
[V02 报告](../../docs/reports/version-directory-v02-validation-20261016.md)和
[小型验收摘要](results/v02-acceptance-20261016.json)。

## V03–V04：准入与 payload 块共享

V03 的 ReadContext、执行租约、共享请求预算和固定 N=128 结果见
[V03 报告](../../docs/reports/version-directory-v03-validation-20261016.md)。
V04 已完成，694 项统一回归、真实共享/恢复/权限、N=128 固定比较及默认 strict 通过。结果见 [V04 报告](../../docs/reports/version-directory-v04-validation-20261017.md)和[验收收据](results/v04-acceptance-20261017.json)，
持久/读取边界见[契约第 10 节](../../docs/contracts/version-directory-content-sharing-v1.md#10-v04完整块映射真实批读与固定内容读取)
和 [runtime JSON](version_directory/specs/version-directory-v04-runtime.json)。

V04 只初始化新前缀，使用 namespace-record/3、content-build/3、file-descriptor/2，
不隐式升级 V01–V03 前缀。上述历史阶段的命令与持久格式须配合当时固定二进制使用。
默认 strict、原数据库、共用账号和许可证保持；owner 仍要求实际 SYNC/partial-insert=false。

```sh
cmake --build BUILD --target iotdb_fs unit_tests v01_store_probe v03_context_probe v04_payload_probe v04_range_probe -j 8
```

- `version_directory/v04_validation.py`：新随机前缀；原 32 科学版、两组各 8 个大文件版本、独立块/范围哈希、权限与 owner 重启；使用 `--v03-content-probe` 和 `--v03-report` 固定旧 inspector/前缀，另传 `--payload-probe`、`--range-probe`。仅在获准的原实例维护窗口运行，原服务由 finally 恢复。非 root 测试使用独立可搜索宿主挂载面，控制目录与日志仍私有。
- `v04_payload_probe inspect --prefix vd_v04_NAME`：只读 Store 图检查，不打开 journal、不初始化/恢复 owner、不清理 staging；报告块引用、真实批读和剩余 staging 行。
- `v04_range_probe --prefix vd_v04_NAME --journal /absolute/owner --path /payload/identical.bin --version VERSION --size BYTES`：必须先卸载该 owner；通过生产 backend/ReadHandle 正常恢复并读取，可能重试已提交 staging 清理，不能称为纯只读 SQL 工具。输出完整/乱序/重复/跨块范围哈希、实际 Store 查询和 head/journal 摘要。
- `version_directory/v04_n128.py prepare` / `ablate`：原 raw/FS/SQLite 原位复用，新 V04 前缀重摄取；pinned V03 和新 V04 各 180 条冻结 runner 观测，计时外另报发布、恢复、7 次挂载/RSS与实际配置。`complete` 只代表运行完成，须另查 `performance_gate_passed`，固定 A 不得替换。

块以 1 MiB 原编码字节定位，尾块保留实际长度；完整 map 不沿历史版本重建。
head 确认后仅清理已提交版本 staging，失败留档并在恢复时重试。共享对象 GC 留待 V05；
唯一被引用块字节不等于数据库物理占用或磁盘压缩率。每个实验使用新 work/report 路径，保留失败证据。

## N=8 单版本／跨版本完整查询矩阵

`version_matrix/n8_matrix.py`、`version_matrix/n8_matrix_prepare.py` 与 `version_matrix/native/version_matrix_native.cpp` 实现
[冻结试跑协议](version_matrix/specs/n8-version-matrix.json)，不改旧 multi_file runner 或生产源码。
固定 N=8/K=2/D=4/depth=2，V=1/2/4/8、identical/single_unit、HDF5/GRIB、
Latest/Exact(v1)/All，比较冻结 V04 r3、V03、native FS scan 与持久 SQLite 索引。
共 192 cells，各 3 campaign × 5 repeats，共 2,880 次完整应用操作；每 cell 每轮排除首项后
12 个温热观测，3 个独立 campaign 分列，不能将重复请求当成独立 campaign。

每个 owner system/scenario/V 使用独立 namespace，共 16 个，发布 960 个科学版本。
FS 为每个 scenario/V/format 复制全部历史文件，不使用硬链接／软链接／reflink，
一份索引覆盖该组全部 V。Latest/Exact 先选择版本再应用六属性谓词；All 不合并同内容版本。
Exact 的真实版本号按 path 分别保存，现有 API 需要 8 次逐 path discovery，全部计入完整时钟；
Latest/All 各一次 subtree discovery。HDF5 按命中 source/version/dataset 分别 typed 读取，
GRIB Latest/All 合并命中路径、Exact 分别读取真实版本，不声称四组等 RPC。

主指标沿用 discovery 加 selected values 到 owned buffers：native 计本地读取，IoTDB-FS
含控制／注册、IPC 和 owned copy，hash/provenance 校验及 profile 等待在计时外。
发布、FS复制／索引、owner恢复、配置和实际启动身份另列；准备会暖 OS 页，不声称冷盘实验。
`version_matrix/tests/test_n8_matrix.py` 提供不触数据库的选择、完整版本 oracle、请求数、计时与统计回归。

运行仅使用原实例、账号和许可证，在已有维护授权下进入 owner 所需 SYNC/partial=false 窗口，
finally 恢复原配置；不新建数据库、账号或隔离实例。使用 `--help` 查看必需路径，
服务器冻结源码／构建／命令／工作目录分别保留，不覆盖旧前缀和失败报告。
最终决策同时检查完整总耗时、相对 V=1 的绝对增量／增长倍数、对 FS 的相对优势及逐 campaign
一致性；空间节省单列，不以局部计数或去重率替代完整查询收益。

2026-10-17 首次N8完整试跑已完成：192配置、2,880查询通过；V8三轮联合条件仅
identical/single_unit的GRIB-All对FS-scan通过，未通过相对FS-index／V03的联合条件，
不据此全局切换V04主线。完整计时、增量、空间账本和边界见
[矩阵报告](../../docs/reports/version-directory-v04-n8-version-matrix-pilot-20261017.md)。
服务器封存根为 `work/n8-version-matrix-pilot-20261017/`；原服务已恢复，失败的辅助导入预验也保留。

## 大文件有利场景

`large_file/large_corpus.py` 与 `large_file/large_matrix.py` 使用独立的
[实验协议](large_file/specs/large-file-upper.json)，探索当前 V04 在高内容复用场景下的表现。
保持 N=8/K=2/D=4/depth=2，测试 V=1/8、Latest/All、两格式／两场景／四对照；
64 配置、4 轮循环顺序、每轮 5 次，共 1,280 次完整查询。
HDF5 每文件约 32.03 MiB，GRIB 约 7.92 MiB；全部是真实科学数组与编码。
修改组每次只改一个数值，实际每次恰好改变一个 1 MiB 块，未补齐或填充文件。

`large_file/cachefit_corpus.py` 与[补充协议](large_file/specs/grib-cachefit-upper.json)另测约 3.96 MiB 的
720×721 GRIB，保持原 64 MiB/16-field 原生解码缓存；仅 GRIB 的 32 配置、
4 轮共 640 次正式查询。`large_file/large_matrix.py --formats grib --corpus-module cachefit_corpus`
显式选择该数据协议，不做隐式格式推断。该 namespace 不含 HDF5，
跨规模结果不能当作只改变缓存适配状态的因果消融。

查询构造、native 二进制和完整计时边界沿用前述 N8 矩阵，生产守护进程不变。
不同版本仍独立返回，准备后页温热，首请求单列；该结果只描述所测有利场景，
不构成全工作负载的理论上限。原始输入引用复用，FS 对照仍使用独立完整历史副本。

大文件只读审计使用 `large_file/build_large_inspector.py` 独立构建工具，将 namespace 的累计
payload 扫描／对象统计范围调整为 8 GiB；每文件、每批与生产查询的上限不变，旧工具不覆盖。
`large_file/tests/test_large_matrix.py` 覆盖单点修改、块账本、统计与准备续接边界；
`large_file/audit_large_matrix.py` 从原始记录和实际 payload 图独立复算，
`large_file/large_profile_summary.py` 分列缓存／解码／读取计数。
完整方法、准备失败及续接记录见[大文件实验报告](../../docs/reports/version-directory-v04-large-file-upper-20261017.md)。

两次运行已完成，共96配置／1,920条正确查询；36项四轮联合门禁均未通过。
GRIB 4MiB single_unit/All 对FS-scan的合并完整耗时快1.081倍，但仅3/4轮成立；
FS-index仍更快，未据此提升V04为通用查询性能主线。
V8相同内容payload节省87.5%，单点修改HDF5 32MiB／GRIB 8MiB节省84.77%／76.45%；
这些是实际payload图和BLOB账本，不是数据库物理磁盘压缩率。
原服务已恢复，两个stage的`evidence.tar.gz`与逐文件SHA清单均已独立下载校验；
完整计时、增量、准备／恢复成本、原始记录与证据摘要见上述报告。

2026-10-18 的[GRIB 尺寸诊断](../../docs/reports/version-directory-v04-grib-size-diagnosis-20261018.md)
区分了“增大命中网格/输出”与“仅增大无关内容”：原两档输出严格翻倍；独立真实Adapter
直接计数确认8MiB档温热All每次16次native load/eviction、4MiB档16次hit。
固定目标并追加无关合法message后，目标读取range与结果不变。诊断只在独立构建产物中加计数，
不修改生产缓存、协议或数据库配置，Adapter计时不替换完整查询指标。

同日的[GRIB 内容共享解码缓存修复](../../docs/reports/version-directory-v04-grib-native-cache-fix-20261018.md)
按实际 message 字节共享同 namespace 的 Float64 字段，各源身份／预算仍独立；
保持 64 MiB 预算，增加有界两队列和并发解码合并。新旧 V04 配对完整查询、
首请求与 owner 恢复成本、测试和原服务恢复证据单独记录，不替换上面的旧矩阵。


## HDF5 方向 3：选择性大 N／V 实验

设计：[hdf5_route3/specs/route3-hdf5.json](hdf5_route3/specs/route3-hdf5.json)；过程、完成状态与结果边界见[分层实验报告](../../docs/reports/hdf5-route3.md#scale)。本轮不修改生产读取默认路径。

`hdf5_route3/route3_data.py` 生成完整分配且独立拥有的 HDF5 历史文件和数值 oracle；`hdf5_route3/native/route3_hdf5_native.cpp`／`hdf5_route3/route3_native.py` 提供通用版本目录、SQLite 元数据索引、原生 hyperslab 和 owned-array C ABI；`hdf5_route3/route3_iotdb.py` 复用真实生产 discovery、typed 读取和精确历史 FUSE 文件；`hdf5_route3/route3_run.py` 执行五路径对照。固定 D=4、每轮 K=2、64×64 ROI（原点 0/16），只做 Latest 和内部单／双轮历史窄窗。

用户批准的分层矩阵为真实数据库 N=128、V=1/8，及 N=512/1024、V=1；`hdf5_route3/route3_fs_only.py` 另外运行 N=512/1024、V=1/8/32 的原生 FS 扫描／索引机制实验。FS-only 使用一个 N1024V32 主历史的完整 file_id<N 目录范围，保留真实独立文件，不能作为 IoTDB-FS 的大 V 实测。N1024 双轮的 2,048 个候选源仅属 FS-only，超出生产 1,024 源上限。真实五路径矩阵不提高硬预算。

每点 5 个时序 campaign × 5 次重复 × 2 个 ROI；首个观测单列，剩余 4 次为 warm，不作冷缓存或独立数据集声明。五路径完整计时均结束于客户端实际拥有数值和掩码；独立 oracle、profile 等待、生成、普通发布和索引构建分别计费。原生本地文件路径是混合原型，FUSE 路径才读取数据库历史字节。

这组入口需要已授权原实例的维护窗口：`hdf5_route3/route3_control.py` 在服务切换和恢复期间延后 TERM/HUP，并核验实际 SYNC/false 和恢复后的 ASYNC/true、PID／身份／配置／受保护 hash。SIGKILL／断电仍需按原记录人工恢复。每次使用新 work 和新表前缀；不覆盖失败记录、不自动恢复部分科学矩阵、不启动隔离实例或新增账号。

在服务器既有 pinned Python、R2 原生库和冻结 V04 HDF5 守护进程均通过 hash 校验后，可按以下方式发起新的真实五路径运行（会停启原 DataNode，须具备相应维护授权）：

```sh
cd /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C
experiments/single_file/work/session-20261012/venv/bin/python -m experiments.cross_version.hdf5_route3.route3_run \
  --repo /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C \
  --work /absolute/new-work --prefix vd_v04_rt3_new \
  --native-library /absolute/verified/libroute3_hdf5_native.so \
  --native-sha VERIFIED_LIBRARY_SHA256 --N 128 --milestones 1,8 \
  --campaigns 5 --repeats 5
```

本轮 code、build/test receipts、逐条计时、实际时间戳绑定、源 manifest 和控制操作记录存于 `work/route3-hdf5-20261018`，按报告中的 SHA 查验。凭据、许可证、原配置备份、私有 SDK 日志、原始数组及数据库数据目录不纳入公开证据包。运行不同矩阵时不要与其它正式计时并发。


### 选择器下推复验

[完整报告](../../docs/reports/hdf5-route3.md#selector)记录小范围选择器点查、候选权限快照和广范围访问计划的性能回退。`hdf5_route3/route3_selector_ab.py` 在新 work 中复制已完成 owner journal，复用原始 namespace／真实时间戳与原生 R2 库，不重新发布语料。

入口必须显式提供二进制路径／SHA；`--binaries baseline|candidate|both` 控制实际执行对象，`--cases n128r3,n512r1` 限制已有 namespace。`--mode full --roi aligned --campaigns 2 --repeats 3` 保留全部选择器、五路径和单 ROI 的缩减矩阵；默认双 ROI。baseline-only 只需旧二进制；candidate-only 不导入旧计时，不可称新配对 A/B。`hdf5_route3/tests/test_route3_selector_ab.py` 覆盖配置、冻结启动器、实际查询计数和 SDK profile 范围。恢复挂载、服务切换、oracle 与 profile 等待均另计；每次维护窗口结束必须核验原服务恢复。


[目录瓶颈与 FS-index 口径审计](../../docs/reports/hdf5-route3.md#bottleneck)复算现有 R4 profile：混合路径主要耗时在目录发现／全候选准入。FS-index 是预建全历史 SQLite 元数据索引加原生 HDF5，完整 query 包括跨文件／版本／属性选择和实际 ROI owned 输出；构建索引与 context setup 单列。`selected_sources` 是版本条件下的候选数，不能当作实际打开数；scan/index 的 `candidate_datasets` 也有不同计数含义，详见报告。该审计没有新增查询或停启服务。


### 成员查找四因素消融

`hdf5_route3/route3_membership_ablation.py` 只复用已发布的 N512/V1 namespace，固定 Latest、单个 64×64 对齐 ROI 与五条完整查询路径。显式配置四个二进制的绝对路径／SHA：A 两处线性、B 仅 record 二分、C 仅 manifest 二分、D 两处二分；因子字段沿用 `*_membership_set` 名称，实际实现复用排序依赖键，不创建新 set。新输出目录不导入历史计时。

运行前必须提供 A/D 的同源真实后端 probe 二进制／SHA，检查全部原用例、成员语义与实测元数据计费一致后才允许挂载。默认五个 campaign × 五次重复，20 个 warm 样本／格，共 500 条新查询；单组使用一个保留挂载，不构成独立或顺序平衡重复。原实例维护窗口和恢复验证沿用冻结控制器。代码、构建方式、计时边界、结果和限制见[完整报告](../../docs/reports/hdf5-route3.md#bottleneck)。

本轮最终保留 C（只改 manifest），完整 warm 中位数降低 3.9%–5.3%；D 两项合并慢 4.9%–9.1%，record 已回退。首个 typed 观测仍有回退，未证明冷启动收益。最终 C 单独真实后端验收和最新原服务恢复另存 `retained-validation/`；完整已验证服务器源码为该 stage 的 `final-c-source-freeze/source-c`，对应二进制 `variants/C/iotdb_fs`。

### 跨请求的全局热缓存

`hdf5_route3/route3_global_hot_ab.py` 将前轮保留 C 与新缓存二进制重新对照，复用原 N512/V1 namespace，先测 **N512/B4/C128**，再测 **N512/C512**（全部16桶）。四桶按已知命中选择以固定 K=2；不是随机或独立发布的 N128 数据集。两范围都实际执行六属性选择和64×64 ROI owned 输出，保留 FS-scan、FS-index、目录＋本地／FUSE 原生 HDF5、typed 五臂；共210条新查询，不导入历史行。

入口需要显式 `--repo`、`--source-stage`、全新 `--work`、`--baseline`、`--candidate`、`--candidate-sha`、`--native-library`、`--context-probe`／`--context-probe-sha`、`--global-probe`／`--global-probe-sha` 及对应完整快照中的 `--global-probe-source`。冻结 helper、native 库及 baseline SHA 由脚本门禁；同目录还须有冻结 `audit_probe.py`。旧 v03 与新全局缓存实际后端 probe 通过后才挂载计时；原库维护／恢复沿用原控制器。首次索引构建、context setup、owner 挂载恢复与服务维护另列，结束后 fresh 检查 ASYNC/true、配置／许可证 hash 和无遗留挂载。

新实现只作用于显式 owner/HDF5/substructure-index：完整 head 下共享不可变候选、六属性索引和主体证明，每次实际 owner 重新准入，任意提交保守失效；不缓存 ROI／答案，不改 strict、持久格式及请求预算。新增64MiB／16域／64证明池包括构建和retired pins，既有其它缓存预算另计。详细语义见[契约](../../docs/contracts/hdf5-dataset-substructure-index.md)，完整查询验收与适用限制见[报告](../../docs/reports/hdf5-route3.md#hot-cache)。所有源码、构建、probe、逐条计时和恢复收据保存在 `work/route3-global-hot-cache-20261018`。


## 当前正式 HDF5 两模式验证

`hdf5_route3/hdf5_two_modes.py` 使用当前正式 typed 接口，对照冻结 R3、新 `iotdb-indexed` 和新 `native-hdf5`；不借用旧 catalog-native-local/fuse arm。固定 **N512/B4/C128**、stored_V=1、Latest、K2、64×64 ROI，默认 FS-index＋正式 typed，共36条新完整 owned 查询。每个取值请求验证实际引擎 counters、精确版本、Float32 值和 mask；native 的数据库读取成本计入完整时间。预建索引、挂载恢复和服务维护单列。

运行证据、完整源码／构建 binding、启动拒绝检查、真实 owner probes 与取消安全 launcher 在 `work/hdf5-two-modes-20261019`；启动器必须绑定通过审计的完整构建与同二进制 startup receipt，不能直接替换历史 runner 的二进制。功能验收通过；计时可能受49服务器并行数据合成影响。按用户最新指示保留当前新版，不增加源码版本；R3保留为历史对照。另有一次独立反序24条验证，不混入原36行。结果与限制见 [验收报告](../../docs/reports/hdf5-route3.md#modes)。
