# 4.2 单文件查询与写入实验（2026-10-26）

阶段记录（2026-10-27；后续完成状态见文末最新验收条目）：读取、F5 会话和独立发布阶段已通过独立验收，并回填飞书第 4.2 节（revision 1098），脱敏原始数据归档及本地逐成员核验已完成；F6 连续生命周期的两个中断轮次全部排除，维护完成后已用新私有命名空间从 r3 按轮次补跑。联合验收完成前不宣称全部实验完成。

## 范围与实现

按照飞书论文实验表第 4.2 节执行。仅比较 native FS 与 IoTDB-FS，不增加外置 FS-index。HDF5 产品路径分别测量 `native-hdf5` 和 `iotdb-indexed`；GRIB、Arrow 使用 `iotdb-indexed`。本次不修改产品实现，不增加客户端并行或查询内并行。

冻结产品 SHA-256：`64672d18371ce3c734e836d53f3a96dde00cd976b9e0ce6c23e998713a715357`。

服务器根目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/section42-20261026`。原始文件不修改；新私有 owner namespace 发布相同字节，为两种 HDF5 产品路径提供真实受管完整文件和 Exact 版本。原件字节数、完整 SHA-256、源路径、新发布版本、源码及软件环境保留在 preflight 和 publication 收据中。

## 资源与时钟

- 客户端、native FS、产品及 DataNode 共享 CPU 2/3 和两核总配额。query cgroup 上限 2 GiB；DataNode 堆 15,232 MiB、direct 2,176 MiB，单独记录。
- 公共 worker=4、batch=4、owner readers=4；本节在途查询 L=1。HDF5 `read_parallelism=1`、`native_cache=false`，共享解码缓存预算 256 MiB。
- 完整 T 截止客户端拥有独立值、逐值 mask 及适用的坐标或编码字节。逐条严格核验 T=d+q；独立 oracle、完整 hash、profile 采集及进程启动在在线查询 T 外。
- GRIB/HDF5 主矩阵使用 `result_fetch_v1`。Arrow 与科学补充组读取完整 FUSE IPC 后生成独立数组。不同传输组之间不直接排名。
- Arrow 已知 Exact 直接查询，两方法均 d=0、q=T；原生解析仍包含在 T 内，Rd 不适用。不存在被省略的外置 FS 索引准备。
- 查询块每轮 repeat 0 单列首读、repeat 1/2 预热、repeat 3–12 暖测量，共三轮。产品每阶段、轮次、执行模式复用一个 daemon，片段和元数据缓存跨 case/Q 保留；native 每块新 reader、每次重新打开文件。Python 进程、原生库全局缓存、OS 与 DataNode 缓存可以已有热状态，首读不表示冷盘。
- 暖态各阶段分别取轮内中位数，再取三轮中位数；比值先按同轮相对 native FS 计算，再取中位数。分别聚合后的 d、q、T 不保证相加，原始每条时钟必须闭合。

## 会话、写入和不支持项

F5 的 Q=1/10/100 分别独立执行；直接记录 prepare→Q 次完整结果→release 的 session wall，不截取 Q100 前缀，也不相加不同样本中位数。native FS 测试原生句柄/定位复用 0/1，仍每次实际取值和产生独立结果；HDF5 原生库默认 chunk cache 可随打开句柄复用。当前产品构建没有旧实验 PreparedQuery 接口，仅测普通独立请求，不以不同语义的生产模板替代。

GRIB native 会话复用的是文件及消息定位 metadata，每次仍读取编码字节、创建 ecCodes message handle、解码 Double 后将选区转换为 Float32；没有另建应用层编码/解码缓存。HDF5 使用原生库默认缓存；产品保留其现有 256 MiB 共享解码缓存。因此这些比值比较的是完整已声明配置，不能将全部差异单独归因于索引或并行。

F6 将独立写入/发布与真实连续生命周期分别记录。每个 Q、方法、轮次都有新目标和完整 copy→Exact 可见→Q 会话；产品生命周期使用 `iotdb-indexed`、prepare_reuse=0。写入包括完整 1 MiB 分段写、文件 fsync 回执和 close，不声称包含父目录持久化。发布三轮保留不可变内容，后续轮次允许去重，首轮与保留状态分别展示。

GEOS 使用物理 Float32+mask，坐标用于选区并独立核验。GEOS-ALL 的独立客户端输出为 174,424,320 B，单是 IPC Float32 值列也有 139,539,456 B，已超过当前产品 64 MiB 编码结果上限；真实预算失败单列，不能填作成功延迟或比值。OISST 使用 2024-01-01 原件的科学 Float64+mask。

当前构建没有独立 GRIB 编码缓存开关，F7 不执行缓存消融；不更换历史二进制或调整混合公共缓存伪造该变量。F9 固定 batch=0，不沿用旧 batch=2 的压缩 buffer 字节数。

## 验证与证据

本地 26 个 Python 实验测试通过，覆盖原始 IPC dtype 拒绝、mask/独立数组、Exact 来源、会话/生命周期、单轮完整性及错误传播。此前冻结产品的 C++ 测试结果不代替本轮查询正确性。

本地辅助入口：`experiments/single_file/section42_20261026/`；被忽略的阶段目录保存启动器、预注册计划、独立审计及文档渲染脚本。最终证据清单、实际样本总量、资源和服务恢复状态将在完成后补齐。

### 已验收的读取和会话

`formal-r3` 完成 2,145 条正式查询和 63 个会话内的 2,331 次查询，共 4,476 份成功结果；另有 22 条资格查询与 6 条 GEOS-ALL 的真实 code16 预算失败。成功查询覆盖 55 个 case/variant/method 统计单元，会话覆盖 21 个 case/method/reuse 单元。原生 FS / indexed / native-hdf5 的正式查询数分别为 897 / 858 / 390。

独立验收重新核对原始时钟、记录结果 hash 与独立 oracle、Exact 版本和原件 SHA，并逐单元重算轮内中位数、三轮中位数及配对比值。216 个冻结源文件快照和 20 次私有挂载的释放记录通过。历史 IPC 实体未全部归档，主矩阵 schema 检查仍同时依赖冻结 collector 和执行记录；科学补充组另保存 result descriptor 可复核。

以下为部分暖态完整 T（ms），用于呈现结果边界，完整 34 组表格位于[飞书第 4.2 节](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg#doxcn9fPa2qHbZsnVfYh2Eqv8ee)。R 是逐轮配对比值的中位数，不能直接用表中两个汇总 T 相除代替。

| 查询 | native FS | IoTDB-FS indexed | IoTDB-FS native-hdf5 |
| --- | ---: | ---: | ---: |
| G-F1-M22 | 15.6954 | 2.0733 | 不适用 |
| G-F1-M4598 | 1,430.4851 | 4.2849 | 不适用 |
| H-F1-T160 | 0.3982 | 2.1155 | 2.9086 |
| H-F2-R192 | 0.7716 | 2.2731 | 3.3597 |
| H-F8-OISST-R64 | 7.5520 | 2.7784 | 10.4151 |

大 GRIB 的收益主要与避免逐请求扫描文件定位有关；受控 HDF5 小选区中两条产品路径均慢于原生 FS，这些负结果完整保留。不同传输、缓存及准备条件已逐项声明，本节 L=1 的比值不证明四路并行收益，也不单独证明索引的因果贡献。

正式读取阶段周期资源采样 5,723 条，其中实验区间 5,498 条，最大间隔 1.0011 s；query 内存峰值 1,947,426,816 B，未记录 memory.max/OOM 事件。发布阶段峰值 2,148,556,800 B，比配置 2 GiB 高 1,073,152 B，memory.max 事件计数 151,200，无 OOM。内核瞬时记账峰值和配置上限分别报告，不能声称全程占用低于 2 GiB。

读取分析 SHA-256：`c27585ea14520810f46f897cae5359ef04022f865e8c2e40efa5de946c1794e5`；独立审计 `formal-r3/read-independent-audit.json` SHA-256：`a1cfbfbeaa768074324a519624f530b3caf23d1066872126e8213798eedcb432`。采集完成时有独立服务及资源恢复收据，原服务配置 SHA 与下述发布阶段一致。

飞书回填后已逐表格单元和逐段回读核对：5 张表、34 组，数值及待补齐标记均与渲染输入一致；4.3 节内容 SHA-256 `85b29fc627f6e06acc8bb3eb380937c8ba01f5014d14754b926e83183ca48bd4` 前后不变。文档回填凭证位于本地阶段目录 `evidence/feishu-acceptance-receipt.json`。

### 生命周期中断及恢复

`lifecycle-r1` 仅完成 34/90 个生命周期。系统日志在北京时间 2026-10-27 00:40:21 明确记录 `The system will reboot now`，同时数据库连接断开，daemon 抛出 Broken pipe 后中止；00:42 关机、00:43 启动。实验控制器已写回原配置，但重启打断了服务启动及恢复验收。该次全部部分结果保留，未进入表中生命周期统计。

2026-10-27 已恢复隔离实例并实际验证 ASYNC/true、原配置全文 SHA、许可证和节点身份文件 SHA；旧实验 cgroup 及私有挂载在重启后均不存在，凭证 `reboot-recovery/receipt.json`。这份补充恢复凭证不把中断运行改写成成功运行。

补跑使用 `lifecycle-c0-r2`、`lifecycle-c1-r2`、`lifecycle-c2-r2` 三个独立受管窗口，各执行 30 个完整生命周期、1,110 次查询，保留原全局方法/Q 顺序轮换，每轮新 namespace，每轮完整恢复，失败则停止后续窗口。新 controller 从现有隔离实例的 SHA 绑定运维配置中读取连接默认值，仅在内存中使用，替代重启后已不存在的历史 `/proc` PID；产品、原件、预算及在线时钟保持一致。

首轮补跑最后可达时已通过 bootstrap 和 SYNC/false 实效检查，进入 worker；此后 SSH 超时，用户确认服务器正在维护。维护期间不再连接或修改远端，补跑是否继续及最终恢复状态尚不能确认，不能据后台启动记录宣称完成。维护结束后先核对 boot/PID、控制器与资源/服务收据，再决定接续或用新 run 重跑；读与发布阶段的脱敏全量归档尚未启动，本地已回收分析、预期错误、独立审计和恢复凭证。

2026-10-27 10:03 恢复连接后确认维护期间再次重启，boot ID 为 `321d8474-82b8-433f-85da-b0f63d47f808`；`lifecycle-c0-r2` 在 13/30 条后收到终止信号，r2 全部排除，c1/c2 尚未启动。隔离实例重新启动后已通过原始 ASYNC/true、配置全文 SHA、受保护身份文件和旧临时资源消失检查，收据 `maintenance-recovery-r2/receipt.json`。随后使用 `lifecycle-c0-r3`、`lifecycle-c1-r3`、`lifecycle-c2-r3` 重新开始，保留 r1/r2 原件，不跨中断拼接统计。

### 读取及发布阶段脱敏归档

归档于北京时间 2026-10-27 10:05 完成：`read-evidence.tar.gz`，35,297,787 B，SHA-256 `92b67ce75a2dae55e106d209b21db6fa0ddfe0bbc82ddc150a6d9b2110865c32`。包含 542 个原文件及 1 份成员清单，原文件合计 456,985,344 B；清单 SHA-256 `dd18442019c6dce7ab3ec5919fa86cf192d1a94dcf92834c37d880f9030baab0`。已回收本地阶段目录，并核对档案与全部 543 个成员 SHA；535 个文件新解包，8 个已有相同凭证保留。

归档白名单包括 publication-r6/formal-r3 的分析、逐查询/会话/写入、资源采样、发布和恢复凭证、冻结源码快照，以及计划、只读审计、Arrow 元数据和首次重启恢复凭证。不包含原生大文件、owner journals、配置、私有日志或 IPC 结果实体，也不包含任何 F6 中断或运行中数据。原始数据仍完整保留于服务器；后续 F6 证据另行归档。

### 一次性后续验收流程

2026-10-27 11:40 观察：c0-r3 完成 30/30，科学采集及服务/资源恢复门禁均通过；c1-r3 完成 12/30，c2-r3 排队，统一独立验收尚未执行。已启动本机一次性处理进程，最长运行 12 小时，只等待已有三轮，不新增实验或定时任务。原先已验收的读取/发布输入和后处理工具 SHA 冻结；三轮完整恢复后，依次独立重审、归档、回收逐成员校验和渲染。

回填前再次读取飞书：4.2 全文必须仍与 revision 1098 的已验收快照一致；发生他人编辑、主机重启、实验或核验失败时停止，不接受部分数据、不覆盖已变化文档。成功写入后回读全部表格/段落并确认 4.3 不变，最后追加本报告的完整验收记录。进程状态及最终结果保存于 `experiments/cross_version/work/section42-20261026/finalization-r3/completion.json`；未见 `complete=true` 前不将 F6 视为最终完成。

Arrow batch=0 的独立物理参数核验未解码 values：Electricity 未压缩 target.values 为 24,248,320 B；LZ4 对应 buffer 为 6,464,186 B（含 8 B 长度前缀），解压后仍为 24,248,320 B。补充元数据工具单独归档，未参与原始在线查询。

### 已完成的发布阶段

`publication-r6` 完成 35 条写入/发布记录：native FS 15 条、产品 20 条，其中五个补充源只发布一轮。完整字节读回及 Exact 可见性均通过；资源采样覆盖验证通过，原服务配置 SHA-256 恢复为 `f69f408de5c09edabae3a8293cf662327f794b908cbf98d5a02b3878fea154f4`。分析文件 SHA-256 为 `f2f3737b9dbd148234c8f15a61230b38810f5ffd8c445847ab8e9744b61df424`，本地已独立复算全部写入中位数。

以下仅为包含 fsync 与 close 的写入秒数，源定位另外记录，不代表完整查询或生命周期。

| 源 | native FS 三轮（s） | 产品三轮（s） |
| --- | --- | --- |
| G-M22 | 0.015988 / 0.017963 / 0.016545 | 3.192991 / 2.094351 / 2.115263 |
| G-M4598 | 1.388865 / 1.418554 / 1.407229 | 90.300033 / 44.494307 / 45.749665 |
| H-T2 | 0.030704 / 0.030535 / 0.031824 | 5.843095 / 2.699591 / 2.427424 |
| H-T160 | 1.371900 / 1.420599 / 1.406927 | 91.119571 / 47.028602 / 47.747921 |
| H-T1024 | 6.913071 / 6.902542 / 7.667743 | 509.750020 / 289.891561 / 286.122230 |

三个 Arrow 就绪查询的真实 profile 均为 index-only，`payload_calls`、`payload_returned_bytes`、`payload_read_calls/bytes/ranges`、`decode_count` 显式为 0。它们验证提交收据中的 Exact 来源，不预先读取 values。

### 前期记录的排除范围

早先发布 r1–r5 的接口/设置预检失败记录保留于服务器，没有拼入 r6 的 35 条完整记录：分别涉及源注册表字段、owner prefix、journal 权限、GRIB 显式格式声明、Arrow physical subject 的 Exact 要求。各受管运行完成原服务和资源恢复；原始科学文件不修改。

`formal-r1` 在创建资源前被 keeper 时限校验拒绝，零查询及零服务/资源修改。`formal-r2` 只进入 qualification，发现每次 daemon 启动深校验同一全 namespace 的开销后主动停止，正式查询数为 0，并完成服务/资源恢复。正式 `formal-r3` 在冻结采样前调整为每阶段/轮次/执行模式复用 daemon；所有 case 的重复数量、版本、预算与输出核验保持一致。上述前期记录不进入正式统计。

### F6 联合验收的日志排序修正

2026-10-27 13:21 左右，c0/c1/c2-r3 三轮均完成，每轮 30 个生命周期、1,110 次查询及完整服务/资源恢复。首次联合后处理在 13:26:53 停止于 `source visibility profiles differ`，没有尝试写飞书；失败收据及日志保留于 `finalization-r3/`。

原因是独立验收辅助脚本误将 profile 文件的落盘行序当作请求执行顺序：c2-r3 中两处顺序为 457/459/458/460 和 913/915/914/916，而请求序号对应的执行时钟均不重叠且递增。产品在响应发送后生成 profile，再争用日志互斥锁落盘，因此行序可不同；冻结采集器的 `_harvest` 本就按唯一 sequence 排序。原始行及采集结果不修改，验收辅助脚本改为先要求序号完整覆盖 1–1140，再按序号绑定既有 profile、row 和时钟，并把原始落盘错位行写入派生审计。值、Exact、版本、预算、请求时钟、完整性和恢复检查均保留。

原验收脚本 SHA-256 `09830fd62f65b2e8757f435c6867882764ea7a52d3d41f5901201aa97c3e9bf2`，保留为 `merge_lifecycle_campaigns.before-profile-order-fix.py`；修正脚本 SHA-256 `00f44e187b755b2e57b201ec3d75eb1c5148d1e7240cd0425e77acbbce50311c`。产品与冻结实验采集源码未改，未新增实验。重试验收独立保存在 `finalization-r3-retry1/`，不会覆盖失败的首次记录。最终完成状态以其完整收据和下文验收条目为准。


### 连续生命周期最终验收（2026-10-27T13:49:45+08:00）

<!-- section42-lifecycle-r3-finalized:c3c2709fb92b872cba6ab560fed07d43afd9adce04b694100b0e84f5a4a3743e -->

`lifecycle-c0-r3`、`lifecycle-c1-r3`、`lifecycle-c2-r3`三轮均完成并恢复，独立重审得到90个完整生命周期、3,330次新查询；r1/r2及全部中断部分不纳入。汇总是原始完整轮次的派生统计，保留原墙钟，不拼接不同运行的写入与查询。

生命周期分析SHA-256：`c3c2709fb92b872cba6ab560fed07d43afd9adce04b694100b0e84f5a4a3743e`；独立审计SHA-256：`5460a33a40fb2c65e0252ae9a9a044f801e85c9dbca71cf3bbda9b1d06fb70a8`。归档`lifecycle-evidence.tar.gz`，20770393 B，SHA-256 `6e666f4515d7528c51b19efc448a7dbed8a794bdaf47da20acb0f5fee9625e6b`；本地逐成员核验718项全部通过。

飞书4.2最终回填并回读验收，实际revision 1101；5表34组的全部表格单元及段落与生成结果一致，4.3全文SHA-256 `85b29fc627f6e06acc8bb3eb380937c8ba01f5014d14754b926e83183ca48bd4`前后不变。完整收据见`experiments/cross_version/work/section42-20261026/finalization-r3-retry1/completion.json`。先前“补跑中”状态由本次完整验收取代；F7无独立开关仍明确未执行，GEOS-ALL的6次code16失败继续单列。
