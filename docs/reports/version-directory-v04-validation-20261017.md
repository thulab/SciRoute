# V04：payload 内容共享与直接读验收

日期：2026-10-17。开发基线 `6a325cf`。当前状态：**V04 已完成**，最终 r3 构建和 r5 全部真实门禁通过；V05/V06 接续。

用户已确认新实验前缀、namespace-record/3、content-build/3、file-descriptor/2 和 head 确认后清理本版本暂存 payload；不隐式升级旧前缀。原数据库、许可证、共用账号及默认 strict 保持。V03 prefix 和既有构建保留。

实现以原编码字节 1 MiB 定长块去重，尾块保留实际长度；descriptor/seal 绑定完整 payload-map 根，不引用前版。extent-page 保存逻辑范围、块身份、块内偏移和实际长度，父页绑定子页范围、条目数和编码长度。页 64 KiB/256 项、树高 8；每请求 65,536 extent、64 MiB 元数据、1 GiB payload，实际传输同时按 64 项/8 MiB 分批。不额外把每请求 extent 上限当成整文件大小上限。

共享块复用既有 namespace `_objects` 表的 payload-block BLOB 行，内容 ID 直接定位物理行，不另建可原地更改的 locator；Store 原生集合查询后按请求顺序重排并校验。逐冷块读取完整块校验，实际块字节和逻辑输出字节分列。乱序、重叠和重复范围保持原输出顺序；物理块去重不减少逻辑计费。

小范围可命中多个完整块。读取按最多 8 MiB 一批校验并直接 scatter 到有界输出，随即释放块；共享 producer 的整块临时内存和实际 I/O 纳入现有 runtime 预算，批次之间检查取消/资源失败。请求逻辑字节仍由现有 QueryReadBudget 计；共享请求 ledger 另外保守计每范围所触完整块需求及 extent，缓存重放费用、失败 sticky。该费用边界和元数据分账，不声称降低全链可抢占 SDK deadline 限制。

已发布 manifest 携带仅进程内的固定内容 reader，绑定 issuer、exact key、revision 和大小/格式；不进入持久编码、ContentId 或客户端协议。ReadHandle 与训练 worker 复用该能力，避免执行准入后再查新的 Exact。新执行/注册/open 仍走 V03 依赖准入，能力不是永久权限。owner 普通大范围 scalar 受 1 GiB 上限约束，拆成有界读取；strict 路径保持原样。

发布先沿用 staging OBJECT，流式构建和确认共享块/map 后再提交 head。仅确认提交后清理该版本 staging payload；失败保留 orphan 和失败计数，恢复后重试，不把已提交 head 报成未提交。删除共享块留待 V05，不执行物理 GC。

验证计划包括原 32 科学版两轮各 128 typed 单元、9 项非 root 权限、旧 V03 head/journal 不变、默认 strict 原 256 版/1,024 单元；另增大于 3 MiB 的两组各 8 版本 byte-pass fixture。现有科学文件小于 1 MiB，只能证明完全相同文件的共享，不能将修改一个 dataset 误报为跨块部分共享。大文件 fixture 用独立 Python 1 MiB 分块 ContentId 与全文件/跨块 SHA oracle，核对实际引用和 staging 清理。

首轮 Linux 实际依赖统一构建 676 项通过（30.275 s），尚未包含随后新增的故障/内存边界和普通请求预算覆盖修正。r2 候选在相同服务器依赖下通过 **687 项、56 个 suite，0 失败/错误（31.597 s）**，仍未包含后述 GRIB numeric 缓存修正，故作为历史证据保留。PayloadArtifacts 18 项、StructuredBlobStorage 35 项、DecodedFragmentCache 51 项覆盖发布块/页/写后回读故障不切换 head、空文件/尾块/溢出/缺块/损坏、乱序重复跨块范围、缓存重放请求累计预算、65537 extent 文件局部 seek，以及 128 个不同块的小范围读取只保留单批缓冲。最终 r3 构建已通过 **694 项、56 个 suite，0 失败/错误/禁用（29.319 s）**，包括新增 7 项 GRIB 回归。统一测试 XML 和二进制 SHA 均已留存。真实批量 SQL、共享块账本、读放大与恢复结果见下文；最终真实验收已通过，结果见后文 r5 及机器收据。原服务器只读预检确认配置 SHA `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`、DN 2386091、CN 2645019、原凭据来源和 V03 验收摘要一致，未为此改变服务配置。

## 冻结来源与验收入口

r2 源包包含 829 个文件、18,241,397 字节，SHA-256 `62b92a51eee9462656ae3977ade19d2dcbc7358e3480537f7c0358f443484d81`。服务器逐文件核对后构建，实际 daemon SHA-256 `f9fd829e8f088aa5e2f8cf0bca5faa871e27777ad17fcd9996ec2dbb9233b1fc`。原始证据位于服务器 `experiments/cross_version/work/v04-verification-20261017/`；测试命令、XML、构建日志、源码 receipt 与八个二进制摘要分开保存。r1 仅作为诊断历史保留。

真实验收入口为冻结的 `v04_validation.py`、`v04_n128.py` 与 `v01_strict_validation.py`，正确性控制器 `work/v04_accept_r3.py` 和接续控制器 `work/v04_accept_r4.py` 保存每个子命令及日志。工作目录分别为 `work/v04-acceptance-r3-20261017/`、`work/v04-acceptance-r4-20261017/`，每个独立窗口记录原配置、受保护文件摘要、DataNode 停启及恢复事件；ConfigNode 保持运行。只使用原数据库及许可证，不启动隔离实例。

## 大文件真实引用账本

r3 完整正确性通过，独立 Python oracle 与真实 Store 图检查一致：

| 项目 | r3 实测 |
|---|---:|
| 每路径版本数 / 场景数 | 8 / 2 |
| 每版本字节 / extent | 3,145,865 B / 4 |
| 逻辑 payload / 块引用数 | 50,333,840 B / 64 |
| 唯一被引用块 / BLOB 值字节 | 11 / 9,437,458 B |
| 独立版本 seal | 16 |

相同内容的 8 版共享同一完整 map；每次只改变一个块的 8 版产生 8 个完整 map，其余块保留共享。上述字节只计引用到的 payload-block BLOB 值，不含 map/seal、目录 COW、暂存、孤儿及数据库编码，不能称为端到端物理磁盘压缩率；本轮不运行 GC。

## 前置失败与脚本修正

r2 真实正确性首轮已通过科学版前后各 128 typed 单元、16 大文件图/字节检查、两组生产 ReadHandle 范围探针、删除单个引用不破坏其它版本；随后非 root 权限首项失败。原服务由 finally 恢复，失败报告 `results/v04-live-r2-20261017.json` 及所属日志保留，不计为最终通过。

原因是控制器 `umask(077)` 使宿主 `mkdir(mode=0755)` 实际生成 `owner-mount` 父目录 0700，匿名子进程尚未到达 FUSE 即被宿主拒绝。Linux 最小复现仅修改该父目录为 0755 后通过，数据库操作为 0。r3 只在独立 `/tmp/iotdbfs-v04-permissions-*` 测试面显式 chmod 此父目录并记录前后模式，配置/日志仍 0600、控制目录仍私有；不改 FUSE 授权、生产 C++、测试或二进制。`work/v04-harness-fix-r3.json` 保存唯一 diff 和前后 SHA，独立 `work/v04-experiment-r3-20261017/source` 从 r2 全量校验复制后覆盖此一脚本，完整重跑新前缀。最终来源绑定会明确这一脚本差异，不能把实验 overlay 称为新的生产构建。

## 科学语义、权限与生产 ReadHandle（r3 通过）

最终正确性报告 `results/v04-live-r3-20261017.json`，prefix `vd_v04_correctness_ace130ddcbc24a2e`。32 个科学版本重启前后各 128 typed 单元、全文件 SHA、掩码/来源通过；9 项真实非 root 权限通过。新二进制拒绝隐式打开旧 V03 profile，固定旧 inspector 检查及 head/journal 逐文件对照未变。删除 single_unit 的一个精确引用后，其它共享版本的完整字节仍正确。

这里 32 科学版和 16 大文件版均指选定的非空数据版本。现行 O_TRUNC 可另外提交空快照，语义保持原样；发布图实际含 96 个 Exact 文件版本，其中 48 个为空，不将它们隐去或计成额外非空实验样本。全部图、版本号和独立 seal 留在原报告。发布后新 prefix staging payload 行数为 **0**；完整 map/block 图是后续读取来源。

两组生产 `v04_range_probe` 均在 FUSE 卸载后打开实际 backend/ReadHandle，正常恢复可重试 staging 清理，故不是纯只读 SQL inspector；探针内 head/journal 前后摘要相等。每组 full warmup、七个乱序/重叠/重复/跨块范围、再读全文件均通过独立 Python SHA oracle。恢复已温热控制页，应用没有 payload-block 驻留缓存；每次数据读仍逐完整块校验。

| 单组生产观察 | 输出字节 | 实际完整块字节 | 逻辑 extent | 实际查询提交 |
|---|---:|---:|---:|---:|
| 全文件 warmup | 3,145,865 | 3,145,865 | 4 | Store batch 1，旧 SDK 0 |
| 七个混合范围 | 84 | 3,145,865 | 9 | Store batch 1，旧 SDK 0 |
| 再读全文件 | 3,145,865 | 3,145,865 | 4 | Store batch 1，旧 SDK 0 |

混合范围物理去重为四块，但仍按九个逻辑 extent 计费；84 B 输出实际读取 3,145,865 B，放大约 **37,450.77×**，如实体现 1 MiB 粒度下小范围冷块校验的代价。这里 SQL 是 `executeQueryStatement` 提交数，不是 wire RPC；完整控制页冷读另有映射查询，不能从本表推导全冷请求仅一条 SQL。

| 正确性运行成本 | r3 实测 |
|---|---:|
| HDF5 / GRIB 发布中位数（各 16 非空科学版） | 3.076963232 / 2.490214325 s |
| identical / single_unit 大文件发布中位数（各 8 非空版） | 2.783366343 / 2.246534027 s |
| owner 新进程恢复 | 38.209945107 s |
| 发布后 / 重启读取后 daemon RSS | 104,204 / 70,172 KiB |
| 发布后全部 objects BLOB | 918 行 / 11,370,383 B |
| 权限与删除检查结束后全部 objects BLOB | 1,071 行 / 11,689,228 B |

objects 统计含目录旧根、空快照、seal 及不可达中间对象，不是唯一可达量或磁盘压缩率。该运行是正确性案例，不能替代冻结 N=128 的计时比较。

## ReadContext 前置参数与服务控制修正

r3 正确性报告完成后，复用的 `v03_context_probe` 在 arguments 阶段拒绝 `vd_v04_context_*`，completed_cases=0；它只允许新的 `vd_v03_*` 工具前缀，尚未建立数据库会话或创建测试表。随后维护脚本迅速恢复启动时遇到原 IoTConsensus `127.0.0.1:33660` BindException；旧 readiness 只检查 RPC listener，不能证明全部 DataNode 端口已可重绑定。失败 context 报告 `results/v04-context-20261017.json` 原样保留，不能称为已恢复成功。

同一原 DataNode 通过既有 `runtime/bin/env.sh` 与原 release 的 start-datanode.sh 恢复，原配置 SHA、`.env`/license/system_info 不变；只读 SHOW 另证 ASYNC/true。独立恢复收据为 `results/v04-original-recovery-20261017.json` 和 `results/v04-recovery-settings-20261017.json`，未启动隔离实例或获取证书。ConfigNode 全程保持原进程。

`v00_head_probe.py` 的受控 ExistingInstance.start 现先确认原 DataNode 的 33630/33640/33650/33660/33667 五个端口都可重绑定，再启动并要求 RPC 连续稳定 10 秒。只变维护脚本，不改生产 C++、单测或二进制。回归以 fake sockets/subprocess/clock 重现“RPC 空闲但 consensus 忙”时旧 helper 过早启动，新 helper 等待后通过，并覆盖 listener 暂断后重新累计稳定窗口；不实际绑端口、启动进程或操作数据库。原始回归与唯一 diff 见 `results/v04-service-guard-regression-20261017.json`、`work/v04-service-harness-fix-r4.json`。

r4 控制器只接续未完成的 context/N=128/strict，绑定已通过 r3 正确性报告 SHA 与相同 r2 daemon。新 context prefix 为 `vd_v03_v04_context_*`，仅满足不变探针的命名 guard；使用当前 V04 构建建立全新 profile/3 namespace，不打开旧 V03 前缀。实验源 overlay 在 `work/v04-experiment-r4-20261017/source`，相对 r2 源包仅有权限面与服务控制两个脚本修正。后续证据路径使用 `work/v04-acceptance-r4-20261017/`；r2/r3 失败和 r3 成功的正确性证据全部保留。

## ReadContext 与 DataNode 重启后范围复验（r4 通过）

`results/v04-context-r4-20261017.json` 完成且原服务恢复。当前 V04 构建在全新 `vd_v03_v04_context_3e9c6a7b489a4ebf` 通过 11 项真实 backend/HDF5 检查。直接 Store head 校准为 1 次真实查询、140 B BLOB，旧 SDK 路径 0；热准入/manifest 共 6 次调用、热发现/最终准入共 4 次调用均两条 SDK 路径 0 查询。payload_map.root_requests 确有增加，控制页缓存命中仍记录逻辑费用，不能解释为跳过验证。

257 次无关提交后 history_gaps=1、dependency_rereads=8，重验 2 个冻结源与 2 个当前祖先；此次控制对象仍命中缓存，SQL=0，但依赖重验没有省略。谓词未命中候选仍保留在上下文；新 Latest 不改旧 exact；源权限、祖先搜索权限撤销和精确源删除分别拒绝新准入；温热源/catalog 与累计 metadata 限额检查通过。

正确性窗口结束已恢复/重启原 DataNode，r4 再开启原节点 SYNC 窗口后，通过相同实际 ReadHandle 重读两组 ordinal=8 大文件。两组均完整 SHA 与七范围 oracle 相同，四块仍一条 native batch SQL；探针 head/journal 前后不变。随后原服务再次恢复。这里新增的是 **正常 DataNode 停启与 owner 新进程恢复**，没有新做数据库 SIGKILL 用例；V00 原语故障证据仍按原范围保留。

预算口径补充：重复直接范围和独立直接读取分别计费；同一请求重复消费同一个已解码 producer 的稳定费用身份只重放一次，代表复用同一 DAG 工作节点。不同 source/version 的 fragment key 独立，完整块放大仍按各 distinct 读取节点累计。HDF5 distinct-fragment logical bytes/ranges、GRIB logical payload 与输出/行数限制另行生效，不以物理共享减免逻辑输出预算。该语义由现有费用重放回归固定，不是缓存命中无限免计。

## GRIB 数值缓存预算复核与修正

最终审查另发现独立 `GribDecodedFieldCache` 的 numeric grid 热命中未重放 producer 费用，numeric 请求也未在命中前预留逻辑 payload。这不是上述共享 DAG 节点的同请求复用：温热同一 message 后，新请求设置 `max_payload_bytes=1`，以及另一新请求 ledger 已占用 `1 GiB−1` 时，均错误成功。真实 ecCodes/adapter/cache 路径的两个新增回归在修改实现前均失败，red JSON 保存在 `/private/tmp/iotdbfs-v02-grib-20261016/v04-red.json`。

为此受控中止 r4 N=128 比较，原报告保留 `complete=false`、中断原因及 `original_service_restored=true`，不把部分观测计入最终比较。r2 的 687 项测试及 r3/r4 成功证据按原二进制范围保留；修复后重新冻结源码、构建、完整正确性、context、N=128 准备/比较与 strict，使用新的工作目录和报告，不修改旧 prepare 的 ingest 二进制身份。原始 raw/FS/SQLite 仍原位复用。

修复后的 GRIB 相关 52 项测试及 ASAN 均通过。相同最终测试源码配合固定旧 HEAD 实现，再次得到原两项失败，形成同测试的前后对照；命令、源码摘要和 JSON 已保存至服务器 `work/v04-grib-budget-repro-20261017/`。新实现逐请求计逻辑字节，中立费用 trace 在热命中和异步 join 重放；native/decoder key 按 namespace/revision 隔离。冷 numeric 与 BinaryView 复用 encoded-message runtime loader，持 lease 的内存视图解码，执行租约传至异步内部 producer。既有 native-grid 分配模型与独立 native-field 缓存上限不改；不把该修正夸大为全部 native 内存的统一预算。

最终 r3 源包仍为 829 文件、18,288,907 B，SHA-256 `6271c8633637bcdee83eec1d6966ab38f609e1c19cbca502b3e074dcca96e642`。独立服务器构建目录为 `work/v04-verification-r3-20261017/`，daemon SHA-256 `83b90b15181f3311e5b70bf8dfbd334295b3e5422bf1ded388a9948ce2144305`；包括权限面和服务控制修正在内的实验源码均直接包含于该冻结包，没有后续代码 overlay。最终控制器为 `work/v04_accept_r5.py`，运行目录 `work/v04-acceptance-r5-20261017/`，重新使用全新 V04 实验前缀完成全部门禁；最终真实运行及环境恢复核验已通过，结果见下文。

## 最终构建正确性复验（r5 通过）

`results/v04-live-r5-20261017.json` 完成并恢复原服务，prefix 为 `vd_v04_correctness_53be0912a03d4767`。最终 daemon 重跑科学版前后各 128 typed 单元、全部完整哈希、9 项非 root 权限、两组生产 ReadHandle 范围及单引用删除检查，均通过；旧 V03 head/journal 不变。新图仍为 96 个 Exact 文件版本/48 个空截断快照，16 个非空大文件的 64 引用仍对应 11 块/9,437,458 B，staging payload 行数仍为 0。四块一条 native batch SQL、混合范围的 37,450.77× 冷块读放大均复现。

| 最终正确性运行成本 | r5 实测 |
|---|---:|
| HDF5 / GRIB 科学版发布中位数 | 2.453183815 / 2.302852780 s |
| identical / single_unit 大文件发布中位数 | 2.074339895 / 2.196163975 s |
| owner 新进程恢复 | 26.938558967 s |
| 发布后 / 重启读取后 daemon RSS | 98,396 / 68,988 KiB |
| 发布后 / 最终全部 objects BLOB | 918 行 / 11,370,383 B；1,071 行 / 11,689,228 B |

该表与前述 r3 的成本是各自正确性运行的观察，不据其差异推导缓存修正的性能收益。最终 N=128 固定比较另行报告。

最终 `results/v04-context-r5-20261017.json` 同样完成并恢复原服务。新 prefix `vd_v03_v04_context_b298ba9b4aa64db8` 的 11 项检查通过，head 为 269；历史缺口后完整依赖重验、热元数据预算和撤销准入均通过。原 DataNode 停启后两组大文件的生产 ReadHandle 全文件及混合范围哈希通过，四块一条 batch SQL 复现，探针 head/journal 未变；报告同时绑定最终探针 SHA 和 r5 正确性报告 SHA。

## 最终 N=128 准备（r5 通过）

`results/v04-n128-prepare-r5-20261017.json` 完成并恢复原服务。最终构建在新 prefix `vd_v04_n128_r5_20261017` 摄取 HDF5/GRIB 各 128 个文件，新进程恢复后验证全部 256 个文件 SHA-256 及两格式独立数值 oracle；head 为 558。原 raw/FS/SQLite、V03 前缀/二进制/prepare 摘要与冻结 A 保留，不修改 r4 prepare 的 ingest 身份。以下成本在冻结客户端查询时钟之外单列。

| N=128 准备成本 | 最终 r5 实测 |
|---|---:|
| HDF5 / GRIB 摄取 wall（含目录创建与 checked copy） | 321.474095605 / 299.292742295 s |
| 空 namespace 初始化 / 完整 owner 恢复 | 2.700437075 / 141.237800744 s |
| 初始化 / 恢复就绪 RSS | 21,164 / 28,424 KiB |
| 发布完毕 / 恢复并完成 oracle 后 RSS | 137,548 / 55,684 KiB |
| 新 prefix 全部 objects BLOB | 6,935 行 / 57,950,049 B |

初始化和恢复是不同工作量，不将二者中位数解释为典型启动。objects 含目录 COW 历史和孤儿，只是 BLOB 值账本；进程 RSS 包含多个缓存与临时/借用内存，不受单个 64 MiB 缓存上限约束。N=128 每路径一版，最终比较衡量整套组织与缓存路径，不能据此归因跨版本 payload 去重收益。

## 最终 N=128 比较、strict 与恢复核验

`results/v04-n128-ablation-r5-20261017.json` 完成，`performance_gate_passed=true`，原服务恢复。pinned V03 与最终 V04 各 180 条冻结 runner 原始观测，共 360 条；两组分别保留 7 次挂载、7 份实际配置与前后 head/journal 摘要。原始 raw/FS/SQLite 未复制或重建。runner/native 的冻结 SHA、固定 N=128/K=2/D=4/depth=2、3 campaigns/5 repeats 及客户端时钟保持。

| 温热 discover-values 中位数 | 固定 A | 本轮 pinned V03 | 最终 V04 |
|---|---:|---:|---:|
| HDF5 | 106.811351 ms | 24.975005 ms | 30.460112 ms |
| GRIB | 57.404444 ms | 42.102842 ms | 42.259243 ms |

V04 两格式通过固定 A 门禁；本轮 HDF5 慢于 V03，GRIB 接近，不宣称 V04 获得额外查询加速。两组、两格式、两个 phase 的温热 discovery 各 12 项观测中，旧 SDK 与 daemon-wide Store 的真实查询提交均为 0，独立 manifest/marker 调用均为 0。value scope 的已观测查询中位数也为 0，但不从 discovery=0 推导所有 payload 读取均为零查询。V03 原二进制缺少 V04 batch 字段，保持 null；其已有 Store 总量仍可观测。V04 batch 字段有 12 个有效样本，是 Store 查询的子集，不重复相加。完整阶段耗时、范围/块放大和计数保留于两组 `warm-analysis.json` 与原始 report。

| 两组各七次挂载的成本 | pinned V03 | 最终 V04 |
|---|---:|---:|
| 进程恢复中位数 | 117.667701025 s | 146.740935693 s |
| 进程恢复 min–max | 112.928345954–131.518541910 s | 131.798587951–157.882088429 s |
| 启动 RSS 中位数 | 27,272 KiB | 28,392 KiB |
| 退出前 RSS 中位数 | 40,520 KiB | 42,032 KiB |
| 进程 HWM 中位数 / 最大值 | 40,520 / 59,028 KiB | 42,800 / 61,176 KiB |

V04 恢复深验 payload 闭包，代价在表中保留；进程冷启动不等于数据库/OS 缓存冷。该比较顺序固定，未做随机化显著性检验，不把整套组织/缓存的差异归因为单一特征。历史 V03 的 21.565456/39.101750 ms 仍保留在其原报告，不替换为本轮控制值，固定 A 也未改变。

默认 strict 报告 `results/v04-default-strict-r5-20261017/publication.json` 使用最终 daemon，不配置任何 version-directory 属性；原 256 版完整哈希及 1,024 typed 单元通过。最终 `results/v04-post-restore-20261017.json` 通过：配置 SHA 仍为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，实际 SHOW 为 ASYNC/true，`.env`、license、system_info 摘要前后相等；ConfigNode 仍为原 PID 2645019，最终 DataNode 为 3470523。全部 V04 窗口共记录 20 次 DataNode SIGTERM、0 次 SIGKILL；保留前述启动失败与独立恢复收据，不称其为专门新增的数据库 crash 实验。原 ASYNC 下 owner 启用明确拒绝，journal 前后字节不变；本轮所有自有挂载和权限测试面均无遗留。

## 最终证据绑定与边界

[机器验收收据](../../experiments/cross_version/results/v04-acceptance-20261017.json)包含 121 个证据引用。服务器 `results/v04-evidence-inventory-20261017.json` 摘要为 `ac3dce4852964391235283d5efe5e18d1f5b206065225e0ca15d879ec077ceaf`；它核对最终 XML 的 694 个实际完成 testcase、八个二进制、源码和原始报告，不用旧 r2 的测试覆盖新构建。历史 r2/r3/r4 的失败、中断、成功及恢复证据单独保留。

最终绑定 `work/v04-final-source-test-binding.json` 的 SHA 为 `dc65b9349aaf7b8afcdb5444e24129f6205aacb5e959ff9d4eed0f24b3cb0871`。本地与服务器逐一核对冻结包中的 633 个非文档执行文件，其中 481 个是 src/include/tests/CMake 文件；该计数不表示每个文件均被编译。源码包共 829 文件，最终文档、验收/绑定产物及实验 Markdown/dotfiles 不参与执行源码绑定。服务器复核收据 `results/v04-final-binding-verification-20261017.json` 的 SHA 为 `7a8a56fd17b66f84f3cb500cbceaff170104e5db68fabb975c65c77e4e9bfa8f`，再次核对 633 文件、全部二进制、XML 和源码归档，无不一致。模块依赖方向检查及 `git diff --check` 通过；没有 Git 写操作。

V04 交付固定 1 MiB 编码字节去重和直接读取，不提供差分链/CDC。小范围读取的整块校验放大、冷恢复、COW/孤儿及暂存成本均按实际范围报告。共享对象 GC 留给 V05，显式迁移/最终版本维度验收留给 V06；不从唯一块 BLOB 比例推导数据库物理压缩率。
