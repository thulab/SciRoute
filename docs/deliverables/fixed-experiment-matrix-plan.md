# 4.3 固定实验组与首轮重采集方案

## Material Passport

- 来源：用户指定飞书文档 4.3 节，文档 `GndRdq8qaoNw5jxcBNrcYmVMnsb`，revision 626；原始响应与 SHA 留存在实验 work/source。
- 方法：academic-research-suite / experiment-agent，plan → run → validate。
- 日期：2026-10-21；状态：**32组、3个campaign主统计已完整验收：3,456条（2,880测量＋576预热）。保留r2完整前两轮2,304条，按用户确认整轮重采第三轮1,152条；原第三轮72条留档但全部排除。14份语料／32个case复用，原服务与资源已恢复，18扩展未执行**。预注册revision 638，变量／目的首列revision 643；本轮使用 `collection-r2` 与 `collection-r3-query-only/accepted`；[完整验收报告](../reports/fixed-matrix-fm1-20261021.md)。
- 本轮全部使用新测量。旧结果只作历史档案，不进入基线、统计、最佳组选择或性能主张；原始文件不删除、不改写。

## 1. 研究问题与固定格式路线

测试同一科学查询在文件数 N、候选路径数 C、原编码文件大小 S、已存版本数 V 改变时的完整查询成本、发现 d、取值交付 q 与资源消耗。检验优势是否出现，不预设 IoTDB-FS 更快，也不按结果挑选后续规模。

三臂固定为 FS-scan、FS-index、当前 IoTDB-FS。GRIB 使用 IoTDB-FS 跨文件和文件内索引；HDF5 仅使用 IoTDB-FS 跨文件索引，取值固定 `native-hdf5`，由 libhdf5 原生文件内索引访问数据库精确版本字节。禁止用本地 raw 文件／自身 FUSE 原件替代正式 VFD 路径，禁止回落 `iotdb-indexed`。Arrow 暂无本节冻结的执行路线和对应科学 case，首轮不填入 Arrow 数据。

产品使用已保留二进制 `36881bde02466fb65730188161c2f764e45d2b000c321901a191af722a468fc9`，不构建新产品版本。可以构建独立的原生基线实验 helper，并记录其源码、构建命令、编译器、动态库及二进制 SHA。

## 2. 固定变量与语料规则

| 变量 | 冻结含义 |
| --- | --- |
| N | 独立完整发布语料的逻辑路径总数；不得把 N512 的 C128 子集记成 N128 |
| V_stored | 计时前每路径真实存储的科学版本数；V1/V8/V32 使用独立冻结 namespace，不能在最终 V32 namespace 上伪称 stored V1 |
| C_paths／C_versions | 谓词前候选路径数／经版本选择后的精确文件版本数，分别报告 |
| D | 每文件版本 4 个 GRIB message 或 HDF5 dataset |
| K | 分别记录匹配文件、文件版本和 unit 数；每个选中科学代次有 2 个匹配 unit |
| 读取输出 | 每命中 unit 的 64×64 Float32 值＋uint8 掩码；每代次共 40,960 B |
| S | 完整原编码文件版本的实际字节分布、总逻辑字节；不把名义档位或解码大小写成实际 MiB |
| 谓词 | 冻结六属性非空等值 AND；文件名／dataset 槽位由固定种子安排，查询不接收 oracle 命中路径 |
| 树和 scope | 同系列保持目录层级、编码参数、谓词及命中工作量；C 子集由计时前的确定性规则选出，记录确切路径及 selector 项数 P |

优先复用现有、通过本轮全量字节及独立原生校验的 raw 文件；历史计时和历史缺失的 oracle 不能复用为新结果。缺少的独立规模按固定生成参数生成，正式组在新 namespace 独立发布并记录成本。raw、FS 版本文件、publication、head、manifest、所有 unit 属性及值／掩码 oracle 完整绑定。不得靠复用某个大语料查询范围制造较小 N 语料。

同一正式组只有一份冻结语料／发布绑定。后续优化重复使用原 corpus/case，不能因时间不利重新生成种子、重排命中或重新发布同名组。内容摘要、实际大小与发布完成后才能完成首个 corpus/case 的最终封印；此后不可原地修改。

## 3. 按 4.3 预注册的矩阵

机器清单见 [catalog.json](../../experiments/fixed_matrix/catalog.json)。组名固定为 `FM1-格式-N…-V…-C…-S档-版本变化-选择`，正式内容身份仍使用 SHA。跨 H1–H4 的完全相同变量组合只执行一次，以 axes 引用共享结果。

飞书原表首列为“对比实验（验证变量与目的）”，逐行说明 H1 的独立语料规模 N（C=N）、H2 的候选域 C（固定 N512，P=C）、H3 的非命中内容大小 S，以及 H4 的版本历史与选择策略。N8/V1/S0 明示为 H1/H3/H4 共用基准，N512/C512 明示兼属 H1/H2；H4 区分固定选1版、选2版和 All＋谓词的输出工作量，并仅在相同 V／选择策略下对比 I/U。首列只解释既有固定组，不新增 case 或改变变量。

| 实验轴 | 首轮固定组 | 共同控制 | 解释范围 |
| --- | --- | --- | --- |
| H1：N | N=8、128、512；C=N | V1、S0、D4、Latest、K=2 | 三份独立完整语料；比较总规模变化 |
| H2：C | N512；C=8、32、128、512 | 同一 N512/V1/S0，Latest、K=2；记录实际路径和 P | 仅比较候选域变化；命中被设计为在各 scope 内，明确其机制实验性质 |
| H3：S | N8/C8；S0、S1、S2 | V1、D4、Latest；匹配 unit 始终 64×64 | S0 非匹配 unit 为64×64；S1为384×384；S2为768×768。只扩大无关 payload，保持命中输出不变；实际 S 在生成后冻结 |
| H4：V | N8/C8；stored V1、V8 | S0、D4；V8 分 identical、single-unit-change 两组 | Latest、首代历史窗、末两代窗、All＋六属性谓词；V1只保留同义基准 Latest，避免重复记样本 |
| 已预注册扩展 | H1 N1024；H4 V32 | 同一协议和生成参数 | 组名和规范先保留；推荐首轮不执行，不根据 R 决定是否启用 |

`single-unit-change` 固定为每次只改一个 unit 的少量值、循环槽位，不改属性或候选命中数；两种历史模式均保留，避免只报告完全相同内容的有利情形。All 是有属性谓词的跨版本查询，非无条件遍历所有 unit。版本代次与真实数据库 timestamp 分开记录；首代／末两代窗口由 version-major 发布的真实边界生成，双方必须选出同一精确版本集合。

首轮主矩阵为每格式16个唯一 case、两格式共 **32 case**；每 case 三臂、三个预先固定 campaign、每臂2次 warmup＋10次测量，共 **3,456 条正式查询**。若选择本轮执行已预注册扩展，则为50 case、5,400条。缺失、失败和超限不能静默删除；不把不完整矩阵写成完成。

## 4. 统一完整查询与阶段时钟

所有组使用同一客户端时钟和交付终点，不再混用 C++ 内部计时与客户端 NumPy 计时。FS 原生 helper 从完整通用元数据索引发现，并直接填充客户端预分配／拥有的 Float32 和 mask 数组；避免把 JSON 数值编码成本人为加到 FS。GRIB 保留 ecCodes 原生读取／解码，HDF5 保留原生 H5Dread。

| 时钟点 | 定义 |
| --- | --- |
| t0 | 新查询开始，包含请求构造及正常查询所需的选择、身份／权限处理 |
| t1 | 获得本次实际发现的精确源／unit 列表；未读值 |
| t2 | 所有选中值和 mask 均成为客户端 owned 数组 |
| d／q／T | d=t1−t0，q=t2−t1，T=t2−t0；每条严格验证 T=d＋q |

独立 oracle 哈希、profile 等待、结果审计、资源采样、挂载恢复及准备不计入 T，分别保存。新框架不复用会把发现和读取交错、再以总时间减累计值计时的旧 FS helper。profile 子阶段有包含关系，不能相加或直接相减作为独占耗时。

本框架的IoTDB-FS路径为一次发现RPC，再对每个匹配unit发起一次精确版本取值RPC；q包含这些IPC、原有准入／预算处理及owned数组交付。FS helper对应为一次select后逐unit原生read。这里测的是该发现＋交付组合的完整客户端成本，不把q称为纯H5Dread／ecCodes耗时，也不把T称为单次产品API调用。

主指标仍为同 case、同 campaign 的 `R=median(T_IoTDB-FS)/median(T_FS-index)`，并列 Rd、Rq、各臂绝对 T/d/q、全部样本。FS-scan 同样记录，另列相对其比值。

## 5. 缓存与索引准备

- 每 campaign 创建新 daemon 和独立 FS-scan／FS-index context，跨查询保留各自正常有界状态；同 campaign 不因格式／arm 切换清空缓存，不缓存最终查询答案。
- FS-index 保留通用 SQLite 索引和原生库正常有界句柄缓存；FS-scan 与 FS-index 使用独立 context，防止扫描污染索引臂的句柄缓存。句柄数、metadata／raw-chunk 配置和实际版本逐一记录并跨组固定。
- 不人为禁用 HDF5 原生缓存以抬高 FS 时间。IoTDB-FS 现版是否保留原生句柄及其缓存大小按实际代码和 profile 记录；不能宣称双方内部缓存算法完全相同。
- 全量输入哈希、原生 oracle 和索引建设会暖 OS 缓存；不清空全局 OS／DB 缓存，因此只能称 retained-process warm，不称冷盘。
- 每份冻结语料本轮新建一次通用全历史 FS-index，记录真实完整建索引时间、大小、资源和查询计划；随后只读复用。多个 case 引用同一个准备收据，不重复建设后取最快者。
- raw 生成／校验、FS 实体版本复制、FS-index 构建、IoTDB-FS 发布／建索引、owner 恢复分别记准备成本。不能把历史发布时间填成本轮实测，也不能把复用成本写成历史总成本为零。

## 6. 环境和实际资源是强制验收字段

| 类别 | 强制记录 |
| --- | --- |
| 主机／平台 | 时间及 timezone、OS/kernel、CPU型号／拓扑／SMT、NUMA、总RAM、磁盘／文件系统／挂载参数、free space、虚拟化可见信息 |
| 软件／实现 | 产品与 helper 完整 SHA、源码快照、干净构建绑定、编译器／参数；JVM、数据库 release/jar、SDK、FUSE、ecCodes、HDF5、SQLite、Arrow/PyArrow、Python、NumPy/h5py 版本及动态库摘要 |
| 数据库 | 原实例／数据库／配置摘要、实际 JVM heap/direct 参数、SYNC/partial 实际状态、账号身份类别（不写凭据）、namespace/incarnation/完整 head、会话池和线程配置 |
| 固定资源 | 原批准的 CPU2/3 isolated partition，总 quota两核；客户端／daemon／FS helper／原 DataNode 共用；query叶组2 GiB；DB原15232MiB heap、2176MiB direct另计，不称全栈2 GiB |
| 周期采样 | 每秒 host load、per-CPU／进程组CPU时间、cgroup用量／节流、PSI、RSS及内核memory峰值／OOM、进程I/O、主机磁盘活动、后台占用；原始样本和采样器开销边界保留 |
| 查询级资源 | 每查询 envelope 的组用量前后差与时间戳；其范围可能包含计时外审计／profile工作，明确标注，不能冒充纯 q 独占CPU时间；全 campaign峰值单列 |
| 恢复 | 原配置字节及许可证／节点身份保护摘要、服务实际设置、所有线程原CPU亲和性／cgroup、临时组删除及所属进程退出 |

2026-10-21 本轮只读预检：主机 timecho、kernel 6.8.0-139-generic，可见32逻辑CPU、约377.69GiB RAM，数据盘剩余约1,516.67GiB；当时 host load约34.84。这些是预检快照，正式运行必须重新记录，不能把该快照代替期间实际占用。隔离CPU不等于隔离内存带宽、存储或IRQ；不人为填充CPU负载来“固定利用率”。ConfigNode及其它未纳入两核限额的支持进程单独记录，不隐瞒为零资源。

## 7. 重复、顺序与分析

三个 campaign 分开重建进程／挂载，使用预先生成并哈希封存的 case 和 arm 顺序；每 cell 的12次采用三臂全部6种排列各两次，每臂前2次预热，其余10次计入。固定收满，不根据速度追加、截停或排除“异常慢”样本。

每 campaign 独立报告中位数、均值和样本范围，再报告三个 campaign 的 R/Rd/Rq 及范围；三个时间 block 不等于三台独立机器或冷缓存重复。首轮只做描述性比较，不以三 block 或相关的30次暖查询宣称显著性、统计等价或稳健 p95 保证。未来增加轮次前先预注册数量。

最佳组按各格式完整主矩阵的 campaign 中位 R 最小值标粗，候选范围和规则计时前冻结。不得按最低单次毫秒、某个子阶段或临时新增语料挑选；未完成矩阵不宣布“最佳”。x/y列本轮留空，后续新功能只复用同组、同数据、同环境／时钟协议另建 run。

## 8. 产物、门禁与执行边界

新公共入口放在 `experiments/fixed_matrix/`，供 multi_file/cross_version 共用；历史脚本保留归档，不再作为正式采集入口。正式 CLI 只接受已批准 group/suite、实现及 run 位置，不接受随意 N/C/S/ROI 覆盖。新增组须先修改提案并明确确认，不允许运行时隐式生成。

产物包含：冻结矩阵／顺序、source snapshot、raw/corpus/case/run 身份、实际环境、原始逐查询记录、原生 oracle 审计、模式证明、索引准备／发布成本、资源时间序列、完成／失败／停止收据、恢复证明、分析脚本和结果表。摘要有引用路径及 SHA；凭据、许可证原件、原配置备份留在服务器私有目录，不导出。

正式采集前依次验收：catalog完整／单变量控制 → 同一固定N8组功能烟测 → 当前二进制真实模式 → 三臂值与mask一致 → 完整时钟与资源门禁 → 全矩阵。准备／挂载／查询均设硬超时；异常不依据有利结果重试，失败目录保留并通知用户。未通过环境或恢复验收的样本不进入主表。

运行继续复用原数据库、共用账号和许可证，沿用已授权的停启／SYNC维护／恢复流程；不启动隔离实例，不申请证书。需要的写入限新固定实验 namespace 与工作目录，不修改已有科学语料及其发布。

结果通过后直接填回[飞书4.3原表](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg#doxcn5FH2k0iTUtMKq2Ch9b9gmg)，在原段落内结构化解释条件，不新增“本次变更”章节。用户于2026-10-22更新展示规则：初始FS列和“10.21基线”历史记录保留，后续优化列仅展示相对同case指定历史参照的ΔR及主线决定，不重复填本轮FS-index原始值或A组结果；备注只列服务器原始记录目录，未执行组留空。完整T/d/q、同期FS-index、A/B、准备、实际资源及恢复证据继续完整保存在服务器，不因精简展示删除或省采。

本次ΔR=(候选配置R／“10.21基线”同case历史R−1)×100%，使用服务器未舍入数据；负数表示改善。历史ΔR用于跟踪进展，是否纳入仍依据同轮配对R与完整T验收，不把跨轮差异当作优化因果证明。后续参照必须绑定同一case的明确历史主线记录，不择取其他case或历史最小值；参照身份、原始比值与计算依据留服务器。

## 9. 本轮登记与执行状态

| 项目 | 可核验状态 |
| --- | --- |
| 表格登记 | 执行前revision 638，32首轮、18扩展；状态说明revision 642。revision 643 在原表新增对比实验／变量／目的首列，登记时表 `doxcnx0MOkPpPLXXglf73TT5XTd`；50组逐行核验，原有全部单元格内容不变 |
| 固定清单 | `experiments/fixed_matrix/catalog.json`，SHA256 `41eff9c7aa824ed62df6c025531c74e731a94e75270f3e936e7b71934bcab47d`；未因启动失败修改组、种子或谓词 |
| 启动失败 | `work/fixed-matrix-20261021/collection-r1` 在worker导入PyArrow时终止；继承产品的动态库路径导致libparquet的Arrow JSON符号不匹配。未生成任何正式语料或查询行，不能填零毫秒或发布性能结论 |
| 修复边界 | 实验Python客户端与产品daemon使用分别管理的动态库环境；新增服务维护前的独立子进程依赖导入门禁。完整导入校验通过，未操作数据库或执行科学查询；产品二进制未重建 |
| 前次服务与资源恢复 | `collection-r1`恢复时原配置SHA256 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`；实际设置`ASYNC/true`；DataNode PID 1104794，所有线程亲和性0–31，临时cgroup已删除 |
| 最新用户决定 | 用户先决定仅保留实验表，后明确授权“重启32个首轮组实验”；`collection-r2` 已执行。未增加实验组、产品版本或许可证 |
| 已封印语料 | 14份独立语料、1,584个科学文件版本、6,336个原生unit oracle，绑定32个case。后续必须复用本轮 corpus/case，不能重新发布同名组；完整ID和实际S见[状态及身份清单](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r2-evidence/status-summary.json) |
| r2原采集 | 2,376条：1,980测量＋396预热。campaign 0/1各32组完整；campaign 2仅GRIB/HDF5独立N128完整，其他30组未执行完。无失败行替换、慢样本排除或局部最佳组选择；续采确认前未生成主分析或R/Rd/Rq结论 |
| 停止原因 | worker包含准备与查询的12,600秒总时限触发，退出收据elapsed=12,638.247秒（含退出清理）。首条正式查询envelope前已有约137.84分钟维护、准备及门禁；首轮GRIB/HDF5 N512启动分别589.483/535.567秒，均在T外。**本轮控制器对总运行预算估计不足，不能将未收齐的三轮方案改称完成** |
| r2实际资源 | CPU2/3、父组总quota两核、query叶2GiB持续验收；原DB堆15,232MiB/direct 2,176MiB另计。12,707个周期样本，查询重叠最大间隔1.239秒；无OOM。全采样期kernel memory.peak最大读值：query 810,172,416B（772.64MiB）、DB 20,023,566,336B（18.65GiB）；父组包含叶组，禁止重复求和。平均父组实占0.673核含准备／审计，不能当作纯查询CPU。主机load1为19.76–43.64；ConfigNode在配额外单列。详见[资源摘要](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r2-evidence/resource-brief.json) |
| 自动恢复故障与修复 | 主SSH断开后，向stdout输出事件抛出BrokenPipeError，在停库前的进度输出处中断回滚；临时cgroup已正常关闭，原DataNode仍以临时SYNC设置存活。已在本轮停止后修复：fsync事件账本仍为权威，断开的进度流不再中断stop/start/restore。本地及服务器11项离线控制器测试通过；未重建产品二进制，原始失败报告不改写 |
| r2后续手动恢复 | 另起脱离SSH、本地文件日志的恢复进程，复用既有回滚方法和原实例。已核验ASYNC/true、原配置SHA256 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`、许可证／节点文件未变、DataNode PID 1389791、全部线程CPU0–31、临时cgroup清除。原登录session scope随旧进程退出结束，新服务在新session scope，CPU／内存控制均无实验限额；此路径差异已[单独留档](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r2-evidence/placement-verification.json)，不声称保留原session编号 |
| 停止后只读审计 | 1,584个版本的raw与FS副本、14份原生索引与owner journal、执行时源码闭包、所有已收集查询时钟／模式／oracle及资源覆盖通过检查。完成挂载的head不变收据保留。原服务恢复ASYNC后，原生head探针因SYNC/false能力门禁拒绝执行；**不声称恢复后重新读取了head**，下次查询准入前须重新核对冻结head。详见[审计收据](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r2-evidence/final-identity-check.json) |

用户已确认并完成的最小续采方案（**第三轮及完整三轮统计已验收**）：新增只读复用已封印语料的query-only入口，不重新生成／发布数据、不重建FS-index、不换产品或改变32组、时钟、缓存、顺序、CPU／内存上限；保留campaign 0/1的2,304条记录，将中断的campaign 2按原顺序整轮重采1,152条。原第三轮72条继续留档，不与新第三轮拼接或择快；主统计固定使用2,304＋1,152＝3,456条。各执行的源码、资源身份、DB维护／恢复和时间间隔分别记录，经逐段验收后合并三轮；原失败report不改写。后续正式重复必须走已绑定corpus/case的新run，禁止调用初始化入口重发同名语料。用户明确批准本次新入口和分段合并；仅重采一次完整第三轮，失败保留，不自动重试。

前次暂停的服务器无凭据摘要：`experiments/cross_version/work/fixed-matrix-20261021/status-table-only.json`。重启授权记录为 `authorization-r2.json`，本轮原始证据位于服务器 `collection-r2/`；已验证的本地白名单包位于 `collection-r2-evidence/`，包SHA256 `f9b2b2ee906d92dd1f91d600ce516c2f3cb9db15fb363edc0af1a37ea803b7c6`。完整资源摘要、原始查询／周期资源序列的路径和SHA见[evidence-manifest.json](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r2-evidence/evidence-manifest.json)。飞书登记／回读收据位于 `source/`；原私有失败日志、配置备份及不可变源码快照保留服务器，不外传凭据或许可证。

续采控制器位于 `experiments/fixed_matrix/replay.py`，调用从r2等价抽取的同一查询循环。服务器35项离线测试全部通过，包含与r2执行快照的AST对比；本地35项中2项因缺少服务器快照／原生依赖跳过。脱离SSH的监督进程PID1453055、授权及固定源码摘要位于服务器 `collection-r3-launch/`；第三轮目录 `collection-r3-query-only/`；worker正常退出，耗时1,801.697秒，新增1,152条。启动前已逐字节核对原始证据、14份语料、索引及32个case，不重发数据；正式计时前在SYNC窗口重新核对所有冻结head。飞书原4.3已更新至revision648的授权状态，目的列、18扩展和预留x/y保持不变；完整三轮统计与独立恢复已通过，飞书原节结果回填及回读收据见 `source/feishu-complete/`。

最终接受目录为服务器 `collection-r3-query-only/accepted/`，使用原始行字节组成3,456条；实际两次采集共3,528条，其中原第三轮72条独立保留，不纳入主统计。原r2失败报告和各原始流未改写。每个格式16case的中位R均大于1：GRIB范围3.502–7.498，最小为G-H3-S1；HDF5范围50.738–438.845，最小为H-H1-N512。本固定负载未显示优于FS-index，不回填历史最优值。

续采段单独记录2,049个周期样本，查询重叠最大间隔1.106秒，无OOM；query kernel memory.peak最大读值271,122,432B（258.56MiB），DB 13,545,099,264B（12.61GiB），父组全观察期平均实占0.610核，host load1为18.62–30.04。与r2分段引用，不串接停机间隔。主统计的288个case/campaign/arm资源cell均完整，每cell10个测量envelope。

新段自动恢复和独立只读核验通过：ASYNC/true、原配置逐字节一致，许可证／节点／.env保护摘要未变，DataNode PID1483561，全部线程CPU0–31，原cgroup归位，临时cgroup清除。恢复后未再次运行要求SYNC的head探针。白名单包共22文件，包SHA256 `ddfeb8f8559a4f7e846a484aa60801f0ee82635c286c7a03657c9376509bc608` 及所有成员SHA逐项核验，解包至本地 `collection-r3-evidence/`；完整记录与大文件引用见[证据清单](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r3-evidence/evidence-manifest.json)。新段及后处理均已结束，不存在自动科学重试。

飞书原4.3节完整结果已回填至revision 651，当前表 `doxcnDxRiHOmi53ngtBkEkfiNhd`；51行×11列逐格回读相符，32首轮结果完整，18扩展、目的列、x/y及最后四项用户条件保持不变。三个写入回执均success且无warnings，核验收据为 `source/feishu-complete/verification.json`。

后续用户已批准将`native-hdf5`取值改为受管FS完整文件，并要求补齐HDF5首轮矩阵；执行边界见[新通路方案6.1](hdf5-native-filesystem-plan.md#61-用户追加授权补齐hdf5固定首轮组)。本轮复用7份原HDF5 corpus／16个case，按同一三campaign协议新建1,728条run，不拼入先行72条路径探针、不改GRIB／18扩展。792个精确版本的位置绑定已准备完成；这些namespace必须用新版和新收据中的当前journal打开，原发布journal只作档案，不能继续交给旧`replay`入口。原FM1所有封印字节和旧结果保持不变。新采集、资源与恢复证据位于`work/hdf5-native-matrix-20261021/`；已完整验收并在飞书原表原位更新对应HDF5行。

本轮已完整验收1,728条、三轮16case，独立复算和792版本／736完整FS文件SHA核验通过；3,544周期样本、3,456逐查询边界帧及原服务／资源恢复均通过。16组中位R为7.031–10.164，没有优于FS-index；结果不替换成先行探针或历史最快值。飞书原表已更新16个HDF5行的80个结果／资源／备注单元，并补16个备注中的同轮T配对；GRIB／扩展、目的／参数、x/y及用户四项条件保留。最终4.3原有条件说明更新至revision664，无新增章节；完整回读位于`work/hdf5-native-matrix-20261021/feishu/complete-section.json`，核验收据`methods-verification.json`。最终表ID为`doxcnzGhegECVvR6FPn0s2yuCMe`。
