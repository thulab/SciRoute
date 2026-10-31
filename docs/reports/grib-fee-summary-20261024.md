# GRIB 跨批次费用摘要：实现与 N8／N128 验证

2026-10-24。实现、全量回归、真实后端、独立 N8 资格组、交错 N8／N128 主实验及独立审计均完成。全部 720 行保留；主分析使用其中 480 行的 320 条正式记录。

**d 阶段优化成立，完整 T 均值本轮没有改善。** N128 的 d 均值下降 41.34%，N8 下降 4.45%；d 的两点经验割线下降 77.66%。正式 d 的 prepared 批次遍历从 353／4571 项降为零，同时保持全部收费和三次真实准入。完整 T 均值反而上升 10.09%／17.60%，不能用归一化结果代替这一事实。

## 接入范围与语义

公共 QueryReadContext 增加格式无关的不可变费用批次绑定／摘要激活接口；QueryModule 仅在 Adapter domain 成功发布后绑定原批次，且只有索引认证成功的缓存命中才激活。QueryDomainCache 的 publish 返回实际发布结果并拒绝替换已有 artifact。IoTDB 后端按候选选择→实际 principal 权限证明→Adapter 域费用编译摘要；末尾再次 proof 的事件仍保留，但两次重复证明由原账本 memo 提前跳过，因此正常暖态为三个有效阶段。

每次请求仍取得新 owner view、使用真实 principal、执行原三次 admission、execution lease 和最终发布检查；完整候选集合包含谓词未命中的源。Latest、head 变化、撤权、phantom、删除、跨 backend／namespace 和淘汰仍按原语义处理。HDF5 dataset 原四阶段路径保持，真实后端 17 项回归通过。

摘要借用既有 selection／proof／Adapter trace，不复制费用身份，不保留请求账本或 PreparedQuery。原 64 MiB 唯一热池及域／证明／句柄配额不变；wrapper 和临时 union 先预留，scratch 随编译退还，借用批次通过原 allocation／reservation pin 计费至最后引用释放。沿用最多 8 个事件、65536 个 union 身份及 128 个覆盖项限制。批次未就绪或 collector 活跃时延期；配额拒绝仅放弃该不可变 proof/domain 的可选构建，普通重放仍可执行。预算预检不足时回放原排序批次，保留成功收费前缀与首个错误；中途安装 collector 后退出摘要并继续原逐项观察。

本轮相对起点只改四个产品文件：QueryReadContext.h、QueryDomainCache.h、IoTDBStructuredBlobBackend.cpp、QueryModule.cpp；另有两份单测和 GRIB standalone probe。既有 selector 句柄优化保留。

## 版本与实验设计

旧程序 A SHA：`50f24526ed71ccb9da8e6b2c06e5de4fd0582e3c851a3e5bc3574916150a296a`；新程序 B SHA：`f09f3742246be3402aaf7dec764f4a7d7d357740a9fb916e30b3b2ca9bed5774`。两侧均使用同一个 format_catalog_by_selector 接口，随后执行原两次串行 typed 取值；本轮隔离的是费用摘要产品变更。基线原 528 文件源码／二进制／XML 未改；历史 standalone probe 的 c05 修订单独留证，不冒充旧程序重建。新候选另行冻结 528 文件并全新构建。

仅复用封印 FM1 GRIB N8/C8、N128/C128，P=C=N，V=1、D=4、K=2、S0、Latest，owned 输出 40960 B。未重新发布语料或建立 FS-index。N8 独立资格组 240 行先验收；主实验 480 行联合采集两档，每档每版本每臂 40 个正式样本。四 campaign 的 N 顺序为 8→128／128→8／128→8／8→128，版本顺序为 AB／BA／BA／AB。资格数据不进入主统计，所有原始数据均保留。

每挂载一个新 FS-index context 和一个句柄，每臂 5 次 warmup＋10 次正式；资格／主实验共 24 挂载、24 context、1128 条有序 query/lifecycle profile。T=d+q 结束于客户端拥有的 Float32／mask，profile／oracle 在 T 外；prepare、release、挂载启动分别记录。沿用两核共享配额、2 GiB query 叶和原 DataNode 内存；原 cgroup keeper、受保护 SYNC 窗口及恢复 supervisor 保留。

## 全部正式样本均值

单位 ms；没有按负载或耗时排除样本。

| N | d A→B | d 降低 | q A→B | T A→B | T 增加 | B 的 q/T |
| --- | --- | --- | --- | --- | --- | --- |
| 8 | 1.541→1.473 | 4.45% | 5.453→6.228 | 6.995→7.701 | 10.09% | 80.87% |
| 128 | 3.107→1.823 | 41.34% | 4.401→7.007 | 7.509→8.830 | 17.60% | 79.36% |

占比按 Σq/ΣT 计算。N128 d 节省 1.284678 ms，但 q 增加 2.606085 ms，完整 T 净增 1.321407 ms。两版本在相同 N 下 q 的计数相同：2 次 selector 编译、8 次消息谓词判断、2 次 native field cache hit、0 load；q 的 prepared visits 均为 N8=104、N128=111，summary stages 均为 0。因此“零遍历／三个摘要阶段”的结论只适用于本轮暖 d。q 的逻辑收费两版本也逐次一致，但本轮未消除其重复准备、物化和交付开销。

| N | FS-index d A→B | FS-index q A→B | FS-index T A→B |
| --- | --- | --- | --- |
| 8 | 0.465→0.505 | 1.744→1.828 | 2.210→2.333 |
| 128 | 0.718→1.117 | 1.429→1.958 | 2.148→3.076 |

## 机制与增长量

| 正式 d／请求 | N8 A→B | N128 A→B |
| --- | --- | --- |
| 选择批次实际访问 | 123→0 | 1257→0 |
| 首次证明批次实际访问 | 141→0 | 1905→0 |
| Adapter 批次实际访问 | 89→0 | 1409→0 |
| 总 prepared visits | 353→0 | 4571→0 |
| 摘要跳过的原批次项 | 0→353 | 0→4571 |
| 摘要阶段／覆盖项检查 | 0／0→3／1 | 0／0→3／1 |
| 账本 consumed bytes | 73030→73030 | 994830→994830 |
| 真实 admission／proof hits | 3／3→3／3 | 3／3→3／3 |
| proof memo hits | 2→2 | 2→2 |

正式暖请求均无 collector 退出或摘要 fallback；两侧 compile=0、输入路径=0、长域键字节=0。上述 visits 只统计 PreparedQueryMetadataTrace 的重放，不是所有 require／observe 或原始 map trace 的总访问量，不能把 collector 未跟踪路径当成零工作。

F 为批次费用项、U 为账本身份数，原逐项 map 更新存在 O(F log U) 工作；固定 D、路径深度时 F 随 C 增长。新暖路径以三个摘要 delta 和有界覆盖查找代替 F 项遍历；覆盖查找仍可含 log F，随后还有 O(log(CD)+K) 索引与 K 个结果处理，客户端列表 scope 校验仍有 O(KP) 上界。冷建域、head／principal 变化、额度拒绝或 collector 路径仍可执行原遍历；摘要编译本身一次性使用 O(F log F) union 工作及受配额约束的临时空间。

经验割线定义为 (mean128−mean8)/120，d 从 **13.049→2.916 μs/新增路径，下降 77.66%**，增长倍数从 2.016→1.238。服务端 d 割线从 14.097→1.979 μs/path。它们是 N=P=C 两个端点的观测，不证明完整算法降为 O(1)，也不外推 N512。机制计数消除了这两组暖 d 的大批次遍历，证据强于单一 wall-clock 割线。

N128 候选＋Adapter 费用重放均值 1.301474→0.013399 ms，约下降 99%；授权阶段 0.854375→0.051302 ms，仍包含真实准入，不能全称为纯计费 CPU。服务端 d 总时钟 2.616638→0.999110 ms（下降约 61.82%），索引 lookup 0.006530→0.015043 ms，本轮收益来自费用处理而非索引加速。N128 B 的 q 服务端时钟 2.496403 ms，完整 q 7.007205 ms，约 64.4% 位于服务端计时之外；残差包含 IPC、FUSE、owned 拷贝和调度，不能解释为纯 CPU。

## 四块原始对照与限制

以下为块内中位数，单位 ms；不把各阶段中位数相加当作 T。

| N | campaign | IoT d A→B | IoT T A→B | FS T A→B |
| --- | --- | --- | --- | --- |
| 8 | 0 | 1.040→0.840 | 5.176→4.848 | 1.540→1.604 |
| 8 | 1 | 1.098→1.222 | 5.393→6.860 | 1.650→2.214 |
| 8 | 2 | 0.901→0.795 | 4.212→4.759 | 1.485→1.685 |
| 8 | 3 | 3.011→2.943 | 12.405→12.341 | 3.548→3.296 |
| 128 | 0 | 3.095→2.846 | 7.297→13.393 | 2.257→4.166 |
| 128 | 1 | 3.157→0.968 | 7.736→5.350 | 1.950→2.099 |
| 128 | 2 | 3.305→0.981 | 7.943→5.282 | 2.246→2.066 |
| 128 | 3 | 2.588→2.722 | 6.387→11.409 | 1.929→4.012 |

原始 d 两档均 3/4 块改善；原始完整 T 两档均两块改善、两块变差。N128 B 在 AB 的 campaign 0／3 同时出现 IoT 与 FS-index 变慢；IoTDB-FS 在 BA 的 1／2 较快，FS-index 并非两块都下降；N8 两侧在 campaign 3 明显升高。早期 campaign 0 已出现慢块，且最后 campaign 的 N128 A 反而最快，不能只归因为必然的“末轮变慢”。存在顺序／时间窗口混杂；现有数据不能将全部回归归因于产品，也不能将其全归因于外部负载。

按每 campaign 先求 R=T_IoT/T_FS、Rd=d_IoT/d_FS，再取 B/A 的四块中位数：N8 R=0.9715、Rd=0.7545；N128 R=0.7910、Rd=0.3667。归一化 Rd 两档均四块改善，但这只是同期对照敏感性证据，不能当作因果负载校正。原始 T 的配对中位 B/A 仍为 1.0623／1.2389；本轮不据归一化 R 宣称完整查询提速。

所有正式端点 cgroup throttle、memory high／max／OOM 增量为零。其他 30 核 CPU 占比 319/320 行分辨率不足；16 块没有完全落在块内的 1 Hz census 区间，只有跨边界观测。观察器共 918 样本，平均每样本 0.171 CPU 秒，61/320 次正式查询的外层调用包络（含 T 外 profile／oracle）与其 capture 窗口相交。无法用这些证据排除频率、虚拟化、调度、客户端或观察器影响；缺失不记作零，不据负载删行或校正时钟。

## 启动、生命周期及验证

挂载启动不计入查询 T，也不计入 handle prepare／release；单列原始启动时钟。N8 启动均值 A/B=7.591/5.945 s，N128=82.386/76.788 s，N128 全范围约 68.5–107.2 s。它们不能作为本次暖查询摘要的冷启动收益主张。包含 prepare＋Q1..15＋release 的实际累计成本，N8 B 仅 1/4 块较低，N128 为 2/4；Q1..15 包括五次 warmup，未验证稳定的独立冷启动盈亏点。

全新候选 1890 项全量、270 项定向、模块依赖边界检查通过；本机 42 项 ASan／UBSan 与 14 项真实 QueryModule 调用点测试通过，源码哈希与冻结版一致。真实后端 GRIB 13 项＋HDF5 17 项通过，覆盖实际 principal／补充组变化、谓词未命中源及祖先撤权、Latest／phantom／删除、精确预算错误和收费前缀、早／晚 collector、首次 collector 延迟构建、共享池拒绝和生命周期。框架本机及服务器 19 项离线检查通过，资格及主实验独立审计通过。

框架在采集前修正 CPU 缺失被当作零及 native 累计计数回退的问题，r3 保留缺失和原始行。实际服务器闭包为 174 文件；本地仅有 105 文件，唯一已有 root small_runner.py 差异未覆盖。实际调用的是历史封印 runner-source，其哈希以及服务器 root 脚本哈希均与前轮封印闭包一致；部署记录明确区分这一事实。

最终 DataNode PID=1397482，实际 253 个线程 CPU0–31、原 cgroup、effective ASYNC/true 均已核验；配置 SHA `f69f408de5c09edabae3a8293cf662327f794b908cbf98d5a02b3878fea154f4`，三个受保护身份哈希一致。主实验 16 个 daemon／挂载、16 个 context、查询 worker 及临时 cgroup 均退出／回收，资格组另 8 组同样通过。未执行 Git 写操作。

服务器原始目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024`。全部原始查询、科学对象、完整 census、配置与 private logs 留服务器。

- [数值汇总](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024/comparison-derived.json)
- [负载与缺失说明](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024/load-derived.json)
- [挂载启动时钟](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024/startup-derived.json)
- [本机归档核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024/local-archive-verification.json)
- [71 文件复现归档](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024/completion-evidence.tar.gz)

归档 SHA256：`67f24ed89a7282a5b93d6454ce3f90d3df7def7124efcd5ae33618b8aa40170b`。全部 71 文件及当前工作区 528 个冻结源码文件已在本机重新核验。
