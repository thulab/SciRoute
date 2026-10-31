# GRIB q 阶段：N128 慢块、正常块与增长系数诊断

2026-10-24。对上一轮已冻结的主实验进行离线再分析；没有新性能样本、产品修改或服务操作。主实验 480 行全部保留，其中 320 条正式记录进入统计，独立资格组不进入本报告统计。

**当前证据显示 q 的增加主要按运行块出现，尚不能认定是普遍回退或仅在较大 N 时回退。** N128 当前 B 的慢块比正常块多 5.304 ms；其中 57.50% 在 control 之外，31.77% 在服务端，10.73% 在 control 的其余包络。已记录的工作量未增加，已计时的候选费用重放仅增加约 0.0013 ms。已定位延迟所在边界，尚未确定频率、虚拟化、调度、观察器或其他共享资源中的具体因果。

## 数据、版本与比较方法

复用 `iotdbfs-fixed-grib-fee-summary/1` 联合主实验。N=P=C 为 8／128，V=1、D=4、K=2、S0、Latest，owned 输出 40960 B。每个 N／版本／系统 40 条正式记录，四个 campaign 各 10 条；每挂载一个独立客户端 context，5 次 warmup＋10 次正式。N 顺序为 8→128／128→8／128→8／8→128，版本顺序为 AB／BA／BA／AB。

A 为上一程序 `50f24526ed71ccb9da8e6b2c06e5de4fd0582e3c851a3e5bc3574916150a296a`；B 为当前 GRIB 费用摘要程序 `f09f3742246be3402aaf7dec764f4a7d7d357740a9fb916e30b3b2ca9bed5774`。两版本均使用相同 d selector 接口、原两次串行 typed q 请求。

N128 B 的 campaign 0／3 已在前一份报告中识别为慢块，1／2 为正常块；本报告沿用该描述性分层以定位差异。它同时对应 B 后运行／先运行，存在版本与时序混杂；不能当作随机负载分组。四个 campaign 始终保留在主统计中，分层结果不替代全体均值，不作负载校正或基线重置。独立单位只有四个块，不把每块十次重复当作四十次独立环境重复。

## N128 慢块与正常块

以下均为正式样本 ms；后三列为互不重叠的 q 均值分解。

| campaign | 版本顺序 | IoT q 均值 | IoT q 中位数 | FS q 均值 | server | control−server | q−control |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | AB，B 后运行 | 10.914 | 10.496 | 2.579 | 3.742 | 1.550 | 5.621 |
| 1 | BA，B 先运行 | 4.391 | 4.390 | 1.447 | 1.643 | 0.684 | 2.065 |
| 2 | BA，B 先运行 | 4.319 | 4.263 | 1.362 | 1.665 | 0.638 | 2.016 |
| 3 | AB，B 后运行 | 8.405 | 8.648 | 2.445 | 2.935 | 0.911 | 4.559 |

合并 C0／C3：IoT q **9.659 ms**，C1／C2 为 **4.355 ms**，相差 **2.218 倍**；FS-index 同期分别 **2.512／1.405 ms**，相差 **1.788 倍**。FS-index 的 native read 累计差分均值也从 1.221 增至 2.034 ms，而每次仍为两次 read、40960 owned bytes、两次 file-cache hit、零 file open／扫描源访问。此处“同期”指相同挂载块中的另一系统测量，不表示两系统每条 query 同时执行。

逐请求恒等式为 `q = Σserver_total + (Σcontrol−Σserver_total) + (q−Σcontrol)`；提取器验证全部实际请求的 `q≥Σcontrol≥Σserver_total`。均值可按此相加，分量的边际中位数不能相加为 q 中位数。

| 互斥区间 | 正常块 ms | 慢块 ms | 增加 ms | 占 q 增量 |
| --- | --- | --- | --- | --- |
| 服务端两次 server_total 之和 | 1.654 | 3.339 | 1.685 | 31.77% |
| 两次 control 之和 − server 之和 | 0.661 | 1.230 | 0.569 | 10.73% |
| q − 两次 control 之和 | 2.040 | 5.090 | 3.050 | 57.50% |

`control` 覆盖客户端 JSON、Unix socket 连接／发送／接收与响应解析。`server_total` 从读取请求帧和前置统计之后开始，在写响应帧、后置统计和 profile sink 之前结束。control−server 因而包含这些未覆盖的服务端工作、socket/framing 及调度等待。

`q−control` 覆盖请求构造、FUSE getattr/open/read、Arrow 读取、客户端 owned arrays/mask 拷贝、provenance 检查、循环及调度；不能称为纯 IPC、纯拷贝或纯客户端 CPU。profile 收集和独立 oracle 在 t2 之后，不进入 q；但服务端写 profile 的工作可能与后续 FUSE 或下一请求重叠。

服务端 inclusive 子阶段如下；**它们互相嵌套，不能纵向相加**：

| 子阶段 | 正常块 ms | 慢块 ms | 增加 ms |
| --- | --- | --- | --- |
| 候选费用重放 | 0.045 | 0.046 | 0.001 |
| 完整 materialize | 1.139 | 2.380 | 1.241 |
| typed prepare | 0.417 | 0.623 | 0.205 |
| typed encode（含 cursor 和 IPC） | 0.510 | 1.402 | 0.893 |
| cursor.next（含 future 等待） | 0.228 | 0.486 | 0.258 |
| Arrow IPC encode | 0.267 | 0.843 | 0.576 |
| route register | 0.044 | 0.164 | 0.121 |

额外服务端耗时主要出现在 materialize 下的 cursor／Arrow 编码等固定工作。cursor 计时包含线程池 future 等待，native field 命中仍需 render；不能将其解释为重新解码。候选费用重放只增加 **1.293 μs**，约为总 q 增量的 **0.024%**，不支持把慢块归因于这段费用遍历。

两次 typed 请求也没有等比例变慢：

| campaign | q1 server ms | q2 server ms | 两次 server 间隙 ms | q−首 server 到末 server 跨度 ms |
| --- | --- | --- | --- | --- |
| 0 | 2.282 | 1.461 | 4.478 | 2.693 |
| 1 | 0.842 | 0.801 | 1.349 | 1.399 |
| 2 | 0.850 | 0.815 | 1.286 | 1.368 |
| 3 | 2.063 | 0.872 | 4.115 | 1.354 |

C3 的 q2 server 与正常块较接近，主要抬高的是 q1 及 q1 结束到 q2 开始之间的包络；其首尾合并剩余包络约 1.354 ms，没有同步增加。C0 则首尾包络也增加。两次 server 间隙包含第一次 IPC、下一请求构造与控制往返的一部分，现有数据无法恢复逐次 IPC／owned-copy 的独立时钟。

## 工作量、N 相关项与代码解释

全部正式 q 在各 N、A/B、campaign 上均为：两次单源 selector 编译、两次 source open、八次 message predicate evaluation、8192 output rows、两个 batches、四次 cursor.next、36688 IPC bytes、40960 owned bytes；native field cache 两次命中，零 load／miss／wait。相同 N 下所有费用项和 consumed 也一致。

N8→N128 的 prepared fee visits 只有 **104→111**，candidate visits **56→63**，summary stages 均为零。metadata consumed 为 **35284→139559 B**；其中包含冷建时捕获的目录整页逻辑费用，也包含 head、权限证明、layout 等其他身份费用。暖 candidate／proof 命中重放身份与数值，不按所记字节数实际读取或复制整页；未逐 identity 分解全部字节增量的来源。不能从费用字节近四倍推导 q CPU 或物理 I/O 近四倍。

q 的 `grib.data.value` 不满足 metadata domain index 认证条件，没有 adapter fee batch。新桥接确有短 arm／defer 检查和有限 proof-map 查找，但不会构建 q 摘要或保留逐请求账本；没有新添 warm O(N) 全文件循环。两个 typed 请求各自仅带一个 exact path/version，GRIB message predicate 工作为 O(KD)，本组 K、D 和输出大小固定。N 仍可通过目录／权限依赖的费用项数、页大小或缓存资源间接影响 q，不能宣称所有 N 成本都为零。

记录的 runtime_delta 与 iotdb_delta 各项均为实测零、没有缺失；其范围是 typed 注册前后共享 daemon 统计快照。**它们不覆盖 control 返回后的全部 FUSE 读取／再准入，也不证明整个 q 没有物理 I/O、CPU 或资源争用。**

## 当前 q 的 N 增长系数

N 与 P、C 同时增加 16 倍。给出增长倍数 `r=q128/q8`、经验割线 `β=(q128−q8)/120`，以及两点经验指数 `α=ln(r)/ln(16)`；它们均是此次观测，不是渐近复杂度证明。

| 统计口径 | N8 ms | N128 ms | r | β μs/新增路径 | α |
| --- | --- | --- | --- | --- | --- |
| 当前 B：均值 | 6.228 | 7.007 | 1.1251 | +6.492 | +0.0425 |
| 当前 B：中位数 | 5.311 | 4.725 | 0.8896 | -4.886 | -0.0422 |
| 之前 A：均值 | 5.453 | 4.401 | 0.8070 | -8.769 | -0.0773 |
| FS-index／B 窗口：均值 | 1.828 | 1.958 | 1.0714 | +1.087 | +0.0249 |
| FS-index／B 窗口：中位数 | 1.802 | 1.616 | 0.8970 | -1.546 | -0.0392 |

当前 B 全体均值 **+12.51%**，中位数 **−11.04%**。当前 B 相对旧 A 的全体 q 均值仍为 N8 **+14.21%**、N128 **+59.21%**，这一回退事实保留；诊断分层不能把它改写为整体加速。

匹配 campaign 后，B 的 N8→N128 q 均值增长如下：

| campaign | N8 q ms | N128 q ms | r | β μs/新增路径 |
| --- | --- | --- | --- | --- |
| 0 | 4.090 | 10.914 | 2.6682 | +56.861 |
| 1 | 5.714 | 4.391 | 0.7685 | -11.022 |
| 2 | 4.041 | 4.319 | 1.0689 | +2.321 |
| 3 | 11.068 | 8.405 | 0.7594 | -22.194 |

四块增长倍数为 **2.668／0.769／1.069／0.759**，配对增长倍数的中位数 **0.919**；2/4 增长、2/4 下降。仅作诊断的 C1／C2 合并均值为 N8 **4.877 ms**、N128 **4.355 ms**，r=0.893，约下降 10.70%；该子集不成为新基线。正常块中 N128 B 相比 A 也没有共同变慢。

N8 的 campaign 3 同样慢：A/B q 均值 **9.771／11.068 ms**，而 N128 A campaign 3 为 **3.769 ms**。N128 B campaign 0 在首块就慢。因此既不支持“只在大 N 变慢”，也不支持“最后一轮必然慢”或“新 B 在所有块均慢”。现有数据更符合块／运行窗口相关的耗时放大；版本、先后顺序与环境影响仍未分离。

## 块内重复与 route 历史假设

源码存在原有 retained-route expiry 的 O(M) 扫描，M 是本 mount 注册历史数，非 corpus N。每 mount IoT arm 15 次已含 5 warmup＋10 正式，K2 共新增 30 个普通 typed registrations；A/B 请求序列相同。该机制不是本轮新引入。

N128 B 正式前五次与后五次：

| campaign | 系统 | 前五次 q ms | 后五次 q ms | 后／前 |
| --- | --- | --- | --- | --- |
| 0 | IoTDB-FS | 11.766 | 10.061 | 0.855 |
| 0 | FS-index | 2.744 | 2.414 | 0.879 |
| 1 | IoTDB-FS | 4.427 | 4.355 | 0.984 |
| 1 | FS-index | 1.432 | 1.462 | 1.021 |
| 2 | IoTDB-FS | 4.547 | 4.091 | 0.900 |
| 2 | FS-index | 1.409 | 1.315 | 0.933 |
| 3 | IoTDB-FS | 7.496 | 9.314 | 1.242 |
| 3 | FS-index | 2.252 | 2.639 | 1.172 |

IoT 的 C0、C1、C2 后半均值分别降低约 14.5%、1.6%、10.0%，C3 才增加约 24.2%；FS 的 C0／C2 下降而 C3 上升。原有 O(M) 扫描仍是可优化项，但这种方向不一致的块内趋势不足以将它确认为本次慢块主因。它也不能直接解释 FS-index 同期变慢。

## 资源与观察器：证据边界

资源取证使用 [load-forensics-r2.json](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-q-diagnosis-20261024/load-forensics-r2.json)，先前 r1 数值保留。全部正式端点 cgroup throttle 和 memory high/max/OOM 增量为零；其他核 CPU ticks 的单次分辨率不足，缺失不作零。1 Hz census 没有完全位于任一主测量块内的相邻采样区间，不能据此证明后台负载一致或排除虚拟化／频率／短时争用。

观察器共 918 次 capture，平均 wall 约 171.15 ms、CPU 约 171.05 ms。此前 61/320 外层调用包络相交不能直接当 q 相交；本次另用实际 server steady timestamps 与外层边界推导 q 起点上下界，确认这 61 条均必定与 timed q 相交，未出现仅可能相交的模糊条目；其中 32 条直接碰到已知位于 q 内的两个 server 首尾跨度。

Python 解释器与历史封印 SHA 相同，perf_counter／monotonic 的当前实现均为 CLOCK_MONOTONIC；历史记录映射的 libstdc++ 当前 hash 匹配，对 steady_clock 反汇编确认 clock id=1。这是匹配二进制的当前只读实现核验，不是历史时钟采样；没有重建客户端精确 t1/t2。起点上下界及 definite／possible／disjoint 的数学经独立手构测试验证。

以下每块 10 次。CPU 与 PSI 范围均为 resource/client 外包络，包含 T 外 profile/oracle/telemetry；不能当作 q 独占 CPU、PSI 或用于扣减 q。其他 30 核为十个互不重叠 resource 包络的 tick 比值，改善采样粒度但保留粗端点误差。

| campaign | 系统 | 确定与 observer 重合次数 | 客户端外包络 CPU ms | query CPU PSI some | 其余 30 核 active ticks 比例 |
| --- | --- | --- | --- | --- | --- |
| 0 | IoTDB-FS | 2 | 13.218 | 4.91% | 29.53% |
| 0 | FS-index | 2 | 11.070 | 0.61% | 29.08% |
| 1 | IoTDB-FS | 2 | 8.456 | 0.38% | 29.01% |
| 1 | FS-index | 2 | 8.030 | 0.15% | 31.54% |
| 2 | IoTDB-FS | 0 | 8.647 | 0.31% | 28.41% |
| 2 | FS-index | 0 | 7.894 | 0.05% | 27.90% |
| 3 | IoTDB-FS | 2 | 12.799 | 0.85% | 28.35% |
| 3 | FS-index | 1 | 11.375 | 0.17% | 27.60% |

C0 的 query／parent CPU pressure 明显升高，C3 未呈现同幅升高；这些外包络不能把其余耗时定量归因于等待。客户端外包络 CPU 在慢块同样上升；由于它甚至大于 q，不能将它当 q CPU、用来计算 q 等待比例或断言纯调度问题。CPU2/3 的 startup 频率项均不可用，不能判断运行时频率；guest 证据也无法恢复 hypervisor 调度。

观察器直接相交不足以解释整块变慢：确定不相交的记录中，C0／C3 IoT n=16，q 均值／中位数为 **9.470／9.146 ms**，正常 C1／C2 n=18 为 **4.377／4.376 ms**；FS-index 慢层 n=17 为 **2.452／2.370 ms**，正常 n=18 为 **1.414／1.390 ms**。这些只用于反证“直接相交是整块变慢的必要解释”，所有记录继续留在全体统计；不将无相交层当作排除观察器间接影响后的新基线。

时间重合不证明观察器占用了 query CPU2/3 或构成因果；无直接重合也不能排除其间接影响。AB 第二个 B 与 BA 第一个 B 的混杂仍然存在，现有负载证据不足以进一步确定唯一根因。

## 结论与下一步定位边界

已经可以排除“慢块因更多 N 个候选遍历、额外 predicate、字段缓存失效或相同比例费用重放而增加工作量”的直接解释。观察到的是固定工作在 server 内外多个阶段同时耗时放大；最大尚未细分的边界为 FUSE／Arrow／owned-copy 等所在的 control 外区间。原始全体均值没有被修正或排除。

如继续区分具体根因，最小对照应固定 N128 的相同 mount／相同输入，单独比较 observer 开关，交错执行 A/B；同时独立计时 q1/q2 的 control、FUSE/IPC、owned copy，服务端记录 worker 排队、future 等待和线程 CPU。先复现正常与慢态，再决定是否改异步 cache-hit、交付或 route 回收路径；不据当前残差直接删除权限、版本新鲜性、预算或 collector 检查。本轮未进行这些新采集或实现变更。

## 复现与核验

`extract_q.py` 从同一冻结 raw/audit 离线提取，固定数值白名单；验证全部 480 行网格、320 正式行、两次 serial typed、输出和时钟关系。提取器 10 项离线测试、取证时钟数学 8 项测试均通过，根节点另将 18 项一并独立运行通过。正式原始查询、完整 PID census 和科学数据仍留服务器，仅新增数值汇总／代码／说明。本地另以旧独立 aggregate 核对 **3360 个数值**、验证 **32 组均值分解恒等式**；当前 **528 个冻结源码文件**及旧完成归档 SHA 全匹配，`git diff --check` 通过。

main audit SHA：`aae403b2dbb393c0fe71ab229ca7b13eccbae2ff2e656756ec3ab2b3c53e2bfd`。
main raw SHA：`ecbb7665eff0dcf02e55b804e3c6368f27c97c66cb845025c251e647d7a5616c`。
q-derived SHA：`c9c3efe32afff48ca2328fc19586e9f11880a305dd009266acb4e1f3fc18d433`。
load-forensics-r2 SHA：`ad3d23dffc8666fe82486609e045124fc721ef7ef2c74427290410cf28dc67be`。

- [完整数值汇总](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-q-diagnosis-20261024/q-derived.json)
- [源码调用图与精确行号](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-q-diagnosis-20261024/q-source-audit.md)
- [独立核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-q-diagnosis-20261024/root-verification.json)
- [提取器与字段定义](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-q-diagnosis-20261024/schema.md)
- [本轮诊断复现归档](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/grib-q-diagnosis-20261024/completion-evidence.tar.gz)
- [之前实现及原实验报告](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/docs/reports/grib-fee-summary-20261024.md)

服务器原始目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/grib-fee-summary-20261024`；本次新增离线诊断目录为同级 `grib-q-diagnosis-20261024`。没有重设服务、重建语料、修改原始记录、更新飞书表格或执行 Git 写操作。
