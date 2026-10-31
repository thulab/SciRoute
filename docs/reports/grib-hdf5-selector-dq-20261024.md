# GRIB 句柄接入与 GRIB／HDF5 d/q 增长测量

2026-10-24。公共句柄接入、真实后端验证、四组采集及独立审计已完成。共 960 条查询、640 条正式记录、1232 条 query/lifecycle profile，全部保留。未执行 Git 写操作或修改飞书表格。

**结论：GRIB 的主要暖 d 复杂度仍来自候选、权限证明和适配器费用批次的逐项重放；HDF5 已通过阶段摘要避免这些大批次重放，完整查询主要受 q 的准备、物化和交付开销限制。** 两种格式的 q 都没有出现与 C 成比例的候选处理量；本轮 q 的跨 N 时延变化不能直接解释为 O(N)。HDF5 N8 有明显慢块，原始跨 N 斜率为负，不能据此宣称算法阶数降低。

## 实现和边界

GRIB 复用既有 PreparedSelectorRegistry、UID 隔离、五分钟 idle TTL、64 总句柄／16 每 UID／1 MiB 单模板／8 MiB 模板子限额，以及唯一 64 MiB 热池。公共 frontend 新增 format_catalog_by_selector，prepare 固定 sources、offset 投影和全部 QueryLimits，execute 只接受 ID 与当次谓词。payload 使用 variant 判型，backend 模板与 resident reference 使用明确 kind。

公共 SelectSources 回调进入 scan_published_versions 的 CandidateDomain 短引用；后续共用原 format_catalog 授权、fresh admission、执行 lease、prepare、locator 检查和最终 admission。GRIB 原三次 admission 保留，HDF5 保留两次；每次创建新账本。head／eviction miss 走原长键路径重绑，Latest 动态，跨 backend／namespace 失效，句柄不强持有大域。GRIB 尚未接入 HDF5 的跨批次阶段摘要，本轮没有改变 GRIB 原费用重放语义。

详见 [协议合约](../contracts/prepared-catalog-selector.md)。与本轮实际起点相比，产品差异为 8 个文件（含独立 probe 的 CMake target 和诊断计数）；既有未提交 HDF5 更改保留。基线是此前合格的 HDF5 句柄实现 a89aabc87da022e42d401a9f71301504b57cc5f714ae5d7a7a0e4bf10422b6c4，未退回 HEAD-only 历史版本。候选为全新构建 50f24526ed71ccb9da8e6b2c06e5de4fd0582e3c851a3e5bc3574916150a296a，冻结 528 源文件；所有性能 A/B 使用同一候选 daemon。

## 口径

仅使用既有封印 FM1 GRIB／HDF5 N8/C8 和 N128/C128，V=1、D=4、K=2、S0、Latest。P=C=N，因此以下是三者共同变化下的端点测量，不是固定 C 的纯 namespace-N 斜率。未重新发布语料、建立索引或创建新实验组。

A=完整显式 selector，B=句柄。每组 4 挂载，AB/BA/BA/AB 首序，每模式每臂每块 5 warmup＋10 正式；每组 240 行，正式每模式每臂 40 样本。每组 8 个独立 FS-index NativeContext，合计 16 挂载／32 context。GRIB q 保持两次串行 typed；HDF5 q 保持一个两项 typed_batch。两者均结束于 40,960 B 的 owned Float32／mask。T=d+q；oracle、profile 采收、资源采样和启动在 T 外，准备／释放另计。

沿用共享两逻辑核配额、2 GiB query 叶、原 DataNode 内存及受保护 SYNC/false 窗口；CgroupV2Resources keeper 始终保留原 cgroup。每组前后均验证源码／二进制／科学身份、服务配置、全部线程亲和性、临时挂载与 worker/context 退出。1 Hz 观察器只作背景描述，不能证明毫秒级负载相等。

## 句柄 B：d/q 占比与原始端点耗时比

以下为全部正式样本均值，单位 ms。占比为 sum(phase)/sum(T)，不是逐样本占比的均值。

| 格式 | N | d | q | T | d/T | q/T |
| --- | --- | --- | --- | --- | --- | --- |
| GRIB | 8 | 1.123 | 4.270 | 5.392 | 20.8% | 79.2% |
| GRIB | 128 | 4.297 | 7.289 | 11.586 | 37.1% | 62.9% |
| HDF5 | 8 | 1.484 | 7.308 | 8.792 | 16.9% | 83.1% |
| HDF5 | 128 | 0.712 | 4.046 | 4.758 | 15.0% | 85.0% |

N 增加 16 倍。经验斜率定义为 (mean128−mean8)/120，单位 μs/新增路径；它不是渐进阶数。

| 格式 | 侧 | d128/d8 | q128/q8 | d 斜率 | q 斜率 |
| --- | --- | --- | --- | --- | --- |
| GRIB | IoTDB-FS | 3.827× | 1.707× | 26.450 | 25.166 |
| GRIB | FS-index | 3.993× | 1.317× | 7.090 | 4.046 |
| HDF5 | IoTDB-FS | 0.480× | 0.554× | -6.426 | -27.188 |
| HDF5 | FS-index | 1.380× | 0.719× | 1.784 | -1.711 |

**HDF5 的负斜率不能当作算法收益。** N8 的第 1、2 块（从 0 编号）IoTDB-FS 较慢，N128 较平稳；FS-index 的 q 均值也从 0.731 降至 0.525 ms，但不能用它解释所有 IoTDB-FS 专属变化。HDF5 B 的 d 中位数比为 0.787，q 为 0.709，仍同向下降。统计 r2 保留原始端点，拒绝把负候选斜率换算为超过 100% 的“增长系数优化”；未剔除慢块。GRIB N128 慢块也集中在中间的 1、2 块，末块恢复，未支持必然的末轮升高。

2026-10-24追加[统计复核](hdf5-growth-stat-audit-20261024.md)：服务器原始480行的独立复算与旧汇总一致，0.480248／0.553594未发现算术或分组错误；两N分属不重合采集窗口，N8慢块抬高端点。这两个数只能作为描述性端点耗时比，不能当作N的算法增长系数或规模增大带来的优化收益。原数据不改、不删慢块。

## 同挂载 A/B 效果

| 格式 | N | d A | d B | d 均值降低 | T 均值降低 | 配对 R B/A | 配对 Rd B/A |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GRIB | 8 | 1.190 | 1.123 | 5.67% | 2.26% | 0.9789 | 0.9291 |
| GRIB | 128 | 4.975 | 4.297 | 13.63% | 5.21% | 0.9213 | 0.8577 |
| HDF5 | 8 | 1.391 | 1.484 | -6.67% | 0.03% | 1.0016 | 0.8967 |
| HDF5 | 128 | 1.496 | 0.712 | 52.36% | 12.96% | 0.8434 | 0.4892 |

负的“降低”表示变慢。R=T_IoT/T_FS、Rd=d_IoT/d_FS；配对值先求块内中位数比，再求四块 B/A 的中位数，与 pooled mean 不同。GRIB d 均值端点斜率从 31.539 降至 26.450 μs/path，降低 16.14%；剩余线性费用使其收益明显受限。HDF5 N128 d 改善明确，N8 均值未改善，不能声明每档每块都更快。完整块级数值和 prepare＋Q1..15＋release 在派生数据中；各格式／N 的 Q15 都只有 2/4 块句柄完整生命周期更低，首轮共享域受顺序影响，不构成独立冷启动盈亏点。

## d 的主要复杂度

全部正式 B 请求均 compile=0、输入路径=0、长域键字节=0、resident short-ref hit=1；A 仍 compile=1、输入 P=8/128、重建长键。句柄已消除这部分重复 O(P) 输入／长键工作和编译工作。

| GRIB prepared 批次实际访问／请求 | N8 | N128 |
| --- | --- | --- |
| 候选选择 | 123 | 1257 |
| 首个权限证明 | 141 | 1905 |
| 适配器完整域费用 | 89 | 1409 |
| 合计 | 353 | 4571 |
| proof hits／实际 admission | 3／3 | 3／3 |
| 后续 proof memo hits | 2 | 2 |

GRIB 适配器费用在这两组严格满足 11C+1；候选与证明还含去重祖先和目录页证据。总 prepared visits 增长 12.95 倍。逐项重放仍需在新账本 map 中去重／检查限额：对 F 项证据、U 个身份，存在 O(F log U) 工作；固定 D 和路径深度时 F 随 C 线性增长。这是额外预算语义的实现成本，不是六属性索引必须遍历全部消息。三次 admission 均保留，但后两次 proof 已由同账本 memo 免重放，不能把证明费用再乘三。

GRIB N128 B 的候选重放 0.776 ms、授权阶段 0.927 ms、适配器重放 0.693 ms；三者顺序不重叠，合计 2.396 ms，占服务端 d 3.409 ms 的约 70.3%。授权阶段包含证明回放和真实 admission，不能全称为纯计费 CPU。相比之下，Adapter 二分范围 lookup 仅 0.0165 ms；随后只为 K=2 个命中构造计划。暖索引本体为 O(log(CD)+K)，不是主时钟来源。

| HDF5 d：账本摘要计数 | N8 | N128 |
| --- | --- | --- |
| prepared visits | 0 | 0 |
| 由摘要跳过的批次项 | 427 | 5727 |
| 成功摘要阶段 | 4 | 4 |
| overlay entries inspected | 1 | 1 |

HDF5 大域费用已通过借用不可变批次＋阶段摘要跳过，账本只处理有界覆盖项；覆盖项查找仍可含 log F，不应称整个 d 严格 O(1)。两种格式都还包含索引、权限／版本新鲜性及 K 个结果构造；客户端 `path in scope` 是列表成员校验，本例比较次数从 14 增至 175，仍有 O(KP) 上界。FS-index 也每请求解析 scope、做 source-count 遍历和相等检查，因此“SQLite 索引命中”不意味着完整 d 与 P 无关。

上述 visited 仅指 PreparedQueryMetadataTrace 批次重放，**不是所有 require/observe 或原始 map trace 的总访问量**；collector 路径另有未跟踪标记。本轮正式暖请求无 collector 退出／摘要回退。冷启动、head 改变、淘汰仍需重新发现和建域／索引；另有 HDF5 冷构建逐源调用 record() 线性成员检查的潜在 O(C²) 工作，未列为本轮暖瓶颈，也未据此推断其独占耗时。

## q 的主要成本

| 每个完整 q 的工作量 | GRIB N8→N128 | HDF5 N8→N128 |
| --- | --- | --- |
| selector 编译／路径项 | 2／2 → 2／2 | 4／4 → 4／4 |
| source opens | 2 → 2 | 2 → 2 |
| prepared visits | 104 → 111 | 110 → 117 |
| 格式内工作 | 8 次 message 谓词判断 → 8 | 2 个 native slab → 2 |
| GRIB native field cache | 2 hit、0 load → 相同 | 不适用 |

q 每次使用精确源，候选域通常为 Cq=1；没有重做发现阶段的 C128 全域扫描。GRIB 的 D 条消息选择仍可随 K×D 增长，当前 D=4、K=2；HDF5 仍有原生 slab 读取与格式规划。原始 alias trace 不在上述 prepared visits 里，所以这些计数不能单独证明整个 q 为 O(1)。结合源码和固定 source/message/slab 数，本轮没有发现 q 中随全局 C 线性扩张的大域工作。GRIB 专用缓存 hit=2、load=0，不能仅依据 generic runtime decode=0 判断缓存。

q 的客户端时钟可分为下列三个不重叠包络，单位 ms。最后两列仍含 I/O、调度、JSON／FUSE／Arrow 和 owned 拷贝，不能解释成独占 CPU 或纯 IPC。

| 格式 | N | 服务端注册／物化 | control 减服务端 | q 减 control：交付包络 |
| --- | --- | --- | --- | --- |
| GRIB | 8 | 1.612 | 0.635 | 2.022 |
| GRIB | 128 | 2.295 | 1.125 | 3.869 |
| HDF5 | 8 | 3.112 | 0.510 | 3.686 |
| HDF5 | 128 | 1.938 | 0.344 | 1.764 |

N128 时，GRIB 约 68.5%、HDF5 约 52.1% 的 q 位于服务端注册时钟之外。服务端仍以 publish_route／query_materialize 为大项，其中 typed_prepare、执行设置和 typed/IPC encode 继续重复；这些 timer 嵌套，不能相加当独占分解。因而 q 的下一步应针对精确源重复规划、控制／结果交付和物化，而不是再优化已经命中的大域二分索引。

N128 B 与 FS-index 的 d／q 均值分别为：GRIB 4.297／7.289 对 1.135／2.017 ms；HDF5 0.712／4.046 对 0.778／0.525 ms。HDF5 发现已接近或略优于本轮 FS-index，完整 T 仍主要被 q 拉开；GRIB 则同时受 d 费用与 q 交付限制。q 的正／负跨 N 斜率都不能仅由这组非平稳背景数据推导成算法阶数。

## 后续优先项与证据限制

1. GRIB 首先研究将候选、实际 principal 的证明和 Adapter 域费用接入借用批次／阶段摘要，保留完整费用、collector、原序失败前缀、预算回退、配额与生命周期；不通过跳过权限或降低逻辑收费制造收益。
2. 公共 q 层优先拆解／减少重复规划、物化与控制／IPC／FUSE 交付。GRIB 批量取值可作为独立方案，但须单独验证逐项预算和失败顺序；本轮未混入该改动。
3. 若要给 HDF5 一个可解释的时间-N 斜率，下一轮应把已确认两档在同一窗口内按块交错、平衡 N 顺序，继续保留全部数据。本轮负斜率只作为实测值，不作为可迁移的性能模型。

1 Hz 正式配对的 mismatch 标记分别为 GRIB N8 4/80、N128 5/80，HDF5 N8 6/80、N128 6/80；未用于剔除或修正时钟。other30 endpoint 计数分辨率缺失分别涉及 80/80、60/80、74/80、80/80 对。HDF5 N8 还出现 resource-hazard 标记。缺失不是零干扰，1 Hz 标记也不能为所有中间慢块定位原因。

## 验证、修订与恢复

候选全量 1883 项、补齐后的定向 237 项通过；本机 14 项 HotQueryCache ASan／UBSan 通过；框架 32 项离线测试及独立审计／派生 7 项合成检查通过。真实后端最终 GRIB 9 项＋HDF5 17 项通过；所有组 before／after live gate 和完整 profile 顺序、科学 oracle、时钟、生命周期、context/worker 退出审计通过。

过程修订全部留证：初始定向 filter 漏选 HotQueryCache，但 1883 项全量已通过，随后用同一二进制补跑 237 项；框架一个限额检查误读服务器仓库根目录头文件，改读并哈希核验冻结源码；GRIB 新 probe r1 误用了三列投影，在进入候选路径前即被 offset-only 契约拒绝，r2 只修正 standalone probe 并增加固定数字诊断，使用同一冻结 core 库独立重编译，产品／单测二进制未变。初次窗口即使 probe 失败也完整恢复，首次失败记录未覆盖。统计 r2 只修正负斜率的解释，不改任何查询样本。

最终 DataNode PID=1322362，292 个实际线程 CPU0–31，原 cgroup 已核验，effective ASYNC/true；配置 SHA f69f408de5c09edabae3a8293cf662327f794b908cbf98d5a02b3878fea154f4，3 个受保护身份哈希、临时 cgroup 删除、4 个末组挂载和实际查询 worker 退出均通过只读 live gate。

服务器原始目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/grib-selector-dq-scale-20261024`。原始查询、科学对象、完整 census、配置及 private logs 留服务器。

- [最终独立数值汇总 r2](../../experiments/cross_version/work/grib-selector-dq-scale-20261024/comparison-derived-r2.json)
- [复现证据归档 r2](../../experiments/cross_version/work/grib-selector-dq-scale-20261024/completion-evidence-r2.tar.gz)，103 文件，SHA256 `5c99bcbd4351c82c2fc7d7c4951c98db84e3fae908883f6db7aa985f0c6e7728`。首版归档保留。
- [本机缓存验证](../../experiments/cross_version/work/grib-selector-dq-scale-20261024/local-cache-validation.json)
