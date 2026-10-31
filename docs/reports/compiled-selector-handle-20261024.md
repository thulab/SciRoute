# 公共 selector 句柄复用与首版 HDF5：实现和实验结果

2026-10-24。实现、回归、真实后端探针、N8/N128 实验及独立审计均已完成。所有正式样本保留；未执行 Git 写操作或更新飞书表格。

**结论：暖发现的重复 selector 编译与长域键处理已消除，d 阶段收益明确；完整 T 收益较小。** N128 的全部正式样本平均 d 降低 43.24%，平均 T 降低 5.01%。N8→N128 的 d 均值两点经验斜率降低 61.69%，不能据此外推总体渐进阶数或 N512。内部索引发现没有变快，本次收益主要位于入口解析、编译和域定位。

## 实现边界

公共 frontend 新增格式无关 `PreparedSelectorRegistry`，通用 `HotQueryCache` 新增 Handle/Auxiliary 预留及 resident 短引用。HDF5 通过三个显式操作接入：`catalog_selector_prepare`、`dataset_catalog_by_selector`、`catalog_selector_release`。准备固定静态 selector；执行可更换受支持的完整六属性等值 AND 谓词和两项结果限额。原普通入口继续使用原完整语法。详见 [协议合约](../contracts/prepared-catalog-selector.md) 和 [已确认设计](../deliverables/compiled-selector-domain-handle-plan.md)。

每次使用真实 socket UID/GID/补充组、新 owner view、新预算账本及原授权/最终 admission。head 更新或淘汰使内部引用失效，使用已编译 selector 走原路径重绑，Latest 保持动态；backend/namespace incarnation 改变使绑定失效。引用校验包含 cache identity、full head、kind、slot、generation 和 resident membership；它不强持有大域，也不能通过 weak_ptr 成功锁定来绕过淘汰。

唯一热池保持 64 MiB，域/证明上限仍为 16/64。句柄单独限制为总数 64、每 UID 16、单模板 1 MiB、模板子限额 8 MiB、单调 idle TTL 5 分钟；控制元数据也从该热池预留。登记前预留，待构造和退休但仍有执行 lease 的对象持续计费；失败响应、断连、release、超时和 stop 具有回收路径。费用批次、阶段摘要、collector 退出与原序回退继续共用原实现。

## 版本与实验口径

从当前 HEAD `fac8688283e6de07108cf9aeef1c9638f1f95746` 冻结并全新构建基线，包含此前阶段摘要和当前 PathScopeSet；没有替用历史二进制。基线 SHA 为 `ac4609a7c9ed3e44617f05cbd2b26465b3b0de0c61dbd7ee111fe63252a295bb`，候选 r2 为 `a89aabc87da022e42d401a9f71301504b57cc5f714ae5d7a7a0e4bf10422b6c4`。产品差异限定在 9 个文件；r1→r2 只纠正一行新测试输入，产品哈希未变。候选源码冻结 527 文件，实际采集代码闭包 170 文件。

性能 A/B 均运行同一个候选 daemon：A=完整显式 selector，B=复用句柄。新基线只承担源码、构建和正确性门禁。使用既有封印 N8/C8、N128/C128、V1、S0、Latest 两组，D=4、K=2，每次 owned 输出 40,960 字节；不重新发布语料，不建立 FS-index。每档 4 次挂载，首序 AB/BA/BA/AB，同臂两模式相邻交替，各模式各臂每块 5 次 warmup＋10 次正式查询。每档 240 行、160 正式行；总计 480 行、320 正式行、496 条 query/lifecycle profile。

T=d+q 从客户端入口计至 owned arrays/masks；两模式的来源、scope 列表检查、oracle 和终点完全相同。每模式每挂载独立 FS-index NativeContext，共每档 8 个。CPU 使用既有两逻辑核共享配额协议（guest CPU 2/3、2 核 quota、查询叶 2 GiB、原 DataNode 15232 MiB heap/2176 MiB direct 不变），服务采用既有受保护 SYNC 窗口。1 Hz 观察器只作负载上下文，不能证明毫秒级负载完全相同。

## 全部正式样本均值

单位 ms。每个 N×模式×臂均为 40 个正式样本；这里使用 pooled mean，未删除任何慢值。

| 组 | d A | d B | d 降低 | T A | T B | T 降低 |
| --- | --- | --- | --- | --- | --- | --- |
| N8 | 0.710 | 0.625 | 12.07% | 4.666 | 4.525 | 3.02% |
| N128 | 1.911 | 1.085 | 43.24% | 8.340 | 7.922 | 5.01% |

| 组 | IoTDB q A | q B | FS d A | d B | FS T A | T B |
| --- | --- | --- | --- | --- | --- | --- |
| N8 | 3.955 | 3.900 | 0.258 | 0.263 | 0.725 | 0.758 |
| N128 | 6.429 | 6.838 | 0.916 | 0.921 | 1.585 | 1.618 |

N128 q 均值反而增加 6.35%，因此发现阶段的大幅下降只转化为较小的完整 T 收益；本轮也没有使 IoTDB-FS 的完整 T 优于 FS-index。

## 预登记配对统计

每块先求各臂中位数，R=T_IoT/T_FS，Rd=d_IoT/d_FS，Rq=q_IoT/q_FS，再取四块 `(B/A)` 的中位数。它与上表 pooled mean 是不同统计量，不能混用。小于 1 表示 B 改善。

| 组 | R 的 B/A | Rd 的 B/A | Rq 的 B/A |
| --- | --- | --- | --- |
| N8 | 0.9288 | 0.8482 | 0.9443 |
| N128 | 0.9390 | 0.4929 | 1.0084 |

N128 四块的 normalized R、Rd 均改善；N8 第 1 块（从 0 编号）两项均变差，不能声称所有轮次均更快。N128 第 1 块两侧共同变慢，位于中间轮，仍全部计入。下表列出块内中位数，单位 ms。

| 组 | 块 | IoT d A | d B | IoT T A | T B | FS T A | T B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| N8 | 0 | 0.618 | 0.550 | 4.229 | 4.008 | 0.647 | 0.637 |
| N8 | 1 | 0.693 | 0.653 | 4.432 | 4.619 | 0.754 | 0.704 |
| N8 | 2 | 0.714 | 0.591 | 4.877 | 4.470 | 0.729 | 0.747 |
| N8 | 3 | 0.787 | 0.688 | 5.075 | 4.744 | 0.689 | 0.824 |
| N128 | 0 | 1.398 | 0.740 | 5.807 | 5.386 | 1.254 | 1.267 |
| N128 | 1 | 3.051 | 2.183 | 13.053 | 12.959 | 2.664 | 2.755 |
| N128 | 2 | 1.669 | 0.658 | 7.836 | 6.604 | 1.266 | 1.374 |
| N128 | 3 | 1.551 | 0.780 | 6.567 | 6.809 | 1.126 | 1.199 |

## 机制与 N 增长

两档全部正式句柄请求均为 compile=0、路径输入=0、长域键字节=0、resident short-ref hit=1；A 每请求 compile=1、路径数 8/128、长键字节 807/9225。两模式均保留两次 proof 命中、两次实际 admission、四阶段摘要；fee replay/fallback/collector exit 均为 0。

| 指标（正式均值） | N8 A | N8 B | N128 A | N128 B |
| --- | --- | --- | --- | --- |
| 请求字节 | 1628.000 | 815.000 | 12684.000 | 815.000 |
| server total（ms） | 0.307 | 0.239 | 1.167 | 0.491 |
| JSON parse（ms） | 0.091 | 0.054 | 0.484 | 0.074 |
| 内部 discovery（ms） | 0.030 | 0.032 | 0.066 | 0.073 |

内部 `dataset_catalog_discovery` 的均值 B 略高；本次没有加速属性索引算法。服务端 total 排除 socket frame I/O 和 profile sink；`d-server_total` 还包含客户端请求构造/序列化、frame I/O、响应解析、scope 检查和调度，不能解释为纯客户端 CPU。

P 从 8 增至 128（16 倍）。经验端点斜率定义为 `(time128-time8)/120`，单位 μs/新增路径；跨档负载差异仍影响绝对时钟。

| 经验斜率 | A | B | 降低 |
| --- | --- | --- | --- |
| 完整 d 均值 | 10.002 | 3.832 | 61.69% |
| server total 均值 | 7.165 | 2.104 | 70.64% |

完整 d 中位数的 N128/N8 增长由 2.405 倍降为 1.264 倍；对应 server total 中位数增长由 3.392 倍降为 1.299 倍。上述数值支持局部重复 O(P) 输入/编译/长键处理消除的机制结论，不构成整体 O(1) 的证明。原费用摘要的有界修正、proof/admission、索引与 K 个结果处理仍存在；客户端列表 scope 校验仍可能为 O(KP)。未测试 N512、高并发或频繁 head/eviction 的性能。

## 准备与完整生命周期

| 组 | prepare 中位数 ms | release 中位数 ms |
| --- | --- | --- |
| N8 | 2.231 | 0.328 |
| N128 | 3.727 | 0.451 |

首个真正查询可能为任一模式建立共享域，因此首次成本受 AB/BA 顺序影响，不能当作独立冷启动比较。保留实际 `prepare+ΣT_query+release` 的 Q1..15 数据；Q15 包含 5 次 warmup 和 10 次正式查询，各档均只有 2/4 块 B 全生命周期更低，不能声称已验证独立准备盈亏点。

| 组 | 块 | Σ15 A ms | prepare＋Σ15 B＋release ms |
| --- | --- | --- | --- |
| N8 | 0 | 133.034 | 66.033 |
| N8 | 1 | 67.291 | 81.823 |
| N8 | 2 | 73.839 | 81.185 |
| N8 | 3 | 102.320 | 82.893 |
| N128 | 0 | 212.262 | 86.138 |
| N128 | 1 | 196.388 | 310.344 |
| N128 | 2 | 115.535 | 162.590 |
| N128 | 3 | 158.697 | 96.385 |

## 验证、过程异常及恢复

基线 1849 项全量、91 项定向通过。候选 116 项定向、1874 项同二进制全量确认通过；另有 13 项 cache＋12 项 registry ASan/UBSan，以及 cache 72、registry 76 个分配故障位置检查通过。框架 27 项离线测试在本机及服务器通过，独立派生脚本 8 项合成检查通过。真实 backend 的 17 项探针覆盖多谓词、空结果、Latest 重绑、静态模板保护、源/祖先撤权、phantom/删除、预算/collector 及跨 backend/cache/namespace 失效。实际跨 UID socket/动态补充组变化未作端到端实测：注册表跨 UID、socket 实际 peer 与 backend principal 变化分别有覆盖。

r1 新测试错误地将相对路径视为非法；原 PathScopeSet 会规范化相对路径。r2 只将该测试输入改为空路径，保留 r1 失败证据，产品源码哈希未变。r2 初次全量有一项既有 GRIB `weak.expired()` 立即释放断言失败，费用 16→47→47 检查通过。原 worker 的 packaged_task future-ready 不与 callable 捕获析构同步，存在短暂保留账本的合法调度；基线/候选各 100 次定向均未复现。同一二进制全量确认通过，不能表述为已修复 GRIB 时序问题。首次失败和所有确认记录均保留。

功能探针首次因原服务 ASYNC/true 不满足既有 SYNC/false 前置条件而拒绝，未进入测试。随后在受保护窗口完成 17 项。独立功能窗口遗漏 cgroup keeper，使原空 session scope 被系统回收；按保存的 domain/空 subtree_control/cpu.max=max 100000/memory.max=max 重建原 cgroup 路径并迁回同一已验证 DataNode，再核验线程 affinity、原配置及受保护哈希。失败、恢复和补充验证均留证。全部正式采集在补充恢复后开始，并使用原资源主管的 keeper/恢复链。

N8 与 N128 均通过独立逐行时钟/oracle/科学身份/注册 run/profile 顺序/生命周期审计、负载描述和 read-only live gate。最终 DataNode PID 为 743077，配置 SHA `f69f408de5c09edabae3a8293cf662327f794b908cbf98d5a02b3878fea154f4`，effective ASYNC/true；原 cgroup、全部线程 CPU0–31、受保护文件及临时资源组/挂载/worker 退出均核验通过。1 Hz 负载标记只作描述，未剔除样本，也不证明负载严格相等。正式配对中 N8 有 3/80、N128 有 9/80 对被 census bracket/observer exposure 差异标记；其余 30 CPU 的端点计数分辨率不足分别涉及 80/80、78/80 对。这些缺失不能按零干扰解释。

## 证据

服务器原始目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/compiled-selector-handle-20261023`。原始查询、完整 census、private log 继续留在服务器。安全派生结果、冻结源码和测试归档：

- [独立数值汇总](../../experiments/cross_version/work/compiled-selector-handle-20261023/comparison-derived.json)
- [安全完成归档](../../experiments/cross_version/work/compiled-selector-handle-20261023/completion-evidence.tar.gz)，SHA256 `75b0f2ab8244ff2dd469bbdbaf767416d280be8411648102a93538db4310476d`，236 个文件。
- [本机缓存验证](../../experiments/cross_version/work/compiled-selector-handle-20261023/local-cache-evidence.tar.gz) 与 [注册表验证](../../experiments/cross_version/work/compiled-selector-handle-20261023/registry-local-validation.json)。

结论限定为这两组封印 case 的暖顺序复用。实现已在工作区，尚未由用户执行 Git 提交或主线合并。
