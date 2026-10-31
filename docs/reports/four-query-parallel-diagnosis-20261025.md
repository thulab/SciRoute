# 四查询的真实并行、争用与串行步骤

2026-10-25。公共查询池确实允许独立查询重叠执行：三波 N128 详细诊断的占槽峰值为 **3／4／4**，真实 CPU 并行峰值均为 **2**。原资源契约只有 CPU2–3、两核 quota，因此4个 worker 不等于4核计算。当前应优先优化请求控制面和提交／恢复链路；暖缓存下的实际 GRIB field 消费和 socket 发送占用很小。

本次没有修改或部署生产查询算法。所有新增计时仅存在于独立诊断快照；版本、权限、费用预算、取消、错误、结果 pin 和资源释放顺序沿用原代码。没有增加客户端 worker，没有增加查询内并行。

## 如何检测

沿用4.3封印的 GRIB N128/C128/V1/D4/S0/latest、K=2、seed20261013。公共池4、reader4、sender1、queue64、batch4，CPU2–3／两核 quota、查询内存2GiB。客户端只有一个 asyncio event loop、恰好4个查询协程。四条首连接全部就绪后，在写首帧之前共同释放；后续自然执行：

`catalog → typed1 → fetch1 → owned1 → typed2 → fetch2 → owned2`

每连接通过 Linux abstract AF_UNIX peer address 标记 wave/query/stage，不修改 wire JSON。独立有界记录器保留绝对单调时间、TID、线程 CPU、锁地址、acquire/hold、公共池生命周期和发送跨度；内核 `sched_switch/wakeup` 将跨度分为 on-CPU、runnable、sleep。先验证 lost=0、dropped=0、请求完整关联和采集尾部，再计算交集。嵌套跨度不相加，独占 CPU 使用每 TID 的最深已标注跨度；锁持有作为覆盖层处理。

N8 ON资格4条；N128 OFF/KERNEL/ON各1波预热＋3波诊断，每波4条，共 **52查询、13波、4挂载**。三条件均保留原普通 query profile 和客户端诊断记录：

| 条件 | 详细服务端记录 | 内核调度跟踪 | 平均完整 T | 平均四查询波长 |
| --- | --- | --- | ---: | ---: |
| OFF | 关闭 | 关闭 | 16.369 ms | 17.764 ms |
| KERNEL | 关闭 | 开启 | 23.640 ms | 25.532 ms |
| ON | 开启 | 开启 | 25.406 ms | 27.481 ms |

每条件统计12条稳态查询，预热全部保留但不混入。KERNEL 的平均 T 比 OFF 高44.42%，ON又比KERNEL高7.47%。这是不同观测条件下的实测差异，含短程／顺序波动；不能将全部差异精确归因于某一个计时操作，也不能扣除观测成本“校正”性能。**详细阶段的绝对耗时不直接代表关闭跟踪时的延迟。** OFF也保留客户端诊断工作，并非原性能客户端的零插桩基线。本次用于辨别执行机制，不用于声明优化提速。

**4条查询in-flight也不等于4个任务同时到达公共池。** OFF三波虽使用首帧前屏障，实际四条首帧写入跨度仍分别为3.012／0.151／0.994 ms；四条catalog的服务端执行计时开始点跨度为3.304／2.121／5.377 ms。后者来自原普通profile，开始点位于runtime／IoTDB快照之后，并非详细记录的Ready或Running边界。可确认提交与进入执行存在错峰，但不能将全部错峰归因于transport解析。

## 哪些真正并行

下表来自 ON 的三波稳态诊断，CPU峰值由内核调度区间与请求阶段相交得到，而非由开始／结束包络推断。

| 步骤 | 三波实际 CPU 峰值 | 解释 |
| --- | --- | --- |
| 公共查询 task | 2／2／2 | 占槽可到4，只有两核能同时运行；不是整条查询被一把全局锁串行化 |
| 接收／解析／提交包络 | 2／2／2 | 不同 transport worker 确有同时执行 |
| typed prepare／cursor／IPC编码 | 2／1／2 | 已观察到跨查询计算并行；第二波没有重叠不等于代码必须串行 |
| profile JSON构造 | 2／1／1 | 构造位于输出 mutex 外，可跨请求并行 |
| catalog locator短阶段 | 1／1／1 | 此样本未观察到重叠，不能据此认定它受全局串行约束 |
| control response write | 1／1／1 | 调用很短；不同连接没有共同发送锁，实测峰值1不是串行机制证明 |
| result sender | 1／1／1 | 配置为单 sender；24次 sendmsg 合计 CPU0.317 ms，平均13.21 µs |

![全部三波时间线](../../experiments/cross_version/work/four-task-diagnosis-r2-20261025/analysis-post-v2/on-three-waves-v2.png)

图中浅色是 task 占槽包络，深色是该 TID 真正 on-CPU 的交集；上方两条 CPU 泳道覆盖实际进程角色，红线为最后一个 owned 输出。三个预声明稳态波次全部绘制，未挑选波次。公开 worker 上的 C/T/F 分别为 catalog、typed、fetch；同一查询仍遵守原依赖顺序，其他查询可交错推进。原始 SVG 可放大检查：
[三波 SVG](../../experiments/cross_version/work/four-task-diagnosis-r2-20261025/analysis-post-v2/on-three-waves-v2.svg)。

## 哪些在争抢

ON三波全部已标注同步工作的独占区间：CPU86.880 ms、runnable61.163 ms、sleep19.615 ms，unknown=0。这里是跨线程工作区间之和，不是可相加到单条查询 T 的关键路径分解。

**锁函数的长 wall 大多是 runnable 排队，不能全部叫“等锁”。** 全部 lock_wait 的35.754 ms由以下部分组成：

| 锁获取区间的线程状态 | 合计 | 占 lock_wait wall |
| --- | ---: | ---: |
| on-CPU | 0.595 ms | 1.67% |
| 可运行、未获得 CPU | 33.093 ms | 92.56% |
| sleep | 2.065 ms | 5.78% |

其中仅 **0.954 ms** 同时满足“等待线程sleep＋另一TID持有同地址锁”，这是明确竞争等待的下界，不能当成总互斥等待的上界。剩余sleep不能凭当前记录全部归属给某一个持有者。

例如 `owner.view` 的 inclusive wall为27.731 ms，只有7.781 ms on-CPU，另有18.823 ms runnable和1.128 ms sleep。它包含锁获取和函数体；不能把27.731 ms都归为Owner临界区计算或互斥阻塞。

公共池交接单独计时：

| 转换 | 样本 | 平均 wall | 核对结果 |
| --- | ---: | ---: | --- |
| Queued→Ready | 60 tasks | 122.698 µs | 尚无分配的执行TID，不归类为该worker的OS等待 |
| Ready→Running | 60 tasks | 316.329 µs | 93.77%的合计区间为指定worker的runnable；约0.105 ms初始状态未知，保留未知 |
| response write结束→slot Idle | 36 control requests | 248.899 µs | 合计8.960 ms中，CPU5.942、runnable3.012、sleep0.0069 ms；响应后仍占槽 |

Ready→Running期间的少量worker CPU可能属于前一任务尾部或唤醒处理，不能标成新任务的业务计算。主同步业务跨度的CPU覆盖完整，并不意味着所有Ready之前的线程历史也已知。

KERNEL三波的两核时间账为：daemon70.597 ms、client48.923 ms、DataNode0.786 ms、idle32.886 ms，总计153.192 ms。平均约1.57核忙，仍有约21.5%的两核时间空闲。**阶段性的CPU竞争与阶段性的空闲同时存在**，不能用“始终双核满载”解释全部延迟。此账属于KERNEL观测条件，不冒称OFF中的进程占比。

## 哪些是确定的串行面

1. **单客户端 event loop。** OFF不启用perf或详细server timeline，仍可从同一个TID的首／末CPU时钟计算整个波次CPU。不能把4条异步query的CPU delta相加。三波客户端CPU合计31.417 ms、波长合计53.292 ms，相当于0.590个核，占两核总时间约29.5%；每波CPU／波长分别63.15%、60.21%、54.08%。这是一块确定的单线程执行面，包含本次客户端诊断本身的成本。
2. **同一查询的依赖顺序。** 下一次typed必须等待前一次结果及owned输出。不同查询可以重叠，但增加服务端worker不能把单条查询自身的依赖链变成四段同时执行。
3. **局部共享资源。** Owner gate、GRIB cache和result/route发布各有短临界区；profile输出在单mutex下flush；dispatcher在单线程上分配；sender1在单线程上发送。这些边界是局部串行，不能统称整个execute串行。

OFF客户端显式同步write/decode/append合计CPU6.394 ms；其他event-loop CPU合计25.024 ms。后者包括协议管理、JSON、连接、协程／回调恢复及客户端记录成本，现有跨度不能继续给出各自的独占份额。不能将这25.024 ms全部称为JSON解析，也不能把全部客户端CPU叫Arrow拷贝。

OFF完整T的逐query可加分解：

| 客户端阶段包络 | 平均每完整query | 占完整 T |
| --- | ---: | ---: |
| 三次control | 11.622 ms | 71.00% |
| 两次fetch | 3.651 ms | 22.30% |
| Arrow decode | 0.436 ms | 2.66% |
| append owned | 0.020 ms | 0.12% |
| 提交／恢复等相邻阶段间隙 | 0.640 ms | 3.91% |

各query整数纳秒严格闭合 `T=d+q`。control/fetch是异步等待包络，包含服务端执行、排队、socket和event-loop恢复，不能把93.3%全叫网络发送、服务端耗时或纯I/O。当前结果交付走 `result_fetch_v1`，没有通过FUSE读取结果body，因此也不能将这些等待包络命名为FUSE独占耗时。

## 后续优化方向

**第一优先级是减少控制面重复工作及交接。** ON中最深已标注span的CPU residual包括：transport.serve_connection11.820 ms、registration_total8.533 ms、owner.view7.448 ms、json_parse6.726 ms、typed.prepare4.787 ms；这些包含各自包装和计时成本，只用于识别阶段，不等于未经插桩的函数成本。

明确的代码候选是 **一次完整JSON解析后传递不可变解析结果**。目前[transport分类](../../src/query/frontend/QueryRegistrationSocket.cpp)先调用完整 `read_json`，`register_json`或fetch执行路径随后再次解析；`task_resources`还在NativeHdf5模式下另做完整解析，其他模式会提前返回。资源hint只负责调度，正式验证、当前版本／权限、metadata费用、预算、取消和发布决定仍应在原执行边界完成；解析失败的code/phase/message/source必须保持原顺序和内容。这样可直接减少重复控制CPU及分配，不依赖增加worker。transport residual还包含其他工作，11.820 ms不能当作这一方案的已证明可节省量。

**第二优先级是缩短响应后占槽尾部。** profile JSON构造在ON共占CPU2.641 ms；response结束到slot释放还包含输出、对象析构和调度。可以评估有界异步profile：只移动不持有Owner/native/payload lease的值快照，保留响应失败rollback及原资源析构顺序，stop时单独drain。不能在原生对象仍可能使用时提前归还资源，也不能把整个8.960 ms都当成可消除的JSON成本。

**第三步补足客户端其余CPU的独占分解。** 用低扰动方法区分连接管理、JSON、event-loop、GC和诊断成本，再决定是否值得改变FS协议或提交链路；不增加客户端worker。连接复用若作为后续方案，仍须逐请求fresh principal／版本／权限admission，不能复用旧授权结果。当前数据尚不支持对这些子项分别给出提速预测。

本轮每条件24次稳态typed调用全部native field/alias命中，miss/load/wait/hash均为0；各profile记录中的IoTDB query/session-acquire增量均为0。ON的24次GRIB field阶段CPU仅0.183 ms。因此本轮没有触及实际ecCodes解码、owner reader容量或数据库查询争用；不应继续用扩这些池或sender路数作为当前暖N128的主要优化方向。结论不外推冷缓存、HDF5、Arrow混合负载或大IPC。

## 验证与可追溯性

- 最终诊断产品 binary SHA：`729ac72677f9895b29704551752b71ef0dbbd1edddf433ec4b8a64552ddb4f57`，source manifest：`468734bd105dca91c324ee64b05dc75d87a52842ef643d89054121e73535868c`；全量2021/2021通过。
- 52条owned值／mask／ordinal均匹配封印oracle。N128 ON18072记录、80个request标签、零drop；N8 ON2924记录、20标签、零drop；9份perf均lost=0。所有请求尾部均在确认采集窗内。
- 原服务恢复ASYNC/true，配置字节／hash一致，228线程恢复原亲和性与cgroup，临时cgroup消失；head/journal/index未变。
- [原始证据本地验收](../../experiments/cross_version/work/four-task-diagnosis-r2-20261025/local-evidence-audit.json)：356文件逐大小／SHA校验。raw archive SHA：`8437adb8c5d00e4435a3eee7eab2cc06adc730e231519de3f909b25207876990`。
- [后处理验收](../../experiments/cross_version/work/four-task-diagnosis-r2-20261025/post-v2-final-audit.json)：14项分析器＋2项制图离线检查通过，最终parser处理同一份raw；post archive SHA：`47bf07be8a38c8339663e48a82736a8a5e67d5804bc28385c1ae4cc656cbba7f`。原raw归档内早期parser快照未覆盖，最终版本单独保存。
- 首轮36查询正确，但一个8192容量的线程缓冲丢2747记录，原run作为不完整诊断保留，未挑选残存阶段作为完整证据。扩大静态缓冲又触发HDF5隔离worker原512MiB地址空间门禁，5项unit失败亦保留；修为仅开启诊断的命名线程按需分配有界缓冲，未放宽限制，新helper BSS为1677B，512MiB独立自检20事件／4线程／零drop通过，随后全量2021项通过。原[失败证据验收](../../experiments/cross_version/work/four-task-diagnosis-20261025/local-failed-evidence-audit.json)与新目录内失败build receipts均保留。

数据和工具：`experiments/cross_version/work/four-task-diagnosis-r2-20261025/` 下 `evidence/`、`analysis-post-v2/`、`post-v2-evidence.tar.gz`；客户端单线程CPU／可加分解见 `client-baseline.json`，交接状态见 `transition-states-v2.json`，阶段与角色账见 `small-summary.json`。这些是诊断结果和下一步候选，未实现的新方案不声明收益。
