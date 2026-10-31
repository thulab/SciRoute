# GRIB N128：响应、profile、执行槽及 FUSE/IPC 尾部诊断

2026-10-25。采集、正确性门禁、开关对照、独立复算和服务／资源恢复均已完成。本轮只新增诊断实验、隔离构建和报告，没有把探针合入生产源码，也没有实施下述优化。

**当前最有证据支持的主要串行瓶颈，是单事件循环内的同步 FUSE/Arrow IPC 交付；全探针还发现了公共 worker 通知／交接的候选瓶颈，生产量级须低扰动验证。响应 write 本身和 profile 文件锁不足以解释完整查询没有下降。**

原 binary 的轻量交错复核中，S4 的同步 IPC＋append 占 wave 的 **57.60%**；profile 结束到控制调用返回的间隔，有 **81.74%** 与全 loop 的同步 IPC／append 相交。完整探针进一步观察到，槽位释放后的 `notify_all` 在 S4 明显变长；它不能算进逻辑 occupied-slot，也不能从 worker 周期中删掉。通知尾部与下一任务交接确有重叠，但轻量复核未测通知，尚不能确定它在无探针时的贡献。这里给出实测阶段与重叠证据，不把它们直接当作某项未来优化的因果收益。

## 1. 条件、时钟与验收

沿用[飞书 4.3 大表格](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg)已缓存 revision983 的封印 FM1 GRIB 组：N=C=128、V1、D4、S0、latest、K=2、seed20261013。每完整查询仍是 catalog＋两次顺序普通 typed，最终 owned values/masks 40960B；未重发 corpus、重建 index 或生成新 scientific case。另用封印 N8 资格组验证。

S1/S2/S4 只调公共 worker 与 max dispatch batch=1/2/4，owner readers=4、queue=64 固定。客户端保持一个 asyncio loop，single=1 或 burst8=8 个既有在途协程；没有增加客户端 worker、线程或查询内任务。两核预算：CPU2/3，cpuset partition=root、父组 quota=200000/100000；query 叶组 2GiB。DB heap15232MiB／direct2176MiB 与 support roles 按固定协议计量。`root` partition 不声称内核级 isolated。

T 仍从完整查询入口计到客户端 owned 数组完成，原 t0/t1/t2 与 T=d+q 保留。profile harvest、oracle hash、采样与 trace dump 在 T 外；探针本身仍可能扰动 T，因此必须单列开关影响。

| 验证 | 结果 |
| --- | --- |
| Linux GCC13.3 Release 诊断构建 | 1986/1986 C++ 单测通过；trace 关闭 |
| trace 开启的关键语义回归 | 7/7；GRIB TLS／预算／取消／异常、publication race、HDF5 争用下 Arrow 推进；398 events、overflow=0 |
| 主诊断 control/off/on | 2916 条正式记录（2430 测量＋486 预热），81 条 N8 资格；全部 oracle／原时钟通过 |
| 原 binary 轻量交错复核 | 1152 条正式记录（960 测量＋192 预热），32 条 N8 资格；60 个测量 pair；全部通过 |
| 本地独立证据审计 | 主包979文件、轻量包272文件逐个 SHA 校验；矩阵、QPS、完整时钟、物理执行顺序和分段闭合复算通过 |
| 恢复 | 每窗口原配置字节、ASYNC/true、JVM 参数、所有线程 affinity／cgroup 核验；临时组移除，原服务恢复 |

原 binary SHA `4d8153bcb554574a4dc7be1e0abceb36087fd91ce27191b0267ca1f1cefce3ea`；诊断 binary SHA `0faae767e9c637eed6ddbd03fd5421577079f7e6df186dcabc838dcfc696eb14`。隔离 overlay 只有两份 trace header、scheduler/socket/main 五个文件。实际 compile/link 参数及动态库字节一致；link driver 的 c++／g++-13 路径别名只按同一 compiler 字节归一化，未忽略其他参数差异。

## 2. 无探针时，收益在哪里被抵消

下表为本轮原 binary control 的 burst8；QPS 和 p50 各为三轮的中位数，平均 T 为全部240条测量查询的 pooled mean。配置顺序轮换，单配置2预热＋10测量波次。三轮描述性结果不宣称统计显著。

| 公共路数 | QPS（三轮中位） | p50 ms（三轮中位） | 平均 T ms（pooled） |
| --- | ---: | ---: | ---: |
| 1 | 246.905 | 27.188 | 28.483 |
| 2 | 237.061 | 29.232 | 29.053 |
| 4 | 239.031 | 29.111 | 28.734 |

以下仅对 pooled mean 做可加分解，不相加 p50。每配置720个 profile（含匿名 catalog），按配置聚合而不伪造逐查询 catalog 关联。

| 路数 | 排队至 profile 开始 ms | profile 内 ms | 控制调用其余区间 ms | 控制调用外 ms | 完整 T ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 12.728 | 2.329 | 8.588 | 4.839 | 28.483 |
| 2 | 9.177 | 2.829 | 10.902 | 6.146 | 29.053 |
| 4 | 7.172 | 3.384 | 11.369 | 6.809 | 28.734 |

1→4 路，排队至 profile 开始少了 **5.556ms**；其余部分合计增加 **5.806ms**，完整平均 T 反而增加0.251ms。第一列包含排队、派发、worker 交接和 profile 前快照，并非纯队列等待；“控制其余区间”含服务端尾部、transport 和 loop 交错，也不是纯 socket 网络时间。这个整数闭合分解表明抵消发生在完整链路，后面的实测再定位其机制。

## 3. 探针开销与轻量复核

主诊断分别运行原 binary/control、诊断 binary/off、相同诊断 binary/on，三个独立顺序窗口。完整探针采用有界内存 ring、正常 drain 后 dump；仍明显扰动，尤其 S1。下表 QPS 均为 pooled，和上一表的三轮中位口径分开。

| 路数 | 原 binary QPS | 诊断 off QPS | 全探针 on QPS | on/off QPS 变化 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 244.057 | 221.375 | 137.341 | -37.96% |
| 2 | 238.177 | 246.734 | 202.564 | -17.90% |
| 4 | 237.533 | 243.887 | 212.780 | -12.75% |

顺序窗口差异不能全算因果探针成本，诊断 off 的 S1 有一个慢 campaign，全部保留。因此不能用全探针 on 的 S1→S4 伸缩性证明生产提速，也不把下面的绝对微秒当作无扰动基线。

补充复核使用**原 binary、无 server/FUSE 探针**，同一 mount／同一 repeat 相邻执行 off/coarse，两臂先后交错；每 mount 的10个测量 pair 各5次先执行。coarse 只包原 control／IPC／append 函数整体，调用原实现，off 恢复原函数对象，没有复制热路径或新增任务。Linux monotonic 时钟核验通过，async control 的 CPU 标作 None，不能归给某个 query。

| 路数 | off / coarse QPS（pooled） | off / coarse 平均 T ms | coarse/off QPS 变化 | 相邻 pair QPS 比中位数 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 235.949 / 227.773 | 29.462 / 30.445 | -3.46% | 1.00684 |
| 4 | 230.829 / 225.687 | 29.741 / 30.264 | -2.23% | 0.99702 |

pooled 比、30个 pair 比的中位数与三轮中位是不同统计量，不混用。轻量观测偏差较小但不为零，慢样本均保留，无自动重跑、剔除或校准扣减。近 off 条件下的交付结果如下：

| 路数 | 单次 IPC wall / CPU ms | 一波 IPC＋append union / wave ms | union占比（总和之比） | profileEnd→control返回含close ms | 该 gap 与全loop同步IPC＋append重叠比例 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.073 / 0.473 | 17.310 / 35.123 | 49.28% | 3.970 | 77.93% |
| 4 | 1.268 / 0.471 | 20.416 / 35.447 | 57.60% | 4.807 | 81.74% |

S4 每 query 两次 IPC 平均合计2.535ms，但8个协程中的16次同步 IPC 在同一个 loop 上依次执行，一波累计 IPC＋append 达20.416ms。其他协程即使 socket 数据已到达，也要等 loop 返回才能继续；后续 typed 提交因此延后。这解释了为什么“单个 query 自己的 IPC 仅约2.5ms”仍能主导整波吞吐和其他 query 的控制等待。Arrow C++ 即使释放 GIL，执行这个同步调用的 loop 线程也不会因此去调度其他协程。

轻量 gap 是 **profileEnd→原 control 完整返回（含 close/wait_closed）**，不是 writeEnd→header continuation，更不是 kernel ready。81.74% 是逐请求 gap 长度加权覆盖，支持阻塞机制；它不是已证明可消除81.74%的因果效应。

## 4. 响应写回与 profile 尾部的独占分段

以下为全探针 S4/burst8、30测量波次、480 typed＋240 catalog。单位 **µs，wall / 本线程 CPU**。每个请求内分段互斥，和从原 profile end 到 finish mutex 解锁后标记的 tail 严格整数闭合。跨线程允许重叠，不能把所有请求时长求和当 wave。

| 互斥阶段 | typed 平均 | catalog 平均 |
| --- | ---: | ---: |
| runtime 后置快照 | 1.63 / 1.50 | 1.64 / 1.57 |
| IoTDB 后置快照 | 1.30 / 1.19 | 1.23 / 1.19 |
| 响应 write_frame | 11.32 / 11.09 | 18.02 / 13.72 |
| 交付结果／回滚分支 | 1.18 / 1.18 | 1.09 / 1.10 |
| profile JSON 构造 | 62.67 / 53.92 | 116.89 / 91.78 |
| profile mutex 获取区间 | 9.64 / 1.32 | 1.66 / 1.27 |
| profile sink write | 2.55 / 2.60 | 24.54 / 24.66 |
| profile sink flush | 27.71 / 22.46 | 3.95 / 3.89 |
| profile sink unlock | 1.37 / 1.32 | 1.22 / 1.19 |
| 持 profile 锁的其余区间 | 3.98 / 3.63 | 3.48 / 3.42 |
| 请求局部对象析构 | 16.51 / 12.18 | 7.38 / 7.42 |
| task 捕获对象析构 | 9.46 / 9.48 | 9.97 / 10.01 |
| 资源 lease 清理 | 1.18 / 1.19 | 5.40 / 1.20 |
| 资源要求清理 | 1.14 / 1.13 | 1.21 / 1.17 |
| scheduler finish mutex 获取区间 | 21.81 / 1.43 | 150.99 / 1.76 |
| finish 持锁区间至解锁后标记 | 7.52 / 2.55 | 13.09 / 2.73 |
| 边界与探针包装余量 | 20.39 / 14.83 | 14.75 / 14.53 |
| **互斥分段合计** | 201.36 / 142.99 | 376.53 / 182.61 |

typed 响应 write 只有11.32µs，catalog18.02µs，未看到大响应阻塞公共 slot 的量级。profile JSON 是 tail 中较大的 CPU 项，typed53.92µs／catalog91.78µs；profile mutex 获取 wall 为9.64µs／1.66µs，不能解释多毫秒交付等待。这里成功路径的 rollback 区间只是原分支检查，不表示发生回滚。

“mutex 获取 wall”包含锁竞争和线程被抢占等；例如 catalog finish mutex wall150.99µs、CPU1.76µs，不能称锁被持有了150.99µs。write 返回只证明应用 write_frame 返回，不能推断 client 已收到或 owned copy 完成。

## 5. 逻辑执行槽释放与 worker 再次可用必须分开

`task captures`、资源 lease／requirements 均先清理，再持 scheduler mutex 取走 job、设 slot Idle、更新 counters；解锁后才兑现 completion promise、`ready.notify_all()`、销毁 job 并回到下一轮。不能通过提前标 Idle 或提前退款来省掉原语义。

下面仍是全探针 S4/burst8；单位 µs，wall / CPU。释放 bracket 为锁内 SlotIdle→解锁后标记，真实其他线程可重派的边界位于其中；抢占可使 bracket 变宽。

| 操作 | release bracket | promise | notify_all | notify结束→job销毁 | 解锁后→job销毁合计 |
| --- | ---: | ---: | ---: | ---: | ---: |
| typed | 5.11 / 1.28 | 3.59 / 3.37 | 451.17 / 12.63 | 2.40 / 2.22 | 459.31 / 20.49 |
| catalog | 11.88 / 1.53 | 7.64 / 3.71 | 1278.31 / 11.31 | 4.06 / 2.46 | 1292.18 / 19.78 |

**通知尾部不是零。** typed `notify_all` wall451.17µs、CPU12.63µs；catalog wall1278.31µs、CPU11.31µs。在共享 condition_variable 上唤醒 dispatcher 与全部 worker，可能涉及内部同步及调度／抢占；本轮没有内核分解，不能把全部 wall 归为 futex 或 scheduler mutex 持锁。

| 路数 | Ready→Idle／解锁后平均逻辑占槽包络 | WorkerStart→Idle／解锁后运行包络 | WorkerStart→JobDestroyed平均服务包络 | Ready→Idle peak | 每波notify union ms | notify在client同步区间外 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.647–0.648 | 0.605–0.606 | 0.619 | 1 | 0.514 | 0.276 |
| 2 | 0.992–0.995 | 0.741–0.745 | 0.911 | 2 | 6.110 | 2.798 |
| 4 | 1.455–1.460 | 0.894–0.899 | 1.370 | 4 | 10.689 | 4.641 |

平均并发为 clipped interval 积分／wave 总时长，peak 则取最大值；没有用 profile 计时冒充实际 slot。S4从SlotReady算起的逻辑占槽平均 **1.455–1.460**，峰值确实4；从WorkerStart算到释放只有0.894–0.899，后者漏掉Ready等待，不能称完整逻辑占槽。主archive中旧字段`slot_occupancy`／`weighted_slot_active`沿用WorkerStart定义，语义在追加`slot-ready-audit.json`明确纠正，既有raw与archive保持不变。两个结束标记括住可重派边界，但探针位置有有限误差，不声称精确的不可见状态转换时刻。

把释放后的promise／notify／job析构计入观测worker服务周期，WorkerStart→JobDestroyed平均为 **1.370**；它不是active CPU，也不是逻辑占槽。JobDestroyed后尚有局部／TLS scope析构及下一轮锁获取，不能声称它是线程下一次可执行的精确上界。下一job可在前一job通知尾中先进入Ready，故逻辑占槽与服务周期互有重叠，不能相加。

S4 每波 notify union10.689ms，其中6.048ms与 client 同步区间重叠，4.641ms在其外。它是待低扰动验证的通知／交接候选；此处只有全探针证据，绝对量级仍需优化后用低扰动1/2/4复验。`SlotReady→WorkerStart` typed0.665ms／catalog1.306ms含前一个 job 的通知和析构，不能全叫“空闲线程唤醒慢”。追加同pid、slot与worker tid的相邻job关联表明：typed交接延迟有40.58%、catalog至少39.23%与前一job的解锁后通知／析构包络相交，进一步证实逻辑slot空闲与worker进入下一任务之间存在间隔；该比例仍来自全探针。

## 6. FUSE 与 IPC 的包含关系及跨线程重叠

全探针 S4 单次 IPC wall **1.253ms**、client CPU **0.452ms**。FUSE callbacks 的 interval union 约0.303ms／次，绝大部分与 IPC wall 相交，不能额外相加；release 等尾部可能在 IPC 调用返回后结束。

| FUSE callback | 互斥 wall µs | 本线程 CPU µs |
| --- | ---: | ---: |
| Getattr | 171.69 | 107.82 |
| Open | 119.76 | 96.32 |
| Read | 9.59 | 8.37 |
| Release | 2.17 | 1.92 |

Getattr＋Open 是 callback 内主要部分，Read本身仅9.59µs。当前每次新 open 都必须重新取 principal／补充组、绑定精确 result、执行 fresh owner admission 和本次 metadata 费用检查；已有 hot proof 也会重放费用和做最终 head gate。代码中的 owner admission 已经在 bridge mutex 外。不能把现有已做的锁外 admission 再当作新优化，更不能通过缓存旧授权或扩大权限TTL绕过这些检查。

| IPC 内分段 | wall µs | client CPU µs |
| --- | ---: | ---: |
| ipc.batch_iterator | 2.57 | 2.60 |
| ipc.map_enter | 2.27 | 2.37 |
| ipc.map_exit_close | 106.76 | 40.96 |
| ipc.memory_map_open | 717.15 | 90.75 |
| ipc.metadata_copy | 16.58 | 16.68 |
| ipc.next_batch | 18.43 | 18.60 |
| ipc.null_zero | 15.59 | 15.47 |
| ipc.numpy_validity_owned_copy | 47.05 | 47.40 |
| ipc.numpy_value_owned_copy | 26.68 | 26.79 |
| ipc.open_stream | 190.52 | 83.10 |
| ipc.reader_close | 1.95 | 1.98 |
| ipc.size | 4.33 | 4.32 |

IPC阶段调用数不同，`next_batch`含正常batch与StopIteration、每IPC两次；各行调用均值不能直接相加。子分段外的 IPC 包装余量另有85.08µs wall／81.97µs CPU，纳入原 IPC 总额。`memory_map_open` 是 Python API 合并的打开／映射区间，不能拆称独占 mmap 系统调用；解析、缺页、FUSE 服务、关闭、owned-copy和抢占可能交织。wall−CPU 不唯一代表 kernel queue／锁等待。

按完整 wave 做 interval union／intersection 后：

| S4全探针，每波平均 | ms | 包含／重叠说明 |
| --- | ---: | --- |
| wave makespan | 37.598 | 原完整查询时钟 |
| client IPC union | 20.054 | 占wave53.34% |
| client 全同步区间 union | 23.920 | 占wave63.62%；含IPC、JSON、请求构造等 |
| FUSE callback union | 4.851 | 与IPC相交4.824ms，约0.027ms在其外，不能另加整个FUSE时长 |
| profile后至slot释放 tail union | 5.496 | 与client同步相交3.284ms |
| tail在client同步区间外 | 2.211 | 观测集合差，不是可兑现提速 |
| slot释放后 notify union | 10.689 | 与client同步相交6.048ms；不在上一个tail内 |

typed 的 writeEnd→header continuation 平均3.191ms；逐请求覆盖有80.26%与client全同步区间相交，按 gap union 后再求覆盖则为80.50%，两者口径不同。绝大多数该应用间隙与 loop 阻塞重叠，但未测 socket kernel-ready，不能把3.191ms叫独占网络传输耗时。

本轮 kernel inventory 是只读计划，没有启用额外内核探针。当前内核无 fuse tracepoint，若要继续细分调度／缺页／内核排队须另作低扰动验证。本轮已定位到应用级主要串行段，未把无法唯一归属的剩余时间强行分配给内核。

## 7. 优化方案与语义边界（未实施）

### A. 先做服务端定向唤醒，保持现有 API

把当前所有角色共享的 `ready` 拆成 dispatcher 条件变量和每个 worker 的条件变量：提交／执行资源变化通知 dispatcher；派发后仅通知被选中的 worker；slot 完成时按原顺序清理 captures／leases、更新 slot／counters、兑现 promise，再通知 dispatcher。资源释放回调、外部资源10ms有界探测、取消／deadline、shutdown／drain仍有可靠唤醒；shutdown才对全部相关 waiter广播。

这针对已测通知／交接放大，不增加客户端 worker、查询拆分或额外查询执行线程。不能把现有 `notify_all` 机械改为 `notify_one`：共享CV上的不同 worker 有不同slot谓词，唤错人可能让可执行slot继续睡眠。需要验证无丢唤醒、容量／攒批／资源bypass、公平进展、取消及shutdown；保留任务清理和promise顺序，不提前释放lease或宣告Success。

这是现有接口下优先落地的服务端优化，但无法令客户端的同步FUSE调用主动让出loop，不能承诺单靠它恢复四倍吞吐。

### B. 主要结构优化：FS提供有界、可await的二进制结果交付

保留现有注册／JSON发布／FUSE接口，增加显式版本的 result-open/fetch socket 操作，供**同一个loop内已有的query coroutine** await。每query仍依次发现→value1→value2；只交付已计算／已编码的immutable结果，不拆查询、不并行GRIB message，不改IoTDB线程池。服务端用有界非阻塞I/O状态机等write-ready，CPU／buffer／inflight计入原预算。这样等待payload时loop可以推进其他query的响应和下一次提交；Arrow解析与owned-copy的CPU仍存在。

- 每次fetch按新的open边界重新获取真实peer credentials／补充组，并复用fresh owner admission、精确版本／head／source及parent权限检查、本次metadata ledger费用。不要把早先注册时的principal或客户端JSON里的uid当凭据；短namespace gate在授权固定handle后释放，不能跨socket等待持有。
- 传输对象显式拥有固定结果handle、buffer、计费pin、取消／deadline／失败状态。物化完成后可让查询执行资源按原生产者完成边界释放，但resident bytes和transport额外复制必须持续计费；不能持裸指针跨异步handoff，不能借交付pending绕过原accepted-task容量或2GiB上限。
- 现有scheduler future不能在未发送完时伪造Success。新增fetch需区分授权worker完成与完整delivery完成，由有界transfer future负责后者；最终科学T仍到客户端完整owned数组。客户端不读、短读／EOF／reset、cancel与write-ready竞争和shutdown必须恰好一次清理，失败不返回部分成功。
- 现有注册响应write失败的publication rollback保持；独立fetch失败只释放本次handle／buffer，不能retire其他query正在使用的published route。保持原QueryStatus的phase/code/message及归因。
- 首版可用有界binary frame＋client immutable buffer，再交给Arrow；协议校验长度／上限并保留行序、mask、null归零、provenance及owned语义。它可能增加buffer／copy，必须计量，不能声称绕过FUSE就是零成本。

不优先把memfd／SCM_RIGHTS当drop-in：远端FD可dup／fork／转送，server close／ACK／连接EOF不能一般性证明最后引用已关，无法直接保持现有handle release后的精确resident退款语义。先验证可控的socket所有权和预算模型。

### C. 小项：profile、FUSE管理和复制

profile优先只减少同字段JSON构造成本，保留原快照／时间戳、同步write／flush、原best-effort错误和完成边界。异步sink属于后续单独设计：需要有界owned envelope、可见性／flush barrier、shutdown drain及原容量／future契约，不能仅把日志扔给后台就声称保持语义或消除了CPU。当前量级不支持把它排在交付前。

FUSE可在保持fresh admission的前提下优化纯bridge到期索引／管理互斥区；是否是热点仍须针对性证据。已授权immutable handle可提供内部copy-into，省去read临时vector再memcpy，但必须保留EOF、offset、errno、released-handle、精确bytes和resident pin。Read仅约9.6µs，收益上限小，不能据此解释整段IPC。

### 验收顺序

先独立实现A并跑scheduler资源争用／取消／错误／shutdown及现有全量回归，再按原N128、同一loop／相同在途、同预算1/2/4交错复验，至少同时看完整T、QPS、p50／p95、notify／ready-to-start、资源等待及真实worker周期。B另作独立协议改动，增加撤权／head竞态、冷／热费用等价、慢读与背压、传输取消、发布回滚和共享pin测试；先用已编码结果隔离交付，再做同条件正式查询验收。不能用增加客户端worker或把owned-copy移出T来制造收益。

四个查询执行槽不等于四个CPU核，也不会把每个查询的顺序控制／交付链自动分四份。两核预算下仍有同步CPU、FUSE往返和交接成本；本轮不支持“完整T理论应下降75%”或任何确定未来加速比例。

## 8. 证据与可复算入口

- 主诊断 runner：`experiments/fixed_matrix/query_tail_diagnosis.py`；轻量复核：`experiments/fixed_matrix/query_tail_coarse.py`。封印测量source closure不因后置分析修改。
- 主诊断归档：`experiments/cross_version/work/query-tail-diagnosis-20261025/evidence.tar.gz`，SHA256 `2730e81c87537524ad9e8ca1efd96c774f6647e73011ae2536b2cc002d58279c`；分析JSON SHA `c466853aea07d7fad18da6fb2bac7a3deb54a5a906610d80962c96512c49bb84`。
- 轻量归档：`experiments/cross_version/work/query-tail-coarse-20261025/evidence.tar.gz`，SHA256 `fe7fab21777b788f9726c2c53d783274cf9b3ed896677d9d2f83da6cd34a36fc`；分析JSON SHA `fd2cc3b54011c5a3029994dbf6945a8e2dad0171e73dd9f782055e6c54118729`。
- 两stage的 `accept_evidence.py`／`local-evidence-audit.json` 独立核验本地包；主stage的 `derive_overlap.py`、`derive_release.py`、`derive_notify_overlap.py`、`derive_handoff.py`、`derive_slot_ready.py` 及各自audit，是明确标注posthoc的追加复算，位于既有不可变archive之外，SHA由 `report-receipt.json` 绑定。主分析v1→v3及已见数据范围记录在archive的 `analysis-revisions.json`，不伪称预注册统计分析。
- 源码依据：`src/query/core/QueryTaskScheduler.cpp:189`、`src/query/frontend/QueryRegistrationSocket.cpp`、`src/query/frontend/FuseTypedQueryBridge.cpp:176`、`src/query/frontend/TypedResultStore.cpp:125`、`src/main.cpp:2372`。行号及源码SHA见主stage `kernel-plan/code-evidence.json`；本报告不把本地未提交源码当作远端binary身份。

归档均使用显式allowlist，不包含配置备份、凭据、private错误、ELF、scientific原始数据或数据库store。资源whole-window计量包含启动／oracle／profile，不可当成query-only CPU；未见OOM，quota throttle仅窗口内毫秒量级，不能作为四路收益被抵消的主要解释。所有已授权实验均已结束并恢复，无新增常驻任务。
