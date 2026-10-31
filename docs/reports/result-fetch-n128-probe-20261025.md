# 有界异步 result-fetch：N128 1/2/4 路性能探针

2026-10-25。固定语料的采集、逐查询正确性、独立复算及服务／资源恢复均已通过。

四路 FETCH 相对同产品 FUSE 的 pooled QPS 变化为 **+12.46%**，完整查询平均 T 变化为 **-8.72%**，p50 变化为 **-7.34%**。两轮四路 QPS 变化分别为 campaign 0：+6.66%, campaign 1：+18.88%。这些数值属于缩减实验探针，不能据此声称统计显著、尾延迟保证或理论四倍扩展。

## 实现与语义

新增显式 `result_fetch` v1：请求指向已发布的确切 immutable result，返回有界 JSON header 和原 Arrow IPC 字节。原注册／发布及 FUSE 接口继续可用。

- 在公共查询 worker 中重新检查可信 peer credentials、route/result 精确身份及当前 read-context admission；metadata fee／预算继续按新 open 收取。fetch 不复用上一次发布的授权。
- 授权和调度仍经公共查询池，随后一个非阻塞 I/O reactor 推进已编码结果；reactor 不执行 source 查询或 GRIB Message。新增 fetch 不生成原 query profile，保留每波 8 catalog＋16 typed 的 24 条 profile。
- 默认最多 16 个 delivery reservation，排队前占用，交付成功／取消／断连／失败才释放；每个传输最多 64KiB 暂存，header 最大 4KiB，连续 3s 无发送进展则中止。16 个暂存 buffer 的上界是 1MiB，不计有界结构体与内核 socket 缓冲；这些内存仍在原资源组限制内。
- 转交时保留确切 result handle、连接和 completion lifetime；原 result store 驻留预算在最后 handle 释放前仍计费。公共执行槽结束不等于 bytes pin 或 delivery reservation 提前退还。
- fetch 失败只释放当前交付，不回收其他读者共享的 published route；原注册响应失败的回滚、取消、错误和停止时释放路径继续有效。服务端 send 完成与客户端 owned 完成分别处理。
- 客户端沿用一个 asyncio loop 和原有 8 个查询协程，逐查询顺序仍为 catalog → typed1 → fetch1／owned1 → typed2 → fetch2／owned2。没有增加客户端 worker、线程、查询数量或查询内拆分。Arrow decode 和 owned copy 仍在该 loop 执行。

以上边界来自实现与正确性验收；性能日志中的客户端 pending 数量不能代替服务器 reservation gauge。

## 固定条件与时钟

沿用[飞书 4.3 大表格](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg)已确认 revision983 的封印 GRIB 组：N=C=128、V1、D4、S0、latest、K=2、seed20261013，每完整查询 owned values／masks 为 40960B。N=128 是候选文件范围，最终仍执行两个选中单位。未重新发布语料、重建 index、生成新 case 或绑定新位置。

FUSE／FETCH 两臂运行**同一个新产品 binary**，只切换结果交付。源码增量单独绑定先前接受的定向唤醒 4c 产物；此次性能对照没有再运行旧 4c 产品，所以不直接证明新代码在原 FUSE 路径上相对旧产品无性能回归。

公共 worker／max batch=1、2、4；owner reader pool=4、queue=64、fragment pool=4。CPU2/3、父组两核 quota、query 叶组 2GiB，cpuset partition=root；DB heap15232MiB／direct2176MiB，其他服务与 support roles 按原协议约束。没有清缓存或施加新背景负载。

两轮配置顺序 1/2/4、4/2/1；各配置 FUSE／FETCH 相邻独立挂载，次序在下一轮反转。每 mount 1 warm＋2 measured、每波 8 查询：正式 288 行／36 波，其中测量 192 行／24 波；每 transport／configuration 的 pooled cell 有 32 条测量查询／4 波。N8 仅 S4 每臂一波，另 16 行，合计 14 次 mount。保留全部样本，无自动重试、替换或剔除。

T 从查询入口 t0 到最终 owned values／masks 完成 t2；t1 为发现完成，保持 T=d+q。包括查询排队、执行、注册和全部交付，fetch 请求与 decode／copy 都在 T 内。profile harvest、oracle 和资源采样在 T 外。波次 makespan=max(t2)−min(t0)，QPS=完成查询数／所有测量波次 makespan 之和，不把单查询延迟除以 worker 数。

## 完整查询结果

下表为两轮 pooled 值，所有时间单位 ms；p95 仅是这 32 条样本的描述性分位数。

| 公共路数 | QPS FUSE→FETCH | QPS 变化 | 平均 T FUSE→FETCH | p50 FUSE→FETCH | 描述性 p95 FUSE→FETCH | 平均 wave FUSE→FETCH |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 199.426 → 240.433 | +20.56% | 35.223 → 31.151 | 36.401 → 31.083 | 46.044 → 34.088 | 40.115 → 33.273 |
| 2 | 234.588 → 266.872 | +13.76% | 30.015 → 27.465 | 30.399 → 26.840 | 33.536 → 31.499 | 34.102 → 29.977 |
| 4 | 234.994 → 264.270 | +12.46% | 29.380 → 26.819 | 29.159 → 27.020 | 32.837 → 31.321 | 34.043 → 30.272 |

每轮配对结果如下，每行各 16 条查询／2 波：

| 路数 | campaign | QPS FUSE→FETCH | QPS 变化 | p50 ms FUSE→FETCH | p50 变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 0 | 246.905 → 251.525 | +1.87% | 28.054 → 30.032 | +7.05% |
| 1 | 1 | 167.262 → 230.279 | +37.68% | 40.876 → 32.316 | -20.94% |
| 2 | 0 | 228.097 → 245.928 | +7.82% | 31.159 → 29.596 | -5.02% |
| 2 | 1 | 241.460 → 291.715 | +20.81% | 28.877 → 25.158 | -12.88% |
| 4 | 0 | 235.892 → 251.594 | +6.66% | 29.958 → 28.525 | -4.78% |
| 4 | 1 | 234.102 → 278.291 | +18.88% | 28.568 → 25.054 | -12.30% |

四路两轮 QPS 比值的中位变化为 +12.77%，p50 比值中位变化为 -8.54%。此统计先逐轮求比再取中位数，和 pooled 比值分别报告；两轮中位数并不能消除顺序和波动影响。

## 查询执行与交付重叠

原 catalog／typed profile 的包络如下。积分平均并行度=各执行包络时长之和／wave makespan；它排除响应、profile 尾部及 slot 释放，不是完整执行槽占用或物理 CPU 核数。

| 路数 | transport | profile peak | profile 积分平均并行度 | 每完整查询 profile 内均值 ms |
| --- | --- | ---: | ---: | ---: |
| 1 | FUSE | 1 | 0.604 | 3.030 |
| 1 | FETCH | 1 | 0.518 | 2.153 |
| 2 | FUSE | 2 | 0.644 | 2.747 |
| 2 | FETCH | 2 | 0.806 | 3.019 |
| 4 | FUSE | 4 | 0.839 | 3.570 |
| 4 | FETCH | 3 | 0.819 | 3.100 |

FETCH 的客户端等待与 owned 工作由原 value call 中的低开销时间戳独立复算：

| 路数 | fetch pending peak | pending 积分平均 | 至少一个 pending 的 wave 占比 | loop owned peak | decode＋append 的 wave 占比 | 每query fetch 等待均值 ms | 每query decode＋append 均值 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8 | 2.284 | 55.17% | 1 | 8.90% | 9.499 | 0.370 |
| 2 | 8 | 2.087 | 58.65% | 1 | 11.37% | 7.819 | 0.426 |
| 4 | 7 | 1.812 | 57.89% | 1 | 11.60% | 6.857 | 0.439 |

pending 从客户端开始 fetch 到字节接收／连接关闭完成，包含连接、公共池排队、fresh admission、socket 等待及事件循环排队。因此 pending 的峰值和平均数说明多个客户端交付请求在途，**不代表 reactor 同时发送的 socket 数、同时运行的 query 数或 CPU 并行度**。decode／append 峰值不超过 1，符合原单事件循环；这部分 owned 工作继续串行。FUSE 原客户端没有同类新增时间戳，不能把其缺省值 0 当成无交付耗时。

## 公共池扩展与传输优化分别计算

相同路数 FETCH/FUSE 衡量此次交付路径变化；同一 transport 的 S2/S1、S4/S1 衡量公共池扩展。两者不是相同问题，也不能当成独立收益直接相乘。

| transport | 路数比 | campaign0 QPS 变化 | campaign1 QPS 变化 | 配对 QPS 变化中位数 | pooled QPS 变化 | 平均 wave 变化 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| FUSE | 2/1 | -7.62% | +44.36% | +18.37% | +17.63% | -14.99% |
| FUSE | 4/1 | -4.46% | +39.96% | +17.75% | +17.84% | -15.14% |
| FETCH | 2/1 | -2.23% | +26.68% | +12.23% | +11.00% | -9.91% |
| FETCH | 4/1 | +0.03% | +20.85% | +10.44% | +9.91% | -9.02% |

四路完整 T 的加法均值分解如下，单位 ms／query，包含三次原控制调用合计。只相加均值，不相加分位数。

| 分项 | FUSE | FETCH | FETCH−FUSE |
| --- | ---: | ---: | ---: |
| 排队／派发至 profile 前 | 5.253 | 6.785 | +1.532 |
| 原 query profile 执行 | 3.570 | 3.100 | -0.469 |
| 原控制调用其余区间 | 13.166 | 8.128 | -5.038 |
| 原控制调用外 | 7.391 | 8.805 | +1.414 |
| **完整 T** | **29.380** | **26.819** | **-2.561** |

queue counter 包含排队／派发／交接至 profile 前；control residual 含传输和事件循环交错。这里的“原控制调用外”对 FUSE 包含原 IPC／owned，对 FETCH 包含新 fetch control＋传输＋owned。它们都不是独占锁、FUSE 或内核时间，不能从 residual 单独归因到某个系统调用。fresh fetch admission 没有新增 query profile，因此其 CPU 和等待也不能算成已量出的原 profile 执行。

## 正确性与恢复证据

- 新产品全量 `2010` 项 unit tests 通过，必需回归列表、XML、unit binary、产品与 frozen source SHA 均在 binding 中核验；同一产物运行两臂。功能正确与性能收益分别验收。
- 正式 288 行＋资格 16 行全部通过原 oracle、版本／mode／owned 字节及 T=d+q 检查；每波 24 原 query profiles，runtime workers／queue／batch 和 route 绑定一致。
- 离线协议 6 组 tests、独立 clock／sweep／fetch-owned 算术 16 项通过；独立程序重算精确矩阵、物理顺序、波时钟、QPS、分位数、配对比及包络积分，与 runner 统计一致。
- 封印 source closure 始末一致；原 raw／head／journal／index 保持；服务恢复验收 complete，原线程 affinity 已核验 292 条，原 cgroup／配置恢复，临时实验资源清理完成。
- 测量 wave 前后父组 throttled 增量合计 0.000ms；这些样本的 query memory.current 最大 118.04MiB（上限 2048MiB）。整个观察窗口 1063.27s，包含启动、挂载、oracle 和恢复，不能把其平均资源使用当作请求独占指标。资源样本 OOM 门禁已通过。

产品 SHA256：`4e10d45ab1b64f63d6f397f42970f8420f845781b3207a3a2007ac9b6bfb4545`。接受的旧定向唤醒 SHA256：`4caa446dc2dadbec9227fbdafd9062425a3ae56910c05c96352e341e98e40cb7`。新生产源码差异为：`cmake/QueryModules.cmake`, `include/iotdbfs/query/frontend/BoundedResultSender.h`, `include/iotdbfs/query/frontend/QueryRegistrationSocket.h`, `src/query/frontend/BoundedResultSender.cpp`, `src/query/frontend/QueryRegistrationSocket.cpp`。

独立分析 SHA256：`540a61719e0073642b01d3abe44ec6a9ec3c927cd7f0ef21517dffbcbe900924`。归档 SHA256 `60e828c4536c0b4282aed8d30e3c5696c07d8b92c5ff2729247bd2d1151f3236`；本地归档核验已通过。

原始 query／wave、计划、产品身份、固定输入证明、服务／资源恢复、独立分析和本地归档 receipt 保存在 `experiments/cross_version/work/result-fetch-probe-20261025/`。本报告由封印前准备的 `write_report.py` 读取已验收数据生成，不修改采集源码或原始数据。

## 结论边界

本次直接回答同一产品下有界异步交付相对原 FUSE 交付的端到端表现，并分别列出公共池 1/2/4 路扩展；并行指标区分原 query 执行、客户端 fetch 在途和单 loop owned 工作。只跑两个 campaign、每轮两个测量波，暖缓存与短程波动仍存在。不能据此推断冷盘性能、其他格式／大小、混合负载、查询内并行、理论 75% 延迟下降或服务器 CPU 核级并行。取消、预算、授权与资源释放的功能保证依赖对应正确性检查，不由吞吐值反推。
