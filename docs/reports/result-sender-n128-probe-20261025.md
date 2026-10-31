# 服务端合并发送与双路发送器 N128 探针

本次固定公共查询池 4 路，依次隔离合并发送代码增量和发送器路数。主性能关闭新增服务端 trace；分段与 syscall 诊断使用独立挂载，不混入主性能统计。

## 实现与语义

合并发送使用不可变结果的带生命周期 pin 的只读 slice，以 `sendmsg/iovec` 同次提交长度前缀、JSON header 和 IPC body；移除原 body 暂存 vector 拷贝，保留部分写偏移推进。发送分片支持 1/2 路，每个连接固定归属一个 reactor，各自排队并定向唤醒。

全局 delivery reservation 仍为 16；单次发送最多 64KiB，3 秒无进展超时。每次 fetch 的 fresh admission、确切版本和 result identity、元数据／驻留预算、取消／断连／错误处理与发布 rollback 保持原边界。未增加客户端 worker，未引入查询内并行。功能保证由对应单测与集成校验提供，不由吞吐值证明。

## 固定条件与时钟

沿用 4.3 固定 FM1 GRIB N128（C128/V1/S0/latest，K=2，owned 40,960 字节）语料、case、index 和 oracle。一个原有 asyncio loop、8 个既有查询协程，每条仍依序执行 catalog→typed1→fetch1→owned1→typed2→fetch2→owned2。公共查询池 4、owner reader 4、queue64、batch4，父资源组两核 CPU（2–3，quota200000/100000）、查询内存 2GiB，未重置缓存或重新发布语料。

OLD 是前次接受的 result-fetch 单路发送产品；NEW1 是新合并发送产品单路 sender；NEW2 与 NEW1 使用同一新产品，sender=2。两轮顺序分别 OLD/NEW1/NEW2 和 NEW2/NEW1/OLD，每次独立挂载 1 warm＋2 measured，正式 144 行／18 波，其中测量 96 行／12 波；N8 三臂资格另 24 行。另 NEW1/NEW2 各独立 1 warm＋1 diagnostic，32 行，共 11 次挂载、200 条查询，保留全部固定样本，无重试／替换／剔除。

完整 T 从客户端 query entry 到 Float32 值与 mask 全部成为 owned 输出，T=d+q；oracle 和 profile harvest 在时钟外。QPS=总测量查询数／总测量波跨度，wave 从最早 entry 到最后 owned 输出，不将延迟除以路数。

## 主性能（服务端 trace 关闭）

每臂 32 条测量查询／4 波。pooled QPS 与 pooled 延迟如下；描述性 p95 不代表尾延迟保证。

| 臂 | sender | QPS | 平均 T ms | p50 ms | 描述性 p95 ms | 平均 wave ms |
|---|---:|---:|---:|---:|---:|---:|
| OLD | 1 | 286.720 | 25.518 | 25.532 | 27.614 | 27.902 |
| NEW1 | 1 | 282.865 | 25.268 | 25.125 | 27.959 | 28.282 |
| NEW2 | 2 | 270.090 | 26.610 | 26.861 | 29.848 | 29.620 |

| 比较 | pooled QPS 变化 | 平均 T 变化 | pooled p50 变化 | 两轮 QPS 变化 |
|---|---:|---:|---:|---|
| OLD→NEW1（合并发送代码增量） | -1.34% | -0.98% | -1.59% | c0: -0.64%, c1: -2.07% |
| NEW1→NEW2（同产品双路 sender） | -4.52% | +5.31% | +6.91% | c0: +1.31%, c1: -9.89% |
| OLD→NEW2（整体增量） | -5.80% | +4.28% | +5.21% | c0: +0.66%, c1: -11.76% |

OLD→NEW1 包括合并发送及新代码中关闭的诊断支架，不能把该比值解释为单个 syscall 的独立因果收益；NEW1→NEW2 在相同产品和查询池条件下只改变 sender 数。两轮各两波属于缩减描述性探针，短程波动与顺序漂移仍可能影响结果。

## 分段诊断（独立 trace 挂载）

以下每臂只使用第二波 8 条查询／16 次 fetch，第一波保留为 warmup。每个挂载 32 条 fetch trace＋1 条 summary，均须零丢失；send spans 的计数、字节数、短写与 EAGAIN 独立闭合。下表单位为每 fetch 的平均 μs。

| 分段 | NEW1 μs | NEW2 μs |
|---|---:|---:|
| accept→transport worker | 343.084 | 147.816 |
| transport worker→完整请求帧 | 201.737 | 629.136 |
| 完整请求→submit | 10.410 | 12.639 |
| submit→公共 worker | 618.274 | 567.927 |
| worker→admit 开始 | 15.900 | 22.761 |
| fresh admission | 22.331 | 30.426 |
| admit 结束→enqueue | 0.804 | 1.597 |
| enqueue→reactor dequeue | 167.293 | 116.691 |
| dequeue→首次成功 sendmsg 返回 | 25.696 | 21.516 |
| 首次→末次成功 sendmsg 返回 | 0.000 | 0.000 |
| 末次返回→sender 引用释放 | 6.399 | 11.237 |

| 发送指标 | NEW1 | NEW2 |
|---|---:|---:|
| 每 fetch 平均 send 调用次数 | 1.000 | 1.000 |
| 每 fetch 平均发送线程 CPU μs | 10.967 | 13.039 |
| 每 fetch send syscall wall 总和 μs | 9.722 | 11.585 |
| 调用区间峰值重叠 | 1 | 1 |
| 调用区间平均重叠 | 0.005658 | 0.005758 |
| EAGAIN 次数 | 0 | 0 |
| 短写次数 | 0 | 0 |
| 各 shard 完成 fetch 数 | {"0": 16} | {"0": 8, "1": 8} |
| 发送返回取时晚于客户端 fetch 结束次数 | 0 | 0 |
| sender cleanup 晚于该查询 owned 完成次数 | 0 | 0 |

分段按 accept→cleanup 相邻时间戳相减，含对应的排队、调度和计时开销。`dequeue→首次成功返回` 包含首个 sendmsg 的执行时间；first/last 表示成功返回后的取时，单调用时包络可为 0。send syscall wall spans 也可包含抢占，发送线程 CPU 包含微小 clock 开销；同 shard 调用不重叠、跨 shard 调用区间重叠均不等于已证明物理 CPU 同时执行。新增两个 sender 仍共享原两核 quota。

客户端 fetch 等待还包含连接、授权、公共池排队、socket 和事件循环等待。成功 sendmsg 后 sender 可能被抢占，客户端可先读完，故不强制返回后取时早于客户端 receive 完成。cleanup 只表示 sender 自身持有的 lifetime／handle／reservation 引用释放；公共任务 capture、bridge 缓存可能仍持引用，不能据此声称全局驻留预算或执行槽最终归还。

OLD 未开启此次新增 trace，不能从该表声称实测 OLD→NEW 调用次数或发送 CPU 降幅。移除 body 暂存拷贝是代码事实，完整查询收益使用上方未开启 trace 的对照数据。

## 验证与证据

三臂 N8 与全部 N128 主性能／诊断查询通过原 oracle、mode、owned 字节和完整时钟检查；精确矩阵、物理顺序、路由／profile、QPS／分位数及 overlap 由独立程序重算。原 scientific heads／journals／indexes 保持，服务、affinity 与 cgroup 资源恢复并单独核验。本地解包逐文件 SHA 校验及原始时钟复算已通过。

OLD binary SHA256：`4e10d45ab1b64f63d6f397f42970f8420f845781b3207a3a2007ac9b6bfb4545`。NEW1/NEW2 binary SHA256：`5862f9335e413503d772199e0d87156e658b3db9e01549ed25b34f5ed10335bb`。

OLD source manifest SHA256：`445cde0d237b86b9becbdc35fb96d7c29747fcd645a476eb78c7e349c31055f4`；NEW source manifest SHA256：`47a9241c058dcacce42447862cba800949c5280610559c8a488d58d6cc867242`。

证据包 SHA256：`dd6d6842c2535cae30c6795f5c8647db71a15f637f9d257455be9201fa894dd1`；独立分析 SHA256：`fd3871db6fc8268ce267f631b59ca8a3ae573b66fbe1328c6467934e2faaae82`。

本次结果限于固定暖缓存 GRIB N128、小型 IPC payload 和既有 8 查询负载；不能外推冷盘、其他格式／大小、混合负载、查询内并行、统计显著性或理论四倍收益。
