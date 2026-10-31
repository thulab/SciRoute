# ERA5 D1：IoTDB-FS 当前训练供给瓶颈

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-07，针对 [D1 50-step 实验](../reports/era5-d1-50step-acceptance-20261007.md)
启用 ERA5 专用挂载已有的 `IOTDBFS_QUERY_PROFILE_JSONL` 服务端 profile，
并以同一 V0 trace 的前 10 steps（20 样本）做冷解码缓存、原样重复的暖态
供给 replay。相同短 trace 还在本地 F1 GRIB reader 上运行。原始服务端
逐查询记录保存在服务器
`/data_01/zzy/iotdb-fs-exp/era5-grib-baseline/results/profile/d1-q2-bottleneck.jsonl`，
精确汇总见profile JSON（服务器归档：`docs/era5-d1-q2-bottleneck-20261007.json`）。两轮
Q2 各 120 个查询全部成功，trace ID 相同；短 trace 与原 50-step trace
的前 20 个样本 ID 完全相同。profiling 会带来一些额外开销，因此比例用
于定位，50-step wall time 仍以未启用 profile 的正式实验为准。诊断后
已移除 profile 环境变量并恢复同一配置的专用挂载；原始 JSONL 保留，
SHA-256 记录在汇总 JSON 中。

## 已测出的关键路径

50-step 训练的 V0 Q2 样本 fetch 为 92.89 秒，其中 socket 提交至结果
可用 74.14 秒、IPC mmap/Arrow 读取 10.26 秒、客户端 NumPy 转换
8.02 秒；前后向与优化器约 1 秒。纯供给 replay 96.36 秒，接近训练的
99.62 秒中位数，说明当前小模型主要在等待数据。

短 trace 冷态的 19.592 秒样本 fetch 可按**互不重叠的关键路径阶段**
近似分为：

| 阶段 | 20 样本累计 | 占 Q2 fetch | 口径 |
| --- | ---: | ---: | --- |
| 服务端 cursor 取数/解码/裁剪 | 6.367 s | 32.5% | `typed_cursor_execute`；未进一步拆分 worker 解码与 IoTDB 等待 |
| 服务端 Arrow IPC 构建及写入 | 2.470 s | 12.6% | `typed_ipc_encode`，其中数组构建 1.381 s、写入 1.070 s |
| 服务端结果发布 | 4.060 s | 20.7% | `result_publish`，含结果哈希、入库及可能的容量重试 |
| 服务端目录选择及注册授权 | 2.155 s | 11.0% | `registration_catalog` 0.777 s＋`registration_authorize` 1.378 s |
| 服务端其他 | 约 0.390 s | 2.0% | 包括 prepare 0.305 s；与上面组成服务端总计 15.442 s |
| 客户端 IPC 映射/读取 | 2.073 s | 10.6% | socket 已返回后读取 FUSE 结果路径 |
| 客户端值和元数据转换 | 1.871 s | 9.6% | NumPy 列转换、逐通道校验与组装 |
| 客户端其余 | 约 0.205 s | 1.0% | 样本对象及框架开销 |

服务端总计 15.442 秒，与客户端测得的 socket 提交至结果可用
15.558 秒接近；Unix socket 帧传输本身不是主要瓶颈。服务端的
`publish_route`、`query_materialize`、`typed_encode` 都是上述阶段的
**父级计时**，不能再次相加。IoTDB 累计 RPC 时间也可与并行 worker
重叠，不能直接加到关键路径上。

## 根因排序

**第一，结果表示对训练值有 11 倍字节放大。** 每个样本按 3 个时刻
× 2 类文件各提交一次 typed 查询，即 6 次；每个 50-step 视图是 600 次。
通用 GRIB 网格结果的每个像元包含 Float32 值（4 B），同时重复输出
offset、paramId、level、validityDate、validityTime 五个 8 B 元数据列，
基本就是 **44 B/值**。短 trace 的 78,643,200 B 有用值产生
867,342,720 B IPC；正式 50-step 的 393,216,000 B 有用值产生
4,336,713,600 B IPC，均为 **11.03 倍**。短 trace 中 IPC 编码、结果
发布、客户端 IPC 读取与转换合计 10.474 秒，占 fetch **53.5%**。
输出行数为 19,660,800，而实际只有 1,200 个“时次×通道”的气象场
片段；消息元数据本可按片段保存一次。这个放大属于当前通用 typed
结果形状与训练张量需求之间的错位，不是 GRIB 压缩比造成的。

**第二，完整 IPC 结果的发布会在内存压力下反复重试。** 当前
`TypedResultStore` 的默认 resident budget 为 256 MiB。短 trace 中
single-levels 结果为 2.892 MB/次、pressure-levels 结果为
11.563 MB/次。后者的发布均值在前 10 次为 25.24 ms，在末 10 次
为 77.14 ms；出现结果缓存压力缓解的查询，冷轮为 40/120、暖轮为
58/120。`TypedQueryService` 在容量不足时释放一个可回收 bridge 条目
后重试；`publish_retryable` 每次重试均重新计算完整内容 SHA-256。
这与发布耗时上升一致，但现有 profile 没记录每次请求的重试次数，
不能把 77 ms 全部精确归因于哈希。暖轮 cursor 从 6.367 秒降至
5.043 秒，发布却从 4.060 秒升至 5.018 秒；最终客户端 fetch 仅从
19.592 秒降至 18.598 秒，缓存收益被部分抵消。

**第三，查询粒度放大了目录与授权工作。** 120 个 typed 请求触发
120 次注册目录选择、120 次源授权、240 次父目录授权元数据检查；
profile 中共有 480 次 IoTDB 元数据查询。对应的目录选择＋注册授权
累计 2.155 秒，约每样本 108 ms。服务端也为每个 typed 路由准备和
发布完整结果。减少每样本 6 次控制往返，可同时削减这些固定成本和
路由表压力；仅增加解码线程不会消除它们。

**第四，GRIB 全场解码与缓存表示仍有成本，但不是当前 Q2 相对 F1
最主要的额外 wall time。** 短 trace 中 Q2 发生 900 次 payload 调用，
请求/返回约 1.869 GB 压缩 GRIB 数据；本地 F1 解码 820 条消息，
读取约 1.703 GB，F1 fetch 为 6.738 秒（其中解码 6.106 秒、源读取
0.573 秒）。Q2 的服务端 cursor 关键路径为 6.367 秒，量级接近 F1
整体 fetch；但执行方式不同，不能据此声称 IoTDB 读取与本地文件一样快。
Q2 缓存的是 canonical Float64 全场，F1 缓存 Float32 全场，同为
4 GiB 时 Q2 可驻留的气象场更少；900 与 820 的差值符合额外驱逐的
方向，但现有 profile 没有 GRIB cache miss/eviction 计数，不能完全
归因于容量。两侧都必须为了 128×128 区域先解码全球 GRIB message，
这是源格式与区域窗口不匹配的共同成本。IoTDB 的 19.346 秒 payload
累计 RPC 时间跨并行 worker 重叠，不能当成 19.346 秒的串行阻塞。

## 优化顺序及需要验证的指标

1. **训练结果采用片段描述＋稠密张量。** 每个 `(time, channel)` 只写
   一份来源/offset/时次描述，值和 mask 连续输出；保留精确版本与逐
   片段 provenance。先在相同 trace 下记录 IPC 字节、server encode、
   publish、客户端读取/转换与总 fetch。通用 GRIB typed 查询可以保持
   当前语义，由训练 adapter 输出紧凑训练结果。
2. **跨时次/文件合批并及时释放 route。** 目标是把一个样本的 6 个
   控制请求合为有界训练请求，再按多个重叠窗口共享解码后的基础场。
   同时量测注册目录/授权次数、typed 查询数、物理 payload 次数和
   每个 view 的公平性；完整 epoch 前必须解决路由寿命，不靠继续
   提高 8,192 的上限。
3. **消除发布重试的重复整流哈希。** 一次编码计算一次内容 ID，
   在持有可回收结果预算后发布，或一次释放足够字节；分别测发布
   retry 次数、结果存储驻留字节和 `result_publish` p50/p95。增大
   resident budget 可作诊断消融，不能代替生命周期修复。
4. **最后再调 GRIB cache 与物理 I/O。** 加 native-field cache 的
   hit/miss/eviction、实际解码耗时、payload 批量范围数和 worker
   等待计时；在相同总内存下比较 Float64/Float32 表示及多范围读。
   原始精度与插值语义须保留，不能直接把所有缓存改成 Float32。

当前 Q2 是 Python 训练 harness 调用通用 GRIB typed 查询，再在客户端
组装窗口；上述开销并非 IoTDB-FS 存储层无法优化的固有代价。F2
本地共享解码服务尚未实现，不能根据 F1 与 Q2 的差距断言共享缓存
机制的相对价值。
