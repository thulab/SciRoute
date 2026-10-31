# A08：按需 Arrow 训练窗口组装

`TrainingQueryModule::prepare` 固定一个 A07 `DatasetSnapshot`、coverage index 和训练视图，并校验进程/backend 的运行时绑定与稳定内容指纹；它不重开全部 shard。视图中的 `snapshot_fingerprint` 和由其生成的 sample ID 可跨进程复用，但每个进程须重新准备自己的快照与索引。`execute(WindowRequest)` 只生成本次请求的窗口：显式坐标维持调用方顺序；anchor range 按 snapshot 中 series 首次出现的顺序、stride 与 worker shard 确定性枚举。规划器每次只物化一个候选；由于必须先判断窗口资格才能确定全局 worker ordinal，分片请求的单页扫描上限为 `max_candidate_windows × shard_count`（溢出时饱和到 `uint64` 上限）。达到上限时返回已组装的部分 batch 及下一候选的游标，下一页继续扫描；默认全 `RejectWindow` 时先由 coverage 推导可行 anchor 区间，跳过大空洞。输入和目标分别检查 split，窗口不能从相邻但不同 `series_id` 的文件拼接。每个实际触及的 shard 在首次打开时复核精确版本和 manifest；未使用的 shard 不影响该次读取。

`DatasetCoverageIndex::lookup_overlaps` 返回与所需 step 区间相交的真实 shard/batch/row/channel 覆盖片段，允许部分覆盖而不把空洞压缩掉。规划器将同 series、同 step 的变量分片按原 channel ordinal 对齐；`WindowChannelSpan.available_ranges` 保留非连续可用位置。未覆盖 step 由 `ragged_channel` 处理，未绑定 channel 由 `missing_channel` 处理，null 父 List 由 `null_series` 处理，null 子 List 和叶子值由 `null_value` 处理。`Mask` 保留原输出槽位和零有效位；`RejectWindow` 在范围枚举中跳过窗口，在显式坐标请求中报不合格错误。绝对时间按已排序的 step/time 片段定位：A07 允许缺口两侧使用不同时间模式、时区或 regular origin，窗口各自使用触及片段的时间定义；一个窗口若触及不兼容的时间定义，或其绝对时间跨越没有声明时间的缺口，则跳过。显式时间戳按片段二分定位，避免每个 step 扫描整条序列。

anchor range 查询返回 `WindowBatch.continuation()`；非空时，将其原样放入下一次 `WindowRequest.continuation`，保持原 `anchors`、epoch、seed、worker shard 及视图不变，`max_windows` 可调整。游标记录 series 顺序位置、下一 anchor 和分片前的合格窗口计数，因此跨 series、跨 4096 条 batch 上限和解码阶段拒绝的窗口均不会重复或跳过。游标以稳定快照内容指纹及完整视图/选择条件生成的摘要校验，不能用于不同快照、视图、范围或 worker；新进程重新准备相同快照后仍可继续。页恰好填满时可能返回一个指向终点的游标，此时下一页为空且游标为 null。显式 coordinates 不分页；A06 的 descriptor-only `WindowCursor` 明确拒绝此游标。

组装器按窗口实际需求才打开 shard、读取 layout 并将 step 通过 List offsets 映射为 leaf 位置。默认以最多 8 个候选构成有界 lookahead；A09 可将组大小设为 1–64，并在需求读取模式关闭预取。预取将同一物理 tile 内真正重叠或相邻的 leaf 需求合并；远隔区间保持分离。lookahead 的固定 lease 集合不得超过 runtime 内存预算的四分之一，超过则回退为逐窗口流式读取。所有 shard 的 provider 共享 `QueryRuntime`，因此 lookahead 之外的重叠窗口和不同请求仍复用规范化的未压缩 tile 或完整压缩 buffer。组装后立即释放 lookahead leases；返回的 `WindowBatch` 只持有有界拷贝，不持有 cache fragment。

**训练取数修订（2026-09-29，已实现）：** [训练批量范围设计](../knowledge/arrow-training-batched-range-design.md)在当前窗口组内按规范 tile 合并需求，通过共享 cache 批量认领缺失片段；同 source/version 的重叠或相接字节范围精确合并，不连续范围由有界多范围请求读取。超出 lease/条目预算时按窗口和 tile 波次读取、组装、释放，不退回逐 tile SQL。普通 Arrow typed 查询保留单片段接口；训练和 typed 查询仍共享规范 key、decoder 和公共 cache。该优化不改变上述 A08 窗口语义；[Electricity 50-step A/B](../reports/public-training-electricity-tilebatch-20260929.md)记录了调用数和当前随机采样下的有限 tile 复用。

每个输出位置按 `[step][channel]` 排列，input 和 target 各自拥有 Float32 values、字节有效位、可选时间数组及 timezone。descriptor 的 provenance 记录每个实际贡献的 source、batch、row、step 子段和 provider 规范 fragment key；null 或显式缺失的覆盖段保留 source provenance，但没有物理 fragment key。请求级预算跨 shard 累计 layout bytes、逻辑 payload bytes、范围数、输出字节与 provenance 数；其中逻辑 payload 与数据范围在窗口实际使用时计数，解码后被 `RejectWindow` 丢弃则退回该窗口的计数，lookahead 预取不预扣；已打开 source 的一次性 layout 读取仍计入请求。共享 runtime 另行约束实际 I/O、解码与缓存内存，因此拒绝窗口不会抹掉已经发生的物理工作。物理执行可先获取 lookahead 中的 fragment，但结果始终按请求顺序返回。

A09 已在组装后的私有窗口上执行明确的视图变换，并由共享 runtime 按实验调度有界窗口组；详见 [多实验视图与调度隔离](arrow-multi-experiment-views.md)。IPC 编码仍属于 A10。当前 API 同步执行一个有界 minibatch，后续训练器可按 anchor range 或显式坐标连续提交 minibatch。

`TrainingQueryModule::execute(request, cancellation)` 可接收线程安全、可复制的 `CancellationToken`；调用方持有其副本即可取消运行中的规划、lookahead、cache 等待、解码和组装。取消返回 `Cancelled`，不返回不完整 batch。一次成功的执行或 provider 生命周期结束不会反向取消调用方的 token；底层同步存储读取仍需等单次读取返回后才能响应信号。
