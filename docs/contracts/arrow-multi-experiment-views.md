# A09：多实验视图与调度隔离

同一个 `QueryRuntime` 可以为固定的 `DatasetSnapshot` 准备多个 `TrainingQueryModule`。每个 `TrainingViewSpec` 明确给出 `experiment_id`、有序输入/目标 channel、L/G/H/S、split、缺失值策略和视图变换。`experiment_id` 只用于资源隔离；窗口及分页身份使用稳定的 `training_view_identity`，由视图的完整语义和快照内容计算，不含进程随机数或实验调度身份。两个实验使用完全相同的视图时得到相同 sample ID；改变变量顺序、窗口形状、统计 artifact 或统计内容会改变身份。`view_id` 仍是可读名称，不能单独作为唯一的语义身份。

首批变换有 `Identity` 和 `Standardize`。标准化必须提供不可为空的训练统计 artifact ID，并对所有选中且去重后的 `(field_id, ordinal)` 恰好给出一份有限的 mean 与正的有限 stddev；输入和目标有效值都执行 `(x-mean)/stddev`。无效值保持原槽位及 validity mask，不做填充。统计顺序不影响视图身份，统计值和 artifact ID 都参与摘要。变换仅在窗口组装得到私有 `WindowSample` 后执行；缓存中的 decoded fragment 不会被修改。未声明的重采样、随机增强、shuffle 均不执行。标准化结果若超出有限 Float32 范围，整次请求失败，不返回部分变换结果。

共享运行时保留 A05 的规范化 fragment key，因此不同实验的重叠窗口可以复用未压缩 tile 或压缩 buffer，不缓存最终窗口结果。一次 `execute` 在 planner/assembler 验证后向 `FairWorkScheduler` 申请请求 lease，按 `experiment_id` 限制同时运行的请求数和保守保留的输出预算。默认每实验最多 2 个请求，按每请求的 `TrainingQueryLimits.max_result_bytes` 保留，累计上限 512 MiB；这些是准入预算，实际输出仍受原请求预算核查。超过预算的请求等待至运行时的 `wait_timeout_ms`，超时返回 `ResourceExhausted`；取消等待只结束该请求。不同实验的请求预算相互独立。

一次执行将窗口分为最多 `window_group_size` 个的组，默认 8、最大 64。每组的预取和组装持有共享运行时的一个调度 turn；同时活跃组默认最多 4 个。队列按有待执行组的实验轮转，某实验有多个等待组时不会占据队列中的多个实验位置。组结束即释放 turn，下一组重新排队；调度只改变内部工作次序，`WindowRequest` 的显式坐标顺序、series/anchor 顺序、worker shard 和分页游标不变。`prefetch=false, window_group_size=1` 是逐窗口 demand-only 对照；默认模式保留有界 lookahead，并仍受 A08 的预取内存回退约束。

缓存、请求和组 lease 的取消彼此独立。一个实验取消或超限不会取消其他实验的等待者或共享 producer；已解码的不可变基础片段可继续复用。A09 的共享证据应同时记录第二视图的 `decode_count` 增量、进程 CPU 时间和 `QueryRuntimeStats.peak_bytes`，而非只比较 IPC 结果或缓存 hit。定向测试在同一 runtime 运行不同变量顺序和窗口形状的标准化/identity 视图，比较值、mask、sample ID、解码次数及两种预取模式；调度测试强制队列竞争，验证轮转、预算与取消隔离。
