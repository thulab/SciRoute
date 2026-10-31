# A06 训练结果模型与窗口语义

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

A06 提供格式无关的 `TrainingQueryModel`。它接收手工声明的独立序列及精确来源，不读取 Arrow/TsFile，也不组装训练值。A07 将提供版本固定的覆盖索引；A08 将按此处的描述符从 A05 缓存取片段、组装窗口。

`DatasetSpec` 包含 dataset ID、snapshot fingerprint 和有序 `TrainingSeries`。每个序列有独立 series ID、一个精确 `(path,version,format)` 来源、可选 batch/row 或 entity 身份、半开 step 范围、字段与 channel ordinal/label/单位及各自覆盖范围。相邻文件、batch 或 row 不自动连成一个序列；A07 只有验证明确声明的 continuation 后才能扩展来源覆盖。channel label 是可核查的绑定，不从 ordinal 猜测。

`TrainingViewSpec` 固定快照、view ID、独立的 experiment ID、输入/目标 channel 的**分别有序**列表、L/G/H/S、命名 split 的输入和目标允许范围，以及 null series、缺失 channel、ragged channel、null value 四种独立的 RejectWindow/Mask 策略。L 与 S 必须大于零，H 为零表示 input-only，所有长度都是**样本个数**。target 的 field ID/ordinal 可与 input 不同。A09 已加入显式 identity 或基于训练统计的标准化变换，详见 [多实验视图](arrow-multi-experiment-views.md)；未声明的填充或增强仍不会发生。

anchor `a` 是目标的首个位置。输入为 `[a-G-L,a-G)`，目标为 `[a,a+H)`；下一候选 anchor 为 `a+S`。候选必须同时满足序列、split 及所选 channel 的覆盖约束。RejectWindow 会跳过不合格窗口；Mask 保留原 step 位置并标记缺失，不删行、缩短 channel 或把间隙后的值左移。源筛选由 DatasetSpec 的精确来源决定；值有效性由 WindowBatch 掩码表达；窗口是否可取由序列、split 和 coverage 判定。普通 typed query 的 residual 行过滤不参与训练窗口枚举。

例如 L=3、G=1、H=2、a=4 时，输入是 `[0,3)`，位置 3 是 gap，目标是 `[4,6)`。一个值为 NULL 的位置仍占据自己的 step。无绝对时间绑定时，结果只保留 step，不编造 timestamp；有明示的 `start_ns + cadence_ns` 或严格递增的逐步 timestamp 时，描述符保留原纳秒值和 timezone。即使 timestamp 不规则，L/G/H/S 仍以样本位置计数。

`WindowRequest` 可以给出半开 anchor 范围，也可以按顺序给出明确的 `(series ID,anchor,预期 sample ID)` 坐标。范围模式懒枚举，先按序列、split 和 coverage 裁剪候选；不会因为范围远超序列而逐一扫描无效位置。显式坐标若不存在或不合格则报错，不静默丢弃。`max_windows` 限制一次游标的交付数，显式坐标数不得超过它。自然顺序先按 DatasetSpec 中的 series 顺序，再按升序 anchor；worker shard 按合格窗口序号取模分配，保持全集不重不漏。sample ID 由稳定 snapshot 指纹、完整 view 语义摘要、series 和 anchor 确定；epoch/seed 随描述符传递，不隐式随机打乱。A09 的实验调度不改变返回顺序。

`WindowDescriptor` 携带 sample/series/view/epoch/shard 身份、输入与目标 step 区间、按 view 顺序的 channel 覆盖、可选 input/target timestamps，以及每个角色和 channel 的精确 source step spans。fragment key 在 A06 为空，留给 A08 的物理覆盖解析填写。`WindowBatch` 是按 sample 组织的有界最终拷贝：每个 sample 的 input/target Float32 值和逐元素有效位按 `[step][channel]` 排列。无效位对应的 Float32 槽只是占位，不构成填充值。构造时核查 shape、角色顺序、窗口数学、掩码与覆盖、时间长度、provenance 和逻辑结果字节上限。它是结果模型，不表示已接入实际训练读取。

`TrainingQueryLimits` 默认最多 65,536 条序列、4,096 个 channel、单窗口跨度 1,048,576 个位置、单批 4,096 个窗口、256 MiB 的逻辑结果数据及 65,536 个 provenance spans。验证发生在实际读取之前；A08 仍须在分配最终数组前执行预算。这里的结果字节限制不等于进程 RSS 限制。

手工序列覆盖了多变量顺序、不同 target 字段、半开边界、gap/horizon、split、input-only 末尾窗口、null/ragged 掩码、规则/不规则时间、独立行、显式样本 ID、分片和极大 anchor 范围。服务器通过 796/796 全量单测，新模型定向通过 10/10 ASan/UBSan 和 10/10 TSan；详细记录见 A06 验证报告（服务器归档：`docs/arrow-a06-validation-20260928.json`）。
