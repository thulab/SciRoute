# Arrow 训练 adapter 的批量范围读取设计

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

状态：2026-09-29 已实现训练窗口组的有界合批，并完成 Electricity 50-step A/B 复验；完整 A13 稳态和独立消融仍待完成。本设计针对 `TrainingQueryModule` / `WindowAssembler` 的多变量窗口供数；普通 Arrow typed 查询继续使用现有 `ArrowFragmentProvider::get()`、固定 tile 和单范围 `QuerySource::read_payload()` 路径，行为与现有 GRIB typed 查询的按物理片段取数方式一致。两条路径共用同一规范 Arrow 片段身份、decoder 和 `QueryRuntime` 缓存，不形成训练专用的另一份解码结果缓存。

## 动机与三种粒度

存储的 Arrow stream、record batch、通道叶子、解码 tile 和训练窗口有不同的边界。Electricity 当前为 channel-major 布局；`L=96,H=24` 时，一个通道的窗口只使用 480 B Float32，一个未压缩规范 tile 有 4,096 个值（16 KiB），370 通道的完整 input/target 窗口却有约 173 KiB 有效 Float32。因而不能用统一的 `message > tile > window` 大小关系描述多变量窗口。GRIB message 是格式天然的解码单位；Arrow tile 是人为固定的可复用缓存单位。**缓存片段与一次物理读请求无需一一对应。**

分段测量（服务器归档：`docs/public-training-stage-profile-20260929.md`）中的 51 个 Electricity 训练 batch 发出 35,671 次 payload 范围调用，累计 121.915 s，占服务端 142.034 s 的 85.84%；每次调用平均 3.418 ms。loader 除 payload 读取外累计 0.449 s。请求返回 584,433,664 B，而有效 Float32 输出为 18,115,200 B。固定两窗口冷探针需要 740 个 tile/读取调用，暖探针在片段驻留时不再读取。三 tile/通道时，341 通道与 layout 共占满默认 1,024 条目；342 通道即发生驱逐和重复读取。这些数字说明当前优先级是减少物理调用与重复加载，同时控制读取放大；它们不能外推成新的批量实现的加速比。

## 模块边界

```text
TrainingQueryModule / WindowPlanner
  -> WindowAssembler 训练 lookahead（按窗口组收集需求）
  -> ArrowFragmentProvider::get_batch（训练专用取数入口）
  -> 共享 Arrow 片段描述/decoder + QueryRuntime 缓存的批量认领入口
  -> QuerySource 多范围读取能力（保持源/版本能力边界）
  -> StructuredBlobReadHandle / backend 的有界多范围传输
  -> 各 tile 的规范解码对象进入现有共享缓存
  -> WindowAssembler 按原 sample/通道/step 顺序拷贝并应用视图

普通 Arrow typed 查询 -> ArrowFragmentProvider::get() -> 原单范围读取
GRIB typed 查询 -> 原有 message 取数路径
```

训练 adapter 只认识窗口需求、Arrow layout 和物理片段描述，不构造 IoTDB SQL。公共多范围读及批量 cache 认领**仅由训练 adapter 调用**；普通 typed 接口和 GRIB adapter 未切换。批量路径与标量路径共用规范 key 规则、`ArrowRangeReader` decoder 和 cache；跨路径 cache 命中、null bitmap 与压缩完整 buffer 由回归测试约束。缓存 key 保持 namespace、精确 source/version、layout/decode 身份、batch、field 和规范 tile/压缩 buffer 身份；不加入窗口、实验、视图或一次批量请求的 ID。

## 一次窗口组的执行

1. `WindowPlanner` 维持现有候选和输出顺序。训练 adapter 仅处理当前有界窗口组，将 input/target 需求通过 A07 coverage 与 Arrow List offsets 映射为规范 tile；压缩叶子以完整物理 buffer 为片段。先按 key 去重，也合并同 tile 内相邻或重叠的**逻辑需求**。跨文件仅按显式连续性组装，取数时仍按精确 source/version 分组。
2. 对整组 key 在共享 cache 内批量认领。已完成的 key 取得 lease，正在加载的 key 加入其等待者，仅真正缺失的 key 由本组认领。认领与观察 miss 必须处于同一 cache 同步边界；先在 cache 外探测再预读，会造成重复 I/O，并绕开公共 single-flight。
3. 把新认领 key 的 values、validity 等依赖映射成经过 layout 验证的字节范围。同一个 source/version 内，重叠范围去重，**直接相接的范围精确合并**；不连续范围保持各自的 `[offset,length)` 并放在一个有界多范围请求中。不同 source/version 不共用一次读请求。默认不跨空洞扩展范围；将来若测量证明跨小空洞有益，须另订额外读取字节和比例上限，并独立消融。
4. 多范围响应必须与请求范围一一对应，有独立长度检查。传输层可以对范围排序；返回后按映射拆出各 key 的数据，执行原 decoder/bitmap 校验，发布到原 cache 条目。窗口结果只在所需片段全部成功后组装；一次失败不发布不完整 `WindowBatch`。失败的未完成 key 可被后续请求重新认领。已经完整验证的片段是否在失败组中保留，必须由批量 cache 契约统一规定；首版采用**整组验证后发布**，避免部分完成造成难以追踪的重试状态。
5. 取数顺序可按 source 和 offset 重排；最终 `WindowBatch`、sample ID、provenance、mask、输入/目标变量顺序及分页游标仍按原请求语义生成。实验变换只作用于私有输出，不能改写缓存中的基础片段。

**分波次准入，而非超阈值退回逐 tile SQL。** 每波由 `max_ranges_per_batch`、`max_payload_bytes_per_batch`、backend SQL/响应大小、runtime 内存/条目和每实验在途预算共同限制。默认 `max_entries=1024` 时不能同时长期 pin 1,110 个 tile；一波读取、组装并释放 lease 后再进入下一波。适合连续窗口组的上限应由这些预算计算，不能将现有 `max_entries/4`（默认 256）当作一次预取失败的开关。批量认领和物理请求都要有取消、等待截止时间和公平调度；某个请求取消只移除它的等待需求，仍被其他请求需要的 key 可继续加载。

## 多范围能力及预算

`QuerySource` 仍是唯一读取能力。新增有界 `read_payload_ranges([{offset,length},...]) -> [bytes...]` 时，调用前逐项检查非零长度、`offset + length` 不溢出、精确版本的 payload 边界、总范围数和总字节数。`StructuredBlobReadHandle` 保留已验证的 manifest，不为每个范围重新解析版本。IoTDB backend 需在**一个真实传输请求**中取得多个不连续范围，例如经验证可用的多表达式 `READ_OBJECT` SQL 或等价的批量 RPC；单纯在新 API 内循环执行 N 条 SQL 不满足本设计的调用次数目标。具体 SQL/返回列顺序、别名、单条 SQL 长度、响应大小和 server 内部实际读取次数须先在实验版本验证，再固定上限。后端无法提供该能力时不得把循环标称为批量请求。

物理范围数、payload 字节和调用数分别记录。合并后的范围字节按实际读取量计入共享 producer 服务预算；请求的逻辑值/bitmap 范围和输出字节继续独立计入每个消费者预算，包括 cache 命中。批量缓冲区在读取前按总响应上界预留临时内存，tile 解码结果按各 key 独立计 resident；不能将原始大响应留在 tile lease 背后而仅按 16 KiB 收费。压缩 buffer 的完整读取、解压和内存上界沿用既有规则。一次多范围失败、短读或错序响应不得发布任何未验证片段。

缓存需要按 key 保持 single-flight，并让一组新认领项由一次有界 producer/传输服务。已在其他组加载的 key 不能重新认领；本组可等待它们，或继续处理与它们无关的新 miss。各等待者的取消/截止时间独立；组 producer 的服务额度和生命周期不能继承首个请求的较小逻辑预算。`read_ranges` 统计请求中的物理范围数量，另增实际 `payload_batch_calls`（SQL/RPC 次数）；不能把一个 batch 调用当作只有一个物理范围，也不能把 N 个范围误记成 N 次 SQL。

## 验证和验收

- 用 recording `QuerySource` 验证：两窗口重叠、370 通道、两个实验不同视图、跨 shard、null bitmap 和压缩整 buffer；与现有逐片段路径及 native Arrow oracle 比较值、mask、provenance 和 sample 顺序。普通 Arrow typed 查询与 GRIB 查询的读取 trace 不变。
- 针对 cache 竞态验证：相同 key 的并发训练/typed 请求只解码一次；等待者取消、组失败、过期、驱逐和后续重试；波次跨越条目上限时没有额外重复读取或未计费内存。压力下保留每实验调度隔离。
- 在真实 IoTDB 上先证明一个请求返回多个不连续 `READ_OBJECT` 范围，再测 `L/H`、通道数、batch size、cache 条目预算的冷/暖对照。分别报告 `unique_tile_misses`、实际 SQL/RPC 次数、SQL 内范围数、返回字节、读取/解码/组装/IPC 时间、样本吞吐和峰值受管内存。740 个冷 miss 若每请求最多 64 个范围，**理论调用数下界为 12**；这不是已验证性能结果，也不表示 IoTDB 内部只执行 12 次物理范围访问。
- 保持公开基线的同源版本、随机 sampler、窗口 ID 顺序、训练统计、模型配置及 loss 校验。比较现有逐 tile 路径、只增 cache 容量、批量多范围、再增加受控范围合并的独立效果；不能通过预先全量读取或改变 sampler 获得表面加速。

## 实现与当前证据

`WindowAssembler::prepare_lookahead` 先收集窗口组的规范 tile 需求；整组超过准入上限时按窗口及 tile 波次执行，并在最后一次使用后释放 lease。`ArrowFragmentProvider::get_batch` 与标量 `get` 共用 key、decoder 和 cache；`DecodedFragmentCache::get_batch` 在同一同步边界认领缺失 key，组结果完整验证后发布。`QuerySource`/`StructuredBlobReadHandle` 对范围数、总字节数及 payload 边界做校验，IoTDB backend 在一条查询中执行多个 `READ_OBJECT`。首版上限为每次 64 个物理范围、8 MiB 返回数据。压缩的整个物理 buffer 沿用原标量片段路径。请求仍逐窗口计逻辑预算，取消、失败与并发等待覆盖在回归测试中。

完整 CTest **952/952** 通过，真实 IoTDB 已验证不连续多范围的一次请求。仅 Electricity 的[50-step A/B 记录](../reports/public-training-electricity-tilebatch-20260929.md)显示，51 个训练查询的 payload 调用从 35,671 降到 571；返回字节和解码次数只各降低约 5%。这符合标准随机窗口很少重叠、默认 cache entry 容量仍不足的边界。尚未把 cache-only、无范围合并、跨请求预取等方案单独消融，也未宣称完整 A13 性能门通过。

## 有界并行与挂载缓存容量（2026-09-30）

挂载的共享 runtime 默认使用 16,384 条目、1 GiB 受管内存、256 个在途片段和 4 个 worker；库级默认值不变。保留条目安全上限以约束未按字节计费的控制元数据，不能将本次扩容理解为无限条目。

`TrainingExecutionOptions.read_parallelism` 支持 1–4，默认 1，由挂载属性 `query.training.read_parallelism` 控制，socket 客户端不能覆盖。只有完整驻留的 lookahead 组并行取数；每个 lane 使用独立 provider，在同一个 source/version 内处理连续分配的有界 tile 波次。所有 lane 共用规范 key 和公共 cache，聚合在途片段数受 `max_inflight` 约束；组驻留内存仍不超过总预算的四分之一，传输临时内存由 cache 另行预留。超预算的 pending-wave 路径继续串行组装并及时释放 lease。普通 Arrow typed 查询和 GRIB 的取数路径不变。

主线程按原窗口顺序组装；取消或失败时停止后续波次并 join 所有已启动任务，不返回部分 batch，也不取消其他消费者需要的共享 producer。Profile 输出有效 runtime 配置、服务端并行度和 `payload_reads_active/peak`；后两者是 runtime 生命周期 gauge，不能像累计计数器一样相减。`payload_read_nanoseconds` 是并发 worker 时长之和，不能从请求墙钟中直接扣除来推算其他阶段。

回归测试覆盖真实 1/2/4 路源读取、values/mask/sample ID/provenance 等价、暖缓存、低容量串行回退、请求预算、取消与失败后重试。50-step 消融由 `run_cache_parallel_ablation.py` 固定 sampler、source/version 与输出校验执行。

[两轮实测记录](../reports/public-training-cache-parallel-20260930.md)：955 个 C++ 测试通过，32 次 B 运行均保持 A/B 正确性。扩容将训练冷读解码从 33,889 降到 6,660；4 路并行将已扩容后的预取 wall 从均值 2.533 s 降到 1.285 s。完整 50-step 墙钟较小缓存串行降低约 41%，暖缓存仍有约 14 s 的准备/组装/传输和训练开销。
