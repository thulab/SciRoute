# Electricity 的可复用 Arrow 派生布局实验

## 要回答的问题

在冻结的 Electricity 数据和标准 HF RandomSampler 轨迹上，将数据事先组织成连续时间块，是否减少 IoTDB 范围请求、传输与解码量？块内转置为 step-major 是否有额外收益？这些收益能否穿过现有训练查询与 IPC 前端，最终改善训练？

实验不修改原始 Arrow 字节、PB00/PB01 清单、采样器或标准化统计。初始原型与后续生产训练适配器接线分开计量。派生文件写入独立目录，每个 artifact 绑定原始 SHA256、精确 source/version、布局类型、块长、codec 和派生 SHA256。

## 布局与对照

完整 140,256 steps × 370 channels，各块独立为一个 Arrow stream record batch，每 batch 一行。最后一个块使用真实长度，不填充。尝试 128、512、2,048、8,192 steps，none/LZ4，共两种布局、16 个 artifact：

- `channel_major`：保留 `target[channel][step]`，缩短时间块。可直接由现有 TrainingQueryModule 消费。
- `step_major`：`target` 为单层 List<Float32>，内容依次为每个 step 的全部 channel。初始实验由独立读取器消费；后续生产训练适配器通过显式 `value_layout: "step_major"` 支持。

两者均保存 start/end step、序列与实体标识，保留 mask 及所有有效 Float32 位模式。builder 对全文件逆变换验证，不只检查抽样窗口。null 父/子 List 和 ragged 行明确拒绝；叶值 null 被保留。

8,192 点派生块通过同一个读取器作为大块对照，避免将批量数组转换优化误归因于布局。它与原始 Arrow 的两行/batch 并不完全相同；原始文件的既有本地 reader 和生产 typed 路径另列。

## 实验读取器的计时边界

`DerivedWindowReader` 对 local mmap、local read、普通 FUSE read、IoTDB direct ranges 使用同一个 PyArrow 解码器、按字节限制的 LRU 和窗口组装器。所有路径返回自有、连续的 [sample,time,channel] Float32 和 bool mask 数组。

批次中多个窗口共享需求；缓存缺失的相邻完整 IPC message 合并，不连续 message 使用 `read_payload_ranges`。遵守后端单次 64 ranges / 8 MiB 上限，大块超出时显式分拆并统计每次调用。一次实验读取器绑定一个不可变 artifact，IoTDB helper 启动时校验精确发布版本与大小。

这是一条实验性的 prepared 读取路径，**不包含生产逐请求授权、snapshot/view 验证、调度和窗口结果 IPC 编码**。socketpair 只把原始压缩/未压缩 IPC message 从本机 C++ backend helper 送到 Python；不能将其耗时称为现有 E 前端耗时，也不能将其全部加速归因于派生布局。生产 typed 回放独立执行。

缓存上限各路径均 1 GiB，按保留的值/validity 数组计费；元数据、临时传输 buffer 和请求在途输出不计入该数组缓存额度。该原型未使用公共 QueryRuntime，也未验证跨训练进程的共享缓存。

## 工作负载与验证

复用 `execution-cost-ablation-final-20260930/r1-patchtst-all-cold.json` 的51次请求、102个训练样本，L96/H24，窗口开始位置为原预测 anchor−96；不人工构造跨文件窗口。回放全370列和前7列。

每个路径、布局、投影做两次独立应用缓存冷启动，每次一遍冷轨迹和十遍暖轨迹。第二次反转 artifact 顺序；暖轨迹在每次重复内部剔除最高/最低值后汇总。未清理 OS/IoTDB 缓存。local mmap 的请求字节是逻辑触及的 message 范围，不能作为物理磁盘字节。

每次请求在读、解码与自有数组组装完成后结束计时，再与原始源独立解码出的 oracle 比较 validity 与有效 Float32 位模式。记录 setup、范围调用、字节、解码块数、缓存及读取/解码/组装时间。整体轨迹耗时为51次请求耗时之和，不含 oracle 比较与统计落盘。

生产 typed 回放保留标准化、prepared_compact、四路读取、公共1GiB/16384条目缓存和完整IPC/FUSE结果读取；首次prepare计入首请求，并在结束后恢复挂载配置。生产 step-major 接线后使用显式训练 binding，不改变普通 Arrow typed 查询的物理模型。

## 实现入口

- `prepare_electricity_derived.py`：不可变派生文件构建、完整逆变换验证与索引。
- `derived_window_reader.py` / `derived_range_bridge.cpp`：实验读取器和只读范围桥接；CMake target `arrow_derived_range_bridge`。
- `run_electricity_derived.py`：挂载发布、完整回读hash和固定轨迹对照。
- `replay_derived_typed.py`：现有生产训练查询的独立回放。
- `train_electricity_derived.py`：复用既有 HF Trainer/model/sampler，显式标记实验读取路径。

所有构建/摄取/校验成本和存储增量独立报告，不能仅用每次查询的收益忽略首次派生代价。

## 64–128 点块的生产接线

`SnapshotRowBinding::value_layout` 显式区分 channel-major 嵌套 List 与 step-major 单层 List。后者必须声明时间长度和物理通道数，叶数组长度必须等于二者乘积；`physical_ordinal` 为块内列位置。布局由声明决定，不从变量名称或时间语义猜测。

注册中的 `channel_tables` 保留为共享不可变表。Coverage index 保留每行公共位置、共享通道表与紧凑引用，查询时只展开命中的 span；诊断接口 `spans()` 仍可按需展开完整内容。快照复制隔离调用方可变别名。旧 channel-major 内容指纹保持不变，step-major 使用版本化内容指纹，sidecar 协议升级为 `/4`。

`TrainingQueryModule::prepare` 编译序列、时间覆盖、允许 anchor 区间、布局和 List 索引。每次 execute 只创建独立游标与请求预算；仍校验所访问来源的 namespace、精确版本、revision、格式和大小，并保留前端授权。已发布 revision 的不可变性允许复用布局，不再逐请求重读全部 layout。编译布局缓存使用 `max_layout_bytes` 作为保守元数据上限，超限来源回到按需加载；未使用且已删除的 shard 不阻断其他窗口。

训练 assembler 将需求按 block、role 和时间交集分组。64/128 点派生文件每个 RecordBatch 一块，整块解码后作为公共缓存片段供不同窗口和视图共享。多个缺失压缩块走批量范围读取；相邻范围合并，不连续范围使用多范围调用。完整有序通道按时间行批量复制，子集与重排使用已确定的列位置。普通 Arrow 查询仍保留原来的 tile 获取方式。

逻辑字节、range 与 provenance 仍按请求选中通道计费，物理读与解码由共享 runtime 计费；null 拒绝退款、取消、输出上限、分页和实验调度语义保留。首次 coverage 构建仍会临时生成最多一百万个展开 span，再压缩为驻留索引，因此首次准备的峰值内存和耗时不等于紧凑索引的驻留成本。

生产接线的测试、51 请求回放、同一上下文复用与 50-step 训练结果见 [64/128 点训练块验证](../reports/electricity-compiled-training-blocks-20260930.md)。

后续使用真实原始两行/batch、统一读取器及未压缩编码的布局/系统收益区分，见 [严格对照实验](../reports/arrow-layout-system-control-results-20260930.md)。该轮修正了实验读取器对本地 FS 强制 8 MiB 分段的额外复制，旧 FS 数字不应与新轮混用。
