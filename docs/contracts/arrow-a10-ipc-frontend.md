# A10 typed query IPC 与可测试前端

日期：2026-09-29。A10 提供原生 C++ 路径注册、执行、IPC 编码和结果句柄；后续 FUSE 接线见 [A11 运行时](arrow-a11-fuse-runtime.md)。

## 编码协议

普通 `QueryBatch` 使用一个 Arrow IPC **stream**，固定 schema，不在同一 stream 中切换字段。schema metadata 的 `iotdbfs.protocol=iotdbfs-query-batch-ipc/1`；字段 metadata 保存 logical field id、display name、role、unit 和 timezone。每个 record batch 的 IPC custom metadata 保存 domain kind、dimension/shape/origin 和完整 source path/version/format identity。空 cursor 仍产生可解码的 schema-only stream；零行 batch 与没有 batch 不混淆。编码按 cursor 增量拉取，跨 batch 累计限制输出字节，读取或编码失败时取消 cursor 且不返回部分 stream。

训练 `WindowBatch` 使用 `iotdbfs-window-flat/3` 固定宽表：每行对应一个 `(sample_index, role, step)`，公共列依次是 `sample_id: utf8?`、`sample_index: uint32`、`role: utf8`、`step: uint64`、`time_ns: int64?`、`descriptor_json: large_utf8?`，然后依视图顺序排列 `input.0…` 和 `target.0…` 的 `float32?` 列。`sample_id` 与 `descriptor_json` 仅在每个 sample 的第一行出现，其余行由 `sample_index` 关联；样本可以跨 record batch，消费者须跨 batch 保留该关联。schema metadata 的 `iotdbfs.sample_id_location=first_row_per_sample` 明确这一契约。`role` 为 `input` 或 `target`；另一个角色的值列总是 null，因此消费者可以区分“不适用”和当前角色中因数据缺失产生的 null。首行 descriptor 包含 sample/view/series/anchor/epoch/seed/shard、时间区间、通道覆盖和全部 source span/fragment key。schema metadata 保存 snapshot/view identity、`L/G/H/S`、样本数、各列的 field id/ordinal/label，以及存在时的分页 continuation。窗口行分组写入同一个 IPC stream，编码输出在每次写入时受字节上限约束。空 batch 产生 schema-only stream。

`/3` 要求 schema metadata 显式声明 `iotdbfs.transform.kind`（`identity` 或 `standardize`）、`iotdbfs.transform.artifact_id` 和 `iotdbfs.transform.statistics.count`。标准化时，每个唯一 `(field_id, ordinal)` 按此二元组排序后写入 `iotdbfs.transform.statistics.<i>.field_id`、`.ordinal`、`.mean`、`.stddev`；统计量以独立于系统 locale、可往返还原的 double 十进制文本保存。input 与 target 中相同的通道使用同一组训练统计。identity 的 artifact ID 为空、count 为 0。消费者可仅凭 IPC schema 知道值是原始值还是 `(x-mean)/stddev` 的结果；null mask 不参与变换。

## 注册路径与结果生命周期

`TypedQueryFrontend` 返回只读路由路径；A11 允许独立客户端通过本地 Unix socket 注册：

```text
/.iotdbfs/query/v1/typed.ipc?id=<192-bit hex>[&version=<int64>]
/.iotdbfs/query/v1/windows.ipc?id=<192-bit hex>[&snapshot=<64-char fingerprint>]
```

注册表持有请求的不可变副本；窗口注册还持有已准备好的 `TrainingQueryModule`，因而固定 snapshot、覆盖索引和 view。注册项默认使用 **5 分钟闲置 TTL**（可配置）；成功解析续期，错误的 route/precondition 不续期。到期项在下一次注册、解析、计数或显式删除时清理，释放容量及其持有的模块；已解析请求保留自己的 shared ownership，可继续执行。长期无注册表调用时尚无后台清扫线程。路径解析限制长度、参数集合、重复项、percent escaping、UTF-8 和版本/snapshot 前置条件。ID 是进程内定位符，不是权限凭证；A11 挂载入口执行文件级授权。

`TypedQueryService::materialize(route)` 解析注册请求，执行一个有界 query cursor 或 window request，编码完成后才将完整字节原子发布到 `TypedResultStore`。结果 store 另设单结果字节、驻留字节、条目数和 TTL 上限。`open_result(id)` 返回固定内容的句柄，支持偏移读取和 EOF；已打开句柄在注册项过期/淘汰后继续指向同一字节序列，所占内存仍计入驻留上限，直至释放。编码、执行或 store admission 失败均不产生可见结果 ID。`WindowRequest.max_windows` 继续限制单次 materialize 的样本组；分页由 A08 continuation 驱动，不在 A10 一次物化整个 epoch。

服务构造时将编码上限收紧为 `min(max_encoded_bytes, store.max_result_bytes, store.max_resident_bytes)`。因此任何超过 store **静态**单结果接纳上限的 IPC 在编码阶段立即停止，不会先完整编码再到 `publish` 才失败。其他已打开句柄造成的**动态**驻留压力仍可能使发布失败；窗口路径目前先生成有界 `WindowBatch` 再编码，静态 IPC 上限不等于窗口解码的预检上限。

## 验证

隔离服务器的 Arrow 21.0.0 下，A10 定向测试 32/32、生产目标构建、完整单元测试 896/896 通过；窗口编码、结果上限和 route 生命周期回归通过 ASan/UBSan。测试覆盖多 batch schema/provenance、identity 与标准化视图的变换元数据、null 与 mask、schema-only、精确和差一字节输出上限、静态 store 上限、失败不发布、路径解析、版本冲突、route 闲置 TTL 与容量回收、result TTL/驱逐、pin/seek/EOF，以及普通 typed 路由和注册窗口的跨文件样本。`tests/benchmarks/arrow_training/a10_ipc_oracle.py` 用独立 PyArrow 21.0.0 解码 C++ 测试导出的普通 query、identity 与 standardize 三个 stream，核对值、mask、形状、字段元数据、训练统计和 provenance。

体积审阅（2026-09-29）：单通道、`L=512/H=96` 的单样本固定测试中，原 `/1` stream 为 **121,344 B**；`sample_id` 仅保留首行后，`/2` stream 为 **33,488 B**，减少约 72.4%。这仍大于 2,432 B 的有效 Float32 值，因为固定 flat schema 保留每行的 role、step、time、sample_index、两组 nullable 值列及 IPC/descriptor 开销。默认 64 MiB 单结果上限在该形状下约可容纳 2,000 个样本，不能假定 `max_windows=4096` 必然装入一个结果；调用方应据实测大小选择较小的 sample group 并用 continuation 分页。新增跨 record batch 测试验证首行 ID 和后续 `sample_index` 关联。
