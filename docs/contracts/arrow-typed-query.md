# Arrow typed query 与公共片段 decoder（A04/A05）

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

A04 通过 ArrowFormatQueryAdapter 接入 QueryModule，直接使用 A03 摄取生成的不可变 Arrow layout。A05 将基础片段与 layout 依赖放入公共 QueryRuntime，让同一 backend 的多个查询或训练实验复用解码。调用方在创建 QueryModule 时注册该 adapter；FUSE/CLI 的 typed 前端属于 A12。公共缓存 API、预算和生命周期契约见 [decoded fragment cache](arrow-decoded-fragment-cache.md)。

## 请求与输出

请求必须选定一个精确 source path/version，并指定 native 字段名、record batch 序号、该 batch 内的 row 序号和物理子路径。当前仅支持 `List`/`LargeList` 的一层或两层嵌套，叶类型为 Float32；不解释时间、变量、实体或数据集注解，不对原始 List 实施隐式 any/all 比较。

| native shape | 物理 paths | capability 中的列 |
| --- | --- | --- |
| `list<float32>` | 空子路径 `{}` | `index: UInt64`、`value: Float32?` |
| `list<list<float32>>` | `{2}, {0}` | `index: UInt64`、`value.2: Float32?`、`value.0: Float32?` |

`index` 是最内层 List 内从零开始的位置。显式 projection 决定最终列顺序；子路径集合决定此次绑定可用的列。谓词需要但未投影的列由 adapter 加入中间结果，core 完成 residual 后删除。谓词只作用于绑定后的标量，不改变 List 绑定。比较遵循现有 core 的 SQL null 语义；NaN 保持有效 Float32，现有 core 将非有限浮点比较视为 unknown。

`DomainSlice` 仅允许 UInt64 的 `index` 维度，支持开闭边界，超出 List 长度的边界裁到实际范围。切片后的 `index` 保留原位置，不重新编号。即使未投影 `index`，adapter 批次也携带 `DomainDescriptor{Rows}`，以 `origin` 和 `shape` 表示该批次的连续位置范围；core 最终投影仍保留它。若 residual 实际删除了行，core 会移除不能再表示连续位置的 domain，需要逐行位置的调用方应显式投影 `index`。没有类型转换、插值或 time slice capability。

用户确认的宽表规则：

- 所有选中的有效 List 长度必须相同；包括有效 empty List。不同长度在 prepare 时返回 `InvalidQuery`，即使后续切片可以等长也不接受。
- null List 短路子路径检查。存在有效同伴时，用其长度生成整列 NULL；全部 null，或全部有效 List 为空时，返回带 schema 的零行游标。
- `arrow_physical_binding(plan)` 返回请求顺序的 `ResolvedArrayValue`，保留首个 null List 的层级：0 表示父行，1 表示选中子 List。该信息属于绑定，不会与叶值 NULL 混淆。
- 仅投影 `index` 且无值谓词时不读取 Float32 payload。

示例（source selector 省略）：

```cpp
QueryFragment fragment;
fragment.physical_array = PhysicalArraySubject{
  "target", 0, 0, {{{2}}, {{0}}}};
fragment.projection = {{"index"}, {"value.2"}, {"value.0"}};
fragment.slices = {{{"index"},
  DomainBound{LogicalValue::uint64(3), BoundKind::Inclusive},
  DomainBound{LogicalValue::uint64(17), BoundKind::Exclusive}}};
```

## 读取、解码与复用

`ArrowRangeReader` 只经由 `QuerySource` 访问数据。`ArrowFragmentProvider` 将读取和规范解码独立于投影、窗口、实验变换；它返回不可变共享 handle，游标从这些基础片段复制精确输出范围及 mask。

- 未压缩：默认按叶数组绝对位置对齐的 4096-element tile。只读取所需 tile 与对应叶 bitmap 字节；非字节对齐的有效位被重定位到 fragment 内。tile 边缘可以带来相邻数据 over-read，计入实际 payload bytes。
- LZ4/ZSTD：以 IPC 物理 buffer 为解压单位；读取完整 float buffer 和必要的完整叶 bitmap buffer。压缩前缀 `-1` 表示该 buffer 直接保存原始 bytes，仍按整个物理 buffer 处理。不同 child 和不同输出批次复用同一个已解码 fragment。
- List offsets 和父/子 List validity 使用 A03 已验证并持久化在 L 的索引，不从 B 重复读取；叶值与叶 validity 从 B 按上述规则读取。Planning 验证 L 的 schema、offset 边界和 bitmap；Execution 校验 bytes 身份后，runtime 从已验证 bytes 重建公共 layout 依赖，不再调用 Arrow schema 解析。
- provider 使用注入的公共 runtime。缓存身份包含 backend namespace、精确 source/version、layout/schema/decode 身份和物理片段边界，不包含窗口、视图、实验或所选 channel 集合。相同物理片段的并发需求只有一个生产者；不同请求共享 metadata、offsets、validity 与 leaf fragment。

arrow_decode_metrics(cursor) 或 provider.metrics() 暴露该请求的 logical_bytes、logical_ranges、output_bytes；命中缓存仍计逻辑需求。实际 I/O、解码、解压、命中、等待和内存通过 runtime->stats() 观察，只在共享服务中计一次。decoded_bytes 是成功转换到未压缩 buffer 的累计字节量，不代表当前保留内存。IPC -1 buffer 不增加 codec 解压次数。Planning/Execution 的 L 读取由 QueryModule 的独立读预算计量，不并入 runtime 的 payload 计量。

## 身份、预算与生命周期

plan 保留 storage namespace、精确 path/version、format、revision、payload/layout size、发布状态以及完整 layout bytes。execute 检查 namespace/manifest，并重新读取 L 逐字节核对 schema/layout 身份；变化返回 StalePlan。runtime 以 SHA-256 标识完整 L，覆盖 schema、offsets、null maps、codec 与 dictionary epochs。source 的不可变性由已发布 Structured Blob 版本契约提供，不对 payload 重新计算全文件 hash。

`capabilities(context, fragment)` 和 `prepare(context, fragment)` 显式共用一个 `QueryPlanningContext`。它绑定一个 exact source 和本次请求的 `QueryLimits`，只在单次规划中使用，不可复制或移动；context 和 source 不会被计划持有。Arrow 首次读取并反序列化 L，之后复用不可变 layout 和共享原始 bytes，仍按当前 fragment 验证物理绑定。不同 source 或不同请求使用独立 context，不以 path/version 建立全局缓存。

正常查询现在共读取两次 L：Planning 一次，Execution 再读取一次进行完整 bytes 校验。Planning 内跨 source 共用实际读取次数/layout bytes/payload bytes 的预算；Execution 使用独立的新预算。默认单 source 的 planning layout 上限因此与 parser 的 64 MiB 上限一致，不再因为双读变成 32 MiB；其他 metadata/index 限制仍生效。子路径数量使用 `context.limits().max_ranges`，QueryModule 将 `request.limits` 原样传入；不会再额外套用默认 65,536 上限。

直接使用 adapter 时，也应在两个规划调用之间复用同一个 context：

```cpp
QueryPlanningContext context(source, request.limits);
auto capabilities = adapter.capabilities(context, fragment);
// 验证 capabilities 与 fragment 后继续；实际调用应检查错误。
auto plan = adapter.prepare(context, fragment);
```

decoder 默认物理/解压 buffer 上限 64 MiB；A04 的 provider 私有 128 MiB LRU 已由 A05 公共 runtime 替代。runtime 默认预算 1 GiB、4 个工作线程、64 个进行中任务和 1,024 个缓存条目，等待超时 2 秒。加载前同时预留 resident 和 temporary 上界，外部 lease 继续占用预算；LRU 淘汰不会把仍存活的 pinned 内存视为已释放。该硬预算约束缓存管理的数据缓冲区，不是整个进程 RSS：Planning artifacts、结果批次、backend 内部缓冲区、分配器元数据及 codec 私有工作区不在此计数内，仍受各自请求、格式或实现约束。

QueryExecutionContext 持有两个拥有生命周期的能力：source 用于请求预算下的校验读取，shared_source 用于共享生产者。QueryModule 分别传入请求预算 wrapper 和原始精确版本 source；它们必须具有相同 namespace/manifest。生产者保留独立 source 和服务预算，首个请求退出不会使其他等待者的读取悬空。直接使用 provider 时，调用方仍须确保传入 L 来自同一精确不可变 source，只有大小相等不足以证明身份。

每个 provider 的 get/metrics/account_output 顺序调用，不同 provider 可以并发共享 runtime。cancel 仅原子标记该请求，可从 read callback 调用；一个等待者退出不取消其他等待者，最后一个等待者退出才向生产者发出协作取消。取消或超时不承诺中断不配合取消的 backend I/O/codec 调用；它们返回后检查取消，不发布部分结果。完整加载若先于最后一个等待者退出完成，可以正常进入共享缓存。取消不清空公共缓存，现有 immutable lease 在取消、LRU 淘汰甚至 runtime 销毁后仍有效。内存或任务槽不足时限时等待，超时返回 ResourceExhausted；用户取消返回 Cancelled。

## 范围

A04 建立普通 typed 查询与后续训练读取共用的 decoder，A05 提供跨查询/实验的公共解码服务。同一 StructuredBlobStorageBackend 实例共享 namespace，不同 backend 实例严格隔离，缓存不跨进程持久化。跨文件时间覆盖、重叠训练窗口组装仍分别属于 A07、A08；A05 不推断相邻 Arrow row 是连续序列，也不缓存训练变换后的完整窗口。

## 验证与复现

A05 验证已通过：780/780 unit tests、131/131 ASan/UBSan、94/94 TSan、8/8 native Arrow oracle。探针比较值、mask 和位置，并验证顺序重叠视图与并发冷缓存查询共享物理解码；TSan 的进程级地址布局设置、插桩范围及完整指标见 A05 验证报告（服务器归档：`docs/arrow-a05-validation-20260927.json`）。以下保留 A04 的历史验证记录。

2026-09-27，在 `root@192.168.99.49:/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/a04-arrow-query-20260927` 的独立 source/build 中完成：739/739 unit tests，59/59 ASan/UBSan 相关测试，8/8 公开 nested IPC oracle 对照。详细环境、源文件 hash 和实际读取指标见 验证报告（服务器归档：`docs/arrow-a04-validation-20260927.json`）。

```sh
cmake --build build --target unit_tests iotdb_fs arrow_query_probe -j12
ctest --test-dir build -L unit --output-on-failure -j12
build/bin/arrow_query_probe /path/to/ETTm1-none-8192.arrows
```

探针逐元素比较独立 native Arrow reader 的值、mask 和位置，并核对 QuerySource trace 与计量相同。ETTm1（1,955,584 B）和 Electricity（207,613,480 B）的两个子路径、14 个位置分别只读 32,768 B payload；这是固定 tile 粒度的实际成本，不代表只读取了 112 B 所需浮点值，也不包含 planning/execution 的 layout 读取。LZ4/ZSTD fixture 的两个子列及三个输出批次复用同一个完整 leaf fragment：float 和 validity buffer 各解压一次。

A04 审阅后的取消修复：移除同线程 read callback 中对已持有 mutex 的 `try_lock`，改为仅设置取消标志、由 `get` 或析构延后清理缓存。增强回调取消测试并新增空闲清理、析构保留外部 owner、并发阻塞读取取消三项测试；服务器完整 unit 回归 742/742 通过。上述 59 项 sanitizer 结果属于初次 A04 验证，不能证明旧实现满足 mutex 前置条件；本次未重复运行 sanitizer。修复后的文件 hash 和测试日志见验证报告中的 `follow_up_validations`。

后续规划/位置/限额修复：新增单次规划上下文，L 从三次读取降为 Planning/Execution 各一次；值列独立投影保留连续位置 domain；Arrow 使用真实请求的子路径上限。完整回归 750/750 通过，包含实际超过 32 MiB 的可摄取 layout 在默认预算下成功查询，以及提高到 65,537 路径的请求校验和物理绑定；新接口下 ETTm1 native oracle 再次通过。GRIB 规划调用同步迁移到 context 接口，执行行为不变。验证报告追加记录了本次 source hashes 和日志。
