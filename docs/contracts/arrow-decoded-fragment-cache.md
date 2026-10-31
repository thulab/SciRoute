# Arrow 公共 decoded fragment cache（A05）

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

A05 把普通 typed 查询和后续训练读取所需的基础数据放入公共 QueryRuntime。不同窗口、变量视图和训练实验可以共享相同物理片段，结果批次仍按请求生成。此阶段提供复用服务，不定义时间语义、跨文件连续性或训练窗口组装规则。

## 接入与所有权

QueryRuntime 拥有通用 DecodedFragmentCache。同一个 adapter 的请求默认复用其 runtime；需要多个 adapter/实验共用预算和缓存时，显式注入同一个 runtime：

~~~cpp
auto runtime = std::make_shared<QueryRuntime>();
auto adapter = std::make_shared<ArrowFormatQueryAdapter>(
  ArrowDecodeConfig{}, runtime);
// 将 adapter 注册到 QueryModule；其他实验可注入同一个 runtime。
~~~

直接调用 adapter 时，source 必须通过 shared_ptr 拥有生命周期：

~~~cpp
// source: std::shared_ptr<const QuerySource>，绑定精确已发布版本。
QueryPlanningContext planning(*source, limits);
auto capabilities = adapter->capabilities(planning, fragment);
auto plan = adapter->prepare(planning, fragment);
// 实际调用必须检查每个 QueryResult。
auto cursor = adapter->execute(QueryExecutionContext(source),
  plan.value().implementation(), execution_options);
~~~

QueryExecutionContext 的 source 是请求读取能力，shared_source 是独立服务读取能力。QueryModule 将请求预算 wrapper 放入前者，将原始 retained source 放入后者。Arrow 执行前核对两者与 plan 的 namespace/manifest；生产者捕获 shared_source，不捕获某个请求的预算或裸 source 引用。因此发起请求结束后，其他等待者仍可安全完成正在进行的加载。

底层 ArrowRangeReader 仍是借用 source 的同步 reader；共享 loader 持有 source 的 owner，覆盖其整个读取生命周期。

## 身份与片段边界

storage namespace 是一个不复用的进程内整数。同一 StructuredBlobStorageBackend 实例打开的不同 source wrapper 使用相同 namespace，不同 backend 实例即使连接相同 endpoint、包含相同 path/version，也严格隔离。独立自定义 QuerySource 默认拥有各自 namespace；复制对象获得新身份。请求预算 wrapper 转发被包装 source 的 namespace。namespace 不是持久化存储 ID，缓存不跨进程共享或恢复。

Arrow 缓存 key 包含：

- decoder 协议版本、storage namespace、精确 path/version、manifest revision、payload/layout 大小。
- 完整 layout bytes 的 SHA-256，覆盖 schema、物理索引、offsets、null maps、compression 与 dictionary epochs。
- 解码 buffer 上限；leaf key 另含 record batch、field、规范片段的起点与长度，未压缩片段还含 tile 大小。

key 不包含窗口起止、实验 ID、语义视图、输出投影或请求的完整 child/channel 集合。字段、batch 和 layout 身份确定实际 values/validity 依赖，避免不同数据源或解码规则误共享。

未压缩 Float32 以叶数组绝对位置对齐的 tile 为单位，默认 4,096 个元素；读取所需 values 和 bitmap 范围，并将 bitmap 重定位到片段起点。请求落在 tile 中间仍复用整个规范 tile。压缩数据以原生 IPC leaf buffer 为单位，复用完整 Float32 叶数组和所需有效位；不同 child List 及 tile 偏好共享同一个压缩片段。LZ4/ZSTD 前缀 -1 表示 buffer 内是原始 bytes，仍沿用该物理单位，但不增加实际 codec 解压次数。

List offsets、父/子有效位和序列位置元数据来自独立的公共 layout fragment。Planning 完整验证 layout，Execution 重新读 L 并逐字节核对后，runtime 从已验证 bytes 重建这些索引，不再重复调用 Arrow schema 解析。provider 持有 layout lease；leaf loader 复制已解析的物理范围与必要标量，不在共享缓存 loader 内递归加载依赖。

## 通用缓存 API 与 lease

DecodedFragment 表示不可变对象，并以 resident_bytes() 报告受管数据容量。cache.get(key, FragmentMemory, loader, waiter) 接受 resident/temporary 上界、加载函数和请求自己的 CancellationToken，返回 FragmentLease。

同 key 的并发请求只有一个生产者。完成之前不发布对象；失败不留下可命中的部分数据，后续请求可以重试。加载器在缓存锁外执行，不能同步递归调用 get()。运行时使用固定工作线程与有界进行中任务队列，没有每请求创建的 detached thread。

FragmentLease::shared() 和 as<T>() 返回带 lease owner 的别名 shared_ptr。复制这些指针会继续保留 pin 与内存收费，调用方不能绕过 lease 另存同一 allocation 的不受管所有权。Arrow provider 的 get() 返回这种受跟踪的 shared_ptr<const ArrowDecodedFragment>，cursor 从中复制请求需要的值和有效位。

达到 max_entries 条目数上限时，LRU 先选择没有外部 lease 的片段；只有不存在未持有的候选时，才驱逐 pinned 片段。内存压力下逐个驱逐，并在锁外释放该片段、更新 live_bytes 后重新评估剩余预算；并发 worker 会等待正在进行的释放结束，避免把整组可用片段清空。evict_all() 可移除缓存所有权，但外部 lease 仍保持对象有效、继续收费；淘汰不等于释放。runtime 销毁后，尚存 lease 也保持对象有效，其跟踪状态随最后一个 owner 释放。

## 服务预算与请求预算

默认配置：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| memory_budget_bytes | 1 GiB | 受管 resident、在途 resident 预留和 temporary 的总预算 |
| worker_threads | 4 | loader 工作线程 |
| max_inflight | 64 | 已接纳、尚未完成的加载任务上限 |
| max_entries | 1,024 | 缓存 resident 条目与预留槽位上限 |
| wait_timeout_ms | 2,000 | 一次 get 的限时等待 |
| max_read_bytes_per_load | 1 GiB | 一个共享 loader 的物理读取预算 |
| max_read_ranges_per_load | 65,536 | 一个共享 loader 的物理读取次数预算 |

Arrow 另有 max_buffer_bytes，默认 64 MiB，限制单个物理/解压 buffer。压缩片段在读取前为压缩输入、解码输出及最终规范片段预留保守空间，允许 IPC buffer 中存在 native trailing bytes。未压缩 tile 同样先预留原始输入和最终值/有效位空间。layout loader 在重建索引前预留其受管对象和临时空间上界。

缓存在 loader 分配数据前同时预留 resident 与 temporary，不能先分配后补记。返回对象后按实际 capacity 缩减 resident 预留；对象超出声明上界时拒绝发布。超大加载立即返回 ResourceExhausted；可容纳但当前预算不足时，先尝试 LRU，再限时、可取消地等待。保守临时上界可能使一个最终很小的压缩片段暂时不能接纳，这是分配前预算的预期行为。

该硬上限是**缓存管理数据缓冲区的预算，不是进程 RSS 上限**。以下对象不纳入 live_bytes：

- Planning 的原始 layout、验证结构和 prepared plan，由 planning/parser 限额约束。
- 返回的 QueryBatch，由请求的 batch rows/bytes 与结果行数约束。
- backend 内部缓冲区，由 backend 实现约束。
- 分配器元数据、codec 私有工作区，以及有界条目/队列的少量控制结构，由实现和格式上限分别约束。

生产者负责遵守声明上界，通用缓存无法截获任意 loader 的所有分配。统计可检查受管内存收费，不应据此声称对上述外部内存或第三方库工作区实施了硬 RSS 限制。

每个 Arrow provider 独立记录逻辑需求。一次 get(batch, field, leaf_position, demand_elements) 将需求裁到该规范片段末尾，计入 4 × 元素数；存在叶 null bitmap 时另加 ceil(元素数 / 8)。每次这样的访问计一个逻辑范围，包括缓存命中；检查通过后即记账，随后加载失败不退回该需求。重复索取同一片段的不同子区间是不同逻辑访问，不按物理 tile 大小收费。

请求的 max_payload_bytes 限制上述逻辑字节，max_ranges 限制逻辑访问次数。一份缓存数据不能让低预算请求绕过自身需求限制。Execution 仍单独读取一次 L 做身份校验，使用请求的 layout 预算并先扣除一个范围，因此 provider 接收的范围上限为 execution_options.max_ranges - 1。Planning 读预算和 Execution 读预算独立；layout preflight 不计入 runtime 的 payload 读取指标。

target_batch_rows、target_batch_bytes 和 max_result_rows 继续限制请求结果。一个请求的超限或取消只影响自身，不消耗其他请求的逻辑需求预算，也不把首个等待者的限制强加给共享生产者。

## 取消、等待和失败

每个等待者有独立的可复制 CancellationToken。cancel() 只写原子标志，不获取 provider/cache/source mutex，可以从读取回调中调用。仍有其他等待者时，生产者继续工作；最后一个等待者退出后向生产者发出协作取消信号，并立即从 key 索引移除该未完成任务。随后同 key 请求可在服务额度允许时启动独立生产者，无须等待旧加载器返回。旧任务仍占用原工作槽与内存预留，直到实际退出；它完成时只可清理属于自己的索引条目，不能删除新的同 key 片段。

等待包括任务接纳、缓存条目/内存以及同 key 的生产结果。每次 get 独立计算自己的截止时间：该等待者到期返回 ResourceExhausted，显式取消返回 Cancelled。共享生产者等待内存预留时，使用**当前等待者中最晚的截止时间**；新的等待者可延长生产者的等待期限，等待者退出后立即重新计算。一个请求不会因另一个请求较早到期而被迫失败。最后等待者离开不会提前回收仍在读取或解码的缓冲区。

取消/超时不保证中断不配合取消的 I/O 或 codec 调用，正在执行的调用可继续占用工作线程与预留空间，直到返回并观察到取消。runtime 销毁会停止接纳并等待工作线程退出，同样不能保证绕过挂起的 backend。调用方若要求底层 I/O 的硬截止时间，应在 backend 实现。

失败与取消不发布半成品；后续请求可以重试。完整加载若先于最后一个等待者退出完成，可以正常进入共享缓存；请求取消不要求丢弃这份完整、有效的数据。取消 provider 不清空其他请求共享的缓存，已经返回的 lease 保持有效。对单个 provider 的 get、metrics、account_output 应顺序调用；多个 provider 可以并发访问同一 runtime。

## 计量

arrow_decode_metrics(cursor) / provider.metrics() 是请求视角：

| 指标 | 含义 |
| --- | --- |
| logical_bytes | 已接纳逻辑值与有效位需求，命中也计入 |
| logical_ranges | 已接纳 provider get 访问次数 |
| output_bytes | adapter 生成的输出批次字节；不等同于 core residual 后最终输出 |

runtime->stats() 是共享服务视角：

| 指标 | 含义 |
| --- | --- |
| hits / misses / waits | 命中已完成对象、创建生产者、等待共享任务或接纳的访问计数 |
| producer_count | 接纳的共享生产者总数，包含 layout metadata 任务 |
| decode_count | Arrow 物理 leaf 加载尝试次数，包含最终失败的尝试；不含 layout 重建 |
| decoded_bytes / decompressions | 成功解码的 buffer 字节与实际 codec 解压成功数；可能来自最终未发布的尝试 |
| read_bytes / read_ranges | 生产者读取前预留/计量的物理 I/O；失败尝试也可能计入 |
| copy_bytes | decoder 构造规范值/有效位及 adapter 输出复制的累计量 |
| evictions / redecodes | 淘汰次数，以及近期淘汰 key 再生产次数；有界历史使后者是累计重新解码的下界 |
| cache_owned_bytes | 缓存持有的已完成 resident 字节 |
| pinned_bytes | 外部 lease 持有的 resident 字节；可与 cache-owned 重叠 |
| inflight_bytes / temporary_bytes | 当前已预留的输出 resident 与临时数据空间 |
| live_bytes / peak_bytes | resident 去重后加在途/临时预留的当前量与峰值 |

不能用 cache_owned_bytes + pinned_bytes 推导 live：二者可能描述同一 allocation。一个片段有多个 lease 时也只收费一次。已被淘汰但仍被 pin 的对象只出现在 pinned/live 中，直到最后一个 lease 释放。

read_bytes 是服务计量的读取请求量，不承诺等同于磁盘设备实际 I/O；backend 可以另有分块读取或缓存。在成功的内存/FileSource oracle 上它应与 source trace 相同。layout preflight、原生 Arrow oracle 自己的 I/O 和训练器其他操作不在该指标中。

## 验证入口与范围

公共 arrow_query_probe 仍从 tests/benchmarks/arrow_training/a04_query_probe.cpp 构建。它保留原生 Arrow 值/mask/位置对照，并新增：

- 首次读取两个子路径的 [3,17)，随后以子路径子集读取 [7,14)，检查服务生产者、读取和解码不增加。
- 在独立空 runtime 中并发运行上述两种视图，检查物理生产者/读取/解码等于单独首个查询的需求。
- 比较四份查询结果与原生 Arrow，并检查 runtime I/O 与受同步保护的 FileSource trace 一致。

2026-09-27 验证：780/780 unit tests、131/131 ASan/UBSan、94/94 TSan、8/8 native Arrow oracle 通过。TSan 使用仅测试进程的 `setarch x86_64 -R` 避免服务器地址映射冲突。服务器 28 个源码/构建/测试文件与工作区 hashes 一致；详细日志、指标、插桩范围与复现环境见 A05 验证报告（服务器归档：`docs/arrow-a05-validation-20260927.json`）。A04 的历史结果保留在 [typed query 文档](arrow-typed-query.md)。

2026-09-28 驱逐回归修复及 781 项单测、132 项 ASan/UBSan、95 项 TSan 的记录见 驱逐修复报告（服务器归档：`docs/arrow-a05-eviction-fix-20260928.json`）。

2026-09-28 条目驱逐顺序修复：`max_entries=2` 时，旧 provider 持有的 Arrow layout 先于未持有的 leaf 被驱逐，导致新 provider 重复解析。修复后保留 layout；完整回归与 sanitizer 结果见 条目驱逐修复报告（服务器归档：`docs/arrow-a05-pinned-eviction-fix-20260928.json`）。
