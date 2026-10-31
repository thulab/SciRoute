# H06：公共 chunk cache、批量读取与并发隔离

HDF5 typed adapter 已接入现有 `QueryRuntime` 的 `get/get_batch`。同一 runtime
中的查询复用 immutable decoded fragment，不再为每个 cursor 留一份独立值缓存。
原生 chunk 是 chunked dataset 的复用单位；contiguous 仍以固定逻辑 tile 为单位。
请求矩形和 hard-link 别名不改变 fragment 边界。

## 身份与生命周期

缓存键绑定 storage namespace、source path/version/revision、payload/layout 大小、
layout SHA-256、dataset 对象 token、原生 chunk 网格位置或 contiguous tile 序号、
解码 profile 版本，以及 tile 大小和 adapter 的解码安全上限。它不包含选区、
输出列顺序或 dataset 别名路径。不同 backend、版本、layout 或解码配置严格隔离。
请求的字节／range 限额不进入键。

计划仍固定并验证精确 published source。execute 通过 request source 重读 layout
header 检查身份；共享 producer 只保留 `context.shared_source`、不可变块计划和
独立生命周期的 decoder／统计对象，不保留首个请求的预算、取消 token 或 provider。
后续删除源或改变身份不会被 warm cache 的返回值掩盖。

provider 只持有当前有界块组的 lease，复制相交值后按需要换组；申请新组前释放
旧 lease，避免自己 pin 住旧组而等待无法腾出的内存。输出矩形、C 顺序和投影
契约不变。cache hit 直接复制请求片段到输出，不另行复制整个 decoded chunk。
不同 query 的重叠 misses 由公共 cache 单次认领，未完成的同 key 加入 single-flight。

## 按需合批与真实传输

每个输出 slab 懒枚举其相交 chunk／tile，不预取无关 dataset 或不相交 chunk。
默认每组最多 64 个 fragment、8 MiB 编码需求；同时受 runtime entry/inflight
上限与内存预算约束。单个超过 8 MiB 的 fragment 仍可作为单独一组，不被组上限
误拒绝。组内只有本 producer 认领的 misses 进入传输。

`QuerySource::payload_read_capabilities()` 明确描述 multi-range 能力、每调用
字节和 range 上限。普通 source 默认 scalar；StructuredBlob source 如实转发
backend 能力，IoTDB backend 声明支持多范围。不以多次 scalar 冒充一次批量 RPC。

`Hdf5RangeReader` 对 misses 的物理范围排序，仅合并相邻或重叠区间，不跨 gap
读额外数据。合并后再按 source 实际上限和 64 ranges／8 MiB 硬上限拆分调用：
支持的 source 一次请求多个不连续范围，否则执行有界 scalar 调用。一个大 chunk
可跨多个调用，按原始片段次序收齐后完整解码。返回条数、长度和容量必须匹配。
未收齐、短读、异常、checksum 或解码失败均不能发布该组的部分 misses；已存在的
其他缓存项不受影响。

`read_parallelism=1/2/4` 控制**每个 producer** 的同时调用数，默认 1。
runtime 的 `worker_threads` 控制并行 producer 数；例如四个 workers 各四路时，
总调用峰值可达 16，而不是全局四路。真实 IoTDB session 池还有自身边界。失败和
取消会等待已启动调用结束，再释放临时内存配额；底层同步 I/O 无法被强行打断。
transport 规划最多 65,536 个范围，防止极小 source 限额放大控制结构。

## 三种独立预算

- 请求预算：header 读取由 request source 收费；每个首次触及的物理 fragment
  将完整编码长度与一个逻辑 range 计入请求需求，warm hit 同样计费，跨 slab 不
  重复收费。稀疏 fill 不产生 payload 需求；其输出与内存仍受限。这是 fragment
  需求口径，不是实际 RPC 数；合并和拆分不会改变其值。
- 服务预算：QueryRuntime 对实际读取、解码、resident capacity、临时缓冲与并发
  loader 计费。producer 使用服务 `max_read_bytes/ranges_per_load`，不继承首个
  waiter 的请求上限。输出 batch 不计入 cache 的 live_bytes。
- cursor buffer：`max_buffer_bytes` 限制当前输出加本次块组工作集；规划使用
  transport-aware 单块峰值缩小 slab，运行时继续限制组大小。目录／索引和一般
  allocator 元数据、source 自身存储，以及调用方保留的历史输出不属于该预算。
  因而这些值都不是全进程 RSS 限制。

resident 按 `BinaryValue.capacity()` 计费，包含 Fletcher checksum 移除后仍
保留的容量。工作集覆盖编码输入、同时存活的传输响应、codec scratch、中间解码
和输出。单块原位处理时，encoded buffer 会成为 resident，只计一次；传输响应
在解码前释放，因此峰值取传输与 codec 两阶段的较大者。32 MiB 原始 chunk 在
默认 64 MiB、单路配置下可通过四次 8 MiB 传输读取。

HDF5 adapter 自建 runtime 时显式采用 **1 GiB／65,536 entries**；字节预算为主要
容量约束，entry 上限仍限制控制结构。注入已有 runtime 时遵循调用方配置，不
改写它。公共 QueryRuntime 默认值和普通 Arrow/GRIB 取数策略保持原样。

## 取消、等待与失败隔离

cursor 取消只修改该 waiter 的 token，既不获取读回调持有的 mutex，也不直接
取消其他查询的 producer。公共 cache 保持已有规则：最后 waiter 离开后摘除旧
entry，新请求可立即重试；旧 worker 不覆盖新 entry；仍有 waiter 时 producer
继续工作，其等待期限取当前 waiter 的有效期限。全 pinned、retired 和超大块
仍按实际分配计费；内存／admission 等待有超时，默认 runtime 为 2 秒，可配置。

失败为 cursor 的 sticky error。一个组失败不返回该组生成的部分 slab，但此前
返回的 batch 不可追回；消费方仍需检查最终状态。

## 指标与验收范围

QueryRuntime 记录真实 payload calls/ranges/bytes、调用累计 wall time、活跃调用
峰值、fragment load wall time、decode count/bytes、decompressions、copy bytes、
hits/waits/evictions/redecodes，以及 resident/pinned/inflight/temporary/live/peak。
新增 `decode_nanoseconds` 单独统计成功值解码／字节序转换的 wall time；
`decode_cpu_nanoseconds` 使用 worker 的线程 CPU 时钟，`decode_cpu_samples` 标明
成功采样次数。它们不包括 payload I/O、组装复制或 fill 合成；失败 decode 不进入
成功 decode 计数。未提供计时的其他 adapter 仍保持原有计数，新字段为零。
并行调用的累计 wall time可重叠，不能直接相加当作端到端时间。

HDF5 cursor 的 `metrics_observer` 保留 H05 字节指标；producer 统计归属于发起它
的 cursor，joined query 不重复报一次物理解码。取消后尚未结束的 producer 可能
不再向该 cursor 推送最终快照，跨请求分析应以 runtime 计数为准。其 cache_hits
包括本地 lease 复用和由其他 producer 完成的复用；runtime 的 hits/waits 可分别
区分 resident 命中与等待。

优化执行路径不调用 libhdf5，因此没有查询阶段 libhdf5 全局串行锁；native 库仍
用于摄取。本轮在记录型内存 backend 和受控每调用延迟下验证共享缓存、transport
并行和 codec CPU 分离。尚未测量真实 IoTDB session 等待与 RPC 耗时，也不据此
宣称真实系统加速比；mount/socket 接线和真实服务验收仍属于 H08。

```bash
cmake --build <build> --target h06_query_probe -j 4
python tests/benchmarks/hdf5_query/run_h06.py \
  --probe <build>/bin/h06_query_probe --output <results>
```

standalone 工程 `tests/benchmarks/hdf5_query` 的可执行文件位于 build 根目录。


## 2026-10-01 验证结果

| 检查 | 结果 |
|---|---:|
| Linux 全量 CTest（含公共 cache、Arrow、GRIB 回归） | 1,174 / 1,174，含 1,168 单元测试 |
| 新增 HDF5 cache 集成测试 | 19 / 19 |
| 新增 transport 测试 | 9 / 9 |
| Linux / macOS H06 | 各 13 / 13 配置，65 场景 |
| Linux / macOS H04、H05 回归 | 各 185 / 185、416 / 416 |
| macOS ASan/UBSan H04、H05、H06 | 185 / 185、416 / 416、13 / 13 |

H06 的 13 组配置覆盖：64 个不连续 native chunks 的 scalar/multi-range ×
1/2/4 路读取；单个 28 MiB chunk 的分段 scalar/multi-range × 1/2/4；以及
2,048 个小 chunk。每组验证 cold、warm、alias、overlap 和四客户端 concurrent。
实验显式设置 runtime 256 MiB／8,192 entries，cursor buffer 192 MiB；不是默认
参数性能对照。所有 warm/alias/overlap 均零新增 payload I/O／decode；四客户端
冷读时每块仅解码一次。大块四次传输后仅发布一个 decoded fragment，观测到的
调用并发峰值与 1/2/4 一致。小块数据没有因 1,024 entry 阈值产生重复解码。

合成的 64 个不连续范围从 64 次 scalar 调用变成一次真实 multi-range 调用；
该结论是调用数验证，wall time 中每调用另加人工延迟。每例保留实际调用范围、
字节、拷贝量、codec wall/CPU、cache hit/wait 及内存峰值，不能作为真实服务
吞吐／端到端训练收益结论。

测试同时验证 backend/version/revision/layout/tile 隔离、warm hit 独立预算、
压缩复用、稀疏 fill、失败原子发布、single-flight、取消不影响其他 waiter、最后
waiter 取消后立即重试。32 MiB 单块和小 source 限额的新增回归先复现了重复内存
预留／输出未提前缩小问题，修复后均通过。既有公共 cache 测试继续覆盖后来
waiter deadline、旧 producer 不覆盖新 entry、全 pinned 和 retired 的计费。

Linux 构建启用 BUILD_TESTS，关闭 ENABLE_FUSE_INTEGRATION_TESTS；上述全量数字
指该配置下注册的 CTest，不包括真实 FUSE 挂载测试。

sanitizer 覆盖本项目 C++，共享 HDF5/zlib 未插桩，`detect_leaks=0`，不替代
ThreadSanitizer 竞态检查。未重启现有实验挂载，未执行 Git 写操作。

证据位于 `docs/results/hdf5-h06-20261001/`（结果目录沿用 ignore 规则）及服务器
`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h06-20261001/`，包括源码／probe
哈希、构建与完整 CTest、逐例 canonical values、range／指标 JSON 和 sanitizer 日志。
