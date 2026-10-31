# 查询主线：原生 HDF5、GRIB 发现与并行复用

**2026-10-22阶段更新**：FM1、受管FS原生读取及会话缓存×批量消融已完成，方向3的优化前分析见[阶段总结](../reports/hdf5-route3.md#direction3-summary)。随后按用户确认完成GRIB定位提示及`format_catalog`轻量发现：[独立N128三轮](../reports/grib-discovery-reuse-20261022.md)中，两项合用的完整R相对同轮旧产品为0.712、原始完整T为0.717，保留C通路；单独提示B未证明稳定加速，所有原始慢记录保留。HDF5均摊结论及[状态复用细查](../reports/hdf5-route3.md#sharing-audit)中的V8/K16域驻留问题仍保留，本轮未修改HDF5、16域额度或并发。288条新记录和资源／实机恢复审计通过。**下文是2026-10-21停止重测时的历史问题清单；其数据库VFD、每次重开HDF句柄等状态不代表10-22的受管FS＋可选会话池实现。**

更新：2026-10-21。按用户修正后的问题推进；当前产品二进制保持不变。

**执行已按用户要求停止。** 原生重测第二轮停止请求时为 0 条正式样本，不能据此给出新的 N128/C128 原生 q 或 Rq。先重新规范实验框架，再决定重测。本篇保留代码审查结论及明确标注的历史数据；后续探针均为建议，未继续执行。

收尾已核验：r2 原服务恢复为 ASYNC/partial=true，配置字节 SHA 与实验前一致，DataNode PID 1036342、CPU 亲和性恢复 0–31，所有本轮 cgroup 已删除，`queries.jsonl` 未生成。r1 同样已验证恢复。未留下后台实验任务。

## 问题与执行顺序

| 编号 | 问题 | 当前动作 | 验收依据 |
| --- | --- | --- | --- |
| H1 | 使用 HDF5 原生文件内索引重测独立 N128 与 N512/B4/C128 的 q | 用户要求停止，暂无新有效样本 | 规范框架后再测；本轮不发布原生 q 或 Rq |
| G1 | GRIB 的 d 为何不符合单纯 O(log N) 预期 | 审查索引查找之外的工作与历史对照条件 | 源码复杂度、暖命中计数与严格同语料的最小探针 |
| Q1 | 数据库底座能否支持 q 的查询间并行与复用 | 审查线程、HDF5 锁、SDK 会话和租约 | 区分可重叠的工作与串行临界区，列出状态生命周期 |
| D1 | d 能否查询内并行与复用 | 审查候选域、证明、费用轨迹和可并行边界 | 保持权限、版本冻结、准入和请求累计预算语义 |

先规范实验框架，再得到 H1 正确引擎的实测并决定优化；G1/Q1/D1 仅给代码与已有证据分析。没有用新语料制造优化对照，也不以历史最小绝对毫秒替代本轮配对比值。

## H1：冻结的重测口径

上一轮[两核复测](../reports/resource-bounded-ratio-retest-20261021.md)中的 HDF5 使用 **`iotdb-indexed`**。其中 C128 的 q=5.473 ms、Rq=9.189 不能回答原生 HDF5 的性能问题，保留为独立历史记录。

| 变量 | 独立 N128 | N512/B4/C128 |
| --- | --- | --- |
| 实际发布规模 N／存储 V／D | 128／1／4 | 512／1／4 |
| 实际文件 S | 85,072 B（每文件） | 241,559–241,983 B |
| 候选域 | 全部 128 路径 | b00、b01、b05、b12 四个完整桶，共 128 路径 |
| 选择／命中 | Latest，2 个 dataset | Latest，2 个 dataset |
| dataset 形状 | 1×1×64×64 | 1×1×128×128 |
| 本次读取 | 全 dataset，1×1×64×64 | 原点 ROI，1×1×64×64 |
| 值／掩码交付 | Float32＋uint8，共 40,960 B | Float32＋uint8，共 40,960 B |
| IoTDB-FS 引擎 | `native-hdf5`，数据库精确版本字节经 QuerySource/VFD 交给 libhdf5 | 同左 |
| FS-index 原有执行器 | 保留 SQLite 连接；每次重开 H5F/H5D | 保留 SQLite 连接及 H5F/H5D；文件／dataset 各 8 个缓存槽 |
| FS-index 交付时钟终点 | C++ owned value/mask；SHA、JSON、pipe 在钟外；d 为 T 减累计取值子计时的残余量 | NumPy owned value/mask；连续切分 d/q |
| IoTDB-FS 交付时钟终点 | 含 IPC 的 NumPy owned value/mask，连续切分 d/q | 同左 |

两组并非只改变 N 的等价消融，尤其 FS 原生句柄缓存、交付端点、文件形状和发现 API 不同。因此分别报告同组 Rq，不把两组绝对差值直接归因于 N。N128 的 Rq 还包含 C++／NumPy 端点差异；C128 两端均为 NumPy owned 输出。本轮保持原基线以便追溯，不声称端点已完全对齐。

复用当前二进制 `36881bde02466fb65730188161c2f764e45d2b000c321901a191af722a468fc9`，不构建新产品版本。仅启用 `query.hdf5.mode=native-hdf5`；逐条核验实际 mode、原生 slab 和 QuerySource 读取计数，禁止回落到 IoTDB-FS chunk decoder。实验适配器的 `typed` 标签表示调用 API，不表示 `iotdb-indexed` 引擎。

继续使用已批准的隔离两逻辑 CPU，客户端／IoTDB-FS／FS-index／原 DataNode 共用；查询进程组限 2 GiB，数据库保留原固定堆。每点 2 对预热＋10 对测量，交替两端执行顺序。q 从发现结果就绪计到实际拥有值／掩码；同时保留完整查询时间。索引准备、独立 oracle 检查和 profile 收集不进入查询时钟。结束恢复原服务配置、CPU 与 cgroup。

N128 旧语料有值 SHA 而没有掩码 SHA。本轮保持已注册 corpus 不变，使用现有模型的 value-only case；计时外用原生读取补充两命中的掩码证据，绑定源文件 SHA、路径、dataset、形状与旧值 oracle。两端实际仍交付并计时值＋掩码，且必须通过补充掩码校验。补充证据 SHA 进入本轮运行身份，不声称已改变旧 corpus 或补齐历史的 value＋mask case。

### 待检验的解释

1. **数据库字节获取成本**：若主要在 VFD 同步读取，则 q 应伴随多次 QuerySource／SQL，且源读取阶段占比较高。
2. **原生状态未复用**：若每次重新打开 H5F/H5D，暖请求仍需重复读元数据；C128 的 FS-index 保留句柄会扩大 Rq。
3. **交付与安全封装成本**：若读值本身较小，控制请求、Arrow／mmap、拥有数组与精确版本校验仍构成固定差额。现有 inclusive stage 不能相加冒充独占耗时。

## G1：复杂度的比较对象

单次二分查找的纯 log 项有 log₂(128)/log₂(8)=7/3≈2.33；它不是完整发现 d 的预测倍数。还须固定 D、V、候选域、命中 K、谓词、预算轨迹规模、缓存状态及交付协议。

当前两核 candidate：独立 GRIB N128/V1 的 d=5.726 ms；大文件 N8/V8 选择性 All 的 d=7.807 ms。后者返回 16 个文件版本，前者返回 2 个，不能构成 N8→N128 梯度，当前两点也不支持“N128 的 d 更长”这一具体判断。

| 已有暖查询证据 | 独立 N128/V1 | N8/V8/大 S |
| --- | ---: | ---: |
| 候选文件版本数 | 128 | 64 |
| 候选 message 数 M（D=4） | 512 | 256 |
| 匹配源／发现结果数 | 2 | 16 |
| 发现 IPC 字节数 | 4,056 | 16,424 |
| source_open_calls／head_control_calls | 2／12 | 16／54 |
| 授权证明命中／SQL 调用 | 3／0 | 3／0 |

局部复合键索引在 [ImmutableEqualityIndex.h](../../include/iotdbfs/query/core/ImmutableEqualityIndex.h) 二分定位 equal range，在 [GribFormatQueryAdapter.cpp](../../src/formats/grib/GribFormatQueryAdapter.cpp) 遍历命中 postings 并按源去重，复杂度为 **O(log M＋K_postings)**，M 是候选域的 message 数，约为 C_file_versions×D。热全局域命中后，完整 d 仍执行以下工作：

| 工作 | 源码位置（2026-10-21 工作树） | 规模项 |
| --- | --- | --- |
| 返回 CatalogSelection 时复制 manifest vector；prepare 再复制快照 | `IoTDBStructuredBlobBackend.cpp:3316`、`QueryModule.cpp:1024` | O(C)，还取决于被复制字节数 |
| 注册构造全候选 source refs | `QueryRegistrationSocket.cpp:1591` | O(C) |
| 发布时无条件构造授权 std::set | `QueryRegistrationSocket.cpp:1416` | O(C log C)；read_context 路径可能不消费对应回调，值得单独测量 |
| 首次费用批次重放，逐身份查本请求 map 账本 | `QueryMetadataBudget.h:74` | O(F log U)，F 为轨迹项数、U 为账本身份数 |
| 读上下文 source refs 复制 | `TypedQueryService.cpp:149` | O(C) |
| 每命中源准备计划及文件内 message 筛选 | `QueryModule.cpp:1166`、`GribFormatQueryAdapter.cpp:1890` | 与命中源及其 D 有关 |

因此应审查的是“固定控制／IPC＋候选复制与集合构造＋费用重放＋复合键查找＋命中源准备＋输出”。缓存已经去掉大量目录读取／权限重验，并未消除所有候选规模项。上述代码存在不等于已经测得它们占主导；本轮不把 inclusive profile 相加作为独占耗时。

**下一步最小探针**：先在同一独立 N128/V1/D4/S 语料固定 K=2，以完整叶子目录构成 C16/32/64/128，明确标记 **N128/Px/Cx**，P 为 selector scope 项数。分别测 selection 复制、refs/set 构造、首次重放、索引 lookup、命中源 prepare 的独占时间。这验证 C 敏感性；若验证真正 N 梯度，另需相同 V/D/S/K/缓存及端点协议的独立 N8、N128 语料，不能把候选子集重命名为独立 N。

## Q1／D1：并行分析边界

分析区分查询间并行、查询内并行和跨请求状态复用。线程数增加不会自动减少工作；在两核资源约束下同时报告单查询延迟与吞吐，不能把吞吐收益冒充 Rq 降低。任何后续实现仍需保留独立请求预算、精确版本、权限主体、执行租约与提交边界准入。

### Q1：入口和数据库能并发，当前原生 HDF5 临界区串行

| 层次 | 现状与源码 | 对并行／复用的影响 |
| --- | --- | --- |
| 查询入口 | `QueryRegistrationSocket.cpp:53,1251,1310`：4 workers、16 pending | 普通查询可并发进入；训练准备锁不是所有查询的全局锁 |
| owner 准入 | `TypedQueryService.cpp:93,243`：取得执行 lease 后释放准入锁，结果发布前再次准入 | 远端读取不必持有整个提交锁；新的 execute／发布／open 仍须重新准入 |
| 原生 HDF5 锁 | `Hdf5SourceDriver.cpp:24,552,613,646`：进程全局 recursive_mutex 覆盖 open／describe／read | 不同查询、不同文件也串行进入原生调用 |
| 锁内同步数据库读取 | `Hdf5SourceDriver.cpp:249,782`：H5Dread 的 VFD 回调同步调用 reader | 临界区包含数据库读取等待，不能声称只串行解析／解压 CPU 工作 |
| 原生状态生命周期 | `Hdf5QueryKernel.cpp:423–450`：每 execute 新建 driver；raw_chunk_cache_bytes=0；`Hdf5SourceDriver.cpp:624,662`：describe/read 各开 H5D | 全局目录缓存热命中不等于 HDF5 文件／dataset／chunk cache 跨请求复用 |
| DB 会话 | `IoTDBClient.cpp:93,164,258`：独立 session 池，池锁只保护获取；结果持有租约到关闭 | 多 SQL 能跨请求并发，受连接数、结果释放和 DataNode 资源限制 |
| IPC | `TypedQueryService.cpp:166`、`FuseTypedQueryBridge.cpp:76,129` | 不同查询的编码／锁外物化可重叠；新注册不会自动复用旧查询答案 |

因此 **q 可以查询间并发，但当前 native-hdf5 不能靠增加请求线程获得原生读取并行**。它可能重叠锁外规划和交付，也可能增加锁等待；数据库本身具备并发能力不等于上层已经开放了并行。

不能直接把现有 driver 塞进全局缓存：`Hdf5SourceDriver.cpp:564` 捕获当次预算封装 QuerySource，另持有取消回调、累计 read trace 和执行状态。后续复用设计需拆分“精确版本的不可变原生文件状态”和“本请求准入／预算／取消／租约”，或提供独占借用及严格重新绑定；不能复用旧账本或旧授权。

在 H1 之后，最小并发探针只比较并发度 1／2，分别测同一精确源和两个不同源；记录完整延迟、Rq、吞吐、p95、锁等待、open/describe/read 与内存峰值。先定位状态重建和锁等待，再决定复用方案；本轮不删除全局 HDF5 锁。

### D1：先复用和减少工作，再考虑有界并行

| 已有跨请求复用 | 约束 |
| --- | --- |
| 候选域：`HotQueryCache.h:91–106` | namespace／完整 head 键，任意 head 改变保守失效；同键构建合并 |
| 属性索引：`QueryDomainCache.h:78` | 共享不可变派生状态；禁止持有 QuerySource、ReadContext 或请求账本 |
| 授权证明：`IoTDBStructuredBlobBackend.cpp:537–556` | 绑定 uid/gid/补充组与完整 head，同主体构建合并；命中仍费用重放并 owner 准入 |

普通发现和规划的多源 hydrate、seal 检查、prepare 当前主要顺序执行。冷路径存在独立数据库等待时，可以研究有界预取／批读；热路径已零 SQL 时，增加数据库并发并不能直接解决候选复制和费用重放。

费用批次首次重放持同一个请求账本 mutex 遍历整个轨迹，拆线程会争用同一锁。预算、trace collector 与 execution lease 使用 thread_local，新线程不会自动继承；collector 的 map 也不能交给多个线程无锁写入。若后续并行，需显式传递共享请求预算和 lease、使用子轨迹并确定性合并，保持粘性失败、错误优先级及全部候选授权先于内容访问。账本必须每请求独立。

GRIB 已有复用解码线程池、多 message 并行以及同内容 single-flight，见 `GribFormatQueryAdapter.cpp:635,1474,1496–1498`；外层跨源仍主要顺序消费。默认线程数按 hardware_concurrency 取 2–4，不能假定它自动识别两核 cgroup 配额。新并行方案必须合计客户端、daemon、DataNode 的两核总预算，先测并行度 1／2，避免重复建设或调度开销超过收益。

**推进优先级**：H1 原生实测 → 区分原生状态重建／VFD 等待／交付 → G1 的 C 规模独占探针 → 再决定原生状态复用或冷路径批读／并行。优化验收继续以完整查询和同组 R 为主，q/d 子项只用于解释。

## 证据与状态

- 历史结果、计时边界及输入哈希见[两核复测报告](../reports/resource-bounded-ratio-retest-20261021.md)。
- 两种 HDF5 正式模式见[模式契约](../contracts/hdf5-query-modes.md)。
- 原生重测目录：服务器 `experiments/cross_version/work/native-hdf-retest-20261021-r1`、`native-hdf-retest-20261021-r2`。r1 在正式计时前因新脚本导入顺序触发冻结 helper SHA 检查失败，0 样本；配置、服务与资源均已恢复，失败源码已按原 SHA 重建核验并留档。修正后离线 FS 烟测通过；r2 在 N128 挂载恢复期间被用户叫停，停止请求时同为 0 条正式样本。所有失败／停止目录保留，不计入性能结果。
- 新增产品版本：0；新科学语料／发布：0；重建 FS 索引：0。实验适配器保留供框架整理时审查，不再继续计时。
