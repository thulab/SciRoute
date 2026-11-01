# GRIB typed 查询的内存缓存

GRIB typed 查询使用三层格式／runtime缓存，并可复用owner的跨格式候选热域。挂载初始化时创建一个 GRIB Adapter 和一个
公共 QueryRuntime，后续请求及并发请求复用它们；缓存不跨独立挂载进程持久化。

| 缓存层 | 保存内容 | 默认保留边界 | 作用域／身份 |
|---|---|---|---|
| parsed-index | 已解析的 message 目录和属性 | 64 MiB、65,536 条 | Adapter；优先使用已验证 layout 内容身份 |
| native field | canonical Float64 网格值和轴 | 64 MiB、16 字段 | Adapter；同 namespace 的相同编码内容共享 |
| encoded message | 实际编码 message 字节及摘要 | 公共 runtime 默认 1 GiB；挂载默认 16,384 条 | mount 的公共 QueryRuntime；精确源和 message 范围 |

`query.cache.*` 控制公共 runtime，不控制前两层。公共 runtime 还服务其他格式，
并非 GRIB 独占。实现入口见 [挂载初始化](../../src/main.cpp:1034)、
[GRIB Adapter](../../src/formats/grib/GribFormatQueryAdapter.cpp:1715) 和
[runtime 默认配置](../../include/iotdbfs/query/core/DecodedFragmentCache.h:24)。

## Parsed-index 容量和身份

挂载配置使用正整数字节／条数：

| 配置 | 默认值 | 含义 |
|---|---:|---|
| `query.grib.index_cache.memory_budget_bytes` | 67,108,864 | 已解析目录的估算保留内存上限，64 MiB |
| `query.grib.index_cache.max_versions` | 65,536 | 已解析目录保留条目数保护上限 |

字节预算控制主要保留规模；条目数上限用于限制极小条目的总数量。旧默认
16 版本会让超过 16 文件的连续候选遍历反复淘汰随后需要的目录，因此提高为
高保护上限，并由挂载配置显式传入 Adapter。C++ 构造配置任一上限为零仍表示
禁用保留；挂载属性沿用正整数校验，零或格式错误会导致配置验证失败。

每次查缓存前先调用源的 `verified_layout_identity()`。返回已验证内容身份时，
缓存键使用该身份、payload/layout 大小及格式／索引协议，允许相同 layout
跨精确版本复用；未提供内容身份时，键退回 storage namespace、精确
path/version、manifest revision、payload/layout 大小及格式／协议。
验证失败直接失败，不以已有缓存替代。已发布版本不可改写，删除后的 key 永久退役。
新查询仍通过 catalog 冻结完整候选、检查权限并经过owner准入；受支持的热域可仅为实际匹配源打开 source。
身份及加载逻辑见 [parsed-index 实现](../../src/formats/grib/GribFormatQueryAdapter.cpp:63)。

缓存按 LRU 淘汰 ready 条目，以同时满足条数和字节上限；同一身份的并发加载
共享 future，失败结果不保留。单个目录大于预算时仍可查询，但不保留该目录。
字节数沿用目录／字符串及条目开销估算，不代表整个查询进程的 RSS 上限，也
不包含输出、外部请求持有的数据和加载中的临时内存。

## Native field 与 encoded message

| 配置 | 默认值 | 含义 |
|---|---:|---|
| `query.grib.decoded_cache.memory_budget_bytes` | 67,108,864 | native field 及其管理数据的估算保留上限，64 MiB |
| `query.grib.decoded_cache.max_fields` | 16 | 含 pending flight 的字段条数上限 |

native 内容键由协议 `grib-native-f64/2`、namespace、message 编码长度及实际读取、
验证的编码字节 SHA-256 组成。源 alias 则绑定精确 path/version、revision、
payload/layout 大小及 message offset/length；新源首次绑定仍须读取实际字节。
不同版本内容相同可以共用网格值和轴，但版本身份、费用和输出仍分别处理。

字段采用 2Q：首次进入 probation FIFO，第二次驻留访问或 ghost 命中进入 protected
LRU。同内容加载通过共享 future 合并，失败不保留。alias 和 ghost 各最多 256 项；
字段、alias 的费用轨迹、ghost、pending 管理共同占用上述 64 MiB。
不能腾出预算时旁路，超大字段不常驻；临时解码及 cursor 输出另按执行模型管理，
因此该预算不等于 RSS 上限。见 [内容身份及费用](../../src/formats/grib/GribFormatQueryAdapter.cpp:220)
和 [两队列及容量](../../src/formats/grib/GribFormatQueryAdapter.cpp:553)。

encoded-message 缓存同时服务 BinaryView 与 DenseGrid。键绑定 namespace、
精确 path/version、revision、payload/layout 大小和 message 范围；加载时计算摘要。
已驻留 native field 命中无需重新 hash 或调用 ecCodes；仅 alias 仍在、字段已淘汰时
仍可能重新解码。alias 仍保存原始
`QueryMetadataTrace` map，每次消费重放本源费用；这层alias自身没有使用
`PreparedQueryMetadataTrace` 批次复用；新增公共候选域／授权／规划费用使用不可变批次，两者需分别看待。见 [alias 结构](../../src/formats/grib/GribFormatQueryAdapter.cpp:346)
和 [encoded-message 加载](../../src/formats/grib/GribFormatQueryAdapter.cpp:728)。

## 温热查询仍执行的工作

显式owner路径下，GRIB与HDF5共用原`HotQueryCache`：合计64 MiB、16个域、64个主体证明，包含构建与退休pin。GRIB全候选选择、head身份和授权证明通过同一backend机制复用；格式语义索引仍在GRIB Adapter。索引的排序数组与二分范围查找由格式无关的`ImmutableEqualityIndex`提供，借用字符串由不可变parsed-index持有。

精确版本取值可使用同一驻留发现域下的轻量子视图，每个父域最多64个，所有子视图字节仍占该64 MiB池，不新增完整域名额。仅当父域覆盖全部请求精确源、格式一致且完整head相同时才复用；首次子视图仍走原选择并捕获其自身费用，核对源及祖先记录后共享快照。未命中、子树、覆盖不足或缓存额度不足沿用原有路径。子视图有自己的候选、依赖、权限证明与账本重放，不能把父域权限作为永久授权。

完成的typed结果可持有独立计费的轻量ReadContext，保留原来的全部冻结候选（包括谓词未命中项）、祖先、依赖与权限费用，释放发现域及派生索引引用。新open继续实际准入；不能因结果已物化而跳过撤权／删除检查。原规划context随publication cleanup在最终准入门释放之后销毁；可选轻量化失败时保留原context。此机制也用于HDF5两种查询模式，性能验证与原生会话／解码缓存分别记录。

V≤32的后续回收将子视图、Proof、费用summary及派生Entry接入同一字节压力路径，保留各自执行pin的原计费生命周期；结果引用共享可退出Proof槽，原证据和重新准入规则保持。Proof数量满时直接旁路新的可选Proof，避免按版本循环扫描轮换整个热点集合；字节压力和owner退休仍可回收。详细额度、锁外析构、准入取舍和计数边界见[统一压力回收契约](version-directory-content-sharing-v1.md#multiversion-reclamation)。

首版热语义索引只认证六字段非null等值AND的metadata发现：`shortName`、`typeOfLevel`、`level`、`step`、`validityDate`、`validityTime`；无file_predicate、slice、插值、dtype override或physical_array。metadata投影、record完整性、限制与错误规则由Adapter认证。任何未认证域／请求继续普通规划；缺失字段的非匹配记录不能被索引隐藏。可选索引构建仅取得仍已缓存的解析artifact，不因缓存淘汰而补读布局。

GRIB温热完整查询仍需：

1. 取得同完整head的冻结候选域，按uid/gid/补充组使用覆盖全部候选的授权证明，每次execute／发布／新open仍实际准入。新head保守失效，旧计划保留原版本。
2. 重放全候选的选择、授权与布局／metadata费用，保留原请求账本的max/delta、预算失败粘性和收集器语义。首次费用重放仍随证据数量增长。
3. 受认证的metadata发现对复合键二分查找，按原source/message顺序返回匹配源及message ordinals。请求内不透明提示携带parsed-index共享引用、完整谓词及源身份；prepare重新观察verified layout身份并核对namespace／版本／revision／大小、谓词及限额后，直接使用原postings，不再查询解析缓存或逐message扫描。不保存PreparedQuery、ROI结果或客户端数组。无匹配时仍通过首源验证并生成空结果schema；提示失配走普通有界规划。
4. 取值阶段为每个命中源预留逻辑payload并查询native field，再按当前ROI、dtype或插值生成输出，完成编码、IPC和客户端复制。DenseGrid／BinaryView保留普通逐源规划；当前分离的取值RPC本来仅包含命中源。

见 [message 选择](../../src/formats/grib/GribFormatQueryAdapter.cpp:1147) 和
[缓存取值／渲染](../../src/formats/grib/GribFormatQueryAdapter.cpp:1359)。native miss
仍完整解码命中 message 为 Float64 后再应用窗口，见
[解码内核](../../src/formats/grib/GribQueryKernel.cpp:462)。缓存不保存 ROI 查询答案。

## 单 message 的执行位置

DenseGrid cursor 筛选后仅有一个 message 时，冷加载及热命中的取值／渲染直接在
调用 `next()` 的线程执行；经公共调度器提交的请求即使用公共查询 worker，不再
为该 message 向 GRIB 内部 decode executor 提交任务。判断依据是当前计划选中的
message 数量，而非文件总 message 数量。多 message 的预取、顺序交付及无 permit
时的同步回退沿用原行为；本路径不拆分 message 或增加查询内并行。

直接执行仍使用 cursor 创建时捕获的请求账本（允许为空）和 execution lease，
临时隔离调用线程的 metadata trace、profile 和 storage-read control，退出时恢复；
共享 producer 仍通过独立源及 runtime 资源控制读取。每次消费，包括缓存命中，
仍预留逻辑 payload 并重放本精确源的费用，不以缓存替代版本校验、预算或准入。
取消在 cursor 入口及单 message decode 前检查，已运行的共享生产者可继续完成，
不污染其他等待者。原失败状态直接传递；抛出的标准／未知异常继续按原
`future.get()` 路径转换为 `Internal`／`Execution`，保留原错误消息及当前源归属。
见 [单 message 上下文](../../src/formats/grib/GribFormatQueryAdapter.cpp:1431) 和
[执行分派](../../src/formats/grib/GribFormatQueryAdapter.cpp:1574)。

## Profile 指标

请求的 `counters` 增加以下同步事件计数：

- `grib.index.cache.hits`：找到 ready 目录。
- `grib.index.cache.joins`：加入已有加载；不表示测得了阻塞时长。
- `grib.index.cache.misses`：创建一次目录加载。
- `grib.index.cache.loaded_bytes`：成功加载目录的估算字节数之和，包含后续被淘汰的目录。
- `grib.index.cache.evictions`：淘汰 ready 条目的次数。
- `grib.index.cache.version_limit_evictions` / `byte_limit_evictions`：淘汰时违反的上限；
  同时违反两种上限时两项都增长，不可相加视为总淘汰数。
- `grib.index.cache.disabled`：禁用缓存后直接调用 loader 的次数。

这些是请求事件计数，不是全局驻留量 gauge。backend 的 `layout_calls` 与
`layout_returned_bytes` 继续记录实际取数。工作集可保留时，首遍有 N 次 miss，
普通逐源路径后续遍历应有 N 次 hit、零 layout 读取；全局语义域命中后仅为 K 个匹配源消费请求内定位提示，并保留verified layout的原费用观察。容量不足时普通路径仍会出现合法淘汰和重读。

- `grib.domain.message_hint_hits`／`message_hint_units`：当前请求成功消费的源提示／message数量。
- `grib.message.predicate_evaluations`：普通prepare扫描的message数量；提示命中部分应为零。

## 直接交付精确定位

显式控制操作`kind=format_catalog`复用现有`request.sources/subset/limits`。首版要求Listed单Adapter、排除byte passthrough；GRIB仅支持单`grib.message.offset`投影、LogicalValues且无physical_array、slice、插值或dtype override。其他投影仍由typed查询承担；不隐式回退协议。空候选也先校验Adapter的定位能力。

返回`{"ok":true,"kind":"format_catalog","units":[...]}`。每项为`path`、`version`、`format_id`、`revision`、`unit_id`、`offset`、`length`；GRIB身份为`message:<offset>:<length>`，公共层不解析该身份。源与message顺序保持原typed offset结果顺序；length来自验证后的layout。

发现先授权全部冻结候选，取得执行租约，在提交门外准备和遍历定位；交付前仍按完整依赖重新准入，包括空结果。公共层拒绝残余谓词／slice、非法或重复unit、越过payload的范围，遵守请求行数、累计metadata费用和1 MiB控制帧限额。没有Adapter execute、payload读取、Arrow结果流、发布路由或定位FUSE读取。后续取值仍独立授权与准入，客户端定位不是访问凭证。main复用原QueryModule及GRIB Adapter。

DenseGrid 还报告 `grib.native_field_cache.*`：hits/misses/loads、alias 命中、
hashes、content_hits、evictions，以及结束时的 resident_bytes/entries、aliases、ghosts。
事件计数与驻留 gauge 应分别解释；BinaryView／MessageRows 不增加该 native 计数管理。

接入公共热域之前的基线与FS-index对照见
2026-10-20性能检查 (local archive: `docs/reports/grib-current-cache-performance-20261020.md`)。
