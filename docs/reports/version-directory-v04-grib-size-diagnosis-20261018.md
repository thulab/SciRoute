# GRIB 8 MiB / 4 MiB 查询差异排查

日期：2026-10-18（Asia/Shanghai）。范围：复算已封存实验，检查真实代码，并在独立诊断产物中验证原生缓存；不修改生产实现或数据库配置。

结论：原 8/4 MiB 对比把**命中网格与返回数据量同时翻倍**，本就不满足“只扩大无关内容”的前提；此外，大档 All 的 16 个原生字段超过缓存容量，直接适配器诊断确认每次温热请求重新加载并驱逐 16 个字段。固定目标、仅追加无关合法 message 的对照则保持目标读取范围和结果不变。当前主要问题是工作负载解释、原生缓存抖动和大输出成本，没有证据显示温热查询因扫描整个原始文件而变慢。

## 问题与可重复信号

用户指出“查询时间理论上应对文件内大小不敏感”。需要区分文件中未命中的内容、命中 message 的网格规模，以及实际返回的数据量。原实验保持 N=8/K=2/D=4，但把每个 message 的网格从 720×721 改为 1440×721，查询没有空间 slice/crop，返回完整命中网格。因此它同时放大了命中 message、解码工作量和结果，不是固定查询工作量、只增加无关文件内容的对照。

离线复算程序 `experiments/cross_version/work/grib-size-diagnosis-20261018/verification/compare_sealed.py` 先验证原查询记录 SHA，再从两次运行各取 640 条 GRIB 观测，重算 64 个尺寸配对统计单元（含 first/warm），四轮各自保留；输出为同目录 `summary.json`。原结果没有覆盖或重新计时。

以 V04、single_unit、V8-All 温热合并中位数为例：

| 指标 | 约 8 MiB/文件 | 约 4 MiB/文件 |
|---|---:|---:|
| 每个命中 message 网格 | 1440×721 | 720×721 |
| 命中 message 数 | 16 | 16 |
| 返回 Float32 值数 | 16,611,840 | 8,305,920 |
| 实际 IPC 字节 | 66,850,392 | 33,412,568 |
| 完整查询 ms | 892.266 | 388.462 |
| 发现客户端 ms | 31.986 | 22.896 |
| typed cursor execute ms | 125.690 | 31.948 |
| typed IPC encode ms | 384.659 | 161.862 |
| 其中 array build ms | 178.857 | 92.694 |
| 其中 IPC write ms | 218.408 | 69.362 |
| 客户端 IPC 读取/owned copy ms | 154.883 | 66.476 |

各行为各自的中位数；encode 包含 array/write，完整时间也包含服务端与客户端活动。不能相加这些中位数、据此拼回总时间或给出因果百分比。两次运行的网格、namespace 组成和时间不同，差值仅为描述性定位。

## 假设及排查结果

1. **输出与内存工作量增大。** 预测：返回点数、IPC 字节、数组构建及客户端复制应增长；只有两个字段的 Latest 也应变慢。原始记录支持：所有匹配单元的返回点数严格翻倍，IPC 约翻倍；V8/single_unit/Latest 完整时间 131.325/51.334 ms，cursor 为 7.746/3.766 ms。Latest 两个字段均能容纳于缓存，仍存在明显尺寸成本，不能把全部差异归因于 All 缓存。
2. **All 原生解码缓存容量不足。** 预测：大档 All 会加载／驱逐原生字段，小档温热 All 可复用；编码消息缓存命中不能消除原生字段解码。原历史记录的大档 All 每条温热取值请求均有 16 次公共 runtime hits，小档为 0；两者的公共 runtime decode_count、payload 读取均为 0。该模式提供间接证据，下面用独立计数验证。
3. **整个大文件扫描／额外数据库读取。** 预测：若其为温热差异主因，payload 返回量或数据库读提交应增长，发现阶段应承担主要增量。原记录不支持：两尺度、两场景 V8-All/Latest 各 16 条温热请求的两条 SDK 读路径与 payload 字节均为 0；主要增长位于取值和输出路径。这里的 SDK 计数不是 wire RPC 或磁盘 IO 计数，不据此宣称启动／冷请求也零读取。

强对照也随返回工作量增长：single_unit/V8-All 的 FS-index 为 235.619/126.473 ms，V03 为 868.100/413.695 ms。现象不只存在于 V04；V04 相对 V03 的额外开销应另行评估，不能把全部网格规模成本称为 V04 回退。

## 真实实现边界

- [查询构造](../../experiments/cross_version/version_matrix/n8_matrix.py) 的 `iotdb_query` 对 GRIB 使用 `projection: grib.data.value` 和科学属性 predicate，没有传空间 slices。Latest 返回两个字段，All 返回两个路径的八个版本，共 16 个字段。
- [GRIB 适配器](../../src/formats/grib/GribFormatQueryAdapter.cpp) 的 `load_encoded_message`（约 L540）按 `message.off/message.len` 读取；[内核](../../src/formats/grib/GribQueryKernel.cpp) 的 `read_source_message` 对该范围检查和读取。V04 的底层批读再按命中范围定位固定块，不要求读取整个原始文件。
- `decode_cached_grid`（适配器约 L1002）先查原生字段缓存；miss 才调用 encoded-message loader 和 `decode_native_field`。后者在内核约 L504 使用 `decode_values(F64, {}, {})` 完整解码命中 message，然后由 `render_grid` 应用窗口／插值。**当前冷 ROI 也可能受整条命中 message 网格大小影响**，并非原生压缩流上的点级随机访问。
- 原生字段缓存默认 **64 MiB、16 fields**，按值／轴的 vector capacity、Entry、路径与费用轨迹计费；公共 runtime 的 256 MiB 编码消息缓存是另一层。缓存 key 含 namespace、path、version、revision、message range，同内容不同版本不会自动共用一份原生字段。
- 16 个小档字段的 Float64 值与轴合计 66,631,808 B（63.545 MiB），剩 477,056 B 容纳其他计费项；大档为约 127.002 MiB，明显超过 64 MiB。仅由 shape 可证明大档无法全部常驻，小档是否实际容纳还需计数／真实收费结果。
- `render_grid` 即使原生缓存命中，仍复制窗口值、转换 Float32、构建输出轴；[Arrow 编码器](../../src/query/encoding/ArrowResultEncoder.cpp) 随后构建 Arrow 数组并将 IPC 字节写入输出缓冲区。返回点数增大时，这些工作不会因 payload 内容共享而消失。

因此，合理的成本边界是“目录／索引定位 + 命中编码范围读取 + 缓存未命中的 message 解码 + 返回数组／IPC／客户端复制”，不是整个完整查询对所有字节数恒定。固定命中内容和输出时，应避免扫描无关 payload；命中网格／返回值数增长时，当前执行路径必须处理这些值。

## 独立诊断与证据

实验产物位于 `experiments/cross_version/work/grib-size-diagnosis-20261018/`，不会替换原完整查询计时。`grib-audit/build_diagnosis.py` 复制冻结 V04 r3 适配器，在独立产物中加入 `diagnosis.grib.native_field_cache.*` 计数；81 个 archive 成员仅 `GribFormatQueryAdapter.cpp.o` 改变，link map 和每成员摘要记录于 build receipt。缓存 key、Entry/CachedField 定义、容量、费用估计与驱逐逻辑不变；每 cursor 的原子账本从 decode worker 汇总到调用线程，结束快照 gauge 覆盖先前源 cursor 值，不累计成峰值。

独立诊断守护程序 SHA-256 为 `92f416f7107fd30f2526321f05c5c0b83252bada8f33d41fc93bb5d5a7dae0c1`，仅构建，没有成功启动 owner。随后同一诊断 archive 链接独立 `native_driver.cpp`，driver SHA-256 为 `0c7d991b68a72f2d0ff866990da61b337200aa8108969291cfbcedc60db490da`。它通过真实 Adapter 的 prepare → execute → cursor.next 驱动真实文件，使用原 64 MiB/16 fields 限额；每组 Latest 5 次、All 5 次，共三组 **30 条**适配器记录、**200 个来源结果**。每源返回点数、Float32 SHA-256 与 mask SHA-256 均对照原 manifest oracle；独立复核位于 `verification/direct-audit.json`。

以下为两个规模各 4 次温热 All 的每次事件，四次结果一致：

| 实际适配器事件／结束快照 | 8 MiB 档 | 4 MiB 档 |
|---|---:|---:|
| native cache hits | 0 | 16 |
| native cache misses / loads | 16 / 16 | 0 / 0 |
| native cache evictions | 16 | 0 |
| native 字段缓存保留 entries | 8 | 16 |
| native 缓存结束计费字节 | 66,590,552 | 66,641,544 |
| 字节预算 | 67,108,864 | 67,108,864 |

两档 Latest 的四次温热请求均为 hits=2、loads=0、evictions=0。成功 native load 的 loader 调用真实 `decode_native_field`，所以大档 All 确有反复原生解码，小档直接复用。这里是隔离适配器的直接计数；原完整查询只有公共 runtime 计数，不能给旧原始记录补造 native 指标。该诊断没有复现 V04 owner 的全部元数据费用，表中 resident 字节也不是生产总内存。两者事件模式与旧记录“大档 All runtime hits=16、小档为0”相互印证。

### 固定目标、扩大无关文件内容

使用同一 4 MiB 档 v8 原始文件，尾部追加大档中两条不匹配的合法 message，D 从 4 变为 6。保留原目标 message 的字节及 offset；不使用零填充或稀疏扩容。

| 单源指标 | 原文件 | 追加无关 message 后 |
|---|---:|---:|
| 原始文件字节 | 4,153,676 | 8,306,994 |
| message 数 | 4 | 6 |
| 目标 offset / length | 0 / 1,038,419 | 0 / 1,038,419 |
| 返回 Float32 点数 | 519,120 | 519,120 |
| 首次 Adapter payload 回调次数 | 1 | 1 |
| 温热重复新增 payload 回调 | 0 | 0 |

两源返回 value SHA-256 均为 `60c612637a74100ea6c598a23875bf2d0bdde8f32a74546903bb18c708d9cdf7`，mask SHA-256 也相同。QuerySource 严格拒绝任何不等于各自选定 message 范围的读取，30 条记录中没有触发；固定目标组中两个独立 source 各自首次读取该范围一次，之后累计读取数保持 1。这直接验证了适配器不为查询该字段而拉取附加尾部。

此控制包含原文件与派生文件两个 source、同一目标；没有把单源结果与原 16 源 All 总输出错误比较。QuerySource 在 setup 时完整装载 raw 字节、生成 layout 并校验 SHA，适配器执行时才记录 payload 回调，因此**不是物理磁盘／数据库的只读字节测量**。D、layout 与文件身份也变化，不能据此宣称整个发现／owner 恢复／IPC 的端到端时间恒定；driver 的计时还包括哈希，只用于诊断，不作性能门禁。

先前尝试复用原前缀进行只读 owner 探测，在挂载前的 head 检查退出，0 条查询、0 个 owner、0 次发布。原因是 Store/owner 的既有准入要求 SYNC，而原服务恢复后的实际设置为 ASYNC/true。保留 `probe-execution-status.json`、`probe/report.json` 和私有日志，不删除准入条件、不重启数据库。独立的 `SHOW ALL CONFIGURATION` 读取确认服务健康、原 PID 3967180 与配置／受保护文件摘要不变。

后续诊断直接使用同一 GRIB 适配器／cursor 和真实编码文件，在进程内提供 QuerySource；不创建数据库实例、不绕过 Store 准入。该测试隔离了 owner、数据库及 IPC，统计用于解释缓存行为，不能作为新的端到端性能结果。

服务前后只读核验通过：DataNode PID 均为 3967180，实际配置均 ASYNC/true；配置 SHA-256 为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，许可证／节点身份／受保护配置摘要不变，没有本轮 owned mounts，没有初始化、发布或数据库停启。

## 后续修正边界

本轮只检查并补充证据，没有修改生产缓存策略、加大缓存、改变持久协议或接受新的性能优化。原“4/8 MiB”结果应称为**命中网格与完整输出规模实验**，不能作为“仅文件无关内容增大”的结论。后续若评估大小敏感性，应固定目标 message、返回区域、路径／版本数量和缓存状态，再改变未命中内容；若评估冷 ROI，则必须单独记录命中 message 的完整解码成本。缓存复用和输出拷贝的改动仍须以完整查询计时验收，不以本次适配器微测量替代。

原始完整查询来源和完整计时边界见 [大文件实验报告](version-directory-v04-large-file-upper-20261017.md)。其两个证据 archive SHA-256 分别为 `01bfeda4996b2b6da4fadf68d91f5c15cb5c429a440baf42e69547bf4000f4e3`（8 MiB 点所属主矩阵）与 `8d953975f365438bcf3b962dd0bdb94a225ee806975b7ee9d971e813178e6405`（4 MiB 点），原始观测及既有结论保留。

本轮诊断证据 `evidence.tar.gz` 共 39 个清单文件，522,542 B，SHA-256 `5f9588c7f29534e7dd503b3e7317e0b602197891cbbb12400ed3830d141798cc`；清单 SHA-256 `bb3359f5c866d562a395f0a5b24cddf7cfc910f1fea2599767691385c5cde928`。构建参数、唯一 source diff、81 个 archive 成员摘要、link map、真实数据／派生尾部身份、30 条记录、离线复算、失败探测及服务前后回执均保留；原始科学文件和完整二进制留在服务器，不纳入压缩包。39 个清单文件均已独立安全解包核验 SHA 与字节数。本轮不改变生产策略，后续优化仍须用完整查询计时验证。

2026-10-18 后续[内容共享解码缓存修复](version-directory-v04-grib-native-cache-fix-20261018.md)按用户确认的内容身份、独立计费、64 MiB 有界两队列与 singleflight 实现，并另行记录新旧完整查询 A/B。这里保留诊断时的旧实现与观测；不能把新缓存计数替换进旧结果，或将消除重复解码理解为消除每版本输出成本。
