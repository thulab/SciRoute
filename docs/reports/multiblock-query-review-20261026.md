# 同文件多payload块查询：机制核对与优化顺序

日期：2026-10-26。内部实现审查，尚未实施新的多块优化。

当前实现已经对**一次调用**涉及的payload块去重，并批量读取；新加入的跨请求共享仅覆盖`missing.size()==1`。因此，大文件场景的主要缺口是不同调用之间重复读取相同块，以及scalar分片后的重复块与额外结果复制。不同查询访问互不相同的块，本来就应该各自读取、允许独立SQL提交；不能把所有负载的优化目标都定义成“一次SQL”。

以下计数来自真实`Owner`和`PayloadArtifacts`配合内存`ProbeStore`的本机机制探针，10项全部通过。**这里没有连接IoTDB，不是数据库性能测试**；不能据此报告SQL执行耗时、吞吐、DataNode内部并行度或GRIB解码收益。

## 1. 证据范围

探针建立一个17 MiB不可变文件：17个1 MiB块，第`i`块填充字节`11+i`，17个content ID均验证不同。成功输出按原始range顺序逐字节比较，避免“重复填充块意外被内容去重”掩盖真实情况。并发场景用条件变量控制Store入口，不靠sleep决定顺序。

计数仅包含fixture建立完成之后的payload `Store::get_objects`调用，排除metadata、发布和fixture读取。表中的MiB均为1,048,576 B；“块引用数”包含重复物理取回，“不同块数”按content ID去重。峰值是包含条件变量等待的模拟Store API同时在场数，不是CPU或数据库执行并行度。

可追溯材料：

- [原始结果](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/probe-results.json)，SHA-256 `1eee241103887245e07f770175e08803f6a9a8f1eb2bb9d32369a1621c6145ff`。
- [运行回执](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/probe-run-receipt.json)：10项、0失败、0错误、无跳过，未修改产品源码、未进行远程操作或服务变更。
- [构建回执](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/probe-build-receipt.json)、[编译输入清单](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/probe-source-manifest.json)、[源码切片参考清单](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/probe-reference-source-manifest.json)。运行后已重新核对编译使用的16文件依赖快照。

## 2. 十项机制结果

下表的batch是Store调用批次；只有实际IoTDB Store执行时，才对应其对象批量SQL路径，探针本身没有执行SQL。

| case_id／场景 | 请求输出 | Store调用数 | 每批块数 | 块引用／不同块 | 实际取回 | Store峰值 |
|---|---:|---:|---|---:|---:|---:|
| `one_read_four_blocks`：一次读4块 | 4 MiB | 1 | `[4]` | 4／4 | 4 MiB | 1 |
| `one_scatter_read_twelve_blocks`：12个跨块1 B range | 12 B | 2 | `[8,4]` | 12／12 | 12 MiB | 1 |
| `twelve_mib_output_limit`：一次连续请求12 MiB | 12 MiB，返回`Limit` | 0 | `[]` | 0／0 | 0 | 0 |
| `four_queries_same_four_blocks`：4调用同时读相同4块 | 合计16 MiB | 4 | `[4,4,4,4]` | 16／4 | 16 MiB | 4 |
| `four_queries_partly_overlapping_pairs`：`AB/BC/CD/DE` | 合计8 MiB | 4 | `[2,2,2,2]` | 8／5 | 8 MiB | 4 |
| `two_byte_boundary`：块边界两侧各1 B | 2 B | 1 | `[2]` | 2／2 | 2 MiB | 1 |
| `one_call_duplicate_overlapping_ranges`：一次调用4个重复／重叠range | 320 B | 1 | `[1]` | 1／1 | 1 MiB | 1 |
| `four_calls_duplicate_overlapping_ranges`：上述range分成4次顺序调用 | 合计320 B | 4 | `[1,1,1,1]` | 4／1 | 4 MiB | 1 |
| `singleton_and_overlapping_two_block_batch`：单块A与双块AB同时读 | 2 MiB＋4 B | 2 | `[1,2]` | 3／2 | 3 MiB | 2 |
| `unaligned_scalar_two_eight_mib_windows`：offset=1，长度16 MiB的切片序列 | 合计16 MiB | 4 | `[8,1,8,1]` | 18／17 | 18 MiB | 1 |

所有场景`outputs_verified=true`，结束时active flight及retained shared bytes均为0。本表所有joined计数均为0：多块批次绕过singleton共享；顺序调用也没有重叠的在途生产者。部分singleton场景会计入leader，但这不代表发生了合并。

最后一行有明确覆盖边界：探针按源码实际切片顺序，执行了两次真实`PayloadArtifacts::read_ranges`，共用累计metadata预算；**没有直接驱动`StructuredBlobReadHandle`、查询frontend或一次完整16 MiB查询**。两次range分别为`[1, 1+8 MiB)`和`[1+8 MiB, 1+16 MiB)`，分别触及9个块，公共边界块被取回两次；总共18个块引用、17个不同块。它证明该切片序列的机制成本，不能替代完整入口验证。

## 3. 已确认的实现行为

**一次调用内部已有去重和batch。** `PayloadArtifacts::read_ranges`选择与ranges相交的extents，保留每个输出的拷贝描述；`visit_blocks`再按content ID建立去重map，按最多64对象、8 MiB封装批次并同步依次执行。正常1 MiB整块受8 MiB字节上限约束，因此每批最多8块。批次排序依据content ID，不保证文件offset顺序；输出仍按原range顺序还原。[块去重与同步批次](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/PayloadArtifacts.cpp:142)、[输出拼装](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/PayloadArtifacts.cpp:237)、[上限定义](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/include/iotdbfs/storage/versioning/VersionDirectory.h:55)。

“一次输出最多8 MiB”与“一次最多触及8 MiB物理块”是两件事。12个分散的1 B输出总长只有12 B，合法地触及12 MiB物理块，并按`8+4`顺序读两批；单次连续输出12 MiB则在供数前触发`Limit`。物理需求另受`kPayloadRequestBytes`与extent数上限约束，不能把输出限制当作物理需求限制。

**跨调用的多块共享尚不存在。** Owner仅在`payload && !force && missing.size()==1 && coalesce_payload_reads`时使用flight。payload块不进入普通metadata resident cache；同一个4块集合被4条查询读取，仍是4个batch、16 MiB取回。单块A和多块AB也不会自动合并共同的A。[Owner分支及验证](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/VersionDirectory.cpp:606)。

**顺序窄读没有resident payload复用。** 新flight在生产者完成时移出查找表，只服务同时等待的调用，输出保留时间不等于后续调用仍可命中。因此四次顺序窄读同一块仍取回4 MiB；这是当前“只共享在途读取”契约的结果，不是flight失效。

**scalar存在额外输出复制。** pinned reader标量路径先`reserve(length)`，每个最多8 MiB分片通过`read_payload_ranges`生成独立结果，再`insert`到最终vector；即使只有一个分片，也会复制整个输出。backend Owner分支有相似拼接代码。[ReadHandle标量路径](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/StructuredBlobStorage.cpp:482)、[backend路径](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/IoTDBStructuredBlobBackend.cpp:2455)。这是已确认的复制机会，尚无其耗时占比测量。

**预算、校验和取消属于原路径的组成部分。** `read_ranges`在块去重之前按原ranges计算物理需求及extent需求，并调用逻辑计费；映射页的metadata身份包含namespace、incarnation、source和payload root。每批还保留完整块、SDK结果副本与摘要frame的临时预算，当前为`4*bytes`。Owner先检查ready及namespace，完整验证Store返回数量、每块size与digest，完成后再检查ready。真实IoTDB Store自身另验证行身份、重复／缺失行、长度及digest，并只在成功结束后将reader标记可复用。[原始需求计费](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/PayloadArtifacts.cpp:237)、[source限定的metadata费用](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/PayloadArtifacts.cpp:80)、[Store完整批验证](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/IoTDBVersionStore.cpp:470)。

## 4. 与实际格式查询的对应关系

GRIB message通过`read_source_message → source.read_payload(off,len)`读取；解码器的reader回调也调用`source.read_payload`。因此message跨多个1 MiB块时，本次审查的块放大、batch和scalar拼接问题适用；此前33,484 B文件的“四message共享一个块”结果不能外推到大message或大文件多块集合。[GRIB入口](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/formats/grib/GribQueryKernel.cpp:440)。

HDF5 indexed路径的range reader已经排序、合并重叠／相邻spans，按字节和range限额分批；有multi-range能力时调用`read_payload_ranges`。不能再把“增加range合并”当作从零开始的新优化，也不能为本任务开启其已有的多read并行选项来掩盖重复读取。[已有merge/batch](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/formats/hdf5/Hdf5RangeReader.cpp:115)。

正式NativeHdf5取值走`open_local`及受管本地文件handle读取，绕过Owner payload块供数；此处优化不能直接声称改善该路径的数据读取或解决其原生库争用。其版本／元数据操作仍可能涉及其他通道，应分别记录。[NativeHdf5选择](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/formats/hdf5/Hdf5QueryKernel.cpp:445)、[本地handle读取](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/formats/hdf5/Hdf5SourceDriver.cpp:905)。

## 5. 建议的优化顺序与语义边界

| 顺序 | 候选优化 | 预期改变 | 必须保留或先明确的条件 |
|---|---|---|---|
| 1 | scalar单分片结果直接move | 减少一次完整输出vector分配／复制；不改变Store调用数 | 保留range检查、共享metadata预算作用域、返回数量／长度验证及原取消检查；覆盖pinned和backend两条路径。还须测试分配失败与错误转换，不能只核对正常输出 |
| 2 | 完全相同multi-block batch的在途共享 | 同时请求同一完整集合时减少重复batch及字节 | 共享前完整验证整个batch；身份包含namespace/incarnation/type/protocol/content ID/size及集合顺序；结果按各自原顺序返回。保持失败隔离、首错规则、各请求取消、保留字节上限及临时reservation pin |
| 3 | 一个原始scalar／request范围内，有界复用已验证块 | 减少切片边界和回调窄读的重复供数 | 明确定义request生命周期、上限和pin；保留每次原逻辑charge、ready／namespace检查、取消检查及force绕过。先定义成功已验证数据复用的错误契约，再实现 |
| 暂缓 | `AB/BC`部分重叠集合拆分、跨任务拼大SQL | 可能减少共同块取回 | 不能直接继承当前语义；可能增加SQL、改变batch原子可见结果、读取／错误顺序、排队和预算，需另立方案 |
| 低优先 | 摘要计算流式化或减少digest framing临时复制 | 降低hash相关复制或暂存 | 先测hash／copy独占成本；保留Store与Owner双层验证，不直接删除Owner验证 |

推荐第一步是单分片move，范围最小，便于通过实际入口验证；它不需要客户端worker、查询内并行或IoTDB引擎修改。若重点继续消除跨查询物理重复，可先扩展完全相同batch的在途共享；顺序调用的重复读取则需先明确request内复用契约。它们分别覆盖不同负载，不能混称为通用块缓存。

request内复用不能直接挂到prepared `shared_source`上并称其“查询私有”。`QueryExecutionContext`显式区分request预算包装的`source`和供共享生产者使用的`shared_source`；执行时后者来自prepared source，可能跨执行复用。[生命周期约定](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/include/iotdbfs/query/core/QueryPlanning.h:260)、[实际包装关系](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/query/core/QueryModule.cpp:1495)。如需复用，应引入明确绑定一次原始请求的上下文，不能借共享source扩大缓存作用域。

缓存命中不自动意味着“所有错误语义保持”。在不可变且已完整验证的同一内容身份下，可以继续独立执行版本／授权能力、逻辑预算、取消和ready检查；但若命中跳过一次物理读，该次物理访问本来可能出现的瞬态I/O错误，或底层被破坏后的再次检测，也可能不再发生。必须明确契约允许复用已验证成功内容；force／深度验证仍真实读并验证。若要求逐次物理错误可观察性完全一致，就不能采用会跳过该物理读取的resident缓存。

相同batch共享也应沿用已审查的错误隔离原则：生产者失败／取消不能直接作为其他查询的异常重放，存活等待者应保有自己的独立首次读取机会；不发布部分验证结果。保持顺序执行原批次，避免改变当前首个失败终止后续批次的行为。不得将`AB`拆成两个singleton SQL再宣称共享更充分：它先把一次batch变成两次SQL，可能抵消全部收益。

不建议直接把scalar分片改成按1 MiB块边界对齐。原始`offset=1,16 MiB`切片的逻辑需求、metadata预算和检查顺序已经存在；改变切片形状会改变累计逻辑计费和可见的失败位置。若目标是保留现有语义，应先复用已验证公共块，并保留原分片的逻辑需求记录，而不是重写分片计划。

## 6. 后续验证矩阵与观测

下一步仍应先做机制正确性，再做同产品开关的真实IoTDB对照；本报告不新增性能结论。

| 场景 | 当前应保持的参考行为 | 优化后的判定重点 |
|---|---|---|
| 四调用各读不同单块 | 4次Store调用、4 MiB、4个不同块 | 继续独立推进，不将合法独立SQL误判为冲突；不承诺变1次 |
| 四调用同时读相同4块 | 4次batch、16 MiB、4个不同块 | 若实现完整batch在途共享，受控重叠下可验证1次batch／4 MiB；无重叠不承诺命中 |
| `AB/BC/CD/DE` | 4次batch、8 MiB、5个不同块 | 第一阶段允许保持原状；不靠拆成8次singleton实现“去重” |
| 边界2 B、12个scatter 1 B、连续12 MiB | 分别取回2 MiB、两批12 MiB、提前`Limit` | 输出顺序／内容、需求预算与错误代码一致 |
| scalar offset=1,length=16 MiB | 实际入口尚待补齐；切片机制为4批18 MiB | 直接驱动ReadHandle/frontend核对；move不改变计数；request复用才可能降低重复边界块 |
| force／深度验证 | 不复用普通flight或resident命中 | 真实读与完整校验仍发生；不合并验证范围 |
| 批次缺失／重复／乱序／错误size或digest | 完整批验证，拒绝损坏响应 | 不发布部分结果；每条查询收到自身允许的错误；不把生产者异常传播给其他查询 |
| leader取消、waiter取消、预算不足、namespace冲突、Owner冻结 | 各自检查和资源释放 | 无跨请求取消污染；所有flight／pin／reader槽最终释放；多批首错顺序不变 |
| 单块A与多块AB同时读 | 当前2次调用、3 MiB、2个不同块 | 明确是否支持混合共享；未支持时保持原路径，无等待环 |

需要分别记录逻辑range数／字节、候选extent数、去重前需求、去重后content ID数、物理batch数／大小／字节、flight leader／join／bypass／fallback／等待及retained峰值。时间上分离map/page准备、临时预算预留、Owner验证、scalar拼接copy，以及已有owner物理读取的连接获取、execute API、首SDK行、consume、close和release区间；嵌套时钟不能相加冒充独占耗时。TLS profile可能被共享生产者清空，真实物理次数应以独立owner trace或Store实计为准。

## 7. Claude Opus英文只读审查与复核

已调用本机Claude Code，以英文prompt审查20文件策划源码快照。CLI请求别名为`opus`；成功响应的实际`modelUsage`键为`claude-opus-5-5`。成功39轮、577808 ms，permission_denials为空；仅开放Read／Glob／Grep，未开放MCP服务。两次先前沙箱调用因登录不可用失败，不计为审查结果。输入、输出与当前源码hash已复核一致。

- [英文原始review](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/claude-opus-review.en.md)
- [逐项复核与纠正](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/claude-review-assessment.zh.md)
- [审查回执与hash](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/claude-review-receipt.json)
- [英文prompt](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/prompt.en.txt)、[20文件快照清单](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-review-20261026/source-manifest.json)

其关于多块绕过、跨片边界重复、双层hash／framing复制和观测缺口的判断与本地源码／机制结果一致。但不能直接采纳其O5“部分重叠lead-or-join绝不增加SQL”的承诺：原始[B,C]的一次SQL，可能变为先读取自有C成功、再因外部B生产者失败而回退读取B的两次SQL；即使fallback合批也不能消除这个反例，错误顺序亦需另行设计。因此本报告将完全相同batch共享与部分重叠拆分区别处理。

英文review提出的对齐切片、无joiner直接move和HDF5异步TLS问题也都有适用条件，详见复核；未经本轮验证的推测不作为已确认缺陷或性能收益。审查快照未包含随后完成的本地10项机制结果；其“暂无多块证据”表述仅针对当时快照。
