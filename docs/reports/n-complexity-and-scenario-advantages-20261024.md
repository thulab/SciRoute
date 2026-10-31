# IoTDB-FS 对比 FS-index：N复杂度、处理系数与场景优势总结

2026-10-24。基于当前源码、冻结构建及已验收实验的总结；本轮不修改产品、不新增性能实验、不改变服务或科学记录。

**当前没有证据支持“数据库天然把FS-index的O(N)查询变成O(log N)”这一总体结论。** 两侧已有跨文件属性索引，q也只处理命中单位。IoTDB-FS已消除部分自身重复工作，并有GRIB解码缓存、不可变内容复用、集成版本发布等具体能力；其中一些能降低实际工作量，但尚未形成相对当前FS-index的普遍完整查询优势。写场景尤其需要计入写后首次新鲜查询的重建成本。

本报告将证据分成三层：**已实现机制**（代码／计数能证明减少了什么）、**实测时间优势**（同窗口、同输出的d/q/T是否更快）、**潜在场景优势**（条件成立时有理由预期，尚未完成等功能对照）。机制成立不自动证明端到端更快。

## 1. 如何理解当前的“N复杂度”

| 变量 | 含义与本轮边界 |
| --- | --- |
| N／M_v／M_ns | 逻辑文件数／全部存储版本数／namespace目录键规模；V>1时不能都写成N，目录和权限等键也计入M_ns |
| P／C | 请求显式路径项数／实际候选file-version数；当前d发现scope固定组V=1、P=C=N，q每item则P=Cq=1 |
| M_u／D | 索引单位总数／每文件message或dataset数；M_u通常随候选规模与D变化 |
| K／R／G | 命中单位数／返回点数／命中GRIB完整网格点数；当前K=2、R合计8192，owned输出40960B |
| F_fee／U_fee | 费用证据出现数／账本去重身份数；逻辑收费字节不等于物理读取字节 |
| head／Q | 已发布命名空间状态／同一可复用状态内执行的查询次数；决定缓存及索引建设如何摊销 |

d包含候选范围、属性筛选、权限／新鲜性、预算及精确单位定位交付；q从定位交付后到客户端拥有完整值／mask，包含规划、格式取值、编码、控制协议和owned交付。

本批HDF5采用native-hdf5，实际数据经pread VFD读取受管本地文件；GRIB暖native字段命中也避免了重新读取／解码payload。当前实验主要评估跨文件发现与完整产品交付，不能据此证明数据库存储大文件的底层I/O吞吐优势。

不能只写 `T(N)=aN+b`：目前同时改变N、P、C，却固定K、D和输出大小。应分别考虑 `T(N,P,C,V,D,K,R,head,cache)`。特别是**N增大没有使q返回更多数据**，所以不能预设q必须线性增长。

此前HDF5 d=0.480×、q=0.554×已从原始480行复算，算术／分组无误，但跨窗口慢块使其无法作为N的可迁移增长系数。最新交错采集也保留了HDF5 N8慢块；不能靠删块或负经验斜率推导算法降阶。见[统计复核](hdf5-growth-stat-audit-20261024.md)。

## 2. 理论上能否降低复杂度

### 2.1 d：索引检索本体已接近同类复杂度，差异主要在外围

FS-index并不是FS-scan：它已有六属性SQLite复合索引、版本索引、持久连接和statement复用。IoTDB-FS的暖语义索引则是不可变有序数组的二分范围定位。对受支持的高选择性等值查询，双方都能避免逐文件／逐单位扫描；没有“文件系统必然线性、数据库必然对数”的分界。

IoTDB-FS索引本体可描述为 `O(log M_u + K)` 加必要的顺序恢复／结果工作；这里M_u是已缓存候选域中的单位数。FS-index的SQL成本还取决于索引前缀命中数H、scope／版本连接、回表和排序；H可能大于最终K，不能对实际SQL无条件写成 `O(log M_u+K)`。SQLite的索引／覆盖索引／排序选择已有相应能力，具体执行计划应以EXPLAIN为依据。[SQLite查询规划](https://sqlite.org/queryplanner.html)、[执行计划说明](https://sqlite.org/eqp.html)。

| 当前子过程 | IoTDB-FS相对现行FS-index的机会 | 是否是数据库固有优势 |
| --- | --- | --- |
| 重复传入／解析P条scope | 公共selector句柄将静态路径规范化、长键构造移到prepare，execute用短ID；当前FS-index仍每次传入完整scope、查map并比较向量 | **具体实现有差异**；FS-index也可加入编译scope句柄，收益按Q摊销 |
| 统计候选版本数 | 当前FS-index即使走SQLite仍遍历scope内所有存储版本计算selected_sources；IoTDB热域可复用选择结果 | 可消除现行辅助循环；FS亦可预计算／缓存，并非FS语义要求逐次遍历 |
| 全候选权限／费用 | IoTDB已用原不可变证据和阶段摘要，暖态避免逐项费用重放；FS-index没有对应完整owner／预算职责 | 主要是在保留额外功能时减少自身代价，不能算相对FS“省掉其必需工作” |
| 返回K项定位 | 双方都至少付出Ω(K)输出；FS当前还在SQL ORDER BY后再次C++ sort | 重复sort可优化；输出下界不能消除 |
| head变化／冷建域 | IoTDB要重新选择、授权并建立域／格式索引；显式Exact与broad请求路径不同 | 没有普遍降阶；可能由稳定态收益转为失效重建成本 |

当前完整d仍有两个重要边界：

- **IoTDB客户端仍做列表scope校验，最坏O(KP)**；当P=N、K固定时，这本身保留随N增长的项。短ID消除了服务端重复P工作，不等于端到端d已为O(log N)或O(1)。
- **FS-index完整d也不只是一次索引seek**：scope解析／map查找、scope内全部版本计数、热scope向量比较都在计时内。按有界键长，路径检查可含O(P log N)，版本计数随scope历史版本数增长。V=1时仍随P增长。它们是可优化实现，不能作为不可复制的系统理论优势。

费用摘要将IoTDB旧路径的 `O(F_fee log U_fee)` 逐项重放，改为GRIB三个／HDF5四个有效摘要阶段和有界覆盖检查；覆盖定位仍可能含log F_fee。摘要编译、额度拒绝、collector或未认证路径仍有原遍历。权限、Latest、预算不足时的首错／收费前缀必须保留，但**这些语义不要求每次重复所有C或F_fee项工作**。

热模型必须带驻留条件：当前候选域／proof／句柄共用64MiB热池，域最多16个；GRIB native字段另有64MiB／16字段等上限。N／V／K扩大后可能转入miss或fallback，不能外推为无限规模仍全命中。冷建域还存在可优化的实现项，例如当前HDF5逐源调用context.record()在sources向量线性查找，存在额外O(C²)工作；它不是数据库／HDF5理论下界，也不能由暖实验判断其实际主导程度。

### 2.2 q：N不是主要数据变量，当前未证实相对FS的N降阶

两侧q都限定命中源；FS直接消费精确unit locator，IoTDB的GRIB q仍在命中源内重新选择message，当前都没有再扫全部N文件。主要变量是请求／源数、K、D、R、实际读取／解压量和交付字节。

| 格式／环节 | 当前FS-index | 当前IoTDB-FS | 理论判断 |
| --- | --- | --- | --- |
| GRIB单位定位 | 直接消费d的offset/length | 分离q仍按谓词扫描每源D条message，约Θ(Σ调用,源 D·E)；E为谓词求值工作量，本组为两次单源调用 | **IoTDB当前反有可去掉的重复工作**。复用已验证unit hint可减少D项，但属于后续plan设计，阶段A未完成 |
| GRIB解码与渲染 | 每q重新pread编码message、建ecCodes handle、完整解码、生成输出 | native字段热命中不再ecCodes解码，但仍按当前请求渲染和形成输出 | 对反复读取／小ROI可降低实际工作；改变的是G／R相关项，非N本身。FS也可增加同类字段缓存 |
| HDF5 native取值 | libhdf5、file/dataset/raw-chunk缓存、直接填owned数组 | 同样用libhdf5及文件内索引；另有session/setup／逻辑费用／typed结果 | 没有已证实的格式读取算法降阶；两侧都有chunk级能力 |
| R点／B_out字节输出 | 形成Float32与mask | 形成LogicalArray、Arrow结果、FUSE／客户端owned输出 | 两侧均有Ω(R+B_out)下界；当前IoTDB额外分配、拷贝与协议更值得降常数 |
| 静态请求模板 | 当前helper没有对应API | 阶段A复用frontend声明／AST／已编译路径，完整planner和admission仍执行 | 减少重复固定处理；当前q每item只有一个精确源，不构成全局N阶数变化 |

小ROI的条件机会应严格限定：若G远大于R、字段已热且缓存可容纳，IoTDB能避免FS现行每次完整解码的工作；但公共DomainSlice还可能扫描输入轴，冷miss仍要完整解码，当前FS helper只提供完整64×64 case。**这不是已完成等价ROI A/B，更不是FS原则上必须全读。** 同样，不能拿当前native-hdf5结果去证明未测的iotdb-indexed chunk decoder优于H5Dread。

## 3. 实践中是否已有更小的处理系数

### 3.1 最新同窗口的完整d/q结果

采用已验收阶段A联合主实验的模式B：相同candidate `3440025e…`、现有d句柄＋q模板；GRIB／HDF5各N128，每臂40条正式样本，四个独立块，全部保留。下表是**均值之比**，不是对两档N拟合的系数。

| 格式／步骤 | IoTDB-FS（ms） | FS-index（ms） | IoTDB-FS／FS-index |
| --- | --- | --- | --- |
| GRIB d | 0.862 | 0.706 | 1.22× |
| GRIB q | 3.845 | 1.436 | 2.68× |
| GRIB T | 4.707 | 2.142 | 2.20× |
| HDF5 d | 0.703 | 0.786 | **0.89×** |
| HDF5 q | 4.061 | 0.577 | 7.04× |
| HDF5 T | 4.763 | 1.363 | 3.50× |

HDF5暖d是目前一个**特定组的实测时间优势**：均值约低10.6%，四块中3块较低，另1块IoTDB约高4.8%。这支持“该设置下暖发现可接近／略优FS-index”，不支持所有N、所有块或完整查询均更快。GRIB d没有同类优势，完整q／T两格式均仍慢于FS-index。

两臂owned输出相同，但职责不同：IoTDB还提供全候选权限、完整head／版本、预算／collector、执行／发布／new-open准入及typed结果管理；FS-index维护冻结catalog和返回单位的本地身份检查。额外语义解释了部分必要工作，**不能据此把所有额外时延都归为不可避免成本**，重复规划和交付副本仍可优化。

### 3.2 已实现的局部优势与证据强度

| 机制 | 当前确实减少什么 | 能主张到哪一层 |
| --- | --- | --- |
| GRIB native字段／编码内容共享 | 当前N8和N128正式q均为2 hit、0 load，FS实现仍每单位调用ecCodes解码 | **已实现、计数支持的格式工作量优势**；没有单独的同边界decode系数A/B，也未转化为完整q优势 |
| 不可变有序属性索引 | 热d不再逐文件／单位重判六字段；避免SQL VM的一部分固定工作，索引可借用已有metadata | 索引机制成立；FS已有B-tree复合索引，未证实一般更低seek系数。该轮GRIB索引lookup只有约0.015ms，不是总时钟主项 |
| selector句柄 | 不重复发送／编译长scope、构造长域键 | **相对自身旧入口已验证**，FS未做同等句柄消融；完整d仍有scope校验 |
| 跨批次费用摘要 | 保留完整收费，减少账本重复证据遍历 | 对旧自身GRIB暖d，N128从4571 prepared visits降为0；属于额外功能的有效实现，不是FS做了4571项而IoT没有 |
| 公共q模板阶段A | 移走frontend重复解析、路径规范化及AST构造；HDF整batch声明复用 | 当前四组q均值较普通入口低4.41%–7.12%、T低3.95%–5.82%；归一化配对不一致，GRIB N128实际15次q生命周期基本持平 |
| HDF5 session／chunk cache | 可复用打开句柄、chunk数据和部分设置 | FS也有8 file／8 dataset及raw-chunk缓存，不能把“有缓存”作为IoT独有系数优势 |

阶段A的 `selector.compiles=0` **只覆盖socket parse_selector计数**；HDF5 Adapter bind仍会构造单Exact路径等小对象，完整planner／内部复检保留。不能解释为全栈零编译。d费用摘要历史A/B虽然d显著改善，当轮完整T均值却增加10.09%／17.60%；它是减少具体重复工作的一条证据，不是当轮完整查询加速证据。

HDF5此前的native.read均值0.212ms与FS read_ns 0.363ms计时范围不同：session／describe等有的在前者钟外，FS边界包含其自身cache／文件校验。**不能以这两个子计时声称IoT的H5Dread有更低系数。** 最新N128 B仍约1.85ms位于control之外，两格式大部分剩余差距都不是一次索引seek能解决的。

当前FS-index实际native库与冻结源码已重新核对：库 `e1530ee8…`，`fixed_native.cpp` 为 `b6a5cc4c…`，当次prepared／audit和原构建凭据一致；“FS每次GRIB解码”不是由另一份较弱旧源码推测。

## 4. 写入／更新场景：可以期待什么，不能假定什么

### 4.1 先纠正“FS-index必须重建”的范围

现行FS-index helper是**封印语料的只读实验对照**：索引以只读方式打开并校验catalog全文；数据身份变化报错；build函数要求新索引文件并inventory全部源。它没有在线增量更新API，也没有“检测变化后自动全库重建”的机制。若沿用该helper处理变更，只能另建catalog／index，这是一种实现限制。

SQLite本身可以对新增／删除索引键进行更新，并不要求每个数据写入都REINDEX；可设计新增文件只提取其u个单位，增量更新source／unit／latest及B-tree，结构成本约 `O(u log M_u)`，另计格式解析、同步和一致性协议。这是公平的候选FS基线，仓库尚未实现或测量。[SQLite索引键更新](https://sqlite.org/opcode.html#IdxInsert)。

IoTDB-FS当前的持久VersionDirectory是COW B+tree，固定页预算下Δk个键可局部修改约 `O(Δk log_b M_ns)`，既有内容对象不必全量重写；但**单键对数更新不是完整写操作的复杂度**。

### 4.2 当前IoTDB-FS的真实行为

| 场景 | 已实现能力／可能收益 | 仍需支付及当前边界 |
| --- | --- | --- |
| 新增独立文件 | 只摄取新文件、派生其目录；常数个namespace键局部COW更新；不重新摄取其他旧文件 | 新文件全部字节仍需读写／解析／验证／发布；新head下全域热状态可能重建 |
| 同路径新版本／少量内容修改 | 不可变版本、Current更新；相同内容对象、layout或GRIB字段可以共享 | FUSE dirty commit仍将完整staged文件交给ingestion；共享不等于仅处理Δ字节，首次新alias仍需读／哈希 |
| chmod／chown／权限等metadata变化 | 可保留原descriptor／seal和格式派生内容，免重新解析文件格式 | 必须更新head并重新授权；FS增量索引也可不重扫未变payload，性能领先未证实 |
| 同namespace内相同内容的多个GRIB版本 | 唯一编码内容数小于版本数时，可共享解码字段；在热集额度内减少重复decode与存储对象 | 版本身份、首绑定字节验证／哈希、每版本费用及返回输出仍独立；超过额度仍可能失效／重解码 |
| 删除 | Exact／Permission撤销、Liveness tombstone、Current重选；有明确旧计划失效语义 | 当前实现有全Exact枚举及重复重选，不能称完整删除仅O(log M_ns) |
| rename | 目标发布和源撤销纳入owner事务边界，正确管理新identity／seal | 当前代码仍按1MiB循环复制完整payload，再处理layout／metadata；不是零拷贝改名 |

新head的影响必须分层：候选域key包含完整head，新请求不能直接把旧head域和全局格式索引当作新状态使用；新domain可重用仍驻留、内容身份相同的单文件parsed layout，但**跨head的全局语义索引没有通用delta维护**，可能重新扫描域内单位并排序。新head尚无可复用快照时，首次broad／Subtree选择会重建全Exact目录快照；显式Exact路径走单独点查，不能说所有q都全域扫描。旧exact prepared plan则可能经依赖窗口验证继续读其冻结旧版本，不会自动升级Latest。

因此，写场景最可信的优势候选是**统一数据／派生metadata／版本可见性的发布协调，以及不可变内容复用**，而不是“所有索引免重建”。对外置FS索引，这些能力需要可靠变更源、事务／发布协调和恢复设计；但有能力实现它们，不能将当前helper缺功能写成理论上做不到。owner／持久化配置等前提同样要对齐，集成发布也不等于任意外部写入自动具有一致性。

目前没有FM1合格的持续写入、增量索引维护及写后首查询的同功能性能对照。静态准备收据只能说明某次初建成本；早期publication batching对旧IoTDB自身的收益不能作为当前相对FS-index的写优势。

### 4.3 应比较完整更新周期

一次更新后执行Q次查询，比较应包含：

```text
IoTDB-FS = 完整新版本摄取 + 持久目录/内容更新 + 发布确认
           + 写后首次完整查询T₁ + Σ(i=2..Q) Tᵢ

增量FS-index = 文件写入 + 新/变更文件metadata提取 + 增量SQLite维护
              + 一致性/持久化发布 + 写后首次完整查询T₁ + Σ(i=2..Q) Tᵢ
```

这里Q≥1，T₁含实际内联的写后候选域／索引重建与新鲜性处理，不再额外加一份重建时间。Tᵢ应包含实际发生的请求准备／释放等管理成本和完整owned交付；若管理时钟另列，只加一次。维护若延迟到首次查询完成，就归入T₁，不再重复记入发布阶段。

若FS流程重复打开所有文件重建，而IoTDB只处理新文件的持久内容，可能有明显差异；但应同时披露**当前helper对照**和**可增量维护的强FS-index对照**。反过来，若每次写导致IoTDB宽域索引重建且Q很小，稳定head的暖态收益可能无法摊销，甚至落后于局部更新的FS索引。两种方向都需要测量，不能只比较“FS完整build”与“IoTDB几个目录键提交”。

## 5. 后续研究应如何收敛

| 要回答的问题 | 优先观察什么 | 避免的误判 |
| --- | --- | --- |
| 是否真正降低N复杂度 | 固定P/C与输出改变namespace N；另行固定namespace改变P/C；结合实际比较／遍历计数 | 把P=C=N的两个wall端点比当作算法阶数 |
| 是否是同阶更小系数 | 对齐缓存、权限／版本和预算职责；独立计input处理、index lookup、decode、render、交付字节／次数及完整T | 将嵌套timer相加、将0.212与0.363不同读取边界直接比 |
| GRIB格式复用是否变成整体优势 | 相同内容／不同内容、K与G/R、热集是否在额度内；同时考虑FS字段缓存加强版 | 把FS缺一个可实现cache写成数据库本质优势 |
| 写场景是否获益 | 一次变化影响Δ文件／单位、写后首查询、后续Q次查询、持久bytes和恢复成本 | 忽略新head重建，或用query-only初建收据代替持续更新 |
| 是否仍有慢块混杂 | 相同窗口、平衡N及模式次序、独立客户端、全行保留、原始和配对结果并列 | 删除慢块、用FS归一化替代原始T、将缺失CPU／steal记零 |

这些是后续验证问题，本轮没有新建实验组或修改封印语料。优先级上，先减少公共q的重复规划／owned交付，并在需要时研究GRIB可信unit hint；对于写优势，先确认跨head索引维护设计和等功能FS维护边界，再测完整更新周期。继续只追求静态N端点时延的单调性，难以证明所希望的数据库场景优势。

## 6. 可用于后续报告的主张边界

| 当前可以主张 | 当前不能主张 |
| --- | --- |
| IoTDB-FS已将发现中的逐候选费用处理改为可复用摘要，并保留权限／预算／新鲜性 | 完整d已经严格O(1)或对FS-index有普遍N降阶 |
| 特定HDF5 N128暖d组可略快于现行FS-index | IoTDB-FS的HDF5读取／完整q普遍比native FS快 |
| GRIB热字段缓存确实避免当前FS实现中的重复解码，跨版本相同内容可共享 | native缓存命中即代表完整查询必胜；缓存可无限容纳N/V/K |
| 阶段A减少公共frontend重复声明处理，当前暖q均值有有限改善 | 已复用完整plan、消除了GRIB D项扫描，或证明了N渐近阶数下降 |
| 持久目录能局部COW更新，发布与内容身份管理已集成 | FS-index理论上每写必须全量重建；IoTDB所有写都delta-only／免重建／零拷贝 |

## 7. 依据与复核

当前产品源码528文件与阶段A构建清单逐SHA一致；本轮只重算既有安全数值导出，并对服务器实际native库、原构建凭据及当次prepared绑定作只读核验，没有运行查询、构建或服务控制。

- [最新同窗口d/q/T与GRIB计数重算](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/n-complexity-summary-20261024/summary-derived.json)
- [FS-index实现／维护审计与源码锚点](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/n-complexity-summary-20261024/fs-index-audit.md)：[索引及scope处理](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/fixed_matrix/native/fixed_native.cpp:86)、[实际q读取](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/fixed_matrix/native/fixed_native.cpp:108)。
- [IoTDB查询模型审计](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/n-complexity-summary-20261024/iotdb-query-audit.md)：[完整head域key](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/IoTDBStructuredBlobBackend.cpp:3048)、[公共索引](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/include/iotdbfs/query/core/ImmutableEqualityIndex.h:104)。
- [写场景逐入口审计](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/n-complexity-summary-20261024/write-scenarios-audit.md)：[COW目录](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/storage/versioning/VersionDirectory.cpp:238)、[完整摄取](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/Ingestion.cpp:160)、[新head快照／Exact分支](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/IoTDBStructuredBlobBackend.cpp:899)、[rename复制](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/main.cpp:2250)。
- [实际FS库／源码／当次采集绑定](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/n-complexity-summary-20261024/native-binding-check.json)
- [当前阶段A验收](public-q-template-stage-a-20261024.md)、[GRIB费用摘要验收](grib-fee-summary-20261024.md)、[q逐阶段模型](q-model-grib-hdf5-20261024.md)、[GRIB缓存契约](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/docs/contracts/grib-query-cache.md)。这些窗口与版本分别引用，没有混样或串接成一条性能曲线。

当前主数值derived SHA256：`d2a8cb9f795d2111ff65ad3a8428719872fa9da1275aa96d1420833de342f5c7`；对应audit：`6ecd3cf9cf3ef9cba55e2b0292afd7f6415009b244dc0d73aeb815f006b29a88`。原始科学记录与进程census继续留服务器，报告及只读复核材料位于本地／服务器 `experiments/cross_version/work/n-complexity-summary-20261024/`。未执行Git写操作。
