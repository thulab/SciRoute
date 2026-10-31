# GRIB／HDF5 共用热查询模块方案

## 2026-10-22：GRIB定位复用与轻量发现

状态：用户已于2026-10-22确认“按方案实施（推荐）”，现已实现并完成验收。96项聚焦／全部1,825项单元回归、N8门槛72条、独立N128三轮216条及独立资源／恢复审计通过。N128两项合用C/A的完整R为0.712、原始完整T为0.717，三轮均改善，保留C；仅定位提示B的原始完整T三轮均更慢，不能独立宣称稳定收益。详见[完整消融报告](../reports/grib-discovery-reuse-20261022.md)。本节不改变下方既有已验收实现的历史状态。

本轮只处理两项：全局GRIB索引命中后完整传递message定位，以及直接交付发现定位。暂不处理16域驻留、HDF5优化、解码／渲染、批内并行或费用账本重写。

### 建议实现

1. **公共不透明匹配提示。** 扩充格式无关的域选择结果，使其能携带本次匹配的不可变Adapter提示。GRIB提示保存原source/message顺序、parsed-index共享引用和message定位；只在本次请求规划中存在，不存入全局域，不持有QuerySource、请求账本、ReadContext或输出答案。GRIB prepare验证提示的精确源／layout身份、完整六属性等值谓词、投影能力与限额，再使用已认证message集合；不再次扫描命中文件全部message或重新查询同一parsed-index缓存。无法认证的普通查询保留已有有界prepare路径，错误与费用不减免。
2. **通用轻量发现协议。** 新增`kind=format_catalog`控制请求，使用现有QueryRequest的sources、subset和limits；公共层只负责选择、全候选授权、预算、准入、条数／帧界限和稳定结果顺序。定位解释留在Adapter。首版GRIB接受仅offset的metadata发现（无grid／BinaryView／ROI取值），返回精确path/version/format/revision与Adapter产生的unit身份、offset/length。直接JSON返回定位，不创建typed结果路由、不构造Arrow流，也不经FUSE读取定位。
3. **保留独立取值。** 轻量发现的结果不作为客户端授权凭证。后续精确取值继续执行当前typed路径、权限／版本准入和原生解码缓存。Latest不因谓词改选旧版本，未命中候选的权限／错误检查不跳过；发现准备需执行lease保活，最终响应发布前重新准入。
4. **共用现有资源机制。** 不新增全局缓存或调整64MiB／16域／64证明；不改持久格式、默认引擎、HDF5现有接口或摄取。沿用1,024源、64MiB元数据及1MiB控制帧界限，构造定位时检查条数／空间和本请求预算，超限明确报错。定位提示和输出临时状态按原预算计费，不扩大额度。
5. **旧typed入口仍承担其原有查询职责。** 不增加新旧协议别名、隐式协议回退或持久格式兼容层；实验客户端显式选择新的发现请求。未认证的谓词不伪装成快路径，普通typed查询继续可用。

格式无关公共类型不出现GRIB message结构／字段解释，也不把GRIB包装成HDF5 dataset。主daemon必须向轻量入口复用现有QueryModule及同一GRIB Adapter，避免新建Adapter导致解析缓存丢失、域adapter身份失配。

### 可审查的请求／结果示意

请求沿用当前GRIB offset发现的QueryRequest，只将最外层`kind`从`typed`改为`format_catalog`。结果为`units`列表；每项包含精确source、revision、稳定unit身份和编码字节范围。具体GRIB单位身份由Adapter生成；公共层不解析该身份，不接受客户端提供匹配提示或服务端证明。

本轮只对offset发现承诺轻量交付；现有其他metadata投影继续返回typed列。新的定位结果必须与原offset发现的源／版本／offset及顺序完全相同，额外length按原GRIB layout验证。

### 验证与消融

- 机制／等价性：命中文件含多个匹配和非匹配message、重复属性键、无匹配、未知字段、坏的非匹配layout、scope与limits变更、原始与提示路径的错误／顺序／费用、不同namespace与head、撤权／祖先权限／删除／冻结版本，均经现有单测及真实owner验证入口核查。
- 计数：提示命中后不再逐message评估、不重新lookup parsed-index；format_catalog不产生typed IPC／结果路由／定位FUSE读取；仍有真实全候选授权和发布准入。
- 消融分三种发现路径：既有产品typed、候选产品typed（仅定位复用）、候选产品format_catalog（两项合用）。后续取值及FS-index控制保持相同。复用原封印语料、case、oracle和索引，不重新发布。
- 先使用既有独立N8小探针检查输出与完整T，再按固定协议仅复测既有独立GRIB N128/V1/D4/K2/Latest，记录两核／2GiB查询组及原数据库实际资源。正式结论使用三campaign，R/Rd/Rq分开报告；不把局部计数或历史最佳当作完整查询收益。
- 新控制协议和不透明提示属于新的接口设计；依据AGENTS.md“不确定的代码设计问题必须先询问User”，确认后才修改生产代码。若完整查询明显减慢，按原验收规则回退相应优化，保留全部失败与慢记录。

---

2026-10-20。状态：已实施，相关回归、真实后端探针及同轮完整查询A/B通过，保留新版。持久格式不变。

## 目标与依据

让两种格式共用候选选择、冻结状态、授权证明、费用与缓存管理，以及格式无关的索引存储／查找机制；格式 Adapter 保留记录语义、字段解释和索引层级。当前实现依据为`CONTEXT.md`，仓库没有`docs/adr/`目录。该方案不把GRIB包装为HDF5 dataset，也不把格式布局搬进Query Core。

当前GRIB已有跨请求解析目录、编码message和共享解码缓存。N128/V1/D4/K2新测完整暖p50为39.248 ms，FS-index内部1.493 ms／worker往返1.873 ms；暖SQL和native新增解码均为0。完整请求发现／取值独立p50为34.786／4.466 ms。发现仍有128次source open及1,155次内部head-control，表明应优先检查候选与验证状态复用；这些计数不是SQL，也没有独占耗时归因。见[当前GRIB检查](../reports/grib-current-cache-performance-20261020.md)。

## 一个共用方案，三层职责

| Module | 职责与复用内容 | 具体格式保留的内容 |
| --- | --- | --- |
| owner查询域模块，属于backend/storage | 复用`HotQueryCache`池；统一全候选冻结、manifest能力、ReadContext、主体证明、费用轨迹、head失效、同键构建合并、执行pin生命周期 | 不解析GRIB message或HDF5 catalog，不拥有格式字段名与层级 |
| 格式无关查询／索引机制模块 | Query Core继续通过`CatalogSelection`、`QueryReadContext`、`QuerySource`及Adapter Seam工作；共用不可变有序复合键、稳定locator、二分范围查找和容量核算 | 不解释`grib.message.*`／`hdf5.attr:*`，不把格式记录变成Core的数据模型 |
| GRIB／HDF5 Adapter模块 | 各自认证并导出可索引的语义记录，调用公共查找机制，生成当前查询需要的格式计划 | 字段映射／类型／null与错误规则、记录身份与顺序、格式布局与执行内核 |

这里的公共索引机制沿用已有HDF5的**复合键排序数组＋二分范围查找**，首版不另造倒排表、集合求交、通用页面管理或新的优化器。字符串及记录所有权必须有不可变artifact pin，不能留下借用临时AST／源对象的引用。

**Locality：** 失效、主体隔离、容量和费用规则集中维护、统一验证。**Leverage：** 一次机制修复同时覆盖两个Adapter；格式错误规则及解码变化仍集中在对应格式。现有HDF5语义索引继续位于`src/formats/hdf5/`，新增GRIB语义索引位于`src/formats/grib/`；公共层只接收已认证的键、稳定locator及所有权信息。

## 应保留的格式差异

| 项目 | GRIB | HDF5 |
| --- | --- | --- |
| 跨文件记录 | 精确File Version内的message | 精确File Version内的dataset object |
| 记录定位／顺序 | 原layout message ordinal、offset／length；按原source与message顺序 | object identity／canonical path；按现行source与object_id顺序 |
| 属性解释 | `grib.message.*`，GRIB三值逻辑、缺失字段区别、layout／range完整性 | `hdf5.attr:*`，Complete／scalar／shape／类型状态、dataset_path作用域 |
| 文件内层级 | message索引 → 编码字节／ecCodes → 网格、ROI、插值 | dataset索引 → 原生HDF5文件内索引，或IoTDB-FS chunk／layout索引 |
| 执行缓存 | 原有解析目录、native field 2Q、编码message缓存保留 | 原有两种正式执行引擎及其缓存保留 |

复合键必须来自同一个message／dataset，保留属性之间的关联；不能把文件级Sigma中彼此独立的属性集合命中当作精确记录命中。相同内容跨版本共享不合并版本输出，locator仍绑定精确source与record。

## 接入位置与语义约束

1. **共用现有选择Seam。** 沿`PublishedVersionCatalog::scan → CatalogSelection.read_context → QueryModule::select/prepare`接入；HDF5 `dataset_catalog`与GRIB typed发现调用同一候选域模块，不新增第二套SourceCatalog或Factory。
2. **先冻结版本，再做语义过滤。** Latest先确定精确版本，不因旧版本属性匹配而回退历史。全候选域包括未命中记录与源；命中集合是其附属结果，不通过缩减ReadContext丢弃依赖。
3. **先保持全候选授权与错误规则，再减少执行准备。** 共享域证明覆盖全部候选、祖先及Exact／Permission／Liveness等依赖。经过认证的热命中可只为K个实际命中源构造执行资源，但未命中候选原本会触发的类型、capability、layout及错误优先级必须保留。
4. **证明不等于准入。** proof绑定“全候选域＋完整head＋规范化uid/gid/补充组”。每次execute／发布／新open仍执行真实owner准入；Frozen与head冲突不能被缓存掩盖。新head使新请求保守失效，旧计划按现有依赖规则重验且保持原版本。
5. **不共享请求状态或答案。** 缓存只保存查询无关、不可变的artifact及证据，不保存请求ledger、完整PreparedQuery、ROI答案或结果数组。当前`AdapterPlanningState`仍限一次精确源规划；实际执行计划按本次projection／dtype／ROI重新构造。
6. **保持来源与错误归属。** GRIB解析会验证整个layout，逻辑谓词也没有一般性的短路保证。不能仅验证索引命中项，使坏的非匹配message从错误变为空结果。无法证明等价的域或请求不启用此优化，走原通用执行路径。

关键位置：`IoTDBStructuredBlobBackend.cpp`的`HotCatalogBuild/HotCatalogDomain`、`VersionDirectoryReadContext`；`HotQueryCache.h`；`QueryModule.cpp`的`select/prepare`；`Hdf5GlobalAttributeIndex.cpp`；`GribFormatQueryAdapter.cpp`的`parse_index/prepare_messages/prepare`。

## 首版建议冻结的范围

- GRIB与HDF5共用**现有一个**额外热池：64 MiB、16个域、64个主体证明，包含构建中、淘汰后仍被执行pin的状态；不是各加一个64 MiB池，也不是RSS上限。原有格式解码缓存另计。
- 域身份包含backend／namespace incarnation、完整head、规范化路径与版本选择、完整格式选择，以及Adapter的格式内作用域／索引协议；格式payload必须有明确判别，避免跨格式错误命中。
- 沿用当前HDF5优化支持的六属性、非null、正确类型、无重复字段的等值AND；GRIB Adapter映射到对应message字段。公共机制不认识具体字段名。OR／NOT／IN、额外谓词及非标准属性保持原路径。
- 首版对`file_predicate`或其他尚未认证的selector／格式内过滤不启用热域。不得复用首请求的`residual_file_predicate`。后续扩语法另行确认，当前普通查询功能不删减。
- 不改变持久格式、摄取、客户端协议、默认模式、请求预算、权限或准入。冷／热继续按原身份max/delta收费，保持失败粘性、collector顺序与256成功批次pin上限。
- 不重新引入已被完整查询否决的连续Fee／紧凑账本；本轮只做共用模块和GRIB接入。

## 实施顺序与保留条件

| 步骤 | 交付 | 门槛 |
| --- | --- | --- |
| 1. 等价抽取 | 从HDF5现有热路径抽出共用域生命周期和有序索引机制；HDF5语义封装留在Adapter | 结果、错误、费用、顺序、失效与并发规则不变；HDF5完整查询无明显回退 |
| 2. GRIB接入 | GRIB提供认证的message键／locator，公共选择与准备路径在逐源prepare前利用匹配集合；沿用native缓存 | 仍覆盖全候选权限、无关坏layout、空结果、未知字段、输出能力及源绑定；只优化已认证范围 |
| 3. 有界验收 | 重用现成语料，先小型功能探针，再当前N128/D4/V1/K2方案；同轮当前基线／新版／FS-index | 各格首请求和5次暖测量单列；以完整查询作保留依据，明显变慢则回退；不擅自扩N/V/S |

必须包含公共池跨格式隔离、重复键与稳定顺序、字符串所有权、预算边界／opt-out、退休pin、同键并发构建及失败唤醒；并保留现有HDF5索引／池／预算测试。格式验收比较原evaluator与快路径，包括未命中候选错误、主体／祖先撤权、删除、新版本、历史冻结和冷／热费用一致性。HDF5两正式模式做功能回归，默认indexed与GRIB做同轮小规模完整计时；不将局部二分查找收益当作端到端收益。

## 已确认的实施边界

用户已确认上述模块职责、同池额度及首版语法／选择器范围。公共机制落在`ImmutableEqualityIndex`、`QueryDomainCache`及`FormatQueryAdapter::build_domain_index`接口；owner backend统一提供候选域、完整head键、同池预留、费用与授权证明。

首版GRIB认证范围进一步保守限定为metadata发现：六字段等值AND、metadata列投影、无slice／插值／dtype override／physical_array。网格、二进制及其他查询保留现行规划；完整“发现＋取值”计时会同时覆盖新发现路径和原取值路径。缓存不保存首请求的projection或执行计划。可选索引构建仅借用已成功解析且仍在格式缓存中的artifact；若缓存已淘汰则放弃构建，不为缓存补读布局。

## 本轮实现与验证位置

| 位置 | 当前职责 |
| --- | --- |
| `query/core/ImmutableEqualityIndex.h` | 完整等值AND结构编译、借用键、有序locator数组及二分范围查找；字段表、类型槽与tie顺序由Adapter声明 |
| `query/core/QueryDomainCache.h` | 同一候选域内的可选不可变artifact、构建合并及失败唤醒；借用backend同一reservation，退休artifact／费用批次继续计费 |
| `QueryPlanning.h`／`QueryModule.cpp` | Adapter认证全部候选后按source ordinal选择；只构造本请求匹配源的计划；完整计划验证成功才发布artifact；空匹配仍验证请求并保留schema |
| `IoTDBStructuredBlobBackend::scan_published_versions` | owner候选域、完整格式／路径／版本／head键、全部候选授权证明、选择费用批次；HDF5发现与typed候选端点有明确payload类型键，合用原池 |
| `formats/grib/GribFormatQueryAdapter.cpp` | GRIB六字段认证、message元组及原始source/message顺序；持有parsed-index，不持有QuerySource、ReadContext或执行计划 |
| `formats/hdf5/Hdf5GlobalAttributeIndex.cpp` | 保留HDF5属性状态、dataset作用域、object顺序、catalog持有及费用；仅存储／范围查找机制改用公共模板 |

构建证据位于`experiments/cross_version/work/grib-global-index-20261020/`。候选二进制SHA-256为`36881bde02466fb65730188161c2f764e45d2b000c321901a191af722a468fc9`，从保留的干净基线加本轮覆盖重新构建87个C++单元；本轮没有启用失败的连续Fee／紧凑账本试验。

| 验证 | 已完成范围 |
| --- | --- |
| 本地聚焦单测 | 公共／HDF5索引20、域缓存5、规划8、GRIB Adapter 63，合计96项通过；测试使用的产品源码摘要与完整构建一致 |
| 固定HDF5 1.14.4功能门槛 | 12项通过，覆盖native／indexed及原生模式注册与Arrow交付；独立测试第一次因helper不在可执行文件相邻目录而失败，仅指定既有正式worker路径后同一测试二进制全部通过，失败保留 |
| 原数据库真实后端 | 原HDF热域11项及新跨格式6项通过；包含全候选权限／费用／版本、池隔离和热SQL实测零，探针新建小前缀保留，不改计时科学语料 |
| 完整查询A/B | 96条新记录、16格、80条暖观测及84份服务profile独立审计通过；结果、owned数组、完整时钟、源／二进制／head／journal和原服务恢复均核验 |

## 同轮完整查询验收

固定N=128、V=1、D=4、K=2、Latest；每格首条单列，后5条为暖观测。GRIB读取完整两个64×64 message，HDF5保持原选择性dataset查询，两者均交付8,192个Float32及mask、40,960 B owned输出。复用原V04科学namespace、独立FS文件、SQLite及版本映射，不重新摄取科学文件或建索引。两个小型验证前缀不属于计时语料。

下表单位ms；均为本轮新测，基线`bff1dd7a…`、新版`36881bde…`，不引用先前39.248 ms的单格式轮次作为A/B基线。

| 完整“发现＋取值” | IoTDB-FS基线p50／均值 | 新版p50／均值 | p50耗时下降 | FS-index基线p50 | FS-index新版p50 | FS一次建索引准备 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GRIB | 82.461／81.714 | 30.120／32.090 | 63.47%（2.738×加速） | 3.224 | 4.041 | 400.136（复用） |
| HDF5 `iotdb-indexed` | 34.654／34.429 | 29.500／28.603 | 14.87%（1.175×加速） | 4.915 | 4.560 | 67.540（复用） |

| 仅发现／首次观测 | GRIB基线 | GRIB新版 | HDF5基线 | HDF5新版 |
| --- | ---: | ---: | ---: | ---: |
| 暖发现p50 | 65.721 | 21.047 | 9.440 | 8.433 |
| 暖发现均值 | 68.148 | 21.513 | 9.021 | 9.096 |
| 首次发现 | 90.270 | 67.163 | 52.292 | 58.099 |
| 首次完整查询 | 230.532 | 107.849 | 94.349 | 148.158 |

**保留依据：** 两格式暖完整查询p50与均值均下降，没有达到需回退的明显变慢情形。本轮完整查询仍未低于10 ms，且新版相对同块FS-index内部钟仍慢GRIB约7.454倍／HDF5约6.469倍；不据此宣称快于native FS。HDF5首次完整观测反而变慢，暖发现均值也略增，均保留在表内。两块的格式先后顺序不同，首条又在前序查询之后，不将它们称为冷启动或宣称所有请求均加速。owner挂载恢复252.316／193.667 s单列，不计入查询。

新GRIB的10份暖发现profile（发现单测及完整查询内发现各5份）全部实测：候选域hit=1、语义域hit=1、proof hit=3、source open=2、实际owner准入=3、两个SDK SQL路径均为0。接入前同类发现逐次打开128源。完整候选仍保留并收费，未将未命中项从ReadContext剔除；首次费用批次重放仍随证据数增长，不能宣称O(1)查询。

**比较边界：** IoTDB-FS计至客户端owned NumPy值／mask；FS-index内部计至C++ owned值／mask，外层hash／JSON／pipe在其独立worker往返钟中，未实现同等owner/head／全候选安全职责。每挂载一个native worker只有一条SQLite连接，切换格式时重开；仅发现控制组包含这种成本。主完整查询紧随同格式发现，连接已保留。本轮FS完整GRIB控制反而增长25.34%，HDF5下降7.23%；仅报告原值，不用它归一化IoTDB结果。两次顺序执行挂载、每格5条暖样本只支持该选择性点的描述性结论，未验证N/V/S扩展或并发吞吐。

原服务已恢复ASYNC/true，实际PID2920353；原配置SHA-256仍为`ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，无所属挂载残留。独立审计`results-audit.json`对应96条查询摘要`da5d8a7e2be6606250ea7ce4d097745cc66859df0b62f1696cae09dbd4d303a3`；真实后端、科学输入与原／副本journal均留档在同一实验stage。

公开包`public-ab-evidence.tar.gz`已在本地和服务器逐项核对44个成员，SHA-256为`c09b1ffb4d9b81ee942d9be30442768158418a5aac5aff25b3fba45775e30d6b`；包含本轮源码覆盖、查询记录、功能／后端／恢复收据与独立审计，不包含实际凭据、配置、许可证、私有SDK日志、科学数组或可执行文件。HDF功能首轮失败及修正测试运行的14成员包另保留为`functional-gate-public.tar`，SHA-256为`fdfbc057e6c4fe4dc1c6194392733fcc928c4e6c5c576439e7663120b362ed80`。
