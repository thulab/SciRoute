# 当前 GRIB 缓存与 FS-index 性能对照

2026-10-20，服务器 `192.168.99.49`。本文前半保留接入公共热域之前的GRIB-only诊断；之后已按确认方案实现公共模块并完成同轮GRIB／HDF5 A/B，当前结果见文末及[共用方案与验收](../deliverables/shared-cross-format-hot-query-plan.md#同轮完整查询验收)。

**接入前诊断：GRIB当时已有跨请求的解析目录、编码message和内容共享解码缓存，尚未接入公共候选／属性索引／授权证明。** N128 温热完整查询中位数为39.248 ms，FS-index内部查询为1.493 ms；按既有实验端点口径相差26.28倍。即使另列FS诊断worker往返1.873 ms，差距仍为20.96倍。缓存命中后的发现／准备仍有明显成本，当前证据不指向重复解码或温热SQL。

## 接入前代码检查

| 层级 | 该轮基线状态 | 热查询仍需执行 |
| --- | --- | --- |
| 已解析 message 目录 | 同一挂载的Adapter跨请求复用；默认64 MiB／65,536条；验证layout内容身份后可共享；LRU及同键加载合并 | 对每个候选源取得目录，逐message判断谓词，仍有O(N×D)遍历 |
| native Float64字段 | 同namespace按实际编码字节SHA-256共享；64 MiB／16字段；2Q；alias／ghost各256；同内容解码合并 | 本源逻辑费用、准入、当前dtype／ROI渲染及输出 |
| 编码message | 挂载公共QueryRuntime共享，精确源／版本／revision／范围定位 | 本源首次绑定仍须实际读取；native字段已淘汰时仍可能重新解码 |
| 全局候选域／跨文件属性索引／授权证明 | HDF5发现路径专用，GRIB未接入 | GRIB使用typed MessageRows发现，每个新请求仍构造候选和检查状态 |

GRIB alias费用仍保存为原始`QueryMetadataTrace` map，没有使用HDF5热域的`PreparedQueryMetadataTrace`批次与同账本memo。命中解码缓存不等于免除费用、权限或版本准入。具体容量及源码位置见[GRIB缓存契约](../contracts/grib-query-cache.md)；格式门控见[后端](../../src/IoTDBStructuredBlobBackend.cpp:2754)，逐message选择见[Adapter](../../src/formats/grib/GribFormatQueryAdapter.cpp:1147)。

## 实验条件与时钟

| 项目 | 本轮固定条件 |
| --- | --- |
| 数据 | 原V04 namespace-record/3／descriptor/2，N=128、V=1、D=4；512条message中命中2条，选择Latest |
| 单文件／输出 | 每文件33,484 B，总4,285,952 B；完整读取两个64×64 message，8,192个Float32及mask，owned输出40,960 B |
| IoTDB-FS | 当前保留的clean二进制`bff1dd7a…`；五个关键源码文件与构建输入一致；包括2026-10-18保留的GRIB共享解码修复 |
| FS-index | 同一语料的六属性等值复合SQLite索引，持久连接；只对命中源做stat/access，每次取值仍读message并调用ecCodes，无解码结果缓存 |
| 准备 | 复用原始文件、独立FS文件、SQLite及版本映射；初建索引400.136 ms／86,016 B为该语料原始实测，本轮未重建；不重新摄取 |
| 样本 | 一次挂载，2端×2阶段×6次，共24条全新记录；每格首次单列，5次温热测量；顺序逐次交替，不做无条件All查询 |
| 缓存 | 保留同一daemon／SQLite worker，未清OS缓存；探针公共runtime为256 MiB／65,536条，GRIB专用两层沿用默认容量 |

两端沿用原冻结时钟，**交付管线有区别**：

- FS-index内部`total_ns`包括SQLite查询、命中源stat/access、编码字节读取、ecCodes解码及C++ owned Float32／mask形成；不含外层JSON解析／分派、结果哈希、JSON输出和pipe往返。`worker_client_ns`单列包含这些诊断开销，但只返回locator／计数／摘要，不交付Python科学数组。
- IoTDB-FS完整时钟包括发现、typed读取、IPC／FUSE、客户端owned NumPy值与mask；profile等待及oracle在钟外。完整取值含一次发现RPC和一次合并两源的取值RPC。各RPC仍使用现有独立预算，不声称客户端共用一个跨RPC账本。
- 因此，下表保留既有owned输出端点口径，不能称两端具有相同IPC成本或相同安全职责。FS-index不实现IoTDB-FS全候选owner/head、版本与权限租约职责。

## 本轮新测结果

温热中位数，单位ms；每格5次。FS-index两阶段复用同一次初建索引（400.136 ms）。

| 查询阶段 | FS-index内部 | FS worker往返 | IoTDB-FS客户端 | IoTDB／FS内部 |
| --- | ---: | ---: | ---: | ---: |
| 仅发现 | 0.216 | 0.505 | 36.938 | 170.82× |
| 发现＋完整取值 | 1.493 | 1.873 | 39.248 | 26.28× |

完整取值均值为FS-index 1.490／IoTDB-FS 39.396 ms，范围分别1.411–1.566／37.902–40.842 ms。首次完整请求为17.558／160.227 ms；此前已经进行metadata发现，不能称冷盘。全部首请求、每次计时及worker补充均保留，没有只选最快样本。

同一组完整IoTDB-FS请求的客户端发现／取值独立p50为34.786／4.466 ms；它们不能相加。发现RPC的server总时钟p50为27.376 ms，其中`typed_prepare`16.342、`registration_catalog`4.178、`authorize`2.007 ms是有包含关系的阶段，不能视作互斥CPU项或再次求和。

## 温热缓存与剩余工作

以下计数在5次温热请求中保持一致，按RPC分别列出：

| 观测 | 发现RPC | 完整查询的取值RPC |
| --- | ---: | ---: |
| parsed-index hits | 128 | 2 |
| selected sources／source opens／payload-map root requests | 各128 | 各2 |
| head-control调用 | 1,155 | 21 |
| GRIB native-field hits／content hits／alias hits | 此路径不使用native字段 | 各2 |
| native loads／hashes／misses | 未报告 | 各0 |
| 实际SDK查询／Store查询／layout读取 | 各0 | 各0 |

15个温热socket profile都有SDK／Store及layout字段，零值不是缺失默认；`grib.index.cache.misses`在命中请求中没有键，保留为null，未补零。head-control是内部控制调用计数，**不等于1,155次数据库SQL或网络RPC**。

native字段驻留观察为74,902 B／2字段／2 aliases，远低于64 MiB／16字段边界，不能解释为RSS峰值。generic runtime的decode／hit增量均为0，GRIB专用native缓存却明确命中，不能用generic hit为零判定GRIB缓存失效。`hot_query.*`键均不存在，与代码中的HDF5专用门控一致。

发现RPC还观察到3次准入、384次源权限检查、72次祖先权限检查；取值RPC为3／6／24。上述计数揭示候选级重复工作，但本轮没有独占计时，不能将全部差值归因于head、预算重放或某一种权限检查。该诊断据此提出GRIB跨文件候选与已验证状态复用，随后已完成设计确认和下述实现验收。

## 恢复、限制与证据

- 24条均通过冻结源／版本／message身份及值／mask oracle；客户端owned结果摘要也独立比对。一次owner恢复185.526 s，控制器总耗时385.016 s，准备与服务维护未混入查询计时。
- 原数据库、账号及许可证复用，原DataNode恢复ASYNC/true、PID2838702；head、原／副本journal、257个GRIB原始／FS／索引文件、五个生产源码及四个运行helper均重验，无所属挂载。
- 三次预检查失败均为0数据库操作、0查询：两次报告路径差异、一次重组后的Python模块路径差异。最终使用同摘要的接受报告及匹配二进制的冻结模块，查询及计时函数未改；失败收据保留。
- 只有一个N128/V1小探针、每格5个温热观测，不证明N/V/S增长规律、并发吞吐、冷盘或所有GRIB数据均有同样倍数。历史N8/V8表及10-18旧／新缓存修复轮次没有与本轮合并。

服务器证据stage为`experiments/cross_version/work/grib-current-fs-index-probe-20261020/`，本地公开下载位于其`public-transfer/`，计数汇总为`profile-summary.json`。当前daemon SHA `bff1dd7a6fcc42af0f95f529010a04583e2618df460ef95c473a6ed3bbd77467`；native SHA `49f7f7127e6a7c73bab304f9ec230f46e540ebf26b3d3e40fed05b3a6e398864`；acceptance／postflight SHA分别为 `c5c99fa69505c8a1906afc38b4325aaf9cd902e6d2e7aeaaaa18f38514057a85`／`43c4915b9b492eb8bff51e0ded26556f0a00b1fe2097abf507f27a5da1a7ca63`。

公开包`public-evidence.tar.gz`含28文件，SHA `f218d8282458579ec4627b12c143420c96010244768391b0206d453b8c6b57bf`；包含脚本、原查询计时、profile汇总、独立验收、失败预检查及脱敏恢复收据，不含科学数组、实际凭据／配置／许可证内容、私有SDK日志或可执行文件。


## 公共热域接入后的同轮验收

当前GRIB metadata发现已接入公共冻结候选域、复合键有序索引、费用与授权证明；与HDF5合计使用原64 MiB／16域／64证明。格式字段／错误／message顺序仍在GRIB Adapter，公共Core不认识GRIB／HDF5字段。首版仅认证完整六字段等值AND和metadata投影；取值及未认证请求保留原规划，不缓存ROI答案或PreparedQuery。

复用同一N128/V1/D4/K2科学语料，基线`bff1dd7a…`和新版`36881bde…`同轮测量。下表单位ms、每格5次暖观测p50；本轮基线受运行环境影响高于前文独立轮次，不能把39.248与新值拼成A/B。

| GRIB阶段 | IoTDB-FS基线 | IoTDB-FS新版 | FS-index基线 | FS-index新版 |
| --- | ---: | ---: | ---: | ---: |
| 发现 | 65.721 | 21.047 | 1.031 | 0.686 |
| 完整发现＋取值 | 82.461 | 30.120 | 3.224 | 4.041 |

完整查询p50下降63.47%，均值81.714→32.090 ms；新版／FS内部钟比值为7.454，仍有明显差距，交付与安全职责差别仍适用。两格式同轮完整矩阵共96条、84份profile独立审计通过；GRIB全部10份暖发现都只有2次source open，候选／语义域均命中、proof hit 3次、owner实际准入3次，两个SDK SQL路径均为0。没有跳过全候选费用、权限或版本依赖。

本轮一个native worker切换格式时重开唯一SQLite连接，因此发现控制组不等同前文GRIB-only持久连接测量；完整取值紧随同格式发现，连接已保留。保留原FS控制波动，不对IoTDB结果做归一化。仅一轮顺序双挂载，不声称并发、规模增长或显著性。

完整查询两格式均改善，保留新版；首次观测、HDF5轻微负例、所有收据及服务恢复证据统一见[验收记录](../deliverables/shared-cross-format-hot-query-plan.md#同轮完整查询验收)。原服务已恢复ASYNC/true、PID2920353，无所属挂载残留；原数据库、账号、许可证复用。
