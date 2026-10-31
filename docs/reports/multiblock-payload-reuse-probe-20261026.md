# 有界请求内payload复用与多块请求共享：实现和首次读取对照

日期：2026-10-26。

本轮已实现原始scalar请求内有界块复用，以及活跃多块请求之间的不可变块共享。真实IoTDB诊断中，大message四请求从11个物理块引用降为8个，消除3 MiB重复供数；仍为4条批量SQL。性能波中，大message平均整批耗时下降19.107%，单条平均延迟下降21.262%；**小message整批耗时增加13.179%**。因此不能将本轮称为通用提速，小message回退是本轮保留的负面结果。

## 实现与语义

完整约定见[读取契约](../contracts/payload-read-coalescing.md)。

- 块身份包含同一个Owner运行时、namespace、incarnation、type、protocol、content ID和精确长度。每个Owner运行时最多64个保留条目、8 MiB块数据；完成数据由活跃调用或原始请求持有，注册表只保留weak引用，不形成常驻缓存。
- 多块请求在短锁下领取缺块或加入已有读取；先用一次原生Store batch读取自有缺块，再等待其他生产者。保留批量SQL，不改成逐块SQL，不增加客户端worker、查询内线程或IoTDB引擎并行。
- `StructuredBlobReadHandle`和Owner backend的scalar入口建立显式作用域，覆盖一次原始scalar的原有8 MiB分片；嵌套adapter复用外层。作用域结束释放，不放入prepared `shared_source`，不跨独立scalar常驻。超过容量时旁路读取，不承诺任意大范围全部去重。
- 每次物理子批次的数量、长度、digest完整验证后才发布。结果按原请求顺序成为各自私有输出。失败或取消生产者的异常不传给其他查询；存活等待者对失败块执行自己的首次物理batch。不会重试该调用者已经失败的读取。部分重叠失败时可能先读自有块、再读失败共享块，不能声称失败场景SQL数始终不增加。
- 保留版本／namespace／ready及冻结检查、源限定逻辑费用、payload demand、逻辑I/O预算、各自取消checkpoint和错误转换。force不复用，仍真实读取校验。跳过的物理读取不再观察该次瞬态I/O错误；这是复用已成功验证不可变数据的边界。
- 共享条目保留生产者完整临时预约直到最后引用释放。8 MiB是块字节上限，实际临时预约仍计入原运行时预算。后续temporary或metadata trace预约遇到压力时，在写入失败状态前、预算锁外释放当前请求的可选保留，再按原规则检查；其他消费者仍使用的数据继续计费。不增加预算，不捕获并重试已污染的预算失败。
- 可选共享元数据及两套子集请求容器在flight准入前准备；分配失败回退原完整batch。准入后子集选择使用经`static_assert`确认不抛异常的移动赋值及缩小容器。

本轮相对OLD只改6个产品文件：`PayloadBlockFlights.h`、`QueryStorageReadControl.h`、`StructuredBlobStorage.cpp`、`IoTDBStructuredBlobBackend.cpp`、`VersionDirectory.cpp`、`DecodedFragmentCache.cpp`。保留原有scalar move、单块共享、控制面、异步fetch和sender优化。

## 正确性验证

本机24项flight及13项真实Owner／受控Store测试通过。Linux Release最终产品**2,073项单元测试全部通过**，130项聚焦测试及11项启动配置检查通过；源文件、编译／链接参数和运行库经门禁核对。

新增覆盖原生batch保留、四组部分重叠块、反向`AB/BA`无互锁、请求输出顺序与私有所有权、已完成但仍活跃块复用、64／8 MiB容量及pin、请求结束释放、嵌套作用域、取消隔离、完整批次损坏不发布前缀、失败者独立首次读取、force、namespace和冻结检查。

真实`ReadHandle→PayloadArtifacts→Owner`测试使用17个不同1 MiB块、offset=1及16 MiB读取。将公共边界明确放入可准入的首8 MiB批次时，物理供数由18降到17 MiB；下一次独立scalar重新读取，证明无跨请求常驻。该用例明确限定容量条件，不声称超过8 MiB保留上限的所有边界都能命中。另以40 MiB临时预算验证主动回收、完整输出、原18 MiB／18 extents逻辑计费及最终零残留。真实DecodedFragmentCache另覆盖temporary和metadata trace压力下锁外回收，原资源不足仍保持失败状态。

## 实验条件与统计范围

两版均开启既有`payload_read_coalescing=true`，公共worker／owner reader／sender固定4／4／1。单客户端一个asyncio事件循环并发4条独立请求，分别选择同一文件中4个不同message；没有客户端worker。每条先发现catalog位置，再取值并fetch完整结果。共享CPU 2、3及两核quota，查询侧2 GiB；其余配置和依赖一致。

| 条件 | 小message | 大message |
|---|---:|---:|
| 每message编码长度 | 8,371 B | 2,076,659 B |
| 四message文件长度 | 33,484 B | 8,306,636 B |
| 每message网格 | 64×64，4,096点 | 1440×721，1,038,240点 |
| 四查询完整owned值与mask | 81,920 B | 20,764,800 B |
| 语料来源 | 原封印N128／V1语料 | 原已发布N8／V1大文件语料 |
| 本次候选／每条命中 | C1／K1 | C1／K1 |

没有重发语料、重建索引或修改head。大小组分别与自己的OLD对照；不是N128／C128正式多文件实验，也不是只改变块数的纯尺寸A/B。大文件为已有ecCodes受控合成科学GRIB，不称为天然观测数据。

先完成大小×OLD／NEW共4个独立资格诊断挂载，再进行两轮ABBA。每轮两种大小顺序反转；每种大小／每版4个性能波、16条查询。每次新daemon只执行一波4查询：总20个挂载、80条查询，其中16条诊断、64条性能。所有结果保留，无删慢样本或补跑替换。

T覆盖单条实际进入至完整owned值／mask就绪，D至catalog交付，Q为后续取值及交付，T=D+Q。wave从最早进入至最后owned输出完成；QPS为查询总数／wave时间总和，不将单条T除以4。oracle哈希核对、profile收集、挂载、采样和恢复在时钟外。性能挂载关闭Owner trace及preload；诊断不进入性能均值。

首次读取仅指新daemon的field／payload cache miss；四个性能cell各16次native field load、0次hit。未清空OS／DataNode／磁盘缓存，资格诊断也会预热共享状态。

## 独立诊断：消除大message的重复供数

| 指标 | 小OLD | 小NEW | 大OLD | 大NEW |
|---|---:|---:|---:|---:|
| payload批量SQL | 1 | 1 | 4 | 4 |
| 物理块引用／SDK返回行 | 1 | 1 | 11 | 8 |
| 返回payload字节 | 33,484 | 33,484 | 11,452,364 | 8,306,636 |
| reader数 | 1 | 1 | 4 | 4 |
| SDK execute API重叠峰值 | 1 | 1 | 4 | 4 |

大message减少3,145,728 B，即**物理读取量下降27.468%**；3个重复边界块对应的供数已消除。小message原来已共享一个块，没有进一步减少物理供数。所有查询仍完整返回自己的message。

Owner trace记录数量和字节，不记录对象ID；8个唯一块身份来自冻结源证明和机制测试，不能把聚合trace说成逐ID轨迹。SDK调用重叠峰值4及性能typed区间峰值4，证明客户端调用／FS执行重叠，不证明DataNode内部使用4核执行。

## 首次读取性能

| 组／指标 | OLD | NEW | 变化 |
|---|---:|---:|---:|
| 小：四查询平均wave | 49.991 ms | 56.579 ms | **增加13.179%** |
| 小：单条平均T | 49.007 ms | 55.483 ms | 增加13.214% |
| 小：单条平均D／Q | 3.904／45.103 ms | 7.158／48.325 ms | — |
| 小：吞吐 | 80.015 QPS | 70.698 QPS | 下降11.644% |
| 大：四查询平均wave | 345.187 ms | 279.232 ms | **下降19.107%** |
| 大：单条平均T | 330.234 ms | 260.020 ms | 下降21.262% |
| 大：单条平均D／Q | 3.086／327.147 ms | 3.668／256.352 ms | — |
| 大：吞吐 | 11.588 QPS | 14.325 QPS | 增加23.620% |

| ABBA轮次 | 小OLD／NEW平均wave | 小wave变化 | 大OLD／NEW平均wave | 大wave变化 |
|---|---:|---:|---:|---:|
| 0 | 51.013／58.730 ms | 增加15.127% | 309.702／284.954 ms | 下降7.991% |
| 1 | 48.968／54.428 ms | 增加11.150% | 380.671／273.509 ms | 下降28.151% |

大OLD四波为311.673／307.732／448.627／312.715 ms，NEW为285.970／283.939／274.899／272.119 ms。448.627 ms慢波完整保留，它放大了合并均值收益；两轮均改善、相邻4对均NEW更快，但只有4波／版，不能宣称固定19%普遍提速。波次才是重复单位，不能把共享波次内16条查询当作16个独立重复。

小OLD四波为51.498／50.528／48.388／49.549 ms，NEW为61.536／55.924／55.601／53.256 ms。两轮均回退，应作为需要继续定位的负面结果，不能简单删除或归为随机波动。

## 时钟解释与适用边界

| 每条性能查询阶段均值 | 小OLD／NEW | 大OLD／NEW |
|---|---:|---:|
| typed cursor | 34.581／37.745 ms | 227.412／180.979 ms |
| IPC编码 | 0.831／0.766 ms | 48.713／33.226 ms |
| IPC写出（编码子阶段） | 0.328／0.197 ms | 22.663／15.772 ms |
| 客户端fetch | 3.551／3.577 ms | 17.174／14.724 ms |
| 客户端decode-owned | 1.342／1.349 ms | 6.419／6.764 ms |

这些区间存在嵌套及跨查询重叠，不能相加当作独占时间。大message的cursor及编码均降低，符合减少供数后整条并发链路压力减轻的方向；不能将后续所有下降逐毫秒归为块复用。SQL数及懒连接需求仍未减少，GRIB解码、完整输出和交付量未改变。

小message物理供数未减少，且发现D和cursor各增加约3.2 ms；发现阶段不位于本次payload复用主路径，客户端fetch／decode均值则接近。当前时钟没有把该回退定位到唯一代码或资源原因，不宜只以共享注册表、复制或网络某一项解释。小message回退尚未解决，本次不能宣称所有GRIB负载都获益。

## 恢复与证据

80条全部通过精确版本／message、完整Float32和mask摘要、输出尺寸、闭合时钟及资源核对。20个挂载前后head和原始／私有journal保持不变；原始文件、发布artifact和小语料index摘要重新核验。独立标准库审计在远端和本机复算结果完全相同。

实际恢复核验通过：原配置字节及protected hashes不变、ASYNC／true有效设置恢复、当前293个DataNode线程均回到原0—31 CPU affinity，原cgroup及JVM内存参数恢复，20个私有挂载／daemon退出，临时cgroup全部移除。只读恢复核验仅额外执行一次SHOW ALL CONFIGURATION。

后处理审计初稿的产品白名单仍为早期batch-only两文件设计；在独立审计执行前的静态检查中同步为已冻结并经产品门禁核对的6文件，另存`independent_reuse_r2.py`及修正回执，原冻结脚本不变。未更改科学查询、结果、排程、统计阈值或重跑查询。

- [完整分析](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/collection-r1/analysis.json)
- [远端独立复算](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/collection-r1/independent-r2.json)、[本机独立复算](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/local-independent-r2.json)
- [原始queries](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/collection-r1/queries.jsonl)、[原始waves](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/collection-r1/waves.jsonl)
- [Linux构建及完整测试](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/products/NEW/build-report.json)、[2,073项XML](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/products/NEW/unit-tests.xml)
- [实际恢复核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/evidence/collection-r1/live-restoration-audit.json)、[本机证据校验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/multiblock-reuse-20261026/local-evidence-audit.json)

OLD SHA256：`593be107697d3daca157c160ca1b7c27e407e6d6dbc153de7e6048e83cc7f351`。

NEW SHA256：`98f8501491a71bfb83e16ccaad7c2f211ce49591831ff6a46d37e841180da044`。

归档SHA256：`5af41ccc7f71ddc1ae9da640624681cb3c44a194f184cc70381bac88297a7b60`。667个归档文件、192个冻结运行源文件均已校验；本轮未提交或推送Git。
