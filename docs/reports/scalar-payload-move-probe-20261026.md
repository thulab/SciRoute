# 标量payload减少拼装复制：实现与探针

日期：2026-10-26。OLD为此前已验证的payload-coalescing产品；NEW只改变两个产品源码文件，在不需要拼接时移动已有结果buffer。本轮不扩大跨请求块共享，不加入查询内并行或客户端worker。

## 实现边界

修改[ReadHandle标量入口](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/StructuredBlobStorage.cpp:514)和[Owner backend标量入口](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/src/IoTDBStructuredBlobBackend.cpp:2481)：原来每个逻辑分片都`insert`到预分配输出；现在当`count==length`时，先保留原batch数量／长度校验，再通过`std::move`接管唯一结果vector。大于8 MiB的标量请求仍按原偏移与长度依次分片、拼接。

“一个逻辑分片”不等于“一次物理SQL”。一个≤8 MiB逻辑range仍可能跨9个1 MiB块，或在Owner层形成多个物理batch；本改动不改变这些读取、去重或分批规则。

原来的`reserve(length)`仍在调用reader之前执行，以保留分配失败先于物理读取的顺序。本轮减少一次长度为输出字节数的复制，不减少这次预分配，也不称整个查询链路为零复制。使用默认allocator的vector move赋值不新增分配或异常点，但返回buffer地址、capacity和具体allocation的释放时点会改变。版本／namespace／完整校验、预算scope及原逻辑费用、取消checkpoint、异常转换、Owner资源与flight pin路径均未改动。

## 正确性

- 本机真实绑定ReadHandle入口38项全部通过，新增3项覆盖1 B／37 B／恰8 MiB的内容和地址转移、offset=1、零长度不调用reader、空／多／短／过长batch拒绝、原错误码及消息、多片累计ledger、第二片取消异常。
- 同一新增测试配合旧ReadHandle源码运行：38项中37项通过，唯一失败为地址转移测试的三个长度断言；所有内容及其他语义检查通过。它确认优化前后确实改变了buffer复制，而非只有代码形状变化。
- Linux Release完整候选构建完成，2054项单元测试全部通过；启动配置检查通过。既有版本、预算、取消、force与共享读取回归继续包含在完整测试中。新增的地址回归直接覆盖ReadHandle；backend Owner分支由构建、既有测试及静态分支核对支持，本轮没有声称其新增地址转移专项集成测试。

本机[正确性回执](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/local-correctness/receipt.json)、[当前XML](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/local-correctness/after-tests.xml)、[旧源码对照XML](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/local-correctness/before-tests.xml)。

## 本机微探针：单分片复制下降

通过真实`StructuredBlobReadHandle::read_payload_range`，reader从内存中分配并复制原始数据。OLD／NEW分别链接旧／新ReadHandle源码，其他输入与编译参数一致，`clang++ -O3 -DNDEBUG`，macOS arm64。按OLD／NEW／NEW／OLD／NEW／OLD／OLD／NEW顺序运行，每臂4个独立进程；每尺寸每进程先16次预热，实际重复次数分别为2048／1024／256／64／32。未固定macOS CPU亲和性，仅作为局部机制与成本探针。

计时覆盖整个真实scalar方法，包括reader自身生成输出；完整逐字节比较在计时外。单片每次均检查NEW返回地址等于reader分配地址、OLD不等于；所有输出内容一致。两臂reader调用数及提供字节完全相同。

| 逻辑读取长度，offset均为1 | OLD平均 | NEW平均 | 平均耗时下降 |
|---|---:|---:|---:|
| 8,371 B | 0.512 µs | 0.441 µs | 13.98% |
| 33,484 B | 1.152 µs | 0.796 µs | 30.85% |
| 1 MiB | 34.656 µs | 18.155 µs | 47.61% |
| 8 MiB | 1264.703 µs | 616.341 µs | 51.27% |
| 16 MiB | 1633.926 µs | 1660.286 µs | -1.61% |

16 MiB是未改的多分片对照，平均值略升、各进程中位数汇总略降，未显示明确收益。8,371 B是当前N128诊断语料中一个message的长度，本机均值只节省约0.072 µs；8 MiB均值节省约0.648 ms。这两个量级不能跨平台当作Linux完整查询收益。

[微探针源码](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/tests/benchmarks/versioning/scalar_move_probe.cpp)、[逐轮原始结果](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/micro/rows.json)、[统计及顺序](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/micro/summary.json)、[构建回执](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/micro/build-receipt.json)。

## Linux N128真实查询探针

复用N128／V1／D4封印GRIB语料，选择其中同一文件、同一精确版本（C1）；单客户端一个asyncio事件循环同时发4条不同谓词请求，每条命中一个不同message（K1），不增加客户端worker。这是此前已确认的诊断子场景，**不是重新跑完整C128实验组，也没有新增或重发语料**。文件共33,484 B，四个message各8,371 B；每条结果为4096个Float32值与mask，owned输出20,480 B。

OLD／NEW均启用现有payload coalescing，公共worker／owner reader／sender均为4／4／1；CPU 2、3、共享两核quota，查询侧2 GiB。Release编译器、实际编译／链接参数和运行库一致，仅两个产品源码文件不同。按OLD→NEW、NEW→OLD运行4个fresh daemon挂载，每次首次读取1波、预热1波、暖缓存测量3波，每波4条查询。共80条、20波，未替换样本或排除慢样本。

单条T从实际进入查询直到owned值与mask就绪；wave从最早一条实际进入到最后一条owned输出完成。QPS按该阶段总查询数除以总wave耗时计算；不将单条延迟除以4。oracle哈希、profile收集、资源采样及启动／恢复在计时外。“首次读取”只表示fresh daemon的数据cache miss，未清理OS／数据库缓存。

| 阶段／指标 | OLD | NEW | 变化 |
|---|---:|---:|---:|
| 首次读取：四查询平均wave | 93.388 ms | 52.496 ms | 下降43.79% |
| 首次读取：单条平均T | 92.650 ms | 51.603 ms | — |
| 首次读取：单条平均d／q | 6.512／86.138 ms | 4.105／47.498 ms | — |
| 首次读取：吞吐 | 42.832 QPS | 76.196 QPS | 增加77.89% |
| 暖缓存：四查询平均wave | 6.379 ms | 5.484 ms | 下降14.02% |
| 暖缓存：单条平均T | 5.417 ms | 4.730 ms | — |
| 暖缓存：单条平均d／q | 1.816／3.601 ms | 1.451／3.279 ms | — |
| 暖缓存：吞吐 | 627.105 QPS | 729.334 QPS | 增加16.30% |

首次读取每臂仅2波／8条；暖缓存每臂6波／24条。预热另每臂2波／8条，完整保留但不混入上述两类统计。profile中首次读取typed执行区间峰值两臂均为4；暖缓存OLD峰值3、NEW峰值4。这里仅是FS执行区间重叠，不代表DataNode SQL内部执行并行度。

| 首次读取轮次 | OLD wave | NEW wave | wave下降 |
|---|---:|---:|---:|
| 0 | 127.898 ms | 54.155 ms | 57.66% |
| 1 | 58.878 ms | 50.838 ms | 13.66% |

**上述完整查询下降是本轮观测值，不能计为已证实的复制优化收益。** 暖缓存两臂均24次native field／alias hit、0次load、0次message hash；源码中alias和native field双命中不会调用encoded message loader，绕过本次改变的scalar读取，却仍出现约14%的wave下降。这说明系统状态、调度与测量波动等因素仍能影响对比，本轮没有把它们逐一分离。

首次读取两臂各8次native field load、8次alias miss及8次hash，确实触及供数。但OLD首波127.898 ms、第二波58.878 ms，基线波动较大；两个配对改善幅度57.66%与13.66%差异明显。当前message仅8,371 B，本机微探针对应复制节省约0.072 µs，不能据此解释几十毫秒的Linux整批差异，也不能把跨平台数值视为严格上界。本轮未增加独立physical owner trace，因此不宣称两臂每波实际SQL计数、coalescing重叠形状或懒连接次数完全相同；scalar逻辑批次／SQL构造未改变由源码可证。

结论是：**少一次scalar拼装复制的机制与局部大buffer成本下降已验证；当前小message的端到端因果收益尚未证实。** 大于8 MiB的跨片重复块、现有跨查询多块供数和其他hash／copy均仍存在。保留这处小改动，但它不足以解释或解决此前查询并行收益有限的问题；后续物理读取优化仍应优先关注真实多块负载中的重复供数，而不是将这组43.79%计入累计加速。

80条均通过exact message／版本、输出内容及尺寸、owned时钟、执行模式与资源检查；封印head、index及私有journal字节未改变。服务与原配置已恢复，实际DataNode的226个线程均恢复原32个CPU的亲和性，JVM内存参数及ASYNC／true有效配置已核验，临时cgroup已移除。

[实际分析](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/collection-r1/analysis.json)、[原始queries](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/collection-r1/queries.jsonl)、[原始waves](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/collection-r1/waves.jsonl)、[独立复算](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/collection-r1/independent-r1.json)、[实际恢复核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/collection-r1/live-restoration-audit.json)。

额外恢复核验首次误用系统Python，因缺pyarrow在加载依赖时退出；随后使用既有实验venv成功，未重跑任何科学查询或替换样本，见[核验尝试记录](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/postcheck-attempts.json)。主实验及其恢复流程本身均成功。


## 产品身份与证据

OLD SHA256：`111768760be796c2d6caf83a49b7a752449e964fb21bb4bb4f47945ca379a99b`。

NEW SHA256：`593be107697d3daca157c160ca1b7c27e407e6d6dbc153de7e6048e83cc7f351`。

新增[回归测试](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/tests/unit/test_StructuredBlobStorage.cpp:1074)。没有提交或推送Git；保留此前已授权优化的工作区改动。

[Linux构建与测试回执](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/product-evidence/build-report.json)、[2054项XML](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/evidence/product-evidence/unit-tests.xml)、[本地接收审计](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/scalar-move-20261026/local-evidence-audit.json)。264个归档文件、185个运行源码、1033个完整候选源码文件及本地213个产品文件均已核对；本地独立复算与远端一致。实验归档SHA256：`c0d6f1069ffd823164343b4b65dfdc1a953ff0b164b1e421f39eca07304c4f2b`。
