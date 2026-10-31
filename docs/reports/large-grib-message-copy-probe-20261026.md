# 大GRIB message跨payload块：减少复制的首次读取对照

日期：2026-10-26。

本次真实IoTDB查询探针中，四查询平均整批耗时从**355.588降至339.470 ms（下降4.533%）**，单条平均完整延迟从**324.018降至320.285 ms（下降1.152%）**，吞吐增加4.748%。3轮ABBA的整批均值均改善；6对相邻比较中4快2慢，属于小幅收益的初步证据，不能宣称已经获得稳定、通用的固定比例提速。

独立诊断确认：旧版实际执行4次标量拼装复制、共8,306,636 B，新版为0；两版均发生4次多块payload SQL、11个块引用、取回11,452,364 B，且完整数值及mask一致。本次确实触及减少复制路径，没有用暖缓存命中代替验证。

## 文件与查询

复用已有N8／V1受控大文件语料中的一个精确版本，查询候选集为C1。该文件由ecCodes实际编码，包含4个不同message，无padding；它是受控合成科学GRIB，不能称为ERA5等天然观测数据。未生成新语料、未重新发布、未重建索引，也未修改产品代码。这里不是N128／C128正式实验组，不能与上一轮小message的绝对耗时做纯尺寸A/B。

文件共8,306,636 B（约7.922 MiB），SHA256为`6a8b4bea84d201d1a7931c05643a7a5ca0b8ea54e4f8954d9ad7d3e7334f2119`。数据库逻辑路径为`/large/grib/raw/b003/b001/f000000-812fcb699e7a.grib`，精确版本`1792239886746`。命名空间`vd_v04_large_d4d9025322274a36`的实际head在14次挂载前后均保持seq=54及相同root／commit，原始和私有journal字节不变。

| message | offset | 编码长度 | 1 MiB块编号 | shortName／level／step／validityTime |
|---|---:|---:|---|---|
| 0 | 0 | 2,076,659 B | 0,1 | t／500／0／0 |
| 1 | 2,076,659 | 2,076,659 B | 1,2,3 | u／1000／1／100 |
| 2 | 4,153,318 | 2,076,659 B | 3,4,5 | v／700／2／200 |
| 3 | 6,229,977 | 2,076,659 B | 5,6,7 | t／850／3／300 |

共同谓词为`typeOfLevel=isobaricInhPa`、`validityDate=20261013`。每个message都大于1 MiB并跨多个payload块，同时小于8 MiB，命中本次优化的单个逻辑分片；这不等于单个payload对象。8个原始块均验证内容不同，4个范围共11个块引用，3个边界块重复取回。

每条查询返回完整1440×721网格，即1,038,240个Float32值与同数量的uint8有效性mask，owned输出5,191,200 B；每波4条共20,764,800 B。预先用原有ecCodes 2.34.1重新解码，所有单位属性、数值、mask及坐标摘要与原封印manifest一致；每次实际查询的完整值与mask均核对native oracle。

## 对照与时钟

直接复用上一轮冻结并已测试的两版：OLD为payload-coalescing产品，NEW为只改变两个标量入口的move版。重新核对产品、完整源码、原2051／2054项单元测试门禁、实际编译／链接参数和依赖；没有重新编译产品或叠加优化。两版均开启payload coalescing，公共worker／owner reader／sender固定4／4／1。

同一个客户端asyncio事件循环发出4条不同的独立请求，无客户端worker；每条先按六字段谓词取得一个message的catalog位置，再执行typed取值并异步fetch完整结果。共享CPU 2、3及两核quota，查询侧2 GiB；未引入查询内并行。两版均保留原来的前置reserve、校验、版本、预算、取消和错误路径。

先各执行OLD／NEW一个新的qualification diagnostic挂载，确认完整结果和实际读取／复制路径；然后执行3轮OLD→NEW→NEW→OLD，每个挂载只运行一波首次读取。共14个fresh daemon、56条查询：性能12波／48条，诊断2波／8条；每版性能6波／24条。所有波均4次native field load、4次alias miss、0次native field hit。未删除慢样本、替换样本或补跑查询。

诊断发生在全部性能测量之前，会使共用OS／DataNode缓存及代码可能已热；“首次读取”严格指每个新daemon的首次数据cache miss，未清空OS／数据库缓存，不是冷磁盘实验。性能挂载禁用Owner trace及LD_PRELOAD；两种诊断时钟完全不进入性能均值。

T从单条查询实际进入直到完整owned值和mask就绪；D为进入至catalog交付；Q从catalog交付到owned输出。wave从最早一条实际进入到最后一条owned输出就绪，吞吐为总查询数／总wave时间；单条延迟不除以4。oracle哈希、profile收集、资源采样、挂载与服务恢复位于计时外。

## 完整查询观测

| 指标 | OLD | NEW | 变化 |
|---|---:|---:|---:|
| 四查询平均wave | 355.588 ms | 339.470 ms | 下降4.533% |
| 单条平均完整T | 324.018 ms | 320.285 ms | 下降1.152% |
| 单条平均发现D | 4.081 ms | 3.181 ms | 下降22.060% |
| 单条平均取值Q | 319.937 ms | 317.104 ms | 下降0.885% |
| 吞吐 | 11.249 QPS | 11.783 QPS | 增加4.748% |

两版性能typed执行区间峰值均为4。这里是FS profile区间重叠，不能当作DataNode内部SQL的CPU并行度。

| ABBA轮次 | OLD平均wave | NEW平均wave | wave下降 |
|---|---:|---:|---:|
| 0 | 404.684 ms | 385.213 ms | 4.811% |
| 1 | 343.731 ms | 333.388 ms | 3.009% |
| 2 | 318.350 ms | 299.809 ms | 5.824% |

| 相邻配对 | 顺序 | OLD wave | NEW wave | NEW−OLD |
|---|---|---:|---:|---:|
| 0/0 | AB | 386.660 ms | 336.545 ms | -50.115 ms |
| 0/1 | BA | 422.708 ms | 433.882 ms | +11.174 ms |
| 1/0 | AB | 354.525 ms | 322.262 ms | -32.264 ms |
| 1/1 | BA | 332.936 ms | 344.515 ms | +11.579 ms |
| 2/0 | AB | 327.539 ms | 297.134 ms | -30.405 ms |
| 2/1 | BA | 309.161 ms | 302.484 ms | -6.678 ms |

3个轮次的整批均值同向，但相邻6对中有2对变慢；配对差值NEW−OLD均值−16.118 ms、中位数−18.541 ms、范围−50.115至+11.579 ms，样本标准差25.384 ms。波次才是统计单位，不能把24条共享波次的查询当成24个独立重复。样本少且共享一个DataNode，本报告不作强显著性或普遍性主张。

单条平均T只下降1.152%，不能用wave下降4.533%替代单条延迟改善。发现阶段也有变化，而修改位置在payload结果拼装；这提示仍存在本轮未完全消除的系统状态与调度波动。可以报告该场景下的观测净收益，尚不能将全部16.118 ms逐毫秒归因为少复制。

## 独立诊断：复制确实消失，物理读取相同

诊断只对冻结产品两个scalar入口的准确`memmove`返回PC计数：通过反汇编及产品SHA绑定，用main加载偏移适配ASLR，排除Owner块拷贝、摘要framing和GRIB解码等其他复制。helper无新增线程，最多128条，挂载就绪后创建普通ready门控文件才开始采集，退出后一次收集日志；两版完整输出均正确。helper的额外调用开销会扰动调度，因此只用于验证路径与局部时钟。

| 指标 | OLD诊断 | NEW诊断 |
|---|---:|---:|
| payload对象SQL | 4 | 4 |
| 每条SQL对象数 | 2／3／3／3 | 2／3／3／3 |
| 块引用数 | 11 | 11 |
| 取回payload字节 | 11,452,364 B | 11,452,364 B |
| reader数／新建连接数 | 4／3 | 4／3 |
| SDK execute API区间重叠峰值 | 4 | 4 |
| 标量拼装复制调用 | 4 | 0 |
| 标量拼装复制字节 | 8,306,636 B | 0 B |
| 这些复制的wall-clock时长总和 | 11.894 ms | 0 ms |

OLD的4次目标复制分别为2,076,659 B，全部归属于真实ReadHandle入口；NEW有成功加载helper的ready记录、4次实际大message加载及4次多块SQL，因此零记录不是未触及供数路径。减少复制并未减少SQL次数或重复的边界块读取。

OLD目标复制区间彼此不重叠，union=11.894 ms；它们分布在176.222 ms包络内，其中3.078 ms与其他payload Owner读取调用包络重叠。其余时间仍可能与其他解码、编码、交付或客户端活动重叠，不能命名为完整查询的独占复制成本。该wall-clock包含调度抢占／内存访问等等待，不是线程CPU时间；不能直接从性能wave扣除11.894 ms，也不能与SDK嵌套时钟相加。

两版诊断SQL调用次数和返回量相同，但SDK时间本身仍有差异：OLD／NEW的payload Owner调用union分别234.249／275.311 ms，execute API union分别177.026／204.180 ms。它们来自单独的两个诊断挂载，不是性能样本；这再次说明不能把不同挂载的耗时差机械归为同一局部优化。

当前证据确认了少一次大message复制，并观察到约1%单条延迟、约4.5%整批耗时的小幅净收益；它没有复现本机8 MiB内存微探针的51%全链路收益。完整查询仍包括多块SQL、完整性验证、GRIB解码、IPC交付和客户端owned复制，本轮没有对所有阶段做互斥分解，不能据差额指定唯一主要瓶颈。

## 正确性、恢复与取证

56条均验证精确路径／版本／message、完整元素数、Float32及mask摘要、owned输出及首读cache状态、闭合时钟与资源。14次挂载均保持实际head、原始与私有journal不变，原始GRIB和封印artifact摘要不变。未修改产品、科学数据、原namespace head或索引。

服务、原配置与ASYNC／true有效设置已恢复；225个DataNode线程均恢复原32个CPU的亲和性，JVM内存参数不变，临时cgroup全部移除。原始数据和两版产品源码、测试XML、运行源码及本地独立复算均已核验。

保留的准备／后处理修正：

- 初始使用旧`head`工具时，其写入能力门禁拒绝当前ASYNC原服务；改用只读`inspect`核对同一head与精确文件，未改服务或重新发布数据。
- 首次离线审计错误地要求工作plan与源plan字节相同；工作副本由`atomic_json`排序键，parsed对象完全一致。r2只修正为验证冻结源plan的原始hash以及源／工作／preflight对象精确相等；保留r1失败回执，未修改运行源码、原始查询或重新取样。
- 首次打包因native检查脚本仅在本机而远端缺该源文件中止。补齐归档源文件，将部分目录保留为`evidence-incomplete-r1`，重新打包；不涉及科学查询重跑。

## 证据

- [完整分析](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/analysis.json)
- [独立复算](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/independent-r2.json)
- [原始queries](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/queries.jsonl)
- [原始waves](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/waves.jsonl)
- [OLD复制诊断](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/corpora/existing-large-grib/diagnostic-OLD/scalar-copy.jsonl)
- [NEW复制诊断](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/corpora/existing-large-grib/diagnostic-NEW/scalar-copy.jsonl)
- [OLD Owner诊断](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/corpora/existing-large-grib/diagnostic-OLD/owner-read.jsonl)
- [NEW Owner诊断](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/corpora/existing-large-grib/diagnostic-NEW/owner-read.jsonl)
- [实际恢复核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/evidence/collection-r1/live-restoration-audit.json)
- [审计修正记录](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/independent-correction.json)
- [本地接收审计](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/large-message-copy-20261026/local-evidence-audit.json)

OLD SHA256：`111768760be796c2d6caf83a49b7a752449e964fb21bb4bb4f47945ca379a99b`。

NEW SHA256：`593be107697d3daca157c160ca1b7c27e407e6d6dbc153de7e6048e83cc7f351`。

归档SHA256：`68b856c81501bb2333380c339b6e71bda443fa3ee283092401eeb351de261402`；373个归档文件、196个冻结运行源码文件，本地独立复算与远端一致。原科学查询没有重跑或替换。
