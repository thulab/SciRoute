# 不同 GRIB message 的 payload 读取合并探针

本次合并消除了同一不可变块的重复供数。首次访问的四查询整批耗时从 **122.725 ms 降到 60.019 ms，下降51.095%**；独立诊断中的物理对象SQL从4次降到1次。暖缓存统计没有显示稳定收益：吞吐合并值下降2.171%，两个轮次的变化方向相反。

这是同一新产品关闭／开启合并的对比，不能解释为相对历史串行产品的累积收益，也不能把SQL调用时间当作DataNode内部CPU执行时间。

## 场景和统计范围

复用原有 N128／V1／D4 封印GRIB语料，只选择其中同一个文件、同一精确版本，即 **C1**。同一个客户端事件循环同时发送4条独立请求，每条使用不同谓词，分别命中一个不同message，即每条 **K1**。每条返回4,096个Float32值及mask，完整owned输出为20,480 B。没有增加客户端worker，也没有引入查询内并行。

| 条件 | OFF | ON |
|---|---|---|
| 产品 | 同一候选二进制 | 同一候选二进制 |
| `payload_read_coalescing` | `false` | `true` |
| 客户端在途查询数 | 4 | 4 |
| 公共查询worker／owner reader／sender | 4／4／1 | 4／4／1 |
| 资源 | CPU 2、3，共享两核quota；查询侧2 GiB | 相同 |
| 单查询候选文件／命中message | 1／1 | 1／1 |

性能顺序为第0轮 OFF→ON、第1轮 ON→OFF，每次使用新挂载及私有owner journal。每个性能挂载执行首次访问1波、预热1波、暖缓存测量3波，每波4条查询；另外使用独立OFF／ON诊断挂载，各执行1波首次访问。

共完成 **88条查询、22波**：首次访问16条、预热16条、暖缓存主统计48条、独立诊断8条。所有查询保留，没有剔除慢样本或补跑替代样本。这里的“首次访问”指新daemon的首次查询，未清理操作系统或数据库缓存，不能称为冷磁盘实验。

单条T从实际进入查询到最终owned值和mask就绪；整批时钟从最早一条进入到最后一条owned输出就绪。吞吐按总查询数除以总整批时长计算。oracle哈希核验、profile收集、资源采样和启动／恢复位于该时钟外。单条延迟没有除以4。

## 首次访问：整批耗时下降约一半

每个配置有2波、8条查询。

| 指标 | OFF | ON |
|---|---:|---:|
| 四查询平均整批耗时 | 122.725 ms | **60.019 ms** |
| 单条平均T | 121.027 ms | 58.927 ms |
| 单条平均发现阶段d | 5.932 ms | 6.227 ms |
| 单条平均取值阶段q | 115.095 ms | 52.700 ms |
| 吞吐 | 32.593 QPS | 66.646 QPS |

整批耗时下降 **51.094974%**。本轮变化主要体现在取值阶段；发现阶段的均值没有降低。

| 轮次 | OFF整批耗时 | ON整批耗时 |
|---|---:|---:|
| 0，OFF→ON | 152.970 ms | 55.839 ms |
| 1，ON→OFF | 92.481 ms | 64.198 ms |

两轮都观察到首次访问收益，但样本只有每配置2波，OFF本身波动较大。该结果支持继续使用合并读取，不能作为稳定51%提升的保证。

## 暖缓存：本轮没有稳定收益

每个配置有6波、24条查询。此阶段原生场已缓存，不能用它衡量首次payload供数节省。

| 指标 | OFF | ON |
|---|---:|---:|
| 四查询平均整批耗时 | 6.988 ms | 7.143 ms |
| 吞吐 | 572.440 QPS | 560.010 QPS |
| 单条平均T | 6.044 ms | 6.267 ms |
| 第0轮吞吐 | 611.671 QPS | 661.585 QPS |
| 第1轮吞吐 | 537.938 QPS | 485.474 QPS |

合并吞吐变化为 **−2.171471%**，整批均值增加2.220%。第0轮ON更快，第1轮ON更慢，因此不宣称暖缓存稳定获益，也不据此认定存在确定的性能回归。

## 独立诊断：重复SQL和懒连接是可见成本

诊断使用独立挂载，开启owner读取分段时钟，其他详细观测关闭。挂载就绪后才创建普通门控文件、开始录制；4096条上限保持。所有有效记录均位于实际查询wave内，启动SQL没有录制。诊断耗时不混入上述性能统计。

| 诊断指标 | OFF | ON |
|---|---:|---:|
| 物理payload对象SQL调用 | 4 | **1** |
| 使用的owner reader数 | 4 | 1 |
| 取回payload字节 | 133,936 B | **33,484 B** |
| 新建reader连接次数 | 3 | 0 |
| SQL执行API区间重叠峰值 | 3 | 1 |
| payload读取包络跨度 | 33.767 ms | 14.233 ms |
| 诊断四查询整批耗时 | 67.919 ms | 51.860 ms |

该文件只有33,484 B，落在一个payload块内。不同message请求原来分别读取同一个块；本次ON只供数一次，物理读取次数和字节量均下降75%。这里减少的是重复物理读取，四个请求及四份输出仍独立。

以下均为**每次实际物理调用的平均值**，不是四查询整批的独占耗时：

| SDK客户端分段 | OFF | ON |
|---|---:|---:|
| 获取reader，含必要的新连接 | 14.509971 ms | 0.001129 ms |
| `executeQueryStatement` API | 10.531062 ms | 12.841372 ms |
| execute返回至首个SDK行就绪 | 0.271636 ms | 0.339513 ms |
| 首行就绪至消费完毕 | 1.196870 ms | 0.991121 ms |
| 关闭operation handle | 0.000522 ms | 0.001083 ms |
| 归还reader | 0.0007565 ms | 0.000828 ms |

OFF需要3次懒连接，获取reader阶段有明显成本；执行API重叠峰值为3，已经能够并发发出不同请求的对象SQL。ON复用一个现有reader，避免另外3次同块读取及相应连接需求。ON唯一一次SQL的API时间并没有更短，收益不能归因为每次SQL执行速度提高。

关闭handle和归还reader均在约微秒量级，本次未显示它们足以解释几十毫秒等待。最后一次payload读取完成后，OFF仍有23.306 ms、ON仍有28.500 ms才到整批owned输出完成。读取包络之外仍有发现、准备、编码和交付等工作，但当前时钟没有完整分解这些部分；不能把整批与payload包络的差额命名为串行计算或CPU瓶颈。

这些时钟记录的是SDK调用边界、SDK首行就绪、消费、校验和资源释放。连接包含于获取reader；copy／digest／visitor等局部计时存在嵌套，不能再次加到已闭合的独占分段总和中。SQL API区间重叠证明客户端调用并发，不能证明DataNode内部在多少核上同时执行。

单message原生生产者会按既有逻辑清除`active_query_profile`，因此本次profile中的SQL／BLOB线程局部计数均为0，不能用于交叉校验物理读取，更不能相减推算控制SQL数量。物理计数以独立owner trace为准。

## 实现边界与正确性

合并位于Owner的不可变payload读取入口，按命名空间、incarnation、对象类型／协议／内容ID和精确长度匹配。上限为64个保留entry、8 MiB共享数据，包含已完成但仍由消费者持有的条目；只共享同时发生的读取，完成后从查找表移除，后到请求重新读取，不增加payload常驻缓存。接口与诊断开关见[实现约定](../contracts/payload-read-coalescing.md)。

仅单块、非强制的payload读取进入合并。强制校验读取和多块批量读取继续使用原路径。物理读取及完整数量、长度、digest验证由生产者完成后才发布，各调用方取得独立owned字节；共享期间保留原temporary reservation，直至最后一个使用者完成复制。

版本／Owner就绪检查、逻辑预算和调用方资源记账保留。取消遵循原有`storage_read_control`上下文及隔离边界；本次未扩大GRIB底层SQL的即时取消能力。等待者只使用自身已有checkpoint；失败生产者不把异常缓存或直接重放给其他查询，存活等待者执行自身的首次物理读取。

Linux候选产品的 **2051项单元测试和11项启动配置检查通过**。有效r3的88条查询均通过精确message／版本、输出内容及尺寸、owned时钟和执行模式检查；独立复算通过。服务、原数据库配置和资源分配已恢复。

## 失败尝试与后处理修正

| 尝试 | 状态及处理 |
|---|---|
| r1 | 科学查询0条；发现TLS分析校验错误后人工停止，恢复服务／资源，保留证据，不入统计。 |
| r2 | 84条查询正确，其中性能80条；启动读取耗尽4096条trace上限，诊断缺少查询内记录而被拒绝。自动恢复，完整保留，不入r3统计。 |
| r3 | 加入就绪门控后，使用新冻结产品重新完成全部88条，结果完整有效。 |

r3首次离线独立校验还发现一个比较器错误：各挂载必需的私有owner目录被当作调参差异。修正仅放在`postprocess-r1`：逐项核对launcher receipt及私有目录归属后，再比较公共设置；其他设置差异仍拒绝。冻结运行脚本和原始结果未修改，没有为此重跑查询。修正原因、原／新脚本SHA及离线验证均有receipt。

本地接收审计随后发现，独立复算的并发直方图使用整数键，而JSON对象读回后使用字符串键，直接字典比较因此失败。`postprocess-r2`仅将这类键规范为JSON字符串再比较，所有值和其他字段仍严格相等。修正后全部审计通过；原归档、冻结源码和查询数据均保持原样，没有重跑。

## 证据位置

- [实际分析](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/analysis.json)、[独立复算](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/independent.json)、[原始queries](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/queries.jsonl)、[原始waves](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/waves.jsonl)。
- [OFF诊断trace](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/corpora/FM1-grib-N128-V1-S0-base/diagnostic/OFF/owner-read.jsonl)、[ON诊断trace](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/corpora/FM1-grib-N128-V1-S0-base/diagnostic/ON/owner-read.jsonl)。
- [构建与测试receipt](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/product-evidence/build-receipt.json)、[恢复审计](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/collection-r3/live-restoration-audit.json)、[私有目录比较修正receipt](../../experiments/cross_version/work/payload-coalesce-20261025/evidence/postprocess-r1/independent-correction.json)、[JSON键比较修正receipt](../../experiments/cross_version/work/payload-coalesce-20261025/postprocess-r2/correction-receipt.json)。
- [本地证据核验](../../experiments/cross_version/work/payload-coalesce-20261025/local-evidence-audit.json)：263个归档文件、190个冻结运行文件全部核对，独立复算与远端结果一致。

候选二进制SHA256：`111768760be796c2d6caf83a49b7a752449e964fb21bb4bb4f47945ca379a99b`。

归档SHA256：`83f3cf8d19bcfed81769e4c0235f9b3db955500c5aaf81d8295f2558ad1fb84c`。

本次证据限于N128语料中的C1、4条不同K1查询、一个可共享payload块及两核资源。尚未覆盖跨文件／跨块、HDF5／Arrow、多个客户端或完整C128扫描，也没有数据库内部CPU／磁盘执行轨迹；不能推广为这些场景的通用收益。
