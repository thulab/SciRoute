# 跨请求块缓存：单客户端不同 GRIB message 的 ON／OFF 短探针

日期：2026-10-28。

**本轮观察到真实的重复供数消除，并伴随完整查询耗时下降。** 保持上层缓存开启，同一客户端依次查询同一文件中的4个不同 message；小文件 payload 返回量下降75%，四查询平均总耗时下降33.48%；大 message 的返回量下降27.47%，四查询平均总耗时下降5.97%。两轮完整耗时均改善，但样本很少，小文件降幅为4.58%～57.99%，不能将平均值当作稳定性能保证。大 message 的首条 ON 查询反而慢25.04%，该负面结果完整计入，原因尚未定位。

## 场景和控制变量

复用[10/26 多块复用探针](multiblock-payload-reuse-probe-20261026.md)已发布的两个不可变文件、精确版本、4组不同六属性谓词及原生 oracle；没有重发文件、建索引或修改 head。访问模式对应同一文件中依次查看不同变量／层次／时次。大文件仍是已有 ecCodes 受控合成科学语料，不能称为天然观测数据。

| 条件 | 小 message | 大 message |
| --- | ---: | ---: |
| 文件编码长度，B | 33,484 | 8,306,636 |
| 每 message 编码长度，B | 8,371 | 2,076,659 |
| 每 message 网格 | 64×64 | 1440×721 |
| 4次查询完整 values＋mask 输出，B | 81,920 | 20,764,800 |
| 查询顺序对应 payload 块下标 | `{0}`，`{0}`，`{0}`，`{0}` | `{0,1}`，`{1,2,3}`，`{3,4,5}`，`{5,6,7}` |

原 payload 块长1 MiB，末块取精确剩余长度。小文件来自封印 N128／V1 语料，大文件来自既有 N8／V1 语料；本轮均为 C1／K1，每条只取一个不同 message。这是已确认访问模式的诊断子场景，不替代正式 N／S 实验组，也不是4路查询并行或纯 S 尺寸对照。

OFF／ON 使用同一冻结产品 `9ce21d6da862695b18dbcdb3019b26467848fe056cac1fe5f0d4a8f7dcb3c987`，仅将 `storage.version_directory.payload_cache_bytes` 从0切换为8,388,608。请求内／在途共享均保持开启；GRIB 解码缓存、原查询路径、版本／权限／预算／取消／错误实现均保持不变，没有调用 clear、force、关闭上层缓存或人为制造驱逐。

一个客户端、一个 asyncio 事件循环、仅1条请求在途；上一条完整结果到手后才提交下一条。这样不存在同时在途的生产者可供复用，命中只能来自跨独立请求的驻留块。每个新 daemon 只查询这4个 message 各一次；每个序列都验证 **native-field loads=4、hits=0**，4份请求和结果哈希均不同。这里是首次查询各 message，不是重复同一查询。

公共 worker／Owner reader／sender 容量仍为4／4／1；未增加客户端 worker 或查询内并行。沿用 CPU 2／3、共享两核配额、查询叶组2 GiB；查询 runtime 预算2 GiB。DataNode 堆／direct 仍为15,232／2,176 MiB。

预先固定12个新挂载、48次查询：先用4个独立诊断挂载核查大小×OFF／ON的16次读取，再用8个不带 Owner trace 的性能挂载执行32次查询。性能为两轮 OFF→ON／ON→OFF，第二轮反转大小组顺序；每个配置每种大小各2个性能序列。没有删慢样本、补采或替换查询。OS／DataNode／磁盘缓存未清理，诊断也可能预热这些共享状态。

## 重复读取量：诊断与性能记录一致

以下均为4个不同 message 的一个序列。读取量是 **Store SDK 返回 payload BLOB 字节**，不是物理磁盘 I/O；独立 Owner trace 区分 payload 与元数据。

| 场景 | payload 批量 SQL：OFF→ON | 块引用／返回行：OFF→ON | payload 返回字节：OFF→ON | 返回量下降 | 消除的重复供数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 小 message | 4→1 | 4→1 | 133,936→33,484 B | **75.00%** | 100,452 B，3次同块重读 |
| 大 message | 4→4 | 11→8 | 11,452,364→8,306,636 B | **27.47%** | 3,145,728 B，3个边界块重读 |

小文件 ON 的后3条查询 payload 读取均为零，但仍各自加载／解码不同 message。大文件 OFF 每条取2／3／3／3块，ON 为2／2／2／2块；后3条仍各有一个缺块子批次，所以 SQL 数没有减少。ON 两组均达到源布局要求的唯一块字节下界，诊断内重复 payload 供数为零；唯一块数来自封印源布局，trace 本身不包含逐块对象 ID。

**未开启 Owner trace 的性能实例也复现了相同读取量。** 两轮小文件取值阶段 SDK 返回字节均为 OFF 133,936、ON 33,484，batch 调用均为4→1；大文件两轮均为11,452,364→8,306,636，batch 调用仍为4→4。这些 SDK 计数本身允许含元数据，但本轮取值阶段总字节恰与独立 payload trace 相符，不能仅凭诊断实例推定性能实例受益。

现有 `payload_cache.*` TLS profile 键在本轮仍未观测到，不将缺键伪造成零次命中。上述复用结论依据源布局、无解码命中的状态、独立 Owner trace、实际 SDK 增量及同一产品的唯一配置差异。

## 完整耗时

单条 `T=D+Q` 从实际进入查询开始，至客户端拥有完整 Float32 values 和 mask 结束；D 包含 catalog 发现与交付，Q 包含取值、结果获取及客户端数组物化。序列耗时从第一条进入至第四条完整结果就绪，包含请求间交接；不是把单条 T 除以4。oracle 哈希、profile 收集、资源快照、服务／挂载启动和恢复在计时外，序列中间不执行完整 oracle 哈希。

下表单位 ms，全部为无 Owner trace 的性能序列；平均及降幅按两轮原值计算。

| 场景 | 第0轮 OFF→ON | 第1轮 OFF→ON | 平均 OFF→ON | 平均序列耗时下降 | 两轮各自下降 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 小 message | 196.176→82.413 | 166.350→158.725 | 181.263→120.569 | **33.48%** | 57.99%／4.58% |
| 大 message | 996.399→921.464 | 881.790→844.617 | 939.094→883.040 | **5.97%** | 7.52%／4.22% |

单条完整 T 均值，小 message 为45.254→30.097 ms，下降33.49%；大 message 为234.224→220.454 ms，下降5.88%。它们与序列降幅略有不同，因为序列还包含请求交接间隙。按全部查询数／序列总时间计算，吞吐分别为22.07→33.18 QPS、4.26→4.53 QPS；这是顺序单客户端吞吐，不是4路并发吞吐。

为解释首条未复用与后续复用的差异，按事先固定的文件顺序另作分解，**不从主统计删除首条**：

- 小 message 的后3条完整 T 平均27.238→8.285 ms，下降69.58%；首条平均99.302→95.533 ms。第1轮 ON 首条122.754 ms，高于 OFF 的85.134 ms，明显压低该轮总收益。
- 大 message 的后3条完整 T 平均225.424→185.308 ms，下降17.80%；首条平均260.624→325.891 ms，反而增加25.04%。两轮首条 ON 均较慢，且首条返回字节完全相同；这一成本抵消了部分后续节省。该现象不能从主结果剔除，也尚不能归因于缓存插入或环境停顿，需另作诊断才能区分。

因此，本轮比此前大 S 探针增加了明确的机制证据：缓存确实减少跨请求重复供数，并且两轮完整耗时均向好。小 message 还减少了3次数据库请求，大 message 仅减少返回字节而保留4次 SQL；完整输出和首次解码仍全部执行，不能期望读取字节下降比例等于完整 T 下降比例。现有数据不足以给出各步骤的独占因果贡献，也不构成任意 S、并发度、访问序列或紧预算下的加速保证。

## 正确性、恢复与证据

48次查询均核对原精确版本／路径／unit、完整 values／mask oracle 和长度，12个序列均验证4次 native-field 加载、0次命中。发布 head、原／私有 journal 与原始文件身份前后相同。本轮未修改或重建产品；沿用上一轮通过2,115项单元测试、22项启动配置检查的冻结版本，不冒充本轮重跑了这些测试。新增实验协议离线检查本机和服务器均4/4通过；197个运行闭包文件及本机536个产品／测试源文件冻结清单均核验一致。

独立标准库审计从48行原始查询、12行序列、Owner trace及源证明复算读取量和所有主比例，本机从下载原件再次复算通过。归档331个成员逐项大小／SHA-256校验通过。现场只读审计确认原配置字节、ASYNC／true、DataNode进程身份及225个线程的原亲和、原cgroup／controller恢复，12个私有挂载及查询进程退出；只另发出一次配置查询，不执行科学查询。

后处理第一次启动时仅看到 worker 完成标记，恢复收据尚未写齐，因此在独立审计前因缺字段退出；随后将入口改为等待全部恢复标记后执行。该操作没有重新运行科学查询、修改产品或替换任何样本；最终复算、现场审计及归档均成功。

服务器证据根：

```text
/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/
  payload-cache-distinct-20261028/
    collection-r1/          # 全部查询、序列、profile、Owner trace、资源和恢复收据
    evidence-r1.tar.gz      # 白名单归档，331成员；私有配置／凭据／stderr留服务器
```

- [本机独立复算](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/payload-cache-distinct-20261028/local-independent.json)、[原始查询](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/payload-cache-distinct-20261028/evidence-local/collection-r1/queries.jsonl)、[原始序列](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/payload-cache-distinct-20261028/evidence-local/collection-r1/waves.jsonl)
- [实际服务恢复审计](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/payload-cache-distinct-20261028/evidence-local/collection-r1/live-restoration-audit.json)、[本机证据校验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/payload-cache-distinct-20261028/local-evidence-check.json)
- 归档 SHA-256：`704772666c3e05aa1a6a8a5c13721a6fb17e012b189f0eaacd04d0657b106fa3`。

论文及飞书表格未改写；未执行 Git 暂存、提交或推送。
