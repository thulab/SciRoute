# 单客户端查询同一GRIB文件中4个不同message：现有架构探针

2026-10-25。已完成88条查询、22个四查询批次，其中48条用于暖缓存性能比较；所有结果通过独立原生oracle与版本验证，原服务和资源恢复。本轮未修改或重建生产产品。

**当前架构支持不同message的完整查询并行推进：四查询暖缓存整批时间从12.117降至6.687 ms，吞吐提高81.21%。但单条在途延迟上升；独立诊断未观察到原生values调用重叠，因此不宣称4路原生解码或IoTDB SQL并行。**

## 探针范围与四个真实不同的查询

复用原封印GRIB N128/V1/D4语料，确定性选择候选路径排序首个文件。四条查询的候选文件和精确版本完全相同，分别使用不同的完整六属性等值AND谓词，每条仅命中一个不同message。实际范围为**库存N128、候选C1、每query K1**；不是原FM1 N128/C128/K2，不替换原case，不与原表性能直接比较。

同一文件版本为`1792568971714`，文件33,484 B；所有查询的typeOfLevel均为isobaricInhPa、validityDate均为20261013。每个message的4096个Float32与uint8 mask由客户端持有，共20,480 B／query，整批81,920 B。四个原生value SHA均不同。

| 查询 | shortName | level | step | validityTime | 物理message |
|---|---|---:|---:|---:|---|
| Q1 | v | 700 | 0 | 0000 | message:0:8371 |
| Q2 | t | 850 | 1 | 0100 | message:8371:8371 |
| Q3 | u | 1000 | 2 | 0200 | message:16742:8371 |
| Q4 | v | 700 | 3 | 0300 | message:25113:8371 |

每条完整链路均为`format_catalog → 单message typed → result_fetch → owned值／mask`，不是先在客户端准备好四个结果或仅取元数据。相同文件只读仍共享索引与存储块；不同内容不保证没有共享资源，但本轮首次取值的四个field cache loads均为1、waits均为0，没有相同field生产者合并等待。

## 对照、资源与时钟

两臂使用同一个当前bfee产品：公共worker4、owner reader4、sender1、queue64、batch4；同一个Python进程与asyncio事件循环，不增加客户端worker／executor，不修改查询内并行策略。SEQ每波四条实际顺序完成；FOUR四条同时在途。每次请求仍新建连接，采用同一raw_socket/result_fetch_v1协议。控制变量是offered concurrency=1或4，而非切换产品或扩服务端线程数。

两轮顺序分别SEQ→FOUR、FOUR→SEQ。每个独立性能挂载固定1个first_touch、1个warmup、3个measurement波，每波四个不同message；查询提交顺序按固定规则轮换。另有两个独立诊断挂载SEQ／FOUR，各1波，只有它们加载原生观测库，不进入性能统计。共6挂载、88query，无失败替换、慢样本剔除或追加有利样本。

资源沿用CPU2–3、共享父组两核quota、query叶2GiB、原DataNode heap15232MiB／direct2176MiB。每挂载为新daemon，未清OS缓存，first_touch不代表冷盘。

每条T从实际进入查询到最终owned值／mask，T=d+q；四查询整批使用实际最早入口至最后交付，QPS=总完成查询数／测量批次总跨度。尚未启动的串行查询等待不计入其单条T，但计入真实整批完成时间。oracle／请求hash、profile收集、资源采样、启动及恢复均在查询波之外；Arrow解码与owned复制在T内。原生诊断开销仅存在独立诊断波。

## 暖缓存性能

每臂24条测量查询／6个四查询批次，统计所有固定样本。

| 指标 | 顺序SEQ | 4路FOUR | 变化 |
|---|---:|---:|---:|
| 吞吐 QPS | 330.11 | 598.19 | +81.21% |
| 四查询整批平均 ms | 12.117 | 6.687 | 下降44.82% |
| 单条平均T ms | 2.995 | 5.392 | +80.00% |
| 单条发现d平均 ms | 0.896 | 1.633 | 含在途争用／排队 |
| 单条取值交付q平均 ms | 2.099 | 3.759 | 含取值、编码、socket与owned交付 |

| 轮次 | 顺序QPS | 4路QPS | 吞吐提升 |
|---|---:|---:|---:|
| 1 | 339.54 | 597.56 | +75.99% |
| 2 | 321.18 | 598.83 | +86.44% |

两轮均有吞吐改善，但这是小规模描述性探针，不提供统计显著性或尾延迟保证。单条延迟和整批吞吐回答不同问题：一次处理完四个不同结果更快，不表示每条查询更快。

首次访问单独保留，未混入上述暖缓存统计：

| 轮次 | SEQ整批 ms | FOUR整批 ms |
|---|---:|---:|
| 1 | 168.175 | 71.082 |
| 2 | 85.124 | 78.530 |

每个首次访问批次均有四次真实原生field加载。首次访问包含目录／版本／索引／payload等缓存填充，两轮串行结果差异较大；这些数值不证明冷盘伸缩或稳定冷启动加速。

## 哪些阶段实际并行

| 观测 | SEQ | FOUR | 可支持的结论 |
|---|---:|---:|---|
| 客户端完整查询在途峰值 | 1 | 4 | 四个不同查询确实同时在途 |
| 暖缓存typed服务端profile包络峰值 | 1 | 3 | 不同message取值请求的执行包络重叠 |
| 首次访问typed包络峰值 | 1 | 4 | 四个真实加载请求可以重叠推进 |
| 独立诊断实际values调用数 | 4 | 4 | 每个不同message均进入原生值读取 |
| 诊断values调用的不同服务端线程数 | 1 | 4 | 原生取值任务确实由多个服务端线程执行 |
| 诊断values调用区间重叠峰值 | 1 | 1 | **本次未观察到values解码重叠** |

诊断拦截`codes_get_double_array(key="values")`，保留真实调用与返回状态，并记录单调时钟／线程CPU。FOUR四次调用壁钟累计0.156 ms，平均0.039 ms；第一次调用开始至最后结束跨度4.552 ms，主要是调用之间的间隔。对应SEQ为累计0.165 ms、跨度32.833 ms。

因此不能把完整查询吞吐改善归因于四个解码器同时计算。这组64×64数据的values调用很短且到达错峰；重叠峰值1也不足以证明解码必然受全局串行锁限制。该观测没有覆盖全部句柄创建、坐标提取或库内部阶段，不用它们的包络差值推断独占锁耗时。源代码中的句柄创建短临界区仍存在，本轮没有测其独占时间。

原生事件未携带query ID，FOUR的事件与typed包络存在2种一对一时间匹配，不能唯一归属；每条独立正确输出、每profile一次field load及四次原生调用共同验证不同内容确实执行。profile包络也不等于CPU核数。本轮只有两核，不宣称四核计算或数据库SQL并行，亦不把并发窗口内的daemon-wide IoTDB counter增量相加当实际SQL数。

## 判断与后续方向

现有公共池可以承载单客户端查询不同message的查询间并发，尚无证据要求先重构为新的通用调度结构。当前收益来自完整请求链路的重叠；不同message的原生值解码本轮仍无重叠证据。后续应继续围绕这个明确场景检查共享文件／候选集准备、任务到达错峰和结果交付，再决定是否需要优化原生解码入口。任何共享准备都应保留每条查询的版本准入、权限、预算、取消和错误语义。

## 验证与证据

- 88条查询全部通过原封印原生oracle、exact版本、message offset／length、独立路由与owned值／mask验证；48条用于暖缓存性能统计。
- 观测库Linux stub检查通过：真实函数转发、值／长度、成功／失败及errno保持；双线程可重叠，缺失／不可写日志不改变真实返回。不在性能挂载加载该库。
- 当前生产产品未改动，复核既有2024项完整单测与构建收据；没有将其表述成本轮重新运行产品单测。
- 281份归档证据逐大小／SHA核对；208份冻结运行文件核验，本地独立原始数据复算与服务器一致。六挂载的科学head、journal、封印index与raw保持不变。
- 服务恢复ASYNC/true、配置字节一致；全部225个现存DataNode线程的原affinity／cgroup核验通过，临时cgroup已清除，原JVM内存参数保持。

产品SHA256：`bfee1d56599b8a0144e68de3b59bdafdc8af21a6ee99c7ed47ca59c0aa34ad27`。
归档SHA256：`1721e0adca13fde1950e245c75877d1934d3e187f8ba7efdaa48a8b99ad39f08`。
独立结果SHA256：`d2175b9bdb992990459f43244c8a0e0595e96b21e23bdb7c5fe4f36a34a67c69`。

[独立结果](../../experiments/cross_version/work/distinct-message-probe-20261025/evidence/collection-r1/independent.json) · [本地证据验收](../../experiments/cross_version/work/distinct-message-probe-20261025/local-evidence-audit.json) · [四查询计划](../../experiments/cross_version/work/distinct-message-probe-20261025/message-plan.json)

服务器原始目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/distinct-message-probe-20261025/collection-r1/`。本轮没有更新飞书历史表、生产源码或执行Git写操作。
