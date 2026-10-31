# FM1 首轮固定矩阵验收（2026-10-21）

32个固定case的三轮主统计已收齐：3,456次调用，其中2,880次测量、576次预热。复用14份独立发布语料、1,584个科学文件版本，未重发数据、重建FS-index或更换产品。18个预注册扩展未执行。

本轮32个case的中位R均大于1，**尚未在这组固定负载中证明IoTDB-FS优于FS-index**。这里的R为同case、同campaign暖完整查询中位数之比，再对三个比值取中位数；不能用下面分别聚合的时间直接重算R。

| 格式 | 16个case的中位R范围 | 最小R的固定case | FS-index T（ms） | IoTDB-FS T（ms） | R及三个block范围 |
| --- | --- | --- | --- | --- | --- |
| GRIB | 3.502–7.498 | G-H3-S1：独立N8/C8/V1/S1/Latest | 5.050 | 18.338 | 3.502 [2.971, 4.031] |
| HDF5 | 50.738–438.845 | H-H1-N512：独立N512/C512/V1/S0/Latest | 5.616 | 290.318 | 50.738 [50.178, 51.696] |

最小R仅按执行前冻结的各格式16个case范围选择，未按最低单次毫秒或局部阶段挑选。完整32组T/d/q、R/Rd/Rq、三个block样本与14份语料的准备／启动成本见[完整结果表](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r3-evidence/results.md)及[飞书4.3原表](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg#doxcn5FH2k0iTUtMKq2Ch9b9gmg)。

## 续采与数据选择

原collection-r2因包含准备的总时限不足而停止。用户在查看R之前明确批准：保留完整campaign 0/1的2,304条，整轮重采第三轮1,152条。原第三轮72条及失败报告完整保留，但全部排除，不与新第三轮拼接或择快。因此物理采集记录共3,528条，主统计固定使用3,456条。

query-only入口为[replay.py](../../experiments/fixed_matrix/replay.py)，调用从r2执行快照等价抽取的同一查询循环。服务器35项离线测试全部通过，包含AST等价检查；第三轮worker正常退出，耗时1,801.697秒，无超时。每次挂载检查冻结head、owner journal与FS-index不变；全部文件版本字节重新核验。原始r2记录和报告未改写。

HDF5固定为native-hdf5数据库VFD，文件内使用HDF5原生索引；GRIB使用冻结产品的跨文件及文件内索引。客户端串行查询，三臂均交付owned Float32与uint8 mask。每条严格满足T=d＋q；IoTDB-FS为一次发现RPC、再逐命中unit精确取值，q包含IPC、准入／预算及交付，不是纯原生库读取耗时。oracle、profile、资源采样及启动均在T外。FS-index一次建设时间另列，不折入暖查询。

## 实际资源与恢复

主机为KVM虚机timecho，Xeon Gold6130@2.10GHz，可见32逻辑CPU、377.69GiB RAM，kernel6.8.0-139-generic。客户端、IoTDB-FS、FS helper及原DataNode共用CPU2/3 isolated partition和两核总quota；query叶组上限2GiB。DB原15,232MiB heap／2,176MiB direct另计，ConfigNode配额外单列。不清OS/DB缓存；2GiB不代表全栈或全机共享缓存上限。

| 执行段 | 主统计保留campaign | 周期样本 | 查询重叠最大采样间隔 | query累计峰值 | DB累计峰值 | 父组观察期平均实占 | host load1范围 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| E1a：r2 | 0、1 | 12,707 | 1.239秒 | 772.64MiB | 18.65GiB | 0.673核 | 19.76–43.64 |
| E1b：query-only | 2 | 2,049 | 1.106秒 | 258.56MiB | 12.61GiB | 0.610核 | 18.62–30.04 |

无OOM。峰值及平均CPU覆盖各段所有采样，包含钟外工作，E1a还保留被排除第三轮的资源；不当作单case或纯d/q独占资源。父组包含叶组，不能相加。两段分别验收采样覆盖，中间停机／批准间隔未采样，未拼接成连续序列。288个case/campaign/arm资源cell及逐查询envelope单独保留。

E1a自动恢复故障及后续手动恢复保持原记录；E1b自动恢复及独立只读核验均通过。原服务实际设置为ASYNC/true，原配置逐字节相同，许可证、节点身份及.env保护摘要未变；DataNode PID1483561，全部线程CPU亲和性0–31，原cgroup归位，临时cgroup已清除。未启动隔离实例或申请许可证。恢复ASYNC后未再调用要求SYNC的head探针；使用准入前及每次挂载后的head证据。

## 可追溯范围

[证据清单](../../experiments/cross_version/work/fixed-matrix-20261021/collection-r3-evidence/evidence-manifest.json)包含22个白名单文件的SHA，以及服务器原始查询／周期资源大文件路径和SHA。下载包SHA256为`ddfeb8f8559a4f7e846a484aa60801f0ee82635c286c7a03657c9376509bc608`，包及各文件均已逐项核验。凭据、许可证原件、私有诊断日志和配置备份留在服务器。

三个campaign是同一主机的时间block，采用retained-process warm协议，不是独立机器或冷盘重复。本轮只描述已注册的选择性合成负载，不推论所有真实数据，不宣称显著性、统计等价或稳健p95。历史环境证据不全的结果未进入本轮基线。后续优化只复用这些corpus/case创建新run；新组先确认并登记，按[固定方案](../deliverables/fixed-experiment-matrix-plan.md)执行。

飞书回填revision 651 已逐格核验全部51行×11列，保留目的列、18扩展及x/y空列；未新增章节。
