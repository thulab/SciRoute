# FS 控制面解析与连接管理 N128 探针

2026-10-25。已实现服务端顶层 JSON 解析复用，并增加保持 fresh one-shot 连接的低层异步 socket 客户端。四臂探针完整采齐、独立复算及恢复验收通过。**仅服务端改动的平均完整 T 下降14.66%、QPS提升16.71%；组合改动相对本轮参考的平均 T 下降17.81%、QPS提升19.97%。组合没有稳定优于客户端单项，不能将两项收益直接相加。**

## 实现与语义

- 服务端每个正常顶层控制帧只完整解析一次；transport分类、资源提示、register执行及result-fetch复用不可变树。正常队列任务不再同时捕获原wire字符串，只额外保存原请求字节数供profile。
- 正式结构／语义验证、当前权限、版本、metadata费用、预算、取消和发布仍在原worker边界执行；资源观察失效重新inspect，不缓存旧授权或版本。JSON语法异常在原错误边界重抛；非语法瞬时异常保留原重新解析机会。批内逐项解析及首错／整体回滚顺序保持。解析树可能比wire大，仍受原帧长与队列／执行槽上限约束；本轮没有新增无界缓存或放宽内存限制，也没有做allocator故障穷举。
- 新客户端同一查询算法选择streams或raw_socket。后者用nonblocking socket、sock_sendall和分块sock_recv_into减少streams对象、回调及body临时复制；保持每请求新连接、fresh认证、部分读写、EOF、超时、取消关闭和原协议界限。没有预连接、重试、客户端worker或查询内并行。四查询仍有20次连接，**没有实现连接池或持久连接**。
- Python优化位于新增共享实验客户端；C++ submit_query_control及服务端one-shot协议保持。客户端默认streams，raw_socket显式选择；这不是给任意已有客户端自动安装新的连接实现。

## 固定条件与比较边界

沿用4.3封印FM1 GRIB N128/C128/V1/S0/latest、K=2、seed20261013、owned值和mask共40,960字节；原corpus、case、head、journal和index，不重新发布／建索引。公共worker4、reader4、sender1、queue64、batch4；客户端一个asyncio loop、4查询协程。CPU2–3、总两核quota，query叶2GiB；原DataNode heap15,232MiB/direct2,176MiB另计。

四臂共用同一新客户端模块的_query、_decode、_drive，AST与原结果交付客户端相同；streams参考臂经过相同薄wrapper。它是本轮同框架对照，不能当作历史旧Python实现逐字重现。四查询负载也不与前次八查询数据直接同比。

两轮顺序为OLD/SERVER/CLIENT/BOTH和BOTH/CLIENT/SERVER/OLD。每块独立挂载，1波预热＋3波测量，每波4查询；N128共128条（96测量／24波），另N8四臂资格16条，共144查询、12挂载。所有预先固定样本保留，无剔除、补有利样本或科学失败重试。关闭perf、详细服务端和客户端trace，普通query profile各臂均开启。

完整T从每条query entry到最后Float32与mask成为owned输出，包含请求构造、正常connect/close、顺序catalog→typed1→fetch1→owned1→typed2→fetch2→owned2；逐行纳秒闭合T=d+q。oracle哈希、profile等待、资源采样、启动和恢复在T外。QPS=总测量查询数／测量波跨度之和，延迟不除以4。

## 主结果

每臂24条测量查询／6波。所有数值从原始行合并计算；p95仅为描述性分位数。

| 臂 | QPS | 平均完整T ms | p50 ms | 描述性p95 ms | 平均波长 ms |
|---|---:|---:|---:|---:|---:|
| 本轮参考：原服务端＋streams | 215.038 | 16.955 | 15.955 | 22.648 | 18.601 |
| 仅服务端解析复用＋streams | 250.977 | 14.470 | 13.981 | 17.815 | 15.938 |
| 原服务端＋raw_socket | 270.891 | 13.405 | 13.704 | 15.587 | 14.766 |
| 解析复用＋raw_socket | 257.978 | 13.935 | 12.720 | 20.027 | 15.505 |

| 比较 | pooled QPS变化 | 平均T变化 | 两轮QPS变化 | 两轮平均T变化 |
|---|---:|---:|---|---|
| OLD->SERVER | +16.71% | -14.66% | +7.82% / +24.36% | -7.72% / -19.85% |
| OLD->CLIENT | +25.97% | -20.93% | +4.26% / +49.12% | -3.36% / -34.10% |
| SERVER->BOTH | +2.79% | -3.70% | +18.83% / -7.90% | -19.74% / +10.14% |
| CLIENT->BOTH | -4.77% | +3.95% | +22.88% / -23.19% | -23.36% / +33.94% |
| OLD->BOTH | +19.97% | -17.81% | +28.12% / +14.54% | -25.93% / -11.72% |

SERVER相对OLD在两轮均同向改善，可支持优先保留服务端解析复用。CLIENT相对OLD也同向，但幅度差异很大；CLIENT→BOTH的两轮方向相反，pooled平均T反而增加3.95%、QPS下降4.77%，同时p50改善7.18%。因此本次不声明组合最佳，raw_socket保留为可选方式；样本不足以保证尾延迟、显著性或稳定叠加收益。

## 分阶段观察

以下为每完整query的平均客户端阶段包络（ms），不是服务端独占CPU。

| 臂 | control合计 | fetch合计 | Arrow decode | append owned | 其他构造／提交／恢复间隙 |
|---|---:|---:|---:|---:|---:|
| OLD | 11.894 | 3.541 | 0.655 | 0.011 | 0.853 |
| SERVER | 9.567 | 3.759 | 0.428 | 0.009 | 0.706 |
| CLIENT | 9.186 | 3.164 | 0.474 | 0.010 | 0.571 |
| BOTH | 9.566 | 3.307 | 0.478 | 0.011 | 0.574 |

主要变化出现在control等待包络。它包含执行、排队、socket与客户端恢复，不能把差值全部叫JSON解析CPU或connect独占开销。各包络与间隙合计闭合完整T；没有在本轮重新采集物理CPU并行跟踪，profile壁钟峰值4仅说明执行包络重叠。

SERVER与BOTH各72个测量control profile均为parse_attempts=72、reuses=72、worker_retry_parse_attempts=0。两臂transport_parse_ns累计分别15.722／17.918 ms，平均每control 218.362／248.859 µs，属于transport侧wall计时。worker的json_parse平均从OLD的221.472 µs变为SERVER的1.417 µs；它现在只是复用阶段，**不能将这一降幅当成完整解析成本或完整查询降幅**。顶层树析构也转移到了task捕获释放阶段，原server_total与新值不构成完全相同的解析／析构成本边界。fetch不生成普通query profile，不能拿12个control profile证明整波20个请求的解析计数；fetch复用由源码及专项测试覆盖。

本轮稳态测量query profile观测窗口中的query_calls、version_store_query_calls和session_acquire_calls增量均为0；本结论限暖缓存GRIB N128、小IPC和当前两核资源，不外推冷缓存、HDF5/Arrow混合、大结果、IoTDB查询并行或四核伸缩。

## 验证、恢复与追溯

- 产品70项定向测试、全量2024/2024通过；新增3项回归覆盖畸形／重复kind、batch完整预验与首错顺序、排队handle退休及fresh权限。服务器56项Python检查通过（40 socket、7协议、5独立变异门禁、4独立分析）。
- 144查询的值、mask、ordinal和mode均按原封印oracle校验通过。314个白名单证据文件逐大小／SHA验收、191份冻结运行源码逐项核验，本地独立重放全部矩阵、时钟、资源、路由绑定与统计通过。
- 838份周期资源样本完整；资源门禁未发现OOM。周期统计包含准备／审计，不能当作纯查询CPU利用率。所有挂载的科学head、journal和index不变。
- 原服务已恢复ASYNC/true，配置字节与本轮维护前一致、保护文件hash未变；292线程原亲和性／cgroup核验通过，临时cgroup已删除。原JVM heap/direct未放宽。
- 首轮product-r1因profile仍引用已移除原字符串而编译失败，未执行科学查询；失败源码manifest、输入和build日志保留。product-r2保存原请求字节数后重新构建、验证并用于所有候选臂，没有覆盖失败记录。

OLD binary SHA256：`5862f9335e413503d772199e0d87156e658b3db9e01549ed25b34f5ed10335bb`。
NEW binary SHA256：`bfee1d56599b8a0144e68de3b59bdafdc8af21a6ee99c7ed47ca59c0aa34ad27`。
NEW source manifest SHA256：`ca099e996f3388f180542d965b3314bdffd5f772aa29a9f38b9c928767cca2d5`。
证据包 SHA256：`df0cedfe3b516e1bad76367a27042bcf363647c5387d3bed0c6b42fa7df1eaaa`。

[本地证据验收](../../experiments/cross_version/work/control-plane-20261025/local-evidence-audit.json) · [独立原始统计](../../experiments/cross_version/work/control-plane-20261025/evidence/collection-r1/independent-primary.json) · [连接设计边界](../../experiments/cross_version/work/control-plane-20261025/connection-design.md)

服务器原始目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/control-plane-20261025/`。本轮未修改实验表或发布正式论文性能结论，未做Git写操作。
