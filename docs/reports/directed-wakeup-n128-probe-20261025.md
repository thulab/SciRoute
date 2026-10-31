# FS 定向唤醒：N128 1/2/4路短程性能探针

2026-10-25。采集、逐查询正确性、独立复算、本地归档核验和服务／资源恢复均已完成。

**本次尚不能确认定向唤醒带来稳定的端到端收益。** 四路 pooled QPS 提升2.00%、p50下降2.75%，但两轮QPS变化为−1.70%／+5.99%，方向不一致；一路合并QPS下降2.78%，二路下降5.86%。二路两轮均为负向信号，需要保留，不能只报告四路的正向均值。

## 条件与缩减范围

沿用[飞书4.3大表格](https://apache-iotdb-project.feishu.cn/wiki/Kx87wsVXpiB5aUkYG50cCKoEnmg)已确认revision983的封印GRIB组：N=C=128、V1、D4、S0、latest、K=2、seed20261013，每完整查询owned结果40960B。语料、case、index、科学head及journal不变，没有重新发布、重建或绑定新位置。

OLD是上一接受的GRIB单message直接执行版，仍包含owner reader pool和资源感知公共调度器；NEW只将公共调度器共享CV改为dispatcher与每worker独立CV。唯一生产源码差异为`src/query/core/QueryTaskScheduler.cpp`。两版均为GCC13.3 Release，实际target参数、编译器字节、动态库一致；只将c++／g++-13链接driver的argv0别名解析到同一文件，没有忽略其他参数。

- 公共worker／max batch=1、2、4，owner readers=4、queue=64、fragment pool=4固定。
- 一个原始asyncio loop、既有8个在途query coroutine；没有增加客户端worker、线程或查询内拆分。每query仍为catalog＋两次顺序typed。
- 保留原profile；关闭server、FUSE及client附加探针，直接调用原query/client函数。本轮没有测`notify`、FUSE或IPC的独占时长。
- 两轮配置顺序为1/2/4、4/2/1；每配置OLD／NEW独立挂载，相邻配对，次序在下一轮反转。每mount只预热1波、测量4波，每波8查询。
- 正式480条／60波，其中测量384条／48波、预热96条。每版本／配置有64条测量查询；N8仅在S4对OLD／NEW各1波做资格校验，另16条。合计14次mount。慢样本全部保留，无自动重跑或剔除。
- CPU2/3、父组两核quota、query叶组2GiB；cpuset partition=root，不声称内核级isolated。DB heap15232MiB／direct2176MiB及support角色按原协议约束。

T保持查询入口到最终owned values/masks完成，包含发现、排队、执行和交付，原t0/t1/t2及T=d+q不改。profile harvest、oracle与资源采样在T外。暖进程／OS缓存条件，未做冷缓存实验。

## 完整查询结果：两轮合并

下表明确使用pooled口径：QPS=64条成功查询／8个测量wave的makespan之和；p50、平均T、描述性p95均基于同一cell的64条查询。变化是所列pooled值之比，和下一节的逐轮配对统计分开。

| 路数 | OLD QPS | NEW QPS | QPS变化 | p50 ms OLD→NEW | 平均T ms OLD→NEW | 描述性p95 ms OLD→NEW |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 254.016 | 246.963 | -2.78% | 27.864 → 28.096 | 27.475 → 28.035 | 31.546 → 33.077 |
| 2 | 265.788 | 250.203 | -5.86% | 26.567 → 28.215 | 26.103 → 27.903 | 31.055 → 32.151 |
| 4 | 249.523 | 254.520 | +2.00% | 27.726 → 26.963 | 27.511 → 26.833 | 32.634 → 31.089 |

四路平均T下降0.678ms（−2.46%）。只有两轮、每轮四个波次，不据此推断统计显著或尾延迟保证。

## 逐轮对照与配对统计

每行32条测量查询／4波。配置内的OLD／NEW相邻执行，但不是同一时刻运行，仍有环境与暖缓存顺序影响。

| 路数 | campaign | QPS OLD→NEW | QPS变化 | p50 ms OLD→NEW | p50变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 0 | 259.606 → 232.910 | -10.28% | 26.574 → 30.072 | +13.16% |
| 1 | 1 | 248.662 → 262.820 | +5.69% | 28.157 → 26.168 | -7.06% |
| 2 | 0 | 280.201 → 250.031 | -10.77% | 25.288 → 28.208 | +11.55% |
| 2 | 1 | 252.786 → 250.376 | -0.95% | 27.913 → 28.821 | +3.25% |
| 4 | 0 | 249.492 → 245.261 | -1.70% | 27.558 → 27.701 | +0.52% |
| 4 | 1 | 249.554 → 264.505 | +5.99% | 27.933 → 25.791 | -7.67% |

先计算每轮NEW/OLD比，再取两轮比的中位数：

| 路数 | 配对QPS变化中位数 | 配对p50变化中位数 |
| --- | ---: | ---: |
| 1 | -2.29% | +3.05% |
| 2 | -5.86% | +7.40% |
| 4 | +2.15% | -3.57% |

这不是两轮QPS中位数之比，也不是pooled比；N=2时中位数就是两值平均，不能掩盖轮间方向。S1与S4均发生方向反转，S2两轮均负向；当前结果不支持把二路下降当成已解释的随机噪声，也不支持把四路约2%认定为稳定提速。

## 并行是否真实形成

| 路数 | profile执行包络peak OLD / NEW | 积分平均并行度 OLD / NEW | 每完整查询profile内总均值 ms OLD / NEW |
| --- | ---: | ---: | ---: |
| 1 | 1 / 1 | 0.565 / 0.565 | 2.223 / 2.288 |
| 2 | 2 / 2 | 0.657 / 0.685 | 2.473 / 2.739 |
| 4 | 4 / 4 | 0.796 / 0.862 | 3.189 / 3.388 |

两版均实际观察到1／2／4的峰值，并行执行存在。这些是profile execution包络，不含Ready等待、响应／profile尾部或执行槽释放，不能冒充完整slot占用或同时使用CPU核数。平均包络变高也可能来自profile本身变长：S4每query profile内均值由3.189升至3.388ms，因此0.796→0.862不能单独解释为有效利用率提升。

同版本S2/S1、S4/S1的吞吐扩展：

| 版本 | 路数比 | campaign0 QPS变化 | campaign1 QPS变化 | 配对变化中位数 | pooled变化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| OLD | 2/1 | +7.93% | +1.66% | +4.80% | +4.63% |
| OLD | 4/1 | -3.90% | +0.36% | -1.77% | -1.77% |
| NEW | 2/1 | +7.35% | -4.73% | +1.31% | +1.31% |
| NEW | 4/1 | +5.30% | +0.64% | +2.97% | +3.06% |

NEW四路相对一路的pooled QPS只高3.06%；两轮分别高5.30%／0.64%。定向唤醒没有恢复接近四倍的端到端扩展。

## 四路收益如何被抵消

以下是pooled算术均值的加法分解，单位ms／完整query，包含三次控制调用的合计。匿名catalog只在cell内聚合，不强行逐query配对。分项与原始整数总时钟闭合；不相加p50。

| 分项 | OLD | NEW | NEW−OLD |
| --- | ---: | ---: | ---: |
| 排队／派发／交接至profile前 | 7.433 | 5.933 | -1.501 |
| profile内执行 | 3.189 | 3.388 | +0.199 |
| 控制调用其余区间 | 10.522 | 10.967 | +0.445 |
| 控制调用外 | 6.366 | 6.545 | +0.179 |
| **完整T** | **27.511** | **26.833** | **-0.678** |

排队至profile前的阶段少了约1.501ms，其余阶段增加约0.823ms，净留下0.678ms。该等待counter包含排队、派发、worker交接及profile前快照，不能全部归于`notify`；control residual含transport和loop交错，控制调用外含IPC／owned交付与边界余量，都不是独占锁或网络时间。

因此本轮显示四路前段等待有所缩短，但完整链路改善很小；它与[此前同步交付主导的诊断](query-tail-diagnosis-20261025.md)相符。本轮未重测IPC／FUSE独占耗时，也未直接量出新旧`notify`差异，不能仅凭这些residual证明通知尾部减少了多少。二路的负向信号同样保留，后续不能以本轮为稳定收益验收。

## 正确性、资源与恢复

| 验收项 | 结果 |
| --- | --- |
| 旧／新实现身份 | 原1986项／新1990项全量通过的固定产物；单一生产源码差异，实际运行exe SHA逐mount核验 |
| 缩减协议离线门禁 | 5组完整矩阵／负例通过；独立interval／时钟11项及完整synthetic兼容性通过 |
| 科学结果 | 正式480条＋资格16条全部oracle／mode／owned字节／版本／原完整时钟通过 |
| 顺序与profile | 14mount完整，OLD／NEW与配置物理顺序吻合；每波24 profiles，实际worker、queue、batch核验；独立重算包络peak／积分 |
| 封印输入 | 源码closure始末一致；原raw／head／journal／index均未改；无新publication、case或位置绑定 |
| 独立复算 | runner与独立程序对QPS、p50／p95、逐轮配对及伸缩比一致；本地283文件逐SHA核验，并再次复算原时钟、pooled QPS／p50及加法均值 |
| 资源 | 所有测量wave前后样本parent throttle增量0；整个1255.33s窗口throttle仅16.855ms。波前后query memory.current样本最大约149.92MiB，2GiB上限，未见high／max／OOM事件 |
| 恢复 | 原ASYNC/true、配置字节与受保护哈希一致，294线程affinity、原cgroup已恢复，临时cgroup已移除 |

1256个周期样本包含启动、挂载、oracle和profile等未计时工作，其CPU均值／pressure不能当作请求独占等待。样本内未见配额或内存耗尽证据；短程波动仍存在，不能保证已排除所有环境影响。

## 证据与结论边界

- OLD binary SHA：`4d8153bcb554574a4dc7be1e0abceb36087fd91ce27191b0267ca1f1cefce3ea`。
- NEW binary SHA：`4caa446dc2dadbec9227fbdafd9062425a3ae56910c05c96352e341e98e40cb7`。
- 归档：`experiments/cross_version/work/directed-wakeup-probe-20261025/evidence.tar.gz`，SHA256 `376d97a64a0faa9de55c3b1ca712b9131276a560c2af3469947002a34e15c4ee`。
- 独立分析SHA：`81f5668075f7f32eb3edd835b5beba78ae1fc8d41e81d9d7a2df64c8392f90ed`；`independent-primary.json`和本地`local-evidence-audit.json`均通过。
- 新runner：`experiments/fixed_matrix/directed_wakeup_probe.py`；固定计划及提前写好的analysis-design、产品／构建对照、封印source、原始query／wave、恢复与资源证据均在归档中。`write_report.py`只是完成后的展示，未修改测量源码或原始数据。

**决策：保留这份优化探针结果，不把定向唤醒标为已验证的稳定端到端提速。** 四路有小幅正向信号和前段等待缩短，二路有负向信号；当前完整查询的主要交付瓶颈仍需处理。下一步若继续优化，优先考虑既有协程可await的有界结果交付；不要通过增加客户端worker、拆查询或改变owned完成时钟制造收益。本轮未进一步改生产代码或自动扩大实验。
