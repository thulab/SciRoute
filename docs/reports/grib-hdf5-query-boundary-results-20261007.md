# GRIB 与 HDF5 分析查询的计时边界和公共成本（2026-10-07）

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

## 问题与实验口径

旧 GRIB 论文报告的单文件子集约 0.88 ms、跨文件约 2.77 ms、跨版本约 8.29 ms，属于历史虚拟文件读路径；其原始 run manifest 与对应源码 revision 未保留。现存 [`read_perf.cpp`](../../tests/performance/read_perf.cpp) 在每个任务开始计时前对查询路径调用 `exists` 和 `file_size`，所以旧式虚拟查询可能已在 `getattr` 阶段物化。HDF5 没有完全相同的旧式路径。为检验“亚毫秒读”是否可在 HDF5 上复现，并抽取两种格式的公共查询成本，本实验同时记录两个**明确不同**的边界：

1. 完整 typed 注册：客户端发起 Unix socket 请求；服务端完成 catalog、授权、查询、IPC 编码与结果发布。`server_total_ns` 排除 socket 帧传输和 profile 写出。
2. 已发布结果的文件读取：在计时前已经完成注册；单独记录 `exists`／`getsize`，随后计时 `open` 到 EOF 的完整 IPC 读取。SHA-256 在读计时外计算。这模仿历史 reader 将文件属性检查置于计时外的**位置**，但读到的是已经发布的 IPC，不能命名为完整分析查询，也不是历史 GRIB 实现的精确复刻。

使用[有界探针](../../tests/benchmarks/hdf5_query/grib_hdf5_boundary_probe.py)在同一服务器、IoTDB 数据库 `h09probe1790939889dbfc1f`、同一 `iotdb_fs` 二进制（SHA-256 `36e008005e5de30feb6811573ffaed8c197300b5e39f32a7d6d60381b650a94f`）内交错运行。HDF5 是公开 GEOS `/geos-pilot-9856a092a812/file1.nc4` 的 `/T` 物理 Float32 单点、精确版本 `1791271058856`；GRIB 是 `/geos-pilot-9856a092a812/grib-control.grib2` 中的 level=500 BinaryView、`latest` 版本。每次新挂载预热两者各一次，之后交错执行两者各 10 次；共三次独立挂载、每种格式 30 个暖态样本。请求级原始报告（服务器归档：`docs/grib-hdf5-query-boundary-raw-20261007.json`）保存每次耗时、SQL 次数、返回字节和 IPC 摘要；三份完整服务端 profile 留在服务器 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/grib-hdf5-boundary-final-20261007/`。所有同种请求的 IPC SHA-256 一致，GRIB 为 67,648 B，HDF5 为 5,752 B；这些请求**不等工作量**，只用于比较入口及成本分类。OS 与 IoTDB 页缓存未清除，不称磁盘冷读。

## 暖态结果

下表取 30 个请求的中位数；三次挂载分别得到的服务端中位数也列出。单位均为 ms，`stat` 是查询后已发布结果的 `exists`＋`getsize`。

| 格式 | 客户端注册 | 服务端完整注册 | 三次挂载的服务端中位数 | `stat` | 已发布 IPC 只读 |
| --- | ---: | ---: | --- | ---: | ---: |
| GRIB | 26.762 | **26.416** | 26.029／25.744／27.677 | 12.422 | **0.462** |
| HDF5 | 22.486 | **22.148** | 21.507／22.361／23.661 | 12.429 | **0.344** |

因此，HDF5 同样可以在“物化后只读”边界出现亚毫秒数字；这不能用来论证 HDF5 完整查询是亚毫秒。包含注册、`stat` 与结果完整读取的每请求客户端时间中位数为 GRIB 40.019 ms、HDF5 35.240 ms。三个阶段从同一请求求和后再取中位数；不能把上表各列中位数直接相加。

## 同一服务入口的成本拆分

`registration_catalog` 与 `registration_authorize` 是不重叠的同步阶段，每请求均执行 4 条通用元数据 SQL：catalog 1 条，源与两个父目录授权 3 条。下表的“公共阶段”先对每请求相加、再取中位数；“余量”先从每请求 `server_total` 扣除公共阶段、再取中位数。backend SQL 时间嵌在公共阶段内，**不可再次相加**。

| 指标 | GRIB | HDF5 |
| --- | ---: | ---: |
| `registration_catalog` | 9.787 ms | 9.624 ms |
| `registration_authorize` | 11.603 ms | 11.767 ms |
| 两项公共阶段 | **21.374 ms** | **21.539 ms** |
| 公共阶段占各请求服务端时间的中位比例 | **81.6%** | **97.2%** |
| 服务端剩余时间 | 4.893 ms | 0.618 ms |
| 元数据 SQL 总后端时间（嵌套） | 19.271 ms | 19.694 ms |
| `typed_prepare` | 0.112 ms | 0.135 ms |
| `typed_cursor_execute` | 4.232 ms | 0.072 ms |
| `typed_ipc_encode` | 0.142 ms | 0.095 ms |

暖态两边 layout SQL 都为零。GRIB 每次仍执行 1 次 payload SQL，返回 66,606 B，后端耗时中位 4.219 ms；HDF5 的 chunk 已在进程内缓存，payload SQL 为零。两者完整注册的约 4.27 ms 差值与 GRIB 这次 payload 读取接近。这说明本组暖态中没有明显的 HDF5 固有额外成本；**不是**两格式在相同 payload 缓存、编码或输出规模下的速度排名。每个挂载的首个 HDF5 请求仍有 3 次 layout、1 次 payload SQL，首个 GRIB 请求有 1 次 layout、1 次 payload SQL；每边仅三个冷样本，不据此推断冷启动分布。

## 对论文和下一步优化的含义

旧 GRIB 论文的 0.88 ms 与本轮 GRIB 已发布结果只读的 0.462 ms 处于同一数量级，但缺失历史原始 trace，**不能据此判定旧论文的 0.88 ms 一定是预物化结果读取**。现存历史 reader 把属性检查置于任务计时前，以及本轮约 12.4 ms 的 `stat`，使计时边界审计成为必须项。完整分析查询应从请求提交计到结果完全消费，同时单列 pre-stat、注册／准备、实际 payload、IPC 与读取；不能把已发布结果只读与完整查询并列为同一性能指标。

在本轮短查询中，最值得进一步处理的是**公共 catalog 与逐源／父目录授权**，而非 HDF5 的 chunk 解码：暖态 HDF5 的 `typed_cursor_execute` 仅约 0.07 ms，却支付约 21.5 ms 公共阶段。可实验的方向是定向 catalog 查找、同请求已有身份与授权证明复用，以及有明确失效规则的短时授权缓存；不能直接跳过权限检查。任何优化应再次对 GRIB 和 HDF5 在同一入口做配对消融，核对 SQL 数、版本身份与权限撤销，并另外保留本地 FS＋等价预建索引对照。对于当前 GEOS 这种每文件只有一个时间点的目录，公共阶段减小也不自动意味着数据库会快于同等索引的本地 FS。

本轮只覆盖旧论文三类分析查询中的**单文件局部子集**，没有把多文件目录发现或版本遍历伪装成单文件请求的倍数。已有[GEOS 跨文件时间实验](hdf5-geos-time-scale-results.md)独立测量了 16 个真实时次的目录成本，但其候选筛选和 FS 预建索引语义与历史 GRIB 脚本不同；跨文件／跨版本同口径实验仍需先冻结两侧相同候选集、版本选择与输出。
