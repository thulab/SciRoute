# HDF5／GRIB 单文件横向矩阵与计时核验 — 2026-10-12

重新读取已有原始记录，未重新运行完整查询。主矩阵使用 HDF5 `session-q100-20261012` 和 GRIB `grib-paired-q100-20261012`；两者 daemon 二进制 SHA-256 完全相同。历史 HDF5 `q100-20261012` 仅作复验参考。

**结论：未发现组别交换、ns/ms/s 换算错误、绑定重复收费或正式查询总计错误。发现需要规范的统计口径与诊断字段，不能把跨格式差异直接归因于查询引擎。**

## 1. 测量定义

所有表格统一为 **毫秒**。暖读 p50 从每组 Q=100 的三轮、共 297 条排除首读的记录合并计算；累计／绑定／生命周期值是三轮独立总计的中位数。阶段及中位数不能直接相加：阶段可能嵌套，且不同列的中位数不必来自同一轮。

- 普通模式：FS 每次重新打开、选择、读取、复制并关闭；IoTDB-FS 每次重新注册，并执行新 cursor、编码、发布 IPC 和挂载读取。daemon 内索引与解码缓存可复用。
- 绑定模式：只付一次绑定成本，读取仍生成独立输出。FS 保留原生句柄／数据；IoTDB-FS 保留 PreparedQuery，仍执行 cursor 和 IPC，不复读旧结果。
- 会话总计 = 一次绑定 + Q 次完整读取 + 一次关闭／释放；普通模式绑定项为零。
- 生命周期 = 文件复制、file fsync 与关闭 + IoTDB-FS 一次精确版本定位 + 会话总计。源文件生成、正确性全文件 SHA、进程启动和父目录准备不计入。
- 首读是 reader／daemon 的首次观测，不是冷盘；OS 和数据库缓存未清空。所有请求重复同一固定子集。

## 2. 负载与缓存矩阵

| 项目 | HDF5 | GRIB | 可比性限制 |
| --- | --- | --- | --- |
| 数据 | 合成有限 Float32，单 `/T` 四维 dataset | 真实 GFS `clwmr`，单文件多 message | 不是相同数据与压缩率的格式转换对照 |
| 文件大小 | 6,728,832 / 537,684,072 / 3,441,125,376 B | 2,446,828 / 549,606,683 B | medium 大小相近；small 大小不同；GRIB 无同条件 large |
| 原生发现 | 已知 `/T` 路径和 ordinal，原生 chunk 索引 | `level=500,step=0,typeOfLevel=isobaricInhPa,shortName=clwmr`，扫描 22／4,598 条 message | 一个是已知对象寻址，一个含语义谓词发现 |
| 物理解码单元 | `[1,1,91,144]` chunk，shuffle＋gzip1 | 1440×721 完整 message，16-bit complex spatial differencing | 单元大小与解码工作不同 |
| point | 1 Float32＋mask | 1 Float32＋两个 Float64 坐标＋mask | 原始值相同类型，返回列不同 |
| ROI | small_roi 2,016 值；cross_chunk 192 值 | roi 16 值 | 不可按 ROI 名称视为等量工作 |
| encoded message | 未测 | 命中 66,606／64,901 B | 没有对应 HDF5 二进制组 |
| 普通 FS 缓存 | 每次关闭 HDF5 句柄，OS 页保留 | 每次销毁发现／解码状态，OS 页保留 | 原生缓存不会随普通请求保持 |
| 绑定 FS 缓存 | H5F/H5D 和 native raw chunk cache 保留，每次仍 H5Dread | 命中 encoded bytes／完整 decoded field 保留，每次重裁剪复制 | GRIB 缓存是实验 reader 显式实现的 |
| IoTDB-FS 缓存 | shared verified layout／decoded chunks，decoded 256 MiB | parsed index／private decoded field，decoded 64 MiB，index／message 独立预算 | 不宣称总 RSS 等额；本组工作集均可容纳 |
| BinaryView 绑定缓存 | 不适用 | FS 缓存 encoded bytes，IoTDB-FS 每次实际 payload SQL | 此组不能代表两侧等价缓存命中 |
| 授权／接口 | private root，TTL=1 s，socket＋FUSE IPC | 相同 | 不包含共享挂载多用户权限压力 |

## 3. 完整暖读矩阵

每格为一次完整读取的 pooled p50，单位 ms；绑定成本不在此表中。

| 格式／规模／查询 | FS 普通 | IoTDB-FS 普通 | FS 绑定后 | IoTDB-FS 绑定后 |
| --- | ---: | ---: | ---: | ---: |
| GRIB / small / binary_one | 5.729 | 11.070 | 0.002 | 5.628 |
| GRIB / small / point | 24.869 | 6.140 | 0.004 | 1.610 |
| GRIB / small / roi | 24.623 | 6.339 | 0.004 | 1.090 |
| GRIB / medium / binary_one | 1175.203 | 12.331 | 0.002 | 5.987 |
| GRIB / medium / point | 1206.863 | 8.233 | 0.004 | 1.453 |
| GRIB / medium / roi | 1205.629 | 8.053 | 0.004 | 1.101 |
| HDF5 / small / cross_chunk | 0.511 | 8.896 | 0.008 | 1.588 |
| HDF5 / small / point | 0.160 | 6.458 | 0.002 | 1.523 |
| HDF5 / small / small_roi | 0.216 | 6.328 | 0.004 | 1.457 |
| HDF5 / medium / cross_chunk | 0.521 | 6.205 | 0.009 | 1.554 |
| HDF5 / medium / point | 0.190 | 6.210 | 0.002 | 1.545 |
| HDF5 / medium / small_roi | 0.227 | 6.629 | 0.004 | 1.721 |
| HDF5 / large / cross_chunk | 0.515 | 6.377 | 0.009 | 1.609 |
| HDF5 / large / point | 0.166 | 6.331 | 0.002 | 1.153 |
| HDF5 / large / small_roi | 0.233 | 6.576 | 0.004 | 1.724 |

FS 绑定后的微秒级数字来自命中内存缓存的小输出，未访问磁盘；不可解释为磁盘随机读取只需几微秒。GRIB medium 普通 FS 的约 1.2 s 则主要是每次扫描 4,598 条 message。两种格式的 IoTDB-FS 数值暖读绑定后收敛到约 1–2 ms。

## 4. Q=1／10／100 完整会话矩阵

单位 ms，绑定组均包含本组的一次绑定与一次释放。HDF5 普通 Q=1／10 是同一 100 次运行的真实前缀；HDF5 绑定及 GRIB 两种模式均为各 Q 独立实际会话。两类 Q=1 并非同一抽样设计，不应把细小差值判作优化效果。

| 格式／规模／查询 | Q | FS 普通 | IoTDB-FS 普通 | FS 绑定会话 | IoTDB-FS 绑定会话 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GRIB / small / binary_one | 1 | 45.906 | 22.132 | 46.059 | 23.696 |
| GRIB / small / binary_one | 10 | 100.843 | 122.193 | 44.895 | 78.919 |
| GRIB / small / binary_one | 100 | 630.640 | 1121.581 | 47.131 | 591.208 |
| GRIB / small / point | 1 | 71.218 | 63.759 | 72.610 | 61.403 |
| GRIB / small / point | 10 | 300.094 | 119.923 | 70.925 | 70.663 |
| GRIB / small / point | 100 | 2559.631 | 670.620 | 74.388 | 229.070 |
| GRIB / small / roi | 1 | 71.669 | 60.304 | 71.186 | 61.264 |
| GRIB / small / roi | 10 | 295.872 | 120.640 | 73.444 | 76.583 |
| GRIB / small / roi | 100 | 2556.281 | 707.883 | 72.180 | 185.390 |
| GRIB / medium / binary_one | 1 | 1216.455 | 87.909 | 1224.816 | 89.224 |
| GRIB / medium / binary_one | 10 | 11663.544 | 213.590 | 1194.681 | 147.728 |
| GRIB / medium / binary_one | 100 | 117675.354 | 1374.227 | 1240.703 | 671.568 |
| GRIB / medium / point | 1 | 1225.809 | 122.327 | 1237.821 | 124.097 |
| GRIB / medium / point | 10 | 12140.943 | 197.754 | 1235.794 | 136.398 |
| GRIB / medium / point | 100 | 121002.843 | 988.102 | 1235.171 | 273.614 |
| GRIB / medium / roi | 1 | 1229.121 | 116.244 | 1246.503 | 123.595 |
| GRIB / medium / roi | 10 | 12195.247 | 199.620 | 1241.916 | 139.856 |
| GRIB / medium / roi | 100 | 120768.457 | 954.831 | 1242.834 | 248.116 |
| HDF5 / small / cross_chunk | 1 | 2.618 | 49.071 | 2.193 | 37.902 |
| HDF5 / small / cross_chunk | 10 | 8.920 | 145.016 | 1.907 | 56.962 |
| HDF5 / small / cross_chunk | 100 | 53.916 | 959.495 | 3.035 | 207.543 |
| HDF5 / small / point | 1 | 1.290 | 27.165 | 1.249 | 26.217 |
| HDF5 / small / point | 10 | 3.883 | 89.748 | 1.204 | 39.185 |
| HDF5 / small / point | 100 | 18.647 | 672.955 | 1.620 | 181.728 |
| HDF5 / small / small_roi | 1 | 1.434 | 26.964 | 1.519 | 29.845 |
| HDF5 / small / small_roi | 10 | 4.144 | 84.383 | 1.562 | 50.044 |
| HDF5 / small / small_roi | 100 | 24.394 | 676.984 | 2.033 | 183.351 |
| HDF5 / medium / cross_chunk | 1 | 2.173 | 33.025 | 2.170 | 34.307 |
| HDF5 / medium / cross_chunk | 10 | 7.938 | 94.093 | 2.228 | 49.793 |
| HDF5 / medium / cross_chunk | 100 | 55.516 | 660.652 | 3.110 | 202.625 |
| HDF5 / medium / point | 1 | 1.581 | 26.940 | 1.378 | 29.419 |
| HDF5 / medium / point | 10 | 5.263 | 82.693 | 1.242 | 42.805 |
| HDF5 / medium / point | 100 | 25.711 | 651.792 | 1.611 | 191.911 |
| HDF5 / medium / small_roi | 1 | 1.624 | 29.870 | 1.476 | 35.206 |
| HDF5 / medium / small_roi | 10 | 4.472 | 94.989 | 1.557 | 49.107 |
| HDF5 / medium / small_roi | 100 | 25.347 | 708.644 | 2.072 | 214.260 |
| HDF5 / large / cross_chunk | 1 | 2.131 | 38.118 | 1.975 | 37.018 |
| HDF5 / large / cross_chunk | 10 | 7.772 | 99.593 | 2.419 | 53.244 |
| HDF5 / large / cross_chunk | 100 | 54.563 | 679.925 | 2.992 | 206.084 |
| HDF5 / large / point | 1 | 1.340 | 28.501 | 1.284 | 29.310 |
| HDF5 / large / point | 10 | 3.922 | 90.131 | 1.342 | 39.717 |
| HDF5 / large / point | 100 | 19.279 | 679.329 | 1.512 | 159.838 |
| HDF5 / large / small_roi | 1 | 1.524 | 27.607 | 1.553 | 30.084 |
| HDF5 / large / small_roi | 10 | 4.405 | 88.037 | 1.535 | 49.064 |
| HDF5 / large / small_roi | 100 | 25.856 | 691.781 | 1.985 | 208.567 |

GRIB 100 次约 117–121 s 的 FS 数字来自上表普通模式，是累计值；绑定会话约 1.24 s 已含一次扫描，不能再次添加这笔发现成本。不同 Q 重新测量的绑定时长有波动，因此 Q=10 总计略小于 Q=1 不构成累计计时错误。

## 5. Q=100 一次性绑定与后续读取

单位 ms，各列单独取三轮中位数；无需强求列值相加等于上一表的中位数总计。FS 建绑定仅保存固定查询需要的状态，不是任意谓词的磁盘完整索引构建。

| 格式／规模／查询 | FS 绑定 | FS 100 次读取 | IoTDB-FS 绑定 | IoTDB-FS 100 次读取 |
| --- | ---: | ---: | ---: | ---: |
| GRIB / small / binary_one | 46.859 | 0.260 | 12.643 | 576.004 |
| GRIB / small / point | 48.445 | 25.461 | 14.613 | 216.254 |
| GRIB / small / roi | 45.619 | 26.022 | 13.689 | 173.254 |
| GRIB / medium / binary_one | 1240.457 | 0.242 | 74.923 | 596.034 |
| GRIB / medium / point | 1207.148 | 26.573 | 72.870 | 199.873 |
| GRIB / medium / roi | 1215.398 | 26.783 | 75.104 | 172.606 |
| HDF5 / small / cross_chunk | 0.918 | 1.966 | 21.530 | 182.833 |
| HDF5 / small / point | 0.963 | 0.482 | 19.146 | 158.315 |
| HDF5 / small / small_roi | 0.913 | 0.960 | 20.859 | 164.469 |
| HDF5 / medium / cross_chunk | 0.874 | 2.155 | 20.265 | 182.178 |
| HDF5 / medium / point | 0.865 | 0.514 | 21.040 | 172.018 |
| HDF5 / medium / small_roi | 0.956 | 0.966 | 21.028 | 192.940 |
| HDF5 / large / cross_chunk | 0.866 | 1.990 | 21.114 | 184.764 |
| HDF5 / large / point | 0.898 | 0.504 | 20.962 | 137.079 |
| HDF5 / large / small_roi | 0.902 | 0.928 | 20.264 | 186.972 |

## 6. Q=100 从文件发布到查询的矩阵

单位 ms，包含复制／fsync／关闭、B 精确版本定位及对应会话。每行是独立场景，不能叠加所有行而重复计文件发布。

| 格式／规模／查询 | FS 普通全流程 | IoTDB-FS 普通全流程 | FS 绑定全流程 | IoTDB-FS 绑定全流程 |
| --- | ---: | ---: | ---: | ---: |
| GRIB / small / binary_one | 646.370 | 1568.027 | 59.867 | 1045.681 |
| GRIB / small / point | 2572.367 | 1134.426 | 90.119 | 675.515 |
| GRIB / small / roi | 2568.806 | 1136.541 | 87.910 | 631.836 |
| GRIB / medium / binary_one | 119291.616 | 13087.758 | 2796.199 | 12466.453 |
| GRIB / medium / point | 122558.339 | 12766.031 | 2843.731 | 12044.460 |
| GRIB / medium / roi | 122323.953 | 12718.810 | 2844.677 | 12029.016 |
| HDF5 / small / cross_chunk | 80.485 | 1542.202 | 28.829 | 778.769 |
| HDF5 / small / point | 44.564 | 1186.010 | 27.321 | 710.393 |
| HDF5 / small / small_roi | 50.131 | 1184.034 | 27.703 | 732.139 |
| HDF5 / medium / cross_chunk | 1609.734 | 7840.923 | 1557.566 | 7371.230 |
| HDF5 / medium / point | 1573.095 | 7888.817 | 1555.664 | 7372.182 |
| HDF5 / medium / small_roi | 1578.814 | 7915.955 | 1556.531 | 7400.534 |
| HDF5 / large / cross_chunk | 9733.204 | 44216.605 | 9681.586 | 43750.819 |
| HDF5 / large / point | 9698.335 | 44245.989 | 9680.250 | 43670.199 |
| HDF5 / large / small_roi | 9703.672 | 44228.461 | 9680.626 | 43745.977 |

该全流程从已生成的 raw 文件开始；没有把合成数据生成、环境安装或全文正确性校验当作文件系统操作。两侧 file fsync 的实现不同，未证明相同断电耐久性。所有已测绑定全流程均为 FS 更快。

## 7. medium：共同成本与不同物理工作

以下暖读诊断统一从逐请求记录 pooled 取中位数。client−server 是逐请求差值的中位数，不是两列中位数相减。server 阶段存在嵌套，不相加。

| 格式／查询／模式 | B 完整暖读 ms | B server ms | B catalog ms | B client−server ms | SQL 次数 | 暖 payload 次数 | 首读 payload B | 输出行数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GRIB / binary_one / unbound | 12.331 | 10.486 | 4.764 | 1.778 | 2 | 1 | 64901 | 1 |
| GRIB / binary_one / bound | 5.987 | 4.474 | 0.000 | 1.494 | 1 | 1 | 64901 | 1 |
| GRIB / point / unbound | 8.233 | 6.396 | 4.603 | 1.855 | 1 | 0 | 64901 | 1 |
| GRIB / point / bound | 1.453 | 0.331 | 0.000 | 1.072 | 0 | 0 | 64901 | 1 |
| GRIB / roi / unbound | 8.053 | 6.333 | 4.550 | 1.689 | 1 | 0 | 64901 | 16 |
| GRIB / roi / bound | 1.101 | 0.251 | 0.000 | 0.824 | 0 | 0 | 64901 | 16 |
| HDF5 / cross_chunk / unbound | 6.205 | 4.957 | 4.298 | 1.198 | 1 | 0 | 419536 | 192 |
| HDF5 / cross_chunk / bound | 1.554 | 0.317 | 0.000 | 1.195 | 0 | 0 | 419536 | 192 |
| HDF5 / point / unbound | 6.210 | 4.723 | 4.194 | 1.525 | 1 | 0 | 52442 | 1 |
| HDF5 / point / bound | 1.545 | 0.291 | 0.000 | 1.134 | 0 | 0 | 52442 | 1 |
| HDF5 / small_roi / unbound | 6.629 | 4.984 | 4.310 | 1.542 | 1 | 0 | 104884 | 2016 |
| HDF5 / small_roi / bound | 1.721 | 0.387 | 0.000 | 1.263 | 0 | 0 | 104884 | 2016 |

HDF5 绑定数值组 server 约 0.29–0.39 ms，GRIB 约 0.25–0.33 ms；外部 socket／挂载打开／IPC 消费合计剩余约 0.8–1.3 ms。普通数值组均还有一条 catalog SQL，约 4.2–4.6 ms。绑定 binary 的例外由一条 payload SQL 解释，不是失效的绑定计时。

## 8. 计时核验与潜在问题

核验三套实验共 **12,393 次／侧、24,786 次合计**。所有原始文件／汇总／环境／数据 manifest SHA 与原记录一致；重新计算三套 summary 得到完全相同内容。原生分段逐条严格等于 total；B 子阶段不超过 total，server 不超过外部注册计时；原生外部进程观测均大于其计入的会话时间。

| 项目 | 核验结果／处理 |
| --- | --- |
| A/B 交换、单位错误 | 未发现。FS native 协议与 B server operation 独立识别；主实验 B 路径也核验。早期 HDF5 没有保存 result_path，不能追加历史路径证据。 |
| 绑定重复计时 | 未发现。HDF5 native 每行重复报告相同 session_bind_ns，汇总仅从第一行收费一次；GRIB 仅首行报告。两者按各自协议核验。 |
| 单次／累计混用 | 矩阵分开。100 次累计不可与单次 p50 直接比较；首读和准备成本不在暖读 p50 中。 |
| Q 采样不同 | HDF5 普通为真实前缀；其余为独立 Q。保留证据，未将前缀伪装为独立实验。 |
| p50 聚合不同 | 原 HDF5 报告取三轮 p50 的中位数，GRIB 合并 297 条。当前矩阵两者统一 pooled p50，因此少量数字与原表不同；非原计时错误。 |
| native_probe_process_wall_ns | HDF5 普通 runner 的该字段包含 subprocess 完成后的 Python JSON 解析与 oracle 校验；是原生波次 wall 上界，不是纯进程 wall。正式总计用 C++ total_ns，不受此问题影响。HDF5 绑定及 GRIB 在 subprocess 返回后立即截表。 |
| 两侧计时边界 | FS 原生库选择／读取／值复制，B 还包含请求、授权、SQL、IPC 编码、发布、FUSE 和 Python 消费。适合测端到端额外成本，不能称为只比较物理读。 |
| validity／描述处理 | FS 的值输出及 GRIB 坐标复制在计时内，all-valid mask 构造／摘要在计时外；B 消费 mask，并取部分 schema metadata 在计时内。小输出固定成本包含这一接口差异。 |
| 输出／解码单元不等 | HDF5 ROI 为 192／2,016 行，GRIB ROI 为 16 行且有坐标；前者解码 chunk，后者整场。不能据此比较每字节或每行的格式效率。 |
| 缓存及源发现不等 | 普通 FS 不保留外部索引／解码状态，B 保留摄取索引；绑定数据组消除了大部分重复工作。FS HDF5 已有文件内索引，不能称它没有任何索引。 |
| Profile 计时开销 | B 均开启 profile，FS 没有同等阶段 profiler。未做 profile-off 消融，不能从该矩阵量化 instrumentation 的开销。 |
| 跨运行环境 | 主 daemon SHA 相同；数据库 namespace 不同，数据、查询输出及容量不完全相同，没有对齐 DB occupancy／执行顺序的同负载消融。仅支持数量级和成本构成比较。 |

GRIB medium 的秒级普通扫描还由独立 replay 支持：native 单次 1.224／1.180／1.188 s，ecCodes 自带 grib_ls 同一谓词约 1.607 s。见 [标签核验](../grib-paired-q100-20261012/LABEL_AUDIT.md)。这不改变两个格式工作负载不等的限制。

## 9. 历史 HDF5 复验参考

两套 HDF5 均重新按 pooled 暖读 p50 计算，单位 ms。旧／新 daemon 二进制不同，native reader 也增加了 bound 分支；此表不构成严格单变量消融。两轮普通模式数量级一致，不能将波动直接判为优化或回退。

| 规模／查询 | 历史 FS 暖读 | 主实验 FS 暖读 | 历史 B 暖读 | 主实验 B 暖读 |
| --- | ---: | ---: | ---: | ---: |
| small / cross_chunk | 0.511 | 0.511 | 8.983 | 8.896 |
| small / point | 0.159 | 0.160 | 7.111 | 6.458 |
| small / small_roi | 0.222 | 0.216 | 6.705 | 6.328 |
| medium / cross_chunk | 0.516 | 0.521 | 6.394 | 6.205 |
| medium / point | 0.166 | 0.190 | 6.359 | 6.210 |
| medium / small_roi | 0.221 | 0.227 | 6.794 | 6.629 |
| large / cross_chunk | 0.503 | 0.515 | 6.996 | 6.377 |
| large / point | 0.167 | 0.166 | 6.594 | 6.331 |
| large / small_roi | 0.223 | 0.233 | 7.043 | 6.576 |

## 10. 后续需要补齐的对照

1. 普通 Q=1／10／100 全部独立会话，统一 pooled 与 campaign-paired 统计；现有历史结果保留原协议。
2. 如果要比较两个格式的数组访问成本，应统一返回值／mask／坐标、选择规模、缓存状态与命中物理单元，不再把 message 谓词发现与已知 dataset 寻址混为一组。
3. 将绑定／索引准备、首次解码、缓存命中读取、完整发布分别报告。若需任意谓词磁盘 FS 索引，应单独测完整构建与加载；当前固定-query binding 不是这项测量。
4. 若目标是系统额外成本，补 profile-off、IPC 消费对照和统一诊断进程 wall 边界；本轮未修改生产代码或旧实验计时，也未外推优化收益。

复算命令：

```bash
python3 experiments/single_file/cross_format_comparison.py \
  --output experiments/single_file/results/cross-format-audit-20261012/audit.json
```

完整归一化 Q 矩阵、逐套核验结果和输入指纹保存在 `audit.json`；`run-manifest.json` 保存本次脚本与输出指纹。
