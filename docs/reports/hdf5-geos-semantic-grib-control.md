# GEOS 语义剪枝与 GRIB 同入口调用对照（2026-10-06）

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

本实验接续 [GEOS 单文件试点](hdf5-geos-pilot-results.md)，在 `root@192.168.99.49` 的隔离数据库 `h09probe1790939889dbfc1f` 上运行。可复现脚本为 [`geos_semantic_grib_control.py`](../../tests/benchmarks/hdf5_query/geos_semantic_grib_control.py)，原始结果位于 [`summary.json`](../results/hdf5-geos-pilot-20261006/semantic-grib-control-04/summary.json) 与 [`profile.jsonl`](../results/hdf5-geos-pilot-20261006/semantic-grib-control-04/profile.jsonl)。两种格式都经同一 `iotdb_fs` 挂载、Unix socket `kind=typed` 注册、内容寻址 FUSE IPC 结果读取。GEOS 使用精确版本，GRIB 使用 `latest`；每个 GRIB 观测读取一个 level=500 的编码 message，GEOS 分别读取单点与 16×16 Float32 区域。因此本实验对照的是入口和 SQL 分类，**不是同规模数据的端到端吞吐量比较**。所有临时挂载均已卸载。

## 1. 属性与 dimension-scale 绑定的物理剪枝

脚本不是按 `/T` 名字猜温度，而是要求文件中恰有一个 `standard_name=air_temperature` 的 dataset，并验证其 Float32 四维形状、`K` 单位和四个一对一 dimension-scale 关联：`/time`、`/lev`、`/lat`、`/lon`。四个坐标须为有限且严格单调的一维数组，长度与对应轴相等；时间单位须为 `minutes since 2011-07-02 09:00:00`，层单位须为 `hPa`、`positive=down`，经纬度单位须分别为 `degrees_north`、`degrees_east`。赤道纬度在文件中为 `6.36e-15`，坐标匹配使用 `1e-8` 容差，仍远小于 0.25 度网格间距。

据此，将“2011-07-02 09:00、750 hPa、赤道及本初子午线”映射为 `[0,1)×[10,11)×[360,361)×[576,577)`；将同一时次、层次、纬度 `[-2,1.75]`、经度 `[-2.5,2.1875]` 映射为 `[0,1)×[10,11)×[352,368)×[568,584)`。两者经 typed HDF5 查询得到的行数与 Float32 SHA-256 均与原生 `h5py` 相同。原文件的 `valid_range=[1e15,1e15]` 与真实温度约 284 K 冲突，因此仅验证物理值视图，不宣称科学值 mask 或 packed-value 语义有效。

| 候选范围 | 物理 chunk 数 | 相对 `/T` 全部 2,688 个 chunk |
| --- | ---: | ---: |
| 全文件所有 dataset | 29,764 | — |
| `air_temperature` 对应 `/T` | 2,688 | 100% |
| 再选单个时次与 750 hPa 层 | 64 | 2.38% |
| 再选 16×16 区域 | 4 | 0.149% |
| 再选单点 | 1 | 0.037% |

`/T` 所有压缩 chunk 合计 33,153,628 B。冷查询中，单点实际读取 1 个 chunk、12,415 B；接着查询重叠的 16×16 区域只补读另外 3 个 chunk、35,312 B。两次合计 4 个 chunk、47,727 B，为 `/T` 压缩字节的 0.144%；其后五轮的这两个查询均为 0 次 payload SQL。这里的“全 `/T`”是 chunk 索引计数与压缩长度，不是一次受结果预算限制的全量查询。**与上一试点已经手工指定同一 ordinal 子集相比，语义绑定不再进一步减少 chunk；它让谓词可验证地、自动地达到该物理剪枝。**

## 2. 相同注册入口的 SQL 分类

使用同一 daemon 的六轮交错请求，每轮顺序为 GEOS 单点、GEOS 区域、GRIB level=500。表中为第 2–6 轮的每请求稳态中位数；GRIB 在正式观测前有一次预热注册，首次读取 3,257 B 布局，此后命中进程内 parsed-index cache。`metadata_query_nanoseconds` 和 `layout_nanoseconds` 是后端计时，不能直接相加当作端到端墙钟时间。

| 请求 | 元数据 SQL | layout SQL／返回字节 | payload SQL／返回字节 | 元数据 SQL 时间 | layout 后端时间 | 注册服务端总时间 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GEOS 单点 | 9 | 4／68,626 B | 0／0 B | 39.6 ms | 13.7 ms | 59.8 ms |
| GEOS 16×16 | 9 | 4／68,626 B | 0／0 B | 41.5 ms | 13.8 ms | 60.9 ms |
| GRIB level=500 | 9 | 0／0 B | 1／66,606 B | 43.1 ms | 0 ms | 51.7 ms |

GRIB 预热请求为 9 次元数据、1 次 layout、1 次 payload SQL；其后 parsed-index cache 使 layout SQL 归零。GRIB 的 BinaryView 仍每次读取编码 message，故稳态有 1 次 payload SQL。HDF5 decoded-fragment cache 使 payload SQL 归零，但每次仍读 layout directory header/正文、目标 chunk 页以及执行时的身份 header，共 4 次。**两种格式都支付 9 次通用元数据 SQL**：注册 catalog 1、注册授权 source 与两个父目录 3、typed 准备 catalog 1 与精确 manifest 1、结果物化授权 3。GRIB 的 message 语义并没有消除这些注册与权限开销。GRIB 的 `latest` 与 HDF5 的 `exact` 版本选择在本次均计为 9 次，尚不能据此推断两者所有版本选择路径完全等价。

## 优化判断与边界

1. **优先复用 HDF5 已验证 layout。** 当前稳态每次重读 68.6 KB、约 13.7 ms 后端时间；GRIB 的解析索引缓存说明该类重复读取可避免。HDF5 缓存需按 storage namespace、精确 source/version 与 layout 身份隔离，并保持请求预算及身份复核。上表提供可消除成本的测量，尚不是 HDF5 缓存实施后的因果消融。
2. **独立处理通用元数据链路。** 两种格式均为每请求 9 次、约 40–43 ms 的元数据 SQL；应合并一次注册内重复的 catalog/manifest 获取，并对授权元数据采用有界、可失效的复用。这属于 query frontend/core，而非 HDF5 chunk 布局优化。
3. **将 GEOS/CF 证据编译为 chunk 选择条件。** 本实验证明变量、单个时间、压力层和规则经纬度范围可以映射到 1–4 个 chunk；可扩展通用 HDF5 绑定以识别经过验证的压力层。但本数据只有一个时次，不能证明跨文件时间选择性；`valid_range` 异常也阻止直接使用当前科学值视图。多文件目录筛选和新绑定进入生产适配器前，需另做正确性与端到端消融。

这次比较隔离了 SQL 分类，却没有让两个格式读取相同输出量、相同压缩算法或相同 payload 缓存状态；59.8 ms 与 51.7 ms 不应解释为 HDF5 与 GRIB 的纯格式差距。

针对这里识别出的重复读取，后续已实现并复测 HDF5 layout 缓存与 typed 注册元数据复用（服务器归档：`docs/hdf5-layout-metadata-cache-results.md`）。
