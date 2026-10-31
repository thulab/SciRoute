# GEOS-5.7.2 生产语义绑定与多时次验收（2026-10-06）

本轮把 [GEOS 语义试点](hdf5-geos-semantic-grib-control.md) 的压力层绑定接入 HDF5 生产 Adapter。语义由 `Hdf5SemanticProfile` 组件在摄取时识别坐标、在查询规划时编译；通用 query core 不识别 dataset 路径或 GEOS 属性。默认注册的 GEOS profile 先于通用 CF profile 试配，且要求根属性 `Source=GEOSadas-5_7_2`、`VersionID=5.7.2` 和 CF 声明。它对 `/T` 的四个正向 dimension-scale 关联、变量属性、原始时间单位、纬经度单调性，以及 `lev` 的 `hPa`、`positive=down`、`PLE_level` 和严格降序数值逐项验证。GEOS 文件中的其他 dataset 可回退到通用 CF profile；压力证据不完整时不会产生压力语义绑定，仍可按物理 ordinal 查询已支持的值数据集。

查询投影可以同时请求物理 `Float32` 值、`hdf5.coord.time`、`hdf5.coord.pressure`、`hdf5.coord.latitude` 与 `hdf5.coord.longitude`；四个坐标可以使用 `Float64` 半开切片。GEOS 的 `/T` 在公开文件中把 `valid_range` 写成 `[1e15, 1e15]`，与实际温度冲突。因此本轮按确认的契约返回未缩放、未按 CF mask 处理的物理值，并在 IPC 描述中标为 `geos572-pressure-grid/1:physical`；既有科学值视图仍执行自己的 CF 缺测/范围规则。时间保留 `minutes since ...` 原单位与 calendar，不暗中转换 UTC 或拼接不同文件的时间轴。

持久 layout 写入协议升为 `/5`，包含 pressure 坐标索引。旧 `/4` 的解析器仍可供原有物理查询使用；它不具备压力语义证据，须重摄取为新 published version 后才能使用新绑定。迁移策略将 `/4` 列为需要重摄取，不原地改写旧版本。GEOS 的 `/time` 是 gzip 压缩的 signed Int32 一维轴；摄取器新增仅用于坐标证据的有界读取，受元素数、字节数、chunk 数、单次读和源 I/O 预算约束，不把任意 Int32 数据集扩充为 typed 值视图。

真实负载使用 [NASA GMAO GEOS-5.7.2 公开样本目录](https://gmao.gsfc.nasa.gov/gmaoftp/ops/GEOS572_Sample_Data/inst3_3d_asm_Np/) 的 2011-07-02 09:00 与 12:00 两个独立 HDF5 文件，分别为 445,984,379 B 和 448,791,823 B。两个文件经同一 IoTDB-FS 实验挂载摄取到隔离数据库，生成两个精确 source/version 和 layout `/5`。跨文件 dataset catalog 发现两条 `/T` 候选并以 `standard_name=air_temperature` 筛出两条；随后对每个精确版本分别执行坐标谓词和物理值读取。这遵循“目录发现＋逐 dataset 查询”契约，不跨文件拼接数组。

验收脚本 [`geos_multitime_semantic.py`](../../tests/benchmarks/hdf5_query/geos_multitime_semantic.py) 对每个时次执行单点与 16×16 区域查询，各重复三次，共 12 个请求。每个结果均与本地 h5py 按同一 dimension-scale 关联计算的 ordinal 范围、C 顺序 Float32 字节 SHA-256、全部逐行坐标、单位和行数相等；压力 749–751 hPa 对应存储序号 10，即 750 hPa。09:00 与 12:00 的时间单位分别保持为 `minutes since 2011-07-02 09:00:00` 和 `minutes since 2011-07-02 12:00:00`。单点每次 1 行，区域每次 256 行；结果描述均标明 `/5` 与 GEOS profile。原始 [摄取清单](../results/hdf5-geos-production-20261006/ingest-manifest.json)、[验收报告](../results/hdf5-geos-production-20261006/semantic-multitime-report.json) 和 [服务端 profile](../results/hdf5-geos-production-20261006/server-profile.jsonl) 保留了精确版本、校验和与逐请求数据。

本次验收也记录了调用成本。跨文件目录发现为 13 次 IoTDB SQL。单次 typed 请求的中位服务端时间：单点 29.62 ms、区域 30.22 ms；每次 4 次元数据 SQL。每个新文件的首次请求有 4 次 layout SQL 与 1 次 payload SQL；后续同文件请求通常为 2 次 layout 身份读取、0 次 payload SQL，解码片段命中共享缓存。首次单点返回约 12–13 KB 编码 chunk，区域约 35 KB，而非读取 445–449 MB 全文件。这里的 h5py 本地读取中位数仅供正确性 oracle 记录，缓存和远程数据库条件不同，不作为公平的速度比。此负载验证语义坐标确实把大文件裁为少量 chunk；尚未证明跨文件目录筛选在大量不匹配候选上的收益，也未消除每请求通用元数据与身份复核固定成本。

服务器构建的 HDF5 单元测试 265/265 通过，包括 Int32 时间轴、旧 `/4` 物理读取与新 `/5` 压力证据；多时次端到端验收 12/12 通过。实验目录为 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-semantic-prod-20261006`，所有测试挂载已卸载。运行需要同一服务器上现有隔离 IoTDB 数据库与 Python 环境；复现命令参数和原始路径见脚本及摄取清单。
