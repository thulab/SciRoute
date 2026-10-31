# H07：元数据与科学语义绑定

> 当前依赖（2026-10-12）：HDF5 C 库统一要求 **1.14.4 EXACT**，shared／serial／thread-safe／DEFLATE；官方维护归档为 `1.14.4-3`。安装与校验见 [CI 依赖说明](../../ci/README.md)。下文阶段验收中的 1.14.6 为历史实测版本，未改写其证据或声称在 1.14.4 下重跑。

> 2026-10-06 扩展：当前写入协议为 layout `/5`；新增 GEOS 压力轴证据，旧 `/4` 保留物理查询但不提供压力语义。下述 H03–H11 记录保留其当时版本；现行操作见 [dataset 发现与 CF 查询](hdf5-dataset-discovery.md)及 [GEOS 生产多时次验收](../reports/hdf5-geos-production-multitime.md)。

H07 在物理 dataset／ordinal 查询之上增加独立的对象属性目录和显式 GWOSC
GPS 秒绑定。摄取阶段建立证据，查询阶段只读取持久化 layout；不扫描坐标数组，
也不调用 H5Dread。HDF5 动态插件加载在整个进程生命周期内保持关闭。

## 持久化对象元数据

layout 升级为 `IOTDBFS-HDF5-LAYOUT/3\n`。目录 checksum 同时覆盖属性、
完整性状态和 dimension-scale 候选；原始 B 不变。旧 `/1`、`/2` 明确拒绝，
需从原文件重新摄取，不添加兼容推断。

root、group、dataset 各自拥有属性目录。每条属性保留名称、native 类型类别和
元素宽度、shape、null dataspace 状态、逻辑类型和结果状态：

- `Complete`：有界读取成功；整数精确规范化为 Int64/UInt64，IEEE Float32/64
  规范化为 Float64；标量与数组 shape 分开保存。数值保存为小端字节，无字符串化比较。
- `UnsupportedType`：compound、reference 等没有通用值解码。
- `UnsupportedVariableLength`：变长字符串或序列，未调用 H5Aread。
- `TooLarge`：属性元素数、字节或整体元数据预算不足。
- `InvalidValue`：属性结构、字符串编码或可选属性读取无效。

`attributes_complete` 表示**属性名称目录完整**，不表示所有值均可用；
`attribute_count` 保留已取得的原生计数，`attribute_count_known` 区分未知与真实零。
仅在目录完整时，未找到某名称才能
判断它缺失。目录无法完成时保留原因，不以缺失推导查询剪枝。真实 source
读取失败和取消仍返回错误；可选属性超限不会消除已完成的物理 chunk 索引。

默认每对象 256 个属性、每文件 16,384 个属性，每属性最多 1,024 个元素和
64 KiB，整体可选元数据 2 MiB；固定字符串另受 4,096-byte 上限限制。读取前
检查 native bytes 和规范化输出 bytes，固定字符串按 padding 解码并校验 UTF-8。
这些是应用层缓冲、目录和 VFD I/O 边界；libhdf5 自身管理元数据分配，不宣称
全进程 RSS 硬上限。

公开 GWOSC 的 `Xunits`、`Xlabel` 为变长字符串。H5Aread 不接受带自定义分配器
的 DXPL，H5Aget_storage_size 返回的是堆描述符大小，不能用它限制解码字符串。
因此不尝试不受限读取，也不截断后声称属性完整。用户确认以显式 GWOSC 配置
声明这两个字段的语义；如果能读取固定长度字符串，则其值必须精确匹配。

文件级 Sigma 只保存结构与元数据汇总：attributes_total（数量已知时）、
attributes_retained、attribute_counts_known、attributes_complete、attribute_values_complete、
dimension_scale_candidates、dimension_scales_complete，并通过同名 `user.hdf5.*`
xattr 提供；dataset 属性不提升为文件全局字段，
`_FillValue`、NaN 等也不会自动改变 H05 的值／fill 行为。当前没有 dataset
属性谓词下推或跨文件 dataset 索引，属性目录的访问与绑定属于有界目录遍历。

## Dimension scales 是候选证据

固定字符串 `CLASS=DIMENSION_SCALE` 及有界、固定布局的 `REFERENCE_LIST`
提供反向候选 `(dataset object ID, axis ordinal)`。仅接受 legacy 本文件 object
reference，目标须已在目录中且轴有效；不跟随外部 reference。候选最多 4,096 条。

候选不能证明前向 attachment 一致、长度相等、单位正确或坐标单调；这些步骤
未实现时不开放对应语义查询。`DIMENSION_LIST` 等变长值明确不可读取，反向
候选也有独立完整性状态。路径名 `time`、`lat`、`temperature` 不会自动赋予语义。

## 显式 GWOSC GPS 秒 profile

调用方在创建 adapter 时配置 `Hdf5SemanticConfig`：

```cpp
Hdf5SemanticConfig semantics;
semantics.gwosc_bindings.push_back({
  {"/science/H1.hdf5", 17}, "/strain/Strain", storage->storage_namespace()
});
auto adapter = std::make_shared<Hdf5FormatQueryAdapter>(
  Hdf5QueryKernelConfig{}, shared_runtime, semantics);
```

声明绑定到 backend namespace、精确 source/version 和 dataset object。
同一对象的 hard-link 别名可以使用；另一个后端、版本或 dataset 不能继承声明。
namespace 是进程内服务配置，不是可跨进程保存的科学内容身份。准备完成的计划
另保留 manifest revision、layout checksum 与 object ID；不同声明不能复用计划。

物理请求仍使用 `hdf5.value:%2Fstrain%2FStrain`、`hdf5.axis.0`。显式语义请求
可以投影或切片 `hdf5.coord.gps_seconds`（Float64、unit=`second`），值字段仍
必须投影；坐标可以只用于筛选而不出现在输出中。

绑定严格验证：rank=1；标量整数 Xstart/Npoints；标量 Float64 Xspacing；
Npoints 等于 dataset extent；Xlabel=`GPS time`、Xunits=`second`，或按上述
限定由显式声明提供。属性目录不完整、单位缺失／不匹配、任意 unsupported
状态均不冒充有效语义。

首版仅接受可精确表示的二进制规则网格：spacing 为正有限的 2 的整数次幂，
start/spacing 为整数，首末采样点的整数单位坐标及 count 满足 Float64 精度
边界，坐标结果有限。这覆盖 GWOSC 的 4096 Hz；其他采样 profile 明确拒绝，
不把四舍五入后重复的坐标当作单调序列。

输出坐标使用 `fma(ordinal, spacing, start)`。半开范围 `[lo, hi)` 用同一函数
上的二分 lower_bound 转为 ordinal，避免除法／ceil 在边界产生偏移。
范围可以缺一个端点，超出数据的部分裁剪，空交集返回保留 schema 的零行结果。
端点必须为有限 Float64；同一请求不能同时对 axis 0 使用 ordinal 与 GPS 切片。

GPS 秒不会转换为 UTC 或整数纳秒。GWOSC 的 1/4096 秒间隔为 244140.625 ns，
整数纳秒不能精确表示。输出 `DomainDescriptor` 始终保留 UInt64 ordinal 轴，
不以浮点坐标替代物理位置。

`hdf5_query_identity(plan)` 返回 profile `gwosc-gps-seconds/1`、grid start／spacing／
count，以及 `units_verified`／`label_verified`，区分原属性已核对与调用方声明。
物理模式仍为 `physical-ordinal-c-order/1`。GPS 绑定仅在请求坐标时启用，错误
科学属性不会阻断普通 ordinal 查询。公共 decoded cache 的 key 保持物理身份，
两种视图和 hard-link 别名共享相同 chunk。

## 验证与后续边界

`run_h07.py` 通过真实 IngestionModule、StructuredBlobStorage、QueryModule 和
adapter 执行，使用独立 h5py H5Dread 校验值及 GPS 坐标字节。覆盖固定／变长
属性、nextafter 采样边界、空／超界范围、错误单位／长度／类型／精度，以及
锁定公开 GWOSC H1/L1。每例断言准备阶段零 payload I/O，之后相同 ordinal
请求零新增 payload 调用并返回相同值。

```bash
cmake --build <build> --target h07_query_probe -j 4
python tests/benchmarks/hdf5_query/run_h07.py \
  --probe <build>/bin/h07_query_probe --output <results> \
  --public-data <hdf5-h00>/raw
```

standalone 工程 `tests/benchmarks/hdf5_query` 的可执行文件位于 build 根目录。
H07 不提供通用经纬度、不规则坐标、跨文件科学查询或新 dataset 属性数据库索引。
H08 的 IPC／socket／FUSE 注册接线仍未实施，因此这里是格式查询闭环验证，
不是实际挂载的服务验收或性能结论。

## 2026-10-01 验证结果

| 检查 | 结果 |
|---|---:|
| Linux 全量 CTest | 1,201 / 1,201（1,195 单测＋6 项其他检查） |
| 新增 metadata／semantic 单测 | 13＋14 项全部通过 |
| macOS HDF5 专项普通／ASan+UBSan | 各 168 / 168 |
| Linux／macOS H07 oracle | 各 38 / 38，含 6 个公开 H1/L1 请求 |
| macOS ASan+UBSan H07 oracle | 38 / 38 |
| Linux／macOS H03、H04、H05、H06 回归 | 各 59、185、416、13 组全部通过 |

H07 的 38 例包含 22 个正确值／坐标场景和 16 个显式拒绝场景。所有成功例
均与独立 h5py H5Dread 的值字节一致；GPS 坐标按基线规则网格独立计算后
逐字节比较。语义与物理视图切换不触发新增 payload 调用。

额外配额回归采用 dense 属性文件，在物理阶段 8 次、完整阶段 29 次读取之间，
逐一截断全部 21 个调用预算（8–28）。每次均保留两条完整 chunk 索引，报告
元数据不完整，关闭后公开原生对象数回到基线；独立进程无 HDF5 关闭异常。
该测试促成改用 native-order 属性迭代与按名称打开，避免原按索引接口的完整
排序与异常清理路径。固定字符串 NULLPAD 的嵌入 NUL 也有拒绝回归。

Linux 构建启用 BUILD_TESTS、关闭 ENABLE_FUSE_INTEGRATION_TESTS；没有重启
现有挂载。sanitizer 覆盖本项目 C++，HDF5/zlib 共享库未插桩，且
`detect_leaks=0`，因此这些数字不等价于原生库 leak sanitizer 或 TSan 证明。
独立 h5py 使用 macOS HDF5 1.14.5／Linux 1.14.4；被测 C++ 固定为 1.14.6。

证据保存在 `docs/results/hdf5-h07-20261001/`（遵循现有 ignore 规则）及服务器
`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h07-20261001/`，包括最终
源码哈希、构建／CTest、每例 layout／值／坐标／读取范围和 sanitizer 日志。
