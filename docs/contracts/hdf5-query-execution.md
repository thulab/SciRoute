# H04：非压缩 typed 查询与子数组组装

> 当前正式引擎边界见 [HDF5 两种查询模式](hdf5-query-modes.md)。本文保留阶段实现与历史验证结果；SourceDriver 已作为 `native-hdf5` 的数据库精确版本通路，indexed 路径沿用 layout/chunk 执行。

本文保留 H04 阶段的实现范围与历史验证结果。当前过滤器、fill 和内存预算
已由 [H05 扩展](hdf5-filter-decoding.md)，持久化 layout 使用 `/2`。

H04 提供 `Hdf5FormatQueryAdapter`、`Hdf5QueryKernel` 和首版
`Hdf5FragmentProvider`。请求沿用 H01：精确 source/path/version，唯一
`hdf5.value:<编码 dataset 路径>`，可选 ordinal 轴投影，以及 UInt64 半开切片。
数值执行直接读取 QuerySource 的 payload ranges，不调用 H5Dread，不打开本地
文件，不通过 source VFD 读取整个 dataset。

本阶段支持 rank 1–3、标准 IEEE Float32/Float64、LE/BE、无过滤器的 contiguous
和 chunked dataset。生产格式 registry 的 HDF5 摄取接线仍保持 H03 状态；
H04 的查询 adapter 可显式传给 QueryModule，mount/socket/IPC 的完整接线留到 H08。
HDF5 动态插件加载策略未改变。

## 规划与身份

每个 QueryPlanningContext 只用于同一 source 和请求。capabilities 完成目录解析、
路径／别名解析、H01 绑定及候选 chunk 页读取；prepare 复用这些不可变结果，
不重复读取 layout 或重建绑定。不同请求或 source 复用同一 context 会返回 StalePlan。
状态和计划不保存 planning source 或读取回调。

计划固定 storage namespace、source path/version/revision、format、publication
状态、payload/layout 大小、layout header、目录 SHA-256、dataset 对象 token、
请求路径、dtype 和 shape。执行核对两个 source capability 与计划身份，并且只
重读 header（70 bytes，含 magic、长度、目录 checksum），复用规划时已校验的
目录和候选页。该设计依赖现有不可变 published-version 契约；不承诺对能同时
绕过 revision 与 checksum 改写存储的写入者提供认证。

hard-link 别名保留调用方字段路径，同时映射到相同对象 token。schema 中的值
FieldRef 提供 dataset 路径和 dtype，batch source reference 提供精确路径／版本，
domain 提供轴顺序／origin／shape。`hdf5_query_identity(plan)` 可额外取得固定
revision、对象 token、layout checksum 和 `physical-ordinal-c-order/1` 描述。
这些信息不作为逐元素字符串列重复输出；其到 IPC metadata 的接线属于 H08。

## 物理读取与输出

contiguous 以 dataset 起点为基准，使用固定 4,096 元素的逻辑 tile，末尾按
extent 裁剪。Float32 tile 最大 16 KiB，Float64 最大 32 KiB；配置可修改元素数，
但 tile 边界不依赖请求切片。一次局部读取可能包含同 tile 的额外元素，该开销
计入实际 payload bytes，不能称为按切片的最小读取。

chunked 只读取与选区相交的已分配原生 chunks，按完整 native chunk shape
解释物理 buffer，包括边缘 chunk；编码长度必须等于完整 chunk 字节数。规划
校验选区所需 chunk 网格及记录唯一性，防止丢失块被当成零值。物理地址顺序
不决定输出次序；组装根据逻辑 chunk origin 定位。

首版按需逐 range 读取，以 cursor 内按字节约束的 LRU 复用完整块／tile。
没有跨请求共享 cache、并行波次或多范围传输，这些属于 H06。代码不引入面向
训练窗口的语义或派生布局。

Hdf5SlabIterator 按投影列宽、目标 rows/bytes 延迟生成矩形 slab。kernel 逐个
最内轴连续 run 查找 native fragment、复制相交片段，大小端转换在读取 buffer
内原位完成，不做数值类型转换。IEEE NaN payload、正负零、无穷和 subnormal
位模式保留。轴列按请求投影顺序输出；即使只投影 value，也携带完整 N-D domain。
每批 `product(shape)==row_count`，整个 cursor 按末轴最快 C 顺序拼接，无重无漏。
空选区有 schema、无 value batch，也不读取 payload。

## 预算、拒绝与取消

QueryLimits 在规划阶段约束实际 layout bytes/ranges、结果行数和 batch 大小。
执行 header 检查单独计一次 range，payload 读取继续受实际 bytes/ranges 限制。
HDF5 adapter 使用 request-budgeted QuerySource，不绕过通用读预算；provider 的
本地计数也保护直接调用 adapter 的场景。超限在相应读取前失败，不发出超预算
range。已有成功 batch 不会被后续失败追回，调用者必须检查 cursor 终止状态。

`Hdf5QueryKernelConfig.max_buffer_bytes` 默认 64 MiB，约束当前输出数组与本地
fragment 字节。H04 读取 buffer 经原位 endian 转换后直接成为 decoded cache
buffer，避免保留第二份编码副本。若最大 fragment 与输出合计过大，先缩小 slab；
连一个 fragment 和一行都容不下则失败。该值不包含有界解析目录、索引容器和
allocator 开销，也不是全进程 RSS 限制；调用方保留多个已返回 batch 的内存亦
由调用方管理。

以下内容明确拒绝并留待 H05：任意非空 filter pipeline（即使某块 skip 了全部
filters）、选区内缺失 chunk、未分配 contiguous 数据。稀疏 dataset 中选区全部
覆盖已分配 chunks 时仍可查询，不需要 fill 合成。结构 unsupported 或不完整
索引不会静默回退 H5Dread。

payload 短读／失败／抛异常、坏布局、错误身份、源删除和超限均明确失败。
HDF5 cursor 错误为 sticky，不继续输出失败后的部分 batch。cancel 只设置原子
标志，能从读回调内安全调用；读取返回、byte swap 和组装循环检查取消。同步
底层 I/O 本身不能被此标志强行打断。通用 CombinedQueryCursor 也改用原子标志，
并在 adapter 返回和最终投影后检查取消，避免运行中取消的数据竞争。
运行中的 next 会清理本地 cache；经外层 cursor 取消后不再进入内层 next 的空闲
cache 可能保留至 cursor 销毁，仍受 cursor 预算约束。

## 验证入口

```bash
cmake --build <build> --target h04_query_probe -j 4
python tests/benchmarks/hdf5_query/run_h04.py \
  --probe <build>/h04_query_probe \
  --output <results> --public-dir <h00-public-dir>
```

root 工程的 probe 位于 `<build>/bin/`；不依赖 IoTDB/FUSE 的 standalone 工程
位于 `tests/benchmarks/hdf5_query`，复用 H00 锁定依赖。
probe 经过真实 IngestionModule → StructuredBlobStorage → published QuerySource
→ QueryModule → adapter，backend 为记录型内存实现。每例保存 canonical LE
values、schema、slab/domain、坐标列、source references 及分阶段 read ranges。
Python 同时对照 h5py hyperslab 和独立公式，并校验完整 C-order、读范围只属于
目标 chunks/tiles、无其他 dataset value I/O、prepare 不读取 values。

两个原始 GWOSC 文件为压缩数据，因此在 H04 验证明确拒绝，而不是声称其数值
查询已通过。真实 IoTDB/FUSE 端到端验收和性能比较尚未运行，现有挂载未重启。


2026-10-01 实测结果：

| 检查 | 结果 |
|---|---:|
| 新增 H04 adapter 单测 | 18 / 18 通过 |
| Linux 全量 CTest | 1,114 / 1,114 通过（含 1,108 单元测试） |
| Linux typed oracle | 203 / 203 通过 |
| macOS typed oracle | 203 / 203 通过 |
| macOS ASan/UBSan typed oracle | 203 / 203 通过 |

203 个用例包含 185 个 bitwise 值／domain 验证和 18 个明确拒绝验证，其中两个
拒绝是原始压缩 GWOSC 文件。新增测试同时验证物理 chunk 逆序分配、alias 共享
对象身份、只读取一个候选 chunk 页、contiguous tile 在小 batch 间复用、tight
buffer 限额缩小 slab、错误／短读／抛异常的 sticky failure、删除源、回调内取消
及真实 QueryModule 跨线程取消。

macOS sanitizer 覆盖本项目 C++，共享 HDF5/zlib 未插桩，leak 检测关闭；它不
替代 ThreadSanitizer 或原生库审计。首轮远端构建使用了尚未完成 API 修正的测试
快照，出现编译错误；最终源码同步后构建及全量测试通过，初次日志仍保留。
初轮 Python range oracle 将 contiguous over-read 错当成精确切片，已按固定 tile
契约修正，并增加多 tile 大数组用例，避免放宽成任意整 dataset 读取。

证据保存在 `docs/results/hdf5-h04-20261001/`（沿用结果目录 ignore 规则），服务器
位于 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h04-20261001/`。
内容包括构建日志、完整 CTest、源码／可执行文件 SHA-256、h5py/HDF5 版本、每例
canonical values/query report、read ranges 与 sanitizer 结果。
