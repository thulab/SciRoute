# H05：过滤器、填充与可复用解码

本文记录 H05 阶段契约与验证。当前缓存、批量传输和预算扩展见
[H06 公共 chunk cache](hdf5-shared-cache.md)。

H05 将压缩与稀疏 HDF5 dataset 接入现有 typed 查询。仍采用精确 source/version、
单 dataset、rank 1–3 的 Float32/Float64 矩形选择；查询执行通过 QuerySource
读取目标原生 chunk 或 contiguous tile，不调用 H5Dread，也不启用动态插件。
HDF5 插件在进程整个生命周期内保持关闭。

## layout /2 与边缘块

持久化协议升级为 `IOTDBFS-HDF5-LAYOUT/2\n`，目录新增原生
`H5Pget_chunk_opts` 值，纳入目录 checksum；Sigma `hdf5.layout_version=2`。
旧 `/1` 返回 `UnsupportedLayoutVersion`，必须重新摄取，不提供兼容猜测。

`H5D_CHUNK_DONT_FILTER_PARTIAL_CHUNKS`（bit 0x2）可使边缘块完全跳过过滤器，
而 `H5Dget_chunk_info` 仍报告 mask=0。执行先验证原始 mask，再根据 dataset
shape、chunk origin/shape 和 chunk options 计算有效 mask。原生 mask 原样保存；
未知 options 不进入可查询 profile。边缘块仍按完整 native chunk shape 解码。

## 过滤器与严格失败

`Hdf5ChunkDecoder` 是不可变对象，在一个 provider 中编译一次并供各块复用。
支持以下明确验证的 pipeline：

- 无过滤器，以及 `shuffle → DEFLATE → Fletcher32` 的所有保持顺序的子组合。
- 公开 GWOSC 文件使用的 `Fletcher32 → DEFLATE`。

每块按有效 mask 逆序还原；mask 位对应声明位置。shuffle 参数必须等于元素字节数，
DEFLATE 参数为单个 0–9 level，Fletcher32 参数为空。重复、其他顺序、未知 filter、
未知参数或 flags 明确拒绝，即使某块跳过了这些过滤器。

Fletcher32 校验保存的小端 checksum，checksum 对数据按大端 16-bit 字计算。
不接受历史宽松 checksum 变体。DEFLATE 必须恰好产生声明长度，并消费完整编码流；
截断、额外尾部、拼接流、字典依赖、超长输出及 checksum 错误返回失败。
GWOSC 顺序的中间输出包含额外 4-byte checksum，先按该长度解压再校验。
解码后在需要时按元素原位转为主机字节序，不做浮点数值转换。

一次 slab 内后续块失败时不返回该 slab 的部分值，cursor 保持 sticky error。
此前已成功返回的 batch 仍由调用方持有；调用方必须检查最终 cursor 状态。

## 稀疏存储与 fill

只有完整且已验证的索引才能证明逻辑 chunk 未分配；缺失页、坏计数或 checksum
继续报错，不能作为 fill。未分配 contiguous 还需原生 allocation status 确认为
NOT_ALLOCATED，避免 userblock 下 HADDR_UNDEF 的地址回绕。

对于需要合成的存储，fill 必须为 Default 或 Explicit、标量宽度匹配且
fill-time 为 ALLOC 或 IFSET。Undefined 或 NEVER 明确拒绝。已分配块按原生字节读取；
本接口不宣称能从 allocation 元数据恢复块内逐元素写入历史。

规范小端 fill 位模式转为 native bytes 后填充固定 tile／完整 chunk，复用同一
cursor 的 cache；合成不读取 payload。默认零、显式 sentinel、NaN、正负零都是
非 null 值，不根据 `_FillValue` 属性自动生成 mask。

## 内存、复用与指标

默认 `max_buffer_bytes=64 MiB`，`max_encoded_chunk_bytes=64 MiB`，
`max_decoded_chunk_bytes=64 MiB`，`max_zlib_scratch_bytes=1 MiB`。
编码输入和完整解码块分别有上限；DEFLATE 中间 checksum 字节计入总工作集。
配置的 zlib 上限限制所有同时存活的 codec 动态分配（含分配头），使用自定义
allocator；不是仅检查最终输出长度。

规划预估最大活动块工作集，必要时缩小输出 slab；连一行加工作集都容不下则在
payload I/O 前失败。运行时先驱逐足量 LRU，再读取与解码。总预算覆盖当前输出、
常驻 fragment **capacity**、编码输入、解码／unshuffle 输出及 codec scratch。
Fletcher-only 和 GWOSC 块移除 checksum 后可能保留多 4 bytes 的 capacity，
继续计费。超出约定容量的 source 回调结果明确拒绝。很小的 scratch 配置可能在
读入编码块后，由 zlib 初始化或后续分配明确失败。

这不是进程 RSS 限制：目录／索引、容器和一般 allocator 元数据，以及调用方
保留的历史 batch 不在该 buffer 预算内。同步底层 I/O 无法强行打断；取消采用
原子标志，在读返回、解码、填充及组装循环检查。取消不会获取已持有的互斥锁。

每个 cursor 的可选 `metrics_observer` 报告累计计数，异常不会改变查询结果：

| 指标 | 口径 |
|---|---|
| `payload_bytes/ranges` | 实际发出的 payload 读取，不含 layout；失败读取也计入 |
| `decoded_bytes` | 成功物理解码的完整 native values 字节，包括边缘 padding |
| `inflated_bytes` | 实际成功 DEFLATE 阶段输出，GWOSC 可含 4-byte checksum |
| `fill_bytes` | 合成 fragment 的完整字节数 |
| `useful_bytes` | 已复制到输出的 value 字节，不含轴列；失败 slab 中已复制部分也计入 |
| `decode_count/cache_hits` | 实际物理解码次数／本 cursor 内复用次数 |

本阶段是 cursor 内按字节约束的复用；跨查询共享 cache、批量范围与并发隔离属于
H06。mount/socket/IPC 接线属于 H08，未改动现有实验挂载。

## 可复现实验

```bash
cmake --build <build> --target h04_query_probe -j 4
python tests/benchmarks/hdf5_query/run_h05.py \
  --probe <build>/bin/h04_query_probe \
  --output <results> --public-dir <h00-public-dir>
```

standalone CMake 工程为 `tests/benchmarks/hdf5_query`，其 probe 位于 build 根目录。
真实 IngestionModule → published QuerySource → QueryModule 路径运行在记录型
内存 backend 上；原生 h5py hyperslab 加独立公式是数值对照，读取 trace 是范围对照。
这证明格式与执行正确性，不是 live IoTDB/FUSE 或性能收益验收。


2026-10-01 验证结果：

| 检查 | 结果 |
|---|---:|
| Linux 全量 CTest | 1,144 / 1,144（含 1,138 单元测试） |
| Linux / macOS H05 oracle | 各 416 / 416 |
| macOS ASan/UBSan H05 oracle | 416 / 416 |
| Linux / macOS H03 layout `/2` 回归 | 各 59 / 59 |
| Linux / macOS H04 非压缩回归 | 各 185 / 185 |
| 独立 decoder ASan/UBSan | 20 / 20 |

H05 的 416 例包括 384 个值对照（其中四例来自两个原始 GWOSC 文件）、25 个
不支持 profile 和七个损坏 payload。H04 当前回归为 183 个值对照和两个拒绝，
旧 H04 压缩／fill 拒绝场景已被 H05 正例替代；历史 203 例数字不作为当前结果。

sanitizer 覆盖本项目 C++，共享 HDF5/zlib 未插桩，leak 检查关闭，不能替代
ThreadSanitizer 或原生库审计。负例 native oracle 采用独立子进程：本机 h5py
链接的 HDF5 1.14.5 对短 Fletcher32 数据崩溃，对部分尾部／长度异常宽松接受；
生产 decoder 对这些输入均严格失败，不将宽松 native 行为当作正确性标准。
Linux oracle 使用其独立安装版本，具体版本记录在 summary 中。

首次完整回归的两个失败来自共享测试夹具给 contiguous 数据保留了 chunk option，
修正为专用 round-trip 夹具后全量通过；初次日志仍保留。
证据保存在 `docs/results/hdf5-h05-20261001/`（沿用结果目录 ignore 规则），以及
服务器 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h05-20261001/`，
包括构建／CTest 日志、源码哈希、probe 哈希、原生版本、值／范围／指标报告与
sanitizer 结果。
