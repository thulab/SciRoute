# H03：HDF5 layout、摄取与身份注册

> 当前依赖（2026-10-12）：HDF5 C 库统一要求 **1.14.4 EXACT**，shared／serial／thread-safe／DEFLATE；官方维护归档为 `1.14.4-3`。安装与校验见 [CI 依赖说明](../../ci/README.md)。下文阶段验收中的 1.14.6 为历史实测版本，未改写其证据或声称在 1.14.4 下重跑。

> 2026-10-06 扩展：当前写入协议为 layout `/5`；新增 GEOS 压力轴证据，旧 `/4` 保留物理查询但不提供压力语义。下述 H03–H11 记录保留其当时版本；现行操作见 [dataset 发现与 CF 查询](hdf5-dataset-discovery.md)及 [GEOS 生产多时次验收](../reports/hdf5-geos-production-multitime.md)。

H03 将原始 HDF5 文件接入现有 B/L/Sigma 发布流程。`Hdf5FormatAdapter`
已在 mount 的格式 registry 中注册 `hdf5` 身份。原始文件 B 由
`IngestionModule` 顺序写入一次，adapter 不复制或重写数值数据；layout L
由 H02 只读 source driver 读取结构元数据后建立。H04 typed 查询 adapter
尚未注册，本阶段不提供 HDF5 typed 执行或性能结论。

## 冻结的持久化契约

当前采用用户确认的 `IOTDBFS-HDF5-LAYOUT/3\n` 二进制协议。H03 最初实现 `/1`；
H05 增加 chunk options 为 `/2`，H07 增加有界对象属性与 dimension-scale 候选为 `/3`。
旧 `/1`、`/2` 明确返回 `UnsupportedLayoutVersion`，需要重新摄取；不提供兼容推断。
新字段与状态详见 [H07 科学绑定](hdf5-scientific-binding.md)。整数采用小端编码，
字符串和数组有长度前缀；固定 header 保存目录长度、完整 artifact 长度和目录
SHA-256。目录保存 payload 大小、profile、解析器版本、完整性状态、对象图、
dataset 描述和 chunk 页描述符；后续独立页保存 chunk 记录。每页具有独立
SHA-256、记录数、坐标包围盒和首末坐标，目录校验和覆盖这些描述符。

对象身份来自锁定 HDF5 1.14.6 的 native object token，仅在同一个不可变
source/version 中有意义，不包含进程地址、临时 handle 或随机数。每个对象
只建立一个目录项，hard links 保存为带名称的图边；别名和环不展开成路径树。
soft/external links 只记为 unsupported，不跟随。目录构建保留一个发现路径，
并校验它解析回同一对象。非 UTF-8 名称可保留在对象图中，相应 dataset 标记
为不可查询；H01 查询路径仍要求规范 UTF-8。

受支持 dataset 保存 Float32/Float64、字节序、rank/shape、原生类型类别、
存储方式、chunk shape、chunk options、filter 顺序／参数、allocation/fill-time/status 及
规范小端 fill 标量。contiguous 保存绝对 offset/size；chunked 保存已分配
chunk 的坐标、绝对 offset、编码长度和 filter mask。未知 filter、外部 raw
storage、VDS、compact 或不支持类型保留原因，不阻止同文件其他 dataset
建立索引。这里的 supported 表示结构 profile 支持，不代表数值已验证。

已分配 chunk 数独立取自 native API，索引记录数必须与之相等，页记录数之和
也必须相等。序列化前、目录读取时和候选页读取时分别验证结构、计数、边界、
顺序及校验和。丢失记录／页、坏 checksum、错误 source 大小和版本均不能
当作稀疏 fill。只有完整索引才能证明未分配的逻辑 chunk；实际 fill 合成属于 H05。
校验和用于检测损坏，不是针对能同时改写记录、计数和校验和的攻击者的认证。

`H5Dchunk_iter` 单次遍历已分配索引；锁定版本返回包含 userblock base 的绝对
地址，已用 512/4096 字节 userblock fixtures 验证。未分配 contiguous 优先
检查 allocation status，规避 HDF5 1.14.6 对 HADDR_UNDEF 加 userblock base
产生的回绕地址。

## 有界构建与局部读取

默认上限如下；它们是独立约束，不是全进程 RSS 承诺。

| 项目 | 默认上限 |
|---|---:|
| artifact / 目录 | 64 MiB / 8 MiB |
| 对象 / links | 65,536 / 131,072 |
| chunks / pages | 1,000,000 / 65,536 |
| 每页 chunks | 256 |
| 单个字符串 / 图深度 | 4,096 bytes / 256 |
| layout 读取 / 读取调用 | 64 MiB / 65,536 |
| 页描述符扫描 | 1,048,576 |

当前 `write_layout(vector)` 仍一次持有最终 artifact。构建过程有界，不能因
编码分页而称为流式构建。单 dataset chunk 索引超限时保留目录和 native 分配
计数，清空该 dataset 的全部部分索引，明确标记 unavailable；整体对象目录、
编码或结构 I/O 超限时保存小型 `directory_complete=false` marker。marker
也必须满足同一 artifact 限额，连 marker 都装不下的配置在 adapter 构造时拒绝。
真实 source 读取失败、结构损坏和取消不会降级成正常文件。

读取目录仅访问 header 和有界目录正文，返回不可变 `Hdf5LayoutDirectory`。
`read_hdf5_layout_chunks` 按 dataset 与页包围盒剪枝，读取并校验候选页，再
筛选实际相交 chunks。`Hdf5LayoutReadBudget` 跨调用累计 bytes、read_calls
和 page_scans，计入初始目录的两次读取及目录页校验。扫描后排除的页也计费。
目录完整性和 dataset index_complete 必须成立；未读取的候选页不能假定为空。
当前页查找为有界线性描述符扫描，不宣称高维最优索引复杂度。

## 摄取、Sigma 与失败行为

内容探测只读取 offset 0、512、1024、2048… 的完整 8-byte 签名，先检查
source 大小，不依赖文件扩展名。空文件／3-byte 文件继续其他格式探测，真实
短读或失败由通用摄取层报告。rename 后使用已保存的 format_id。

adapter 的 consume 阶段只校验流顺序和完整性，finish 阶段建立结构索引，
不调用 H5Dread 扫描数组，不计算 min/max。H03 原阶段不读取用户属性；H07 在
物理索引完成后增加独立预算的可选属性读取。结构读取中可能有
库预读，以及 VDS global heap 等被 HDF5 标为 DRAW 的元数据；不能仅凭 VFD
标签把它们认作数值解码。`hdf5.inventory_read_bytes` 独立记录结构阶段成功
读取的 bytes，包含 H5Fopen 的读取；它不包括 B 复制和签名探测。

有限 Sigma / `user.hdf5.*` xattrs 包括 container、profile、layout_version、
directory_complete、inventory_read_bytes；完整目录另有 datasets、
queryable_datasets、index_complete。不完整目录省略未知 counts，保存
index_status=resource_limit，不将截断目录伪装为精确的 count=0。
queryable_datasets 只计 supported 且 index_complete 的 dataset；文件级
index_complete 只描述受支持 dataset 的索引是否齐备。

B/L/Sigma 完成后仍经过既有 publication barrier 才可见。payload、layout、
metadata 或 publication 任一失败均走现有失败补偿。metadata 完好而数值
Fletcher32 损坏的文件可以完成 H03 摄取：其原字节保留，值解码时才检查 checksum。
进程内 HDF5 动态插件加载保持关闭，driver/VOL 环境覆盖仍被拒绝。

## 验证方式与范围

`tests/benchmarks/hdf5_query/run_h03.py` 使用真实 IngestionModule、
StructuredBlobStorage 和记录型内存 backend，重新打开已发布 QuerySource，
再解析目录和页；逐文件核对原始 B SHA-256、只写一次、发布前不可见，以及
h5py 的 shape、chunk 坐标／地址／编码长度／filter mask。输入包括 H00 的
55 个 fixtures、两个 H03 graph/paging fixtures、原始 GWOSC H1/L1 公开文件，
共 59 个文件。数据 checksum 损坏文件在本阶段验证的是原字节保留及元数据可索引。

回归测试覆盖损坏／缺页、overflow、独立预算、稀疏／unallocated、未知 filter、
用户块、hard-link 环、unsupported 名称、错误后缀、真实 I/O 失败及 publication
失败补偿。该验证不等同于真实 IoTDB 服务或 FUSE mount 的端到端验收；现有
Arrow 实验挂载未重启。HDF5 数值查询、公共缓存与性能对照仍留在后续阶段。


2026-10-01 验证结果：

| 检查 | 结果 |
|---|---:|
| 新增 layout / inventory / ingestion 单测 | 18 + 9 + 12 = 39 通过 |
| Linux 全量单元测试 | 1,090 / 1,090 通过 |
| Linux HDF5 专项（含 H01/H02） | 87 / 87 通过 |
| Linux 其余默认 CTest 检查 | 6 / 6 通过 |
| Linux / macOS H03 文件 oracle | 各 59 / 59 通过 |
| macOS H03 ASan/UBSan 文件 oracle | 59 / 59 通过 |
| Linux / macOS H02 回归 | 各 176 / 176，加各 2 项进程环境检查通过 |
| macOS inventory ASan/UBSan 独立 harness | 14 / 14 通过 |

首轮 CTest 的 6 项非单测检查因只构建部分 targets、远端缺少 `mac/schema.xml`
失败；补齐构建与文件后仅重跑失败项，全部通过，原始及重跑日志均保留。
macOS sanitizer 覆盖本项目 C++ 代码，动态 HDF5/zlib 库本身未插桩，关闭了 leak
检测，不据此声称 native 库没有内存问题。

两个原始 GWOSC 文件各有 15 个 dataset，其中 `/strain/Strain` 可查询。
各自 layout 为 6,738 bytes，结构阶段成功读取 28,200 bytes；这只是摄取 I/O
核对，不是性能对照结果。2,048-chunk fixture 生成 58,798-byte layout。
分页局部读取由单测验证；文件 oracle 为了核对全部记录会读完全部页。

完整证据在本地 `docs/results/hdf5-h03-20261001/`（结果目录沿用仓库 ignore
规则）及服务器 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h03-20261001/`。
包含源码／可执行文件哈希、构建和测试日志、每文件 payload/layout/report、
h5py 与 HDF5 版本、sanitizer 配置。操作及复现入口见
[benchmark README](../../tests/benchmarks/hdf5_query/README.md#h03-ingestion-and-layout-validation)。
