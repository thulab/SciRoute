# HDF5 H02：只读 source driver 与诊断读取

> 当前正式引擎边界见 [HDF5 两种查询模式](hdf5-query-modes.md)。本文保留阶段实现与历史验证结果；SourceDriver 已作为 `native-hdf5` 的数据库精确版本通路，indexed 路径沿用 layout/chunk 执行。

> 当前依赖（2026-10-12）：HDF5 C 库统一要求 **1.14.4 EXACT**，shared／serial／thread-safe／DEFLATE；官方维护归档为 `1.14.4-3`。安装与校验见 [CI 依赖说明](../../ci/README.md)。下文阶段验收中的 1.14.6 为历史实测版本，未改写其证据或声称在 1.14.4 下重跑。

H02 在 H01 dataset 契约之下增加 libhdf5 的只读 source 访问层。它不注册生产 format adapter，不进行摄取或发布 layout，也不接入公共 decoded fragment cache。原生 `H5Dread` 是显式诊断参照；后续 H04/H05 的优化路径仍使用布局索引和受限 raw chunk decoder。

## Source 与生命周期

`Hdf5SourceDriver::open(shared_ptr<const QuerySource>, options)` 保留固定版本的 source 能力。`with_hdf5_ingestion_source(FormatSource, options, operation)` 只在同步回调期间借用摄取 source；所有库句柄在返回前关闭。调用方不得保留该回调中的 driver 引用。

文件名不参与后端定位。库的 VFD 只能通过当前 source 的 size 和 offset/length read 取数，不通过本机同名路径、临时完整文件、内存 file image 或自身 FUSE 路径回退。诊断程序里的本机文件只由 harness 打开，并以 `pread` 回调提供给相同 VFD。

## 预算与 trace

每个 session 独立限制累计 source read 次数、累计字节、单次 read 字节、输出字节和声明的 chunk 解码大小。读取在调用 source 前计费，失败或短读不退款。零长度读取不访问 source。取消在库调用边界和 source 回调前后检查；已进入的阻塞后端调用不能被这个 driver 强制中断。

`trace()` 区分 VFD 的 metadata 与 raw-data 分类，并保留每次 offset、length 和成功状态。此分类来自库的 memory type，不是对文件所有字节的语义反推：compact 值位于 metadata 中；反过来，VDS 映射使用的 global heap 在 HDF5 1.14.6 中被映射为 `H5FD_MEM_DRAW`，也会计入 raw-data 类别，不能据此推断读取了 VDS 的外部 value payload。库预取按实际请求记录。重复读取重复计费，trace 不代表最小物理覆盖集合。

metadata cache 显式配置，原生 raw chunk cache 默认关闭，避免与后续公共缓存隐式重复驻留。应用输出、读缓冲、trace 和声明的 chunk 大小有界；相交 chunk 的存储长度在原生读取前检查。libhdf5 的 metadata/control 临时分配及原生 filter 工作缓冲不能通过公共接口精确计费。特别是 1.14.6 原生 DEFLATE 解码会随流扩容，声明的 chunk 大小上限不能限制损坏压缩流的实际扩容；H05 的受限 raw decoder 才负责实际解码输出上限。因此配置不是全进程硬 RSS 上限，不能作为任意恶意 HDF5 的内存沙箱。

## 库访问隔离边界

HDF5 在初始化时读取 driver/VOL 环境变量；必须在第一次 HDF5 API 之前验证配置，不能先初始化再仅覆盖 FAPL。自定义 VFD 与 native VOL 固定后，也必须拒绝外部链接、VDS 和 external raw storage，才能保证仅访问授权 source。路径逐级检查 hard link，不能先跟随链接再检查最终对象。

用户已确认：在 IoTDB-FS 挂载进程生命周期内保持 HDF5 动态插件加载关闭，不动态启用。实现从第一次 HDF5 source 访问起调用 `H5PLset_loading_state(0)`，后续操作重新确保关闭；成功、失败和最后一个 session 关闭时都不恢复加载开关。`HDF5_DRIVER` / `HDF5_VOL_CONNECTOR` 非空时在库初始化前拒绝；`HDF5_PLUGIN_PATH` 不会重新启用加载。这是当前进程内的运行策略，不删除插件、不修改机器级环境，也不影响独立 Python/h5py 进程。

仅检查 dataset filter 列表不够：锁定版本的 filtered fractal-heap metadata 也可触发插件加载。因此插件限制覆盖整个 metadata/read 生命周期。动态加载限制不撤销其他代码已经注册的 filter；支持边界是由 IoTDB-FS 管理 HDF5 的进程，不是任意第三方 HDF5 插件共存环境。

## 诊断验证

`h02_source_probe` 使用相同 production driver 返回 Float32/Float64 原生值和完整 VFD trace。`run_h02.py` 重用 H00 的可重建公式与 GWOSC 冻结 hashes，同时用 h5py 独立核对；验证成功 trace 的次数、字节及边界与真实 source callback 一致。它不把诊断原生读取称为已实现的优化查询。

```bash
cmake --build <build> --target h02_source_probe
<h00-venv>/bin/python tests/benchmarks/hdf5_query/run_h02.py \
  --probe <build>/bin/h02_source_probe \
  --output /tmp/h02-results --public-dir <h00-public-dir>
```

独立 probe 构建入口仍为 `tests/benchmarks/hdf5_query`，使用锁定的 HDF5 1.14.6 shared/thread-safe C 库。完整项目现在链接该依赖，但尚未注册 HDF5 adapter。

## 验收结果（2026-10-01）

进程级插件关闭策略已获用户确认，H02 运行验收完成：

| 验证 | 结果 | 范围 |
|---|---|---|
| Linux Release 构建 | 通过 | `iotdb_fs`、`unit_tests`、`h02_source_probe` |
| Linux 单元测试 | 1,051/1,051 | 包含 20 个 H02 source-driver 回归；完整 CTest wall time 46.60 s |
| Linux source probe | 176/176＋2/2 | 144 个成功子集、29 个 unsupported、3 个损坏 checksum；2 个 fresh-process 环境拒绝 |
| macOS source probe | 176/176＋2/2 | 同样的 formula/GWOSC、错误分类与环境矩阵 |
| macOS ASan/UBSan | 176/176＋2/2 | standalone 工程全部 translation units 插桩；共享 HDF5/zlib 未插桩，leak detection 关闭 |
| 插件生命周期 | 通过 | 成功／失败 session 关闭、最后一个 driver 销毁及同步摄取返回后，loading mask 保持 0 |
| CI Python helpers | 79 passed，1 skipped | 共 80 tests；include 边界与 diff 检查通过 |

Linux 使用 GCC 13.3.0／HDF5 1.14.6；macOS 15.5 arm64 使用 Apple Clang 17／HDF5 1.14.6。原始公开文件及 frozen hash 仍使用 H00 锁定的 GWOSC H1/L1 baseline。原生库版本与 oracle h5py 的版本分别记录，未将二者混为同一实现。

第一次单测出现一项 trace 断言失败：VDS mapping 的 global heap 被库归入 `DRAW`。复现定位到 `GCOL` 标记的授权文件元数据，修正测试仅允许该 fixture 的 global-heap 读取，其他 unsupported fixture 继续要求零 raw read；未通过放开外部 source 访问消除失败。初始失败与最终通过日志均保留。

一个实际 trace 示例：H1 `/strain/Strain[65536:73728]` 返回 65,536 B，累计 metadata 类读取 6,952 B、raw-data 类读取 63,522 B，共 18 次 source callback。跨 chunk 的 4 值选择返回 32 B，但 raw-data 类读取 31,767 B。该结果反映原生压缩 chunk 的读取放大，不是未来 raw decoder、公共 cache 或 FS／IoTDB-FS 性能对照。

日志、逐次读取记录、canonical 输出、fixture manifest、source hashes 和 sanitizer provenance 保存在 `docs/results/hdf5-h02-20261001/`；该目录受现有 `.gitignore` 排除，不会自动进入 commit。

Linux 隔离工作目录：`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h02-20261001/`。macOS 编译目录：`/private/tmp/hdf5-h02-macos/`。本阶段未重启现有 Arrow daemon、替换其二进制或写入 IoTDB 数据；HDF5 format adapter 注册、摄取与优化查询仍在后续阶段。
