# HDF5 H00：依赖、公开基线与范围读取验证

日期：2026-10-01。范围：H00 代码准备与本地文件正确性 spike。**没有注册生产 HDF5 adapter、摄取 HDF5 到 IoTDB、迁移数据或运行 FS/IoTDB-FS 性能对照。**

源码基点：`f32520c3ae8df7927f23698e182b4f7c1b4db9da` 加本次工作区修改。实际编译文件、binary 与依赖哈希见 `docs/results/hdf5-h00-20261001/`。未执行 Git 写操作。

## 1. 本次交付与边界

| 入口 | 职责 |
| --- | --- |
| `ci/dependencies.lock.json`、`ci/install_dependencies.py` | 固定 HDF5 源码；完整 CI 依赖安装与独立 `--only hdf5` 安装、文件 manifest 验证 |
| `ci/activate_dependencies.py` | 完整依赖前缀导出 `HDF5_DIR` |
| `tests/fixtures/hdf5/generate_h00.py` | 可重复小文件、独立公式 oracle、文件与输出哈希 |
| `tests/benchmarks/hdf5_query/h00_hdf5_probe.cpp` | C++17 + HDF5 C API inventory、定位真实范围、受限 raw 解码与 native 比较 |
| `tests/benchmarks/hdf5_query/public-data.lock.json` | 公开产品 URL、许可、原文件 SHA256、profile、选择与值哈希 |
| `tests/benchmarks/hdf5_query/run_h00.py` | 重建 fixtures，执行正／负例与公开产品检查，保存逐请求 trace |
| `tests/benchmarks/hdf5_query/README.md` | 独立复现命令、root opt-in 构建与口径限制 |

生产与单测 source list 尚无 HDF5 adapter；`cmake/QueryModules.cmake` 无新增格式注册。根 CMake 仅增加默认 OFF 的 `IOTDBFS_BUILD_HDF5_H00_PROBE`，显式构建 `h00_hdf5_probe`；普通发布包不包含 probe，也不因此新增运行时 HDF5 链接。完整 CI 安装器已准备 HDF5，并包含 COPYING 与 COPYING_LBNL_HDF5；CI 系统依赖增加 `zlib1g-dev`。

## 2. 固定依赖与实际环境

HDF5 固定为 **1.14.6**，非“系统最新版”。[官方源码归档](https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.6/hdf5-1.14.6.tar.gz) SHA256：

```text
e4defbac30f50d64e1556374aa49e574417c9e72c6b1bea6e9b
```

已核对官方 sha256sums 与实际下载字节。构建选项保存在 `HDF5_CMAKE_OPTIONS`：共享 C 库、serial/thread-safe、v114 API、系统 zlib DEFLATE；关闭 MPI、C++ wrapper、Fortran、Java、HL、SZIP、外部插件包和网络 VFD。工具保留，示例及上游测试关闭。HDF5 线程安全配置不构成并行加速证明。[锁定版本源码 H5private.h](https://github.com/HDFGroup/hdf5/blob/hdf5_1.14.6/src/H5private.h#L1213) 将 API lock 映射到全局 `H5_g.init_lock`；小型并发诊断见下文，不用 wall time 单独反推锁等待占比。

| 项目 | Linux 实验服务器 | macOS 本地 |
| --- | --- | --- |
| OS / 架构 | Linux 6.8.0-139-generic / x86_64 | macOS 15.5 / arm64 |
| 编译器 | GCC/G++ 13.3.0 | Apple Clang 17.0.0 |
| CMake | 3.28.3 | 4.3.1 |
| C++ probe libhdf5 | 1.14.6，shared，thread-safe | 1.14.6，shared，thread-safe |
| Python | 3.12.3，独立 venv | 3.13.5，已有本地环境 |
| h5py / NumPy | 3.12.1 / 2.1.3 | 3.12.1 / 2.1.3 |
| h5py 使用的 HDF5 | 1.14.4 | 1.14.5 |
| probe zlib runtime | 1.3 | 1.2.12 |

Python oracle 与 C++ probe 使用不同 HDF5 minor build，均记录实际版本；二者之外，fixture 还有不读 HDF5 的公式值 oracle。`requirements-h00.txt` 固定 Python 包版本；不承诺不同平台 wheel 中的 HDF5 相同。

Linux 独立安装与 `--verify-only` 已执行；macOS 采用相同源码、同组 CMake profile 手动构建并验证，**不代表 Linux-only 安装器已支持 macOS**。实际导出的 CMake target 是 `hdf5-shared`。`find_package(HDF5 1.14.6 EXACT CONFIG REQUIRED COMPONENTS C shared)` 可供根项目 `LANGUAGES CXX` 使用，无须为此修改生产项目语言集合；独立 probe 工程启用 C/CXX。

首次 Linux 构建的库正常，但安装 verifier 没有接受上游头文件的 `# define H5_HAVE_THREADSAFE 1`，导致安装标记未生成。已修复正则、增加同形式测试，并在新前缀 `hdf5-prefix-verified` 完整重建、验证；未绕过失败校验或补写旧 prefix 标记。

## 3. 公开 baseline：GWOSC GW150914

采用原始 GWOSC 事件教学产品 **H1/L1、32 秒、4096 Hz、V2**。这是实际引力波仪器数据，不是重新包装的训练数组。产品与文件名来自 [GWOSC 官方 V2 教程](https://gwosc.org/s/events/GW150914/LOSC_Event_tutorial_GW150914.html)。数据使用 CC BY 4.0，归属与引用遵循 [GWOSC acknowledgement](https://gwosc.org/acknowledgement/)；原始文件不改写。

| 文件 | 字节 | SHA256 |
| --- | ---: | --- |
| `H-H1_LOSC_4_V2-1126259446-32.hdf5` | 1,040,592 | `6e6976e932074a3b4e4a398ed02c62e30fcaa2781811762caea04937133583f6` |
| `L-L1_LOSC_4_V2-1126259446-32.hdf5` | 1,007,420 | `56706e68c811b15548f1d7ed69d7c48d5fcc62aeb21fbaf131355f3cc5c07189` |

合计 **2,048,012 字节**。主要查询对象 `/strain/Strain`：shape `[131072]`、IEEE Float64 little-endian、chunk `[2048]`、64 个已分配 chunks。实际 filter pipeline 为 **Fletcher32 → DEFLATE(level 1)**，读取时逆序解码。每个 chunk 对应 0.5 秒；GPS 起点 1126259446。

固定选择包括：起始 0.25 秒、4 点跨 chunk 边界、事件所在 16–18 秒片段、末尾片段、全 32 秒及末端空选择；H1/L1 各 6 项。每项有 frozen decoded-value SHA256，并再与 h5py 和 native H5Dread 比较。查询是位置半开区间；本阶段不解析语义时间单位或增加物理谓词。

文件中还含 scalar/string/int quality metadata，inventory 将其标为首版数值 profile 之外。**可查询 `/strain/Strain` 不等于支持整个 HDF5 文件的任意 dataset。** rank 2/3 由公式 fixtures 验证；真实多维、跨文件目录筛选、坐标语义和选择性负载仍需 H11 的其他公开产品补充。

## 4. Fixtures 与解码正确性

生成器输出 **55 个小文件，约 347 KB**（精确大小及 SHA 以各平台 manifest 为准），共 164 个选择；加 12 个公开数据选择，共 **176 项**。

- rank 1/2/3，Float32/Float64，LE/BE，contiguous/chunked，whole/interior/edge 选择。
- DEFLATE、shuffle、Fletcher32 及组合；`filter_mask=7` 的跳过全部 filter chunk；故意损坏 Fletcher32 的负例。
- 已分配／未分配 chunk、contiguous 未分配、默认与显式填充值；undefined fill 的洞拒绝，已分配部分可读取，不将 fill 自动视为 NULL。
- 512/4096 字节 user block，覆盖 contiguous 与压缩 chunk；hard-link 别名与 group cycle；空 extent。
- scalar、null dataspace、compact、integer、vlen、external storage、soft/external link、VDS 显式 unsupported。

计划指定的最小切片 `/a[1:5,2:7,3:9]` 已纳入：`/a` shape `[5,7,9]`、chunk `[2,3,4]`，另有不同 shape 的 `/b`。结果 120 个 Float32、480 字节；触及 27 个 edge-inclusive chunks，读取 2,592 字节。检查 `/b` 实际 allocation 与所有请求范围无重叠，证明没有读取其值 payload。该排除证明不涵盖 libhdf5 metadata I/O。

每次 raw chunk 读取均比较实际文件字节与 `H5Dread_chunk` 返回字节、mask；独立反解 filters、处理 endian、裁剪 C-order 矩形，再与 `H5Dread` 位级比较。Python 层进一步与独立坐标公式／frozen public digest 比较。user block 检查确认锁定 1.14.6 的 `H5Dget_chunk_info_by_coord` 地址可直接作为绝对文件 offset，不能再重复添加 user block 长度。

fixture 文件哈希在同一工具链中跨时间 tick 重建一致；不把不同 h5py/HDF5 版本间可能不同的容器编码当作值错误。runner 防止复用陈旧报告、检查 exit status、拒绝失效哈希和越界／计数不一致 trace。

## 5. 范围读取观测及解释

H1 实测（仅 `ifstream` value payload；见逐项 JSON）：

| 选择 | 输出字节 | raw 调用数 | 编码 payload 字节 |
| --- | ---: | ---: | ---: |
| 起始 1024 点 | 8,192 | 1 | 15,903 |
| `[2047,2051)`，跨 chunk 4 点 | 32 | 2 | 31,767 |
| `[65536,73728)`，16–18 秒 | 65,536 | 4 | 63,522 |
| 全 32 秒 | 1,048,576 | 64 | 1,015,432 |
| 末端空选择 | 0 | 0 | 0 |

这证明原生 chunk 是可定位、可独立解码的物理单元，适合作为后续 subset plan 与 decoded cache 的输入；也证明极小选择存在明显 chunk 解码放大。16–18 秒只读 4/64 个 strain chunks，是后续局部读取基线的合理选择。

**这些数字不证明 IoTDB-FS 比传统 FS 更快。** 原生 H5Dread 同样能定位这些 chunks。本次 trace 不计 libhdf5 inventory、native oracle 读、打开成本、metadata、页缓存或 IPC；每个 probe 都重新打开文件且无跨请求缓存。runner 的 subprocess wall time 只用于诊断，不应用于系统吞吐或延迟结论。

### 5.1 原生库并发诊断

新增独立 `h00_hdf5_concurrency`：每个线程使用独立 file/dataset handle，预热后读取 H1 `[4090,8186)`；每组总计 4,096 次 H5Dread，返回 128 MiB，分别用 1/2/4 线程，轮转顺序各重复 3 次。计时包含值比较、关闭与 join，不包含打开和预热；不清理 OS/native chunk cache。

macOS 平均 wall time 为 15.61 / 17.88 / 20.98 ms，所有值检查通过。这个小型 warm-cache 诊断没有呈现线程数增加带来的加速，与锁定版本的全局 API lock 一致；不能据此估算锁等待占比、远程后端性能或声称 raw decoder 已并行。Linux 同组诊断平均为 27.80 / 43.49 / 87.11 ms，也未观察到加速，全部值检查通过。结果保存在 `macos/concurrency.json` 与 `linux/concurrency.json`。H02/H05 应避免将网络 I/O 或共享 cache mutex 绑定到原生库锁的调用链上。

## 6. 验证与证据

Linux 与 macOS 普通 probe 均 **176/176 通过**：144 个成功结果、29 个预期 unsupported、3 个预期 checksum rejection。macOS ASan＋UBSan 使用相同 176 项也通过；仅 probe 翻译单元插桩，HDF5/zlib 未插桩，macOS leak detection 关闭。

Python generator/harness **17/17 通过**。CI helper **80 项，1 项跳过，其余通过**，其中依赖测试 33 项。Linux 生产 `iotdb_fs`、原有 `unit_tests`、opt-in H00 probe 全部构建；本次 unit CTest **996/996 通过**，用时 45.67 秒，见保存的 `linux/ctest.log`。没有将旧 Arrow 历史运行作为本次测试结果。

证据目录：`docs/results/hdf5-h00-20261001/`，包含：

- `linux/`：构建、安装校验、动态链接／环境、176 项 summary、fixtures manifest 与逐请求 traces、Python 和 CTest 日志。
- `macos/validation/`：176 项普通构建的 summary、manifest 与 traces。
- `macos/sanitized-validation/`、`macos/sanitized-provenance.json`：同组 ASan/UBSan 结果、编译 profile、binary/library hashes 与范围限制。
- `ci-python-tests.log`、源码／证据 hash manifest。

`docs/results` 按仓库现有规则被 Git 忽略；运行证据保留在工作区和服务器，不因代码提交而自动进入 Git。

## 7. 服务器隔离与复现

服务器 `root@192.168.99.49`。本轮审计 `/data_01` 约 3.7 TiB 可用；`/data_02` 约 359 GiB 可用、使用率 96%，不作为本轮准备位置。隔离根目录：

```text
/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h00-20261001/
  source/                  本次可构建源码副本
  setup/                   锁定源码 tarball、安装脚本
  hdf5-prefix-verified/    通过 manifest 验证的共享库与工具
  venv/                    独立 h5py/NumPy
  raw/                     未改写 GWOSC 文件
  build/                   生产、unit 与 H00 probe
  results/                 fixtures、trace、summary
```

现有 Arrow 挂载 `public-arrow-baseline/runtime/mount` 在验证前后均仍挂载。未启动新 daemon、写入 IoTDB namespace、重启原 daemon 或替换其二进制。此阶段不需要 socket、writer lock 或新数据库；这些在 H02/H03/live 阶段单独准备。

完整命令见 [H00 README](../../tests/benchmarks/hdf5_query/README.md)。服务器重跑核心检查：

```bash
cd /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h00-20261001
python3 setup/install_dependencies.py --only hdf5 \
  --prefix "$PWD/hdf5-prefix-verified" --verify-only
venv/bin/python source/tests/benchmarks/hdf5_query/run_h00.py \
  --probe build/bin/h00_hdf5_probe --output results --public-dir raw
ctest --test-dir build -L unit --output-on-failure
```

## 8. 留给后续阶段的明确约束

H00 是可信本地文件 spike，`H5Fopen` 仍用默认 FAPL；测试环境未设置 `HDF5_DRIVER`、`HDF5_VOL_CONNECTOR`、`HDF5_PLUGIN_PATH`。动态插件已禁用，但这不等于已隔离所有驱动配置。H02 必须固定 native VOL、只读 source VFD 和 source 身份，落实 metadata I/O、allocation、请求预算与错误映射，不能直接把本 probe 当生产 decoder。

H01 继续 rank 1–3 IEEE float、单 dataset、unit-stride 半开矩形物理契约；quality integer、scalar/vlen、VDS、外部文件、compact 和未知 filters 不在首版自动支持范围。H03/H05 必须进一步验证 fill-time/allocation 的完整语义、格式损坏与资源边界。H06 才加入共享解码；H11 才比较传统 FS native hyperslab、IoTDB-FS query 和可选派生布局，分别归因布局收益与系统收益。
