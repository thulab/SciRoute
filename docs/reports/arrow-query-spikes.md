# A00：Arrow 依赖、公开数据与范围读取基线

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

日期：2026-09-27。实施计划：[arrow-query-implementation-plan.md](../deliverables/arrow-query-implementation-plan.md)。本页记录已经执行的 A00 结果，不代表 A01–A13 查询服务已经实现。

## 环境与源码

实验主机为 `root@192.168.99.49`，Ubuntu 24.04、x86_64、g++ 13.3.0、CMake 3.28.3、FUSE 2.9.9、ecCodes 2.34.1。隔离目录是 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline`；源码副本在其 `source/IoTDB-FS-C`，构建目录在 `deps/iotdbfs-a00-build`。实验目录当前约 13 GiB，`/data_01` 尚余约 3.3 TiB。没有覆盖原 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C`、GRIB 二进制、数据库或挂载点。

本地基准分支 `feat/arrow`，HEAD `41751b2267137133b2001c1cde24b49ff45aec72`。A00 开始时工作树已有未跟踪的计划和环境文档；本次新增的依赖集成、转换器、fixtures、spike 与文档均保留为未提交改动。远端源码是这一工作树的隔离复制，不是带 `.git` 的 checkout。旧远端构建 `ctest -N` 曾发现 654 项；新隔离构建发现 663 项，其中 `unit` 标签 657 项。

项目构建与测试的实际命令：

```bash
BASE=/data_01/zzy/iotdb-fs-exp/public-arrow-baseline
cmake -S "$BASE/source/IoTDB-FS-C" -B "$BASE/deps/iotdbfs-a00-build" \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON \
  -DENABLE_FUSE_INTEGRATION_TESTS=OFF \
  -DIOTDB_ROOT=/data_01/zzy/iotdb-fs-exp/sdk/iotdb-session-cpp \
  -DArrow_DIR="$BASE/deps/arrow-prefix/lib/cmake/Arrow"
cmake --build "$BASE/deps/iotdbfs-a00-build" --target unit_tests iotdb_fs --parallel 8
ctest --test-dir "$BASE/deps/iotdbfs-a00-build" -L unit --output-on-failure -j 8
python3 -m unittest discover -s "$BASE/source/IoTDB-FS-C/ci/tests" -p 'test_*.py' -q
```

结果：生产 `iotdb_fs` 和 `unit_tests` 均成功编译/链接；657/657 项 CTest `unit` 通过；CI Python 测试在远端 Linux 与本地 macOS/Python 3.13 均为 76 项通过、1 项跳过。安装器的符号链接环校验已修正为跨这两个 Python 环境一致。FUSE live/IoTDB 写入测试不在 A00 范围内。

## 依赖决定

锁定 Apache Arrow C++ **21.0.0** 官方源码包，SHA-256 `5d3f8db7e72fb9f65f4785b7a1634522e8d8e9657a445af53d4a34a3849857b5`，Apache-2.0，安装包含 `LICENSE.txt`/`NOTICE.txt`。构建标准仍是 C++17。配置启用共享库、IPC、LZ4_FRAME、ZSTD，关闭不需要的计算/文件系统等模块。Arrow 官方 [21.0 构建文档](https://arrow.apache.org/docs/21.0/developers/cpp/building.html)列出 C++17 和 CMake ≥3.25；[官方 SHA-256 文件](https://archive.apache.org/dist/arrow/arrow-21.0.0/apache-arrow-21.0.0.tar.gz.sha256)提供源码校验值。

主机实建时远端 GitHub/jfrog 下载源不可达；将已校验的 Arrow 源码及其 `versions.txt` 固定的 bundled 源码从本地传入隔离目录后完成 native build/install。LZ4 1.10.0 SHA-256 `537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b`，ZSTD 1.5.7 SHA-256 `eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3`，xsimd 13.0.0 SHA-256 `8bdbbad0c3e7afa38d88d0d484d70a1671a1d8aefff03f4223ab2eb6a41110a3`。实装前缀的 `ArrowConfig.cmake` 报 21.0.0，`ArrowOptions.cmake` 报 shared/IPC/LZ4/ZSTD 均为 ON，`libarrow.so` 和 LICENSE/NOTICE 均存在。项目 CMake 的生产与单元测试目标实际链接该前缀。

`ci/dependencies.lock.json`、安装器、激活器和验证测试已加入同版本及选项约束；安装器从经过 SHA 校验的官方归档构建并验证共享库、CMake 配置和 LICENSE/NOTICE。CI 完整锁定前缀尚未在这台 Ubuntu 24.04 主机执行：现有安装器固定使用 gcc-11/g++-11 和自定义 SDK ZIP，而此主机没有 gcc-11 或已核验 ZIP。上述 657 项成功验证的是本机 g++13 的独立 Arrow 前缀与项目链接；CI 安装脚本的行为由 76 项测试覆盖，完整 CI 前缀仍需在对应 runner 验证。这一区别不能省略。

## PB00 公开数据与模型

公开转换器、fixture 生成器和 NumPy/PyArrow oracle 在 [tests/benchmarks/arrow_training](../../tests/benchmarks/arrow_training/README.md)。Python 环境锁在同目录的 `requirements.ubuntu2404-py312-cu128.txt`；实际环境 Python 3.12.3、PyArrow 21.0.0、NumPy 1.26.4、Torch 2.9.1+cu128、Transformers 4.57.1、Accelerate 1.11.0、Datasets 4.4.1、Pandas 2.2.3，GPU 为 A100。HF Trainer 的 PatchTST 与 PatchTSMixer 均通过单 batch forward/backward、train/eval/save/resume；原始 smoke 数值见 环境报告（服务器归档：`docs/arrow-hf-environment-smoke-20260927.json`）。这只是运行性验证，未形成训练吞吐对照。

ETTm1 原 CSV SHA-256 `6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e`，69,680 步 × 7 通道；生成 9 个分段逻辑行、5 个 record batches 的 IPC stream，SHA-256 `cbd4833ce525c32dd883bf6e1b174f2d2b2ab7241889358c1a3325883c2b179b`。UCI Electricity 公共 ZIP SHA-256 `f6c4d0e0df12ecdb9ea008dd6eef3518adb52c559d04a9bac2e1b81dcfc8d4e1`，其中 TXT SHA-256 `d51565f2cb5a6b768d06ba1bbd3c084c6e2f3aab07f00c6f2dcb80e90175124b`；140,256 步 × 370 通道，生成 18 行、9 批，stream SHA-256 `536c3c72afb04f09daadf860ef021719bb4f90515c10a6710bd35d78de18fa0f`。每行有 `dataset_id/entity_id/series_id/segment_index/start_step/end_step` 和 `target: list<list<float32>>`，默认每段至多 8192 步、每批至多 2 行。原本的本地时间标签保留为文本，不擅自赋予 UTC 语义。源数据没有 HF `state.json`；所需 series/segment/通道元数据由公开 manifest 明示。

[数据 manifest](../../tests/benchmarks/arrow_training/manifests/ETTm1-pb00.json)和 [UCI manifest](../../tests/benchmarks/arrow_training/manifests/UCI_Electricity_370-pb00.json)记录源哈希、通道顺序、schema、批次、连续性和输出哈希。[六个 fixtures](../../tests/benchmarks/arrow_training/fixtures/fixture-manifest.json)包含多行多批、两层 list、外层/内层/float validity、ragged/空通道、未压缩/LZ4/ZSTD 以及字典 delta/replacement。每个 fixture 的 1,002 个窗口均与独立 oracle 相等；ETTm1 92 个、UCI 182 个窗口通过，含 `[2,0]` 通道投影和跨分段边界。结果在 [PB00 校验报告](../../tests/benchmarks/arrow_training/manifests/pb00-window-oracle.json)。转换器/fixture 单元测试本地和远端均 4/4 通过。

## IPC 范围读 spike

[Python recording range reader](../../tests/benchmarks/arrow_training/a00_range_spike.py)只读 stream frame/message metadata 建索引，不在此阶段读取 batch body；之后按选定 batch、row、channel 和 step 读取两级 offsets、三层 validity 与 float32 值。未压缩值使用固定 64-float tile，重叠窗口复用相同 tile；LZ4/ZSTD 按 IPC 压缩 buffer 整块读取和解压，缓存已解压 buffer。字典消息记录 ID、delta 标志和 batch 所在 epoch。结果与 PyArrow batch reader 及目标值/mask 比较。它是文件级物理 I/O 研究代码，不是 QuerySource/IoTDB 生产实现。

| 输入与请求 | 索引读取 | 首窗口额外读 | 重叠窗口额外读 | PyArrow 顺序读至目标 batch | 请求值字节 | 解压次数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| target-only，batch 0/row 0/channel 0/[2,8) | 832 B | 279 B | 22 B | 944 B | 24 B | 0 |
| ETTm1，batch 0/row 0/channel 3/[128,160) | 3,568 B | 272 B | 16 B | 460,224 B | 128 B | 0 |
| UCI，batch 0/row 0/channel 200/[100,108) | 5,872 B | 272 B | 16 B | 24,252,768 B | 32 B | 0 |
| LZ4 fixture，batch 0/row 0/channel 0/[0,3) | 1,872 B | 375 B | 0 B | 2,016 B | 12 B | 5 |
| ZSTD fixture，同上 | 1,888 B | 269 B | 0 B | 1,880 B | 12 B | 5 |

字典 delta/replacement fixture 在 batch 1 的窗口值/mask 同样一致；消息 trace 记录 epoch 1→2，delta 为 `true`，replacement 为 `false`。全部原始 message/body/buffer offset、物理读取及数值在 [range trace 目录](../../tests/benchmarks/arrow_training/manifests)。索引是一次性全 stream metadata 扫描成本，表中首窗口列不含索引；顺序读比较的是读取到目标 batch 的字节量，不是受控的端到端延迟实验。压缩 fixture 很小，不应从这些字节数推断生产吞吐。重叠窗口额外的 16/22 B 是 offsets/validity；值 tile 或已解压 buffer 没有再次读取。

另有 [C++ Arrow API spike](../../tests/benchmarks/arrow_training/a00_cpp_stream_spike.cpp)用安装的 `libarrow.so` 读取 nested stream；[独立 PyArrow 比较脚本](../../tests/benchmarks/arrow_training/verify_cpp_stream_spike.py)对未压缩、LZ4、ZSTD、ETTm1、UCI 共 5 例/25 个值验证通过，见 [结果](../../tests/benchmarks/arrow_training/manifests/a00-cpp-stream-oracle.json)。此 C++ spike 使用 Arrow 的整 batch reader 来验证 API/codec/link；物理局部读取证据来自上述 Python recording reader。A01–A04 才将局部读取接到稳定 QuerySource 与生产 decoder，A05–A09 才验证共享 cache、跨文件窗口和训练视图。
