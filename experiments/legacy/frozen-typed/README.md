# Frozen-typed 历史实验归档

2026-10-24: the retired test/probe source copies are archived and ignored; they are not current build inputs. Original bytes remain locally and in the SHA-verified PR #7 archive at `/data_01/zzy/iotdb-fs-exp/document-archives/20261024-pr7-content-curation-6e63794/files.tar.gz` (manifest alongside). Existing frozen source bundles below remain the full reproduction inputs.

`experiment_typed_bind`、`experiment_typed_read`、`experiment_typed_release` 是早期固定查询实验协议，已从正式 `QueryRegistrationSocketServer`、响应类型及客户端解析器移除。这里的文件不参与编译、安装或运行时协议注册。正式接口对这三个请求均返回 `InvalidQuery`，没有兼容开关。

`test_QueryRegistrationSocket.cpp.txt` 保留移除前的计数辅助类、请求辅助函数及四个实验测试。文件头记录移除前完整单元测试源文件的 SHA-256；完整 fixture 和对应生产实现应从下列冻结源包复现，不应将摘录重新接入正式构建。

## 完整复现源包

- 仓库实验位置：`experiments/cross_version/work/route3-global-hot-cache-20261018/source-r3.tar.gz`
- 原实验服务器位置：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/route3-global-hot-cache-20261018/source-r3.tar.gz`
- 源包 SHA-256：`3662b48ce6f01599a1868c8174476520ded674a4d2055f729fac92daaba484cb`
- 同目录 `source-r3-manifest.json` SHA-256：`b12c0f20aefffad996e628aa6d49feb9550224be1aba38087fae38b645462265`

该完整快照包含当时的私有 root socket、固定 exact 版本、实验租约及测试实现。复现实验应在单独解包的旧快照中构建，并遵守原证据中的运行条件；正式源码不再提供该协议。历史实验数据、封存证据和以下脚本保留原样，使用当前正式二进制运行这些旧协议会失败。

## 历史依赖清单

- `experiments/single_file/runner.py`：旧实验环境入口。
- `experiments/single_file/session_runner.py`：bind/read/release 客户端。
- `experiments/single_file/grib_binary_cache_ablation.py`：固定查询消融。
- `experiments/single_file/grib_readiness.py`：固定查询准备与读取。
- `experiments/single_file/grib_paired_runner.py`：成对运行客户端。
- `experiments/single_file/cross_format_comparison.py`：旧入口的跨格式实验。
- `experiments/single_file/test_session_runner.py`、`test_grib_paired_summary.py`：上述历史 runner 的测试。

正常 typed 注册、授权发布缓存、owner 请求预算、训练 `prepare_window`/`read_window`/release 路径继续由正式代码及单元测试覆盖。这些功能与此归档协议不同，不属于清理对象。

## 构建边界

Frozen-typed 原本嵌入 daemon，没有独立 CMake target；本目录不增加 legacy target。`tests/benchmarks/versioning/v03_context_probe.cpp`、`global_hot_cache_probe.cpp` 等验证 owner、权限和预算的显式 probe 仍有语义验收用途，应保留。HDF5 各阶段 probe 仍有独立验证用途。strict 专属 `dataset_catalog_probe` 则已另行原样移到 [hdf5-catalog-strict](../hdf5-catalog-strict/README.md)，不再参与正式构建。
