# 历史 HDF5 实验入口

2026-10-24: the retired test/probe source copies are archived and ignored; they are not current build inputs. Original bytes remain locally and in the SHA-verified PR #7 archive at `/data_01/zzy/iotdb-fs-exp/document-archives/20261024-pr7-content-curation-6e63794/files.tar.gz` (manifest alongside). Existing frozen source bundles below remain the full reproduction inputs.

正式 HDF5 模式仅为 [`native-hdf5` 与 `iotdb-indexed`](../../docs/contracts/hdf5-query-modes.md)。本目录保留已移出正式构建的历史实验代码：

- [hdf5-catalog-strict](hdf5-catalog-strict/README.md)：旧 strict/catalog 对照探针，原文件逐字保留。
- [frozen-typed](frozen-typed/README.md)：旧 frozen-typed socket 正例与历史协议来源。

完整历史实现绑定 `cross_version/work/route3-global-hot-cache-20261018/source-r3.tar.gz`（SHA-256 `3662b48ce6f01599a1868c8174476520ded674a4d2055f729fac92daaba484cb`），包括其源码清单。历史 runner 使用其冻结二进制和设置；含旧 `query.hdf5.catalog_mode` 的启动器不能直接运行当前二进制。现行代码明确拒绝旧设置，不做参数别名。

`cross_version` 和 `multi_file` 的已发布实验结果、source archive、runner 与原始数据保留原样；它们不会因正式模式整理而自动变为两模式对照。
