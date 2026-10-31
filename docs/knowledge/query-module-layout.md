# 查询代码的模块布局

本目录布局把格式无关的 typed query、训练查询以及各文件格式的私有实现分开。公开头文件与实现文件使用相同的模块路径；`cmake/QueryModules.cmake` 是这些模块源码的唯一构建清单，单元测试复用该清单。

| 模块 | 公开头文件 | 实现与测试 | 职责 |
| --- | --- | --- | --- |
| Query Core | `include/iotdbfs/query/core/` | `src/query/core/`、`tests/unit/query/core/` | `QueryRequest`、`QuerySource`、`FormatQueryAdapter` 接口、规划/执行、格式无关的 decoded fragment cache 与预算 |
| Training Model | `include/iotdbfs/query/training/` | `src/query/training/`、`tests/unit/query/training/` | 视图、窗口、sample ID 和训练结果契约 |
| Arrow Training | `include/iotdbfs/query/training/arrow/` | `src/query/training/arrow/`、`tests/unit/query/training/arrow/` | A07 snapshot/coverage 与 A08 按需窗口规划、组装；当前直接绑定 Arrow layout 与 fragment provider |
| Arrow Adapter | `include/iotdbfs/formats/arrow/` | `src/formats/arrow/`、`tests/unit/formats/arrow/` | Arrow 检测/摄取、私有 layout、范围解码及 typed query Adapter |
| GRIB Adapter | `include/iotdbfs/formats/grib/` | `src/formats/grib/`、`tests/unit/formats/grib/` | GRIB 检测/摄取、私有 layout、ecCodes 查询内核，以及现有 GRIB 文件系统扩展 |
| HDF5 Adapter | `include/iotdbfs/formats/hdf5/` | `src/formats/hdf5/`、`tests/unit/formats/hdf5/` | HDF5 摄取、科学绑定、跨文件属性索引，以及 native/indexed 两种文件内引擎 |

`FormatAdapter`、`FormatRegistry`、`Ingestion`、`StructuredBlobStorage` 和元数据契约仍在根目录，因为摄取和存储同时供多个格式使用。`src/main.cpp` 是应用装配点，在此注册具体格式。`MetadataProjector` 的 xattr 输出使用通用 `MetadataXattrList`，不再从 GRIB 类型取定义。旧 FUSE query-string 辅助函数 `QueryUtils.h` 仍属文件系统入口，而非 typed Query Core。

模块依赖方向为：Training Model → Query Core；Arrow Training → Training Model、Query Core、Arrow Adapter；各格式 Adapter → Query Core 与共享存储。Query Core 不引用训练模块或格式执行实现；现存 Arrow 请求坐标与 HDF5 请求验证是下文明确列出的例外。各格式 Adapter 的实现不直接相互引用。`ci/check_query_module_dependencies.py` 检查这些规则、精确到文件/include 的请求声明／验证例外以及全部项目头文件路径，CTest 的 `query_module_boundaries` 用例执行该检查。

## 格式查询模型的并列结构（2026-10-01）

```text
query/core/QueryModel.h                 通用值、schema、表达式和结果
query/core/QueryPlanning.h              请求、capabilities、adapter 接口
formats/arrow/ArrowQuerySubject.h       Arrow 请求坐标（仅 STL 依赖）
formats/arrow/ArrowQueryModel.h/.cpp     List 解析、物理绑定、schema/capabilities
formats/grib/GribQueryModel.h/.cpp       GRIB 字段、schema/capabilities
```

两种格式的 `*QueryModel.h` 都依赖公共 `QueryPlanning.h`，不依赖各自 layout/decoder。Arrow adapter 通过 `make_arrow_logical_schema` 和 `make_arrow_query_capabilities` 获取契约，GRIB 对应使用 `make_grib_logical_schema` 和 `make_grib_query_capabilities`。native source/layout 绑定和查询执行仍留在 adapter/kernel。实现与测试同样并列在 `src/formats/{arrow,grib}/`、`tests/unit/formats/{arrow,grib}/`。

旧 `query/core/ArrayQueryModel` 实际只服务 Arrow 的 batch/row/List 模型，现迁至 `formats/arrow/ArrowQueryModel`，不保留旧转发头。`PhysicalArraySubject`、List helper 类型名称和 JSON `physical_array` 契约未改；这不是通用 N-D 数组接口。

为避免 `QueryPlanning → ArrowQueryModel → QueryPlanning` 循环，`ArrayValuePath` 与 `PhysicalArraySubject` 单独放在轻量 `ArrowQuerySubject.h`。核心请求按值携带这个既有结构，`QueryPlanning.cpp` 仍调用它的结构验证函数，因此核心目前并非可以独立链接的完全格式无关模块。这两个依赖只限请求声明／验证，不允许引入 Arrow layout、codec、provider 或训练实现。此整理显式呈现原有耦合；未来如引入通用 subject 扩展机制，需另行设计，不能借文件移动悄悄改变请求接口。

HDF5 实现后对应 `formats/hdf5/Hdf5QueryModel.h/.cpp`；其 dataset＋轴选择不复用 Arrow 的 batch/row/List 地址。

增加 TsFile 时，将其私有 layout、摄取与 typed query 实现放在 `formats/tsfile/`，实现现有 `FormatAdapter` 与 `FormatQueryAdapter` 接口，并在 `cmake/QueryModules.cmake` 与应用装配点登记。训练模型契约可以复用；目前 Arrow Training 的 snapshot/coverage 和组装逻辑不能直接当作格式无关实现。需要 TsFile 训练窗口时，应先定义格式中立的 coverage/fragment 接口，再建立 TsFile 的实现，避免把 Arrow 的物理 batch/row 假设引入 Query Core。

本次迁移同步更改公开 include 路径，例如 `iotdbfs/query/core/QueryModule.h` 与 `iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h`。仓库内调用方已更新；没有保留旧路径转发头。历史验证 JSON 中的旧文件路径和哈希记录保持原样，以保留实验来源。

## 查询模型整理验收（2026-10-01）

在服务器独立目录 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/query-model-refactor-20261001` 构建 `iotdb_fs` 与 `unit_tests` 成功；`ctest -L unit --output-on-failure` **996/996 通过**，含模型契约、原有 Arrow/GRIB 查询、core、frontend 和模块边界回归。13 个改动后的源码／构建／测试文件与本地 SHA-256 一致。未替换既有实验 daemon 或运行性能实验。

本地 C++17 检查通过四个公共头文件独立包含，以及 Arrow/GRIB 模型两种包含顺序；模块检查同时验证允许的请求声明／校验边界和禁止 core 引用 Arrow layout 的负例。List 校验／解析与 native layout 绑定代码和整理前保持一致。sanitizer 脚本已更新迁移后的源文件和测试选择，本轮未重跑 sanitizer。

构建／测试日志和源码摘要见 `docs/results/query-model-refactor-20261001/`；服务器同名目录保留完整构建目录。

## HDF5 跨文件属性索引归属（2026-10-19）

`Hdf5GlobalAttributeIndex` 的源码、头文件与测试归入 `formats/hdf5/`，并由
`IOTDBFS_HDF5_ADAPTER_SOURCES` 登记。它识别 `hdf5.attr:*` 六属性并对封印目录
建立跨文件组合键索引，不是格式无关的 Query Core 实现，也不是 libhdf5 原生的
文件内索引。`native-hdf5` 和 `iotdb-indexed` 共享它；前者的文件内索引由
`Hdf5SourceDriver` 经 VFD/H5Dread 使用原生 HDF5 实现。

owner backend 继续提供已授权的冻结候选域、准入与预算管理，并调用这个格式专属
helper；不将跨多个源的目录状态塞入单源 `Hdf5FormatQueryAdapter`。本次仅原地移动
模块并更新 include/CMake/测试归属，不改变算法、接口或持久格式，不保留旧路径
转发头，也不新增源码快照或性能试跑。历史冻结证据中的旧路径与哈希保持原样。

本地 C++17 Release 聚焦构建及该索引现有12项测试通过，模块 include／依赖方向检查通过。
未运行远端构建或性能试验；已有实验源码快照、二进制与历史证据保持原样。
