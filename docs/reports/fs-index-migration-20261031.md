# FS-index 正式组件迁移验收

日期：2026-10-31。结果：正式 FM1 的 FS-index 已迁入主仓库，独立 Linux 构建与相关功能回归通过。**未实现增量维护，未运行性能实验，未修改论文数据。**

## 迁移内容

| 原路径 | 当前路径 | 处理 |
| --- | --- | --- |
| `experiments/fixed_matrix/native/fixed_native.cpp` | `fs_index/src/native.cpp` | 按字节移动，查询/读取实现不变 |
| `experiments/fixed_matrix/native.py` | `fs_index/native.py` | 按字节移动，ctypes/owned output 不变 |
| `experiments/fixed_matrix/native/tests/test_native.py` | `fs_index/tests/test_native.py` | 只改 import 与库路径环境变量；原七项断言保留 |
| 实验子目录 CMake | `fs_index/CMakeLists.txt` | 独立构建目标 `fs_index_native`，新增 CTest 接入 |

主仓库根 CMake 可通过默认 OFF 的 `IOTDBFS_BUILD_FS_INDEX` 选项加入组件；独立构建不依赖 IoTDB/FUSE/Arrow 服务。现行实验调用改为 `fs_index.native`，source closure 收录新的组件目录。旧活动核心和 Python 模块已移走，无第二份实现和旧 import shim。

保留八个 `fixed_native_*` C ABI 符号和 `iotdbfs-fixed-native/1`，名称表示冻结 profile。源码 SHA-256：

```text
fs_index/src/native.cpp
b6a5cc4ca759287dc5e3fc04825ae366a2777cfdb87079850ea071e43026c42b
fs_index/native.py
ce10863a7a93fa756b4f5c7f78960dd44df3ff52fa0e447bb9b1349bbbb4320b
```

两项均与迁移前一致。新构建库的 SHA-256 是 `ff2c27619ffb551daf3b1f3b26633eff4f4e8b5bfe3280e34df27abd38bdf83d`；它不是原归档二进制的替代身份。

## 验证与环境边界

验证运行在 `root@192.168.99.49` 的独立目录：

```text
/data_01/zzy/iotdb-fs-exp/fs-index-migration-20261031/run-001
```

只复制源码、功能夹具和测试所需的冻结归档。复用已有依赖安装，不重编译或重启数据库，不使用旧实验目录作为写入输出。编译/测试绑核 `2,3`，数值线程上限为 1；没有配置 2 GiB cgroup，因此这些结果仅是功能验收。

| 检查 | 结果与边界 |
| --- | --- |
| 独立 Release 构建 | 成功；GCC 13.3.0、HDF5 1.14.4 thread-safe、ecCodes 2.34.1、SQLite 3.45.1、Boost 1.83.0 |
| CTest 原生功能 | 1 个 CTest 项包含 7 项 unittest，全部通过、无跳过；真实 GRIB/HDF5 小样例 |
| 实验控制器回归 | 314 项中 306 项通过、8 项跳过，无错误/失败 |
| HDF5 补充回归 | 在 h5py 环境补跑上述跳过中的 7 项，全部通过；合计覆盖 313/314 项 |
| 剩余 1 项 | 依赖 collection-r2 封印源码的执行循环 AST 对照，隔离目录未装入该整套归档，明确跳过 |
| ABI / 源身份 | 八个原 C 符号保留；两份核心源码 hash 一致；29 个 Python 文件 AST 解析通过 |
| 补丁检查 | `git diff --check` 通过 |

没有执行整个 IoTDB-FS 根目标构建或全系统集成测试；本次实际验证的构建入口为 `cmake -S fs_index`。本机 macOS 的 HDF5 为 2.1.1，与此组件冻结 Linux profile 不符，故原生验证使用服务器既有 HDF5 1.14.4。ecCodes/SQLite 当前通过环境记录绑定，CMake 尚未对其精确版本设门禁。

独立构建保留了原源码的 `fclose` 函数指针模板属性警告；未为消除警告修改已冻结核心。

## 发现及处理

初次本机回归因缺少 PyArrow/h5py，以及旧测试仍读取已移动的 Python 路径而未通过。远端首轮还缺少历史 helper/manifest 夹具。已按原件补齐隔离目录的测试依赖，没有修改归档文件或伪造测试跳过。

另一个真实边界是 `grib_catalog.py` 被历史 render 实验冻结，而迁移改变了其 import 和路径引用。保留原摘要和 `source_proof` 拒绝行为；回归测试现在明确验证：核心 Python 边界仍等于旧摘要，迁移后的控制器不等于旧身份，并且历史门禁会拒绝它。没有将新摘要写回旧验收记录。严格冻结的旧实验必须使用其归档 source closure，新实验应重新生成源码/库收据。

## 后续能力与 Claude 复审

当前组件仍要求全量新建 SQLite，context 固定 sources/latest，选中单元恰为 4096 个值。它不具备持续更新、Arrow 统一适配或任意尺寸读取能力。

[增量维护计划](../deliverables/fs-index-incremental-maintenance-plan.md) 已列明稳定 ID、目录权威、不可变发布、幂等/CAS、查询刷新、恢复与完整成本计量。实际本机 Claude Opus 5.5 复审完成，模型返回 `claude-opus-5-5`、`is_error=false`；已吸收 checkpoint、固定尺寸、历史门禁与公平性问题。它审查了提供的源文件及前期计划，没有独立执行最终测试。

本地原始材料位于忽略目录 `build/fs-index-migration-20261031/`；远端 `run-001/logs/` 保留配置、构建、CTest、失败与最终回归、依赖链接、符号和服务进程快照。最终成功日志为 `runner-tests-final.log` 与 `runner-hdf-supplement.log`，初次失败记录同样保留。

Claude 输入/输出摘要：

```text
review/input.txt
7cdc477d951d13b56e71e9399ffce4ff913d130b5100b2b54db9893a8ad59bef
review/result.json
272805705eca829538279bcb4a01b262e62210f8b36d8f5c71e671bdd832ac0f
```

源码与文档修改留在工作区，未提交或推送 Git。下一步是按计划实现维护协议并先通过 M0，再开始新的三路径生命周期实验。
