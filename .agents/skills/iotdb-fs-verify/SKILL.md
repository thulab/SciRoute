---
name: iotdb-fs-verify
description: 构建或验证 IoTDB-FS C/C++，选择单元、集成或性能检查；纯文档任务不触发。
---

# IoTDB-FS 验证

在仓根目录操作，按任务只读所需入口。沿用会话已有授权；本技能不授权重置数据库或清空数据／实验目录。不将启停或环境重置作为常规构建检查；集成所需操作按已有任务授权和 harness 行为执行。

| 任务 | 入口与检查 |
| --- | --- |
| 构建 | [CMakeLists.txt](../../../CMakeLists.txt) 定义目标和依赖；仅构建相关目标，无需测试时可设 `-DBUILD_TESTS=OFF`。 |
| 单元 | [单元测试说明](../../../tests/unit/README.md)：构建 `unit_tests`，按模块用 `--gtest_filter`；单元集合用 CTest `-L unit`，避免调用未构建的集成程序。运行不需真实 IoTDB／FUSE，构建仍需开发库。 |
| 集成 | [集成测试说明](../../../tests/integration/README.md)：选择对应 harness 和目标，核对实例、挂载点、独立数据库／namespace 及建库清理行为。真实 IoTDB／FUSE 测试显式启用；默认帮助检查不证明真实服务通过。 |
| 平台／发布 | [打包说明](../../../packaging/README.md)：按平台选检查；`--static` 不证明真实挂载或端到端行为。 |
| 性能 | 正式多文件／跨版本走 [固定实验框架](../../../experiments/fixed_matrix/README.md)；其他基准查 [性能目录](../../../tests/performance/README.md) 及所选脚本。 |

功能检查核对内容、尺寸及错误语义；真实写入使用非空样本和独立前缀，保留原始数据。`Test.md` 仅作历史记录。缺失依赖时报告阻塞并完成可行的静态检查。

## 历史性能脚本

入口在 `tests/performance/scripts/`，先检查所选脚本及调用链：

- `build_perf_test.sh` 调用 `cleanAndRestart.sh` 销毁重建集群，并删除两个 `/mnt/` 数据目录，不能作为仅编译入口；二进制输出在 `build/bin/`。
- `test.sh` 调用上述脚本并重复读写，还引用当前不存在的 `scripts/dev/start.sh`、`stop.sh`，不能直接作为常规验证入口。
- 默认路径、样本名及时间戳是历史值；`read_perf_test3.sh` 默认含 `??function=crop`，须通过 `QUERY_SUFFIX` 指定并验证预期查询。

先验证相同查询的输出，再比较耗时；保留代码、数据、参数、环境、原始输出及失败记录，重复次数按实验协议，不以默认三次视为统计充分。

正式实验遵循 [固定实验组规则](../../../AGENTS.md#固定实验组) 和 [固定矩阵方案](../../../docs/deliverables/fixed-experiment-matrix-plan.md)：复用确认组与封印 corpus/case，优化只新增 run；资源、时钟或恢复证据不全的数据不进入 FM1 基线、统计或主张。

构建、内容正确性和性能证据分别报告，无实测不声称提速。相关检查通过且疑点解决即结束，不自动追加全量测试、重复实验、模型、后台任务或论文改写。
