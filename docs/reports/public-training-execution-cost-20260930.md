# Electricity：prepared 执行与数组转换消融（2026-09-30）

## 改动和语义边界

本轮只优化执行期重复验证、标准化统计访问及客户端数组转换。保留 tile、读取批次、缓存容量、窗口/采样语义和 IPC schema。

1. **prepared 验证复用**：prepare 完整验证 snapshot/index/view 后建立自有深副本，连 rvalue 输入也不移走其 vector buffer，避免调用方保留的可写元素引用影响模块。模块内部 snapshot/index/view 不可变；只有模块能调用私有 trusted planner/assembler factory。公共 factory 仍完整验证可变输入。逐请求 limits、continuation、取消、精确 source/version/layout、storage namespace 和授权检查保持。execute 保留廉价 PID 检查，fork 后拒绝使用继承模块，不进入继承的 runtime。
2. **预编译标准化**：每个 prepared view 一次性验证并按 input/target 通道顺序编译 mean/stddev 数组。逐值执行 double 减法和除法，消除逐样本建 map、逐值构造字符串 key 和 map 查找。保留错误顺序、finite/Float32 范围、mask、无效值、两角色失败原子性；没有改用 reciprocal 或较低精度。统计计划纳入 prepared 元数据预算，不修改基础 decoded cache。
3. **buffer 转换**：客户端默认 `conversion_mode=buffers`，直接读取 Float32 buffer 和 bit-packed validity，遵循本机字节序、Arrow offset 与 bitmap bit offset；只有多 chunk 时合并。跳过逐列 fill_null/is_valid 计算，复制到独立、可写、C-contiguous NumPy 输出后按 mask 清零 null；有效 NaN 保留。schema、role、sample、step、descriptor、transform artifact 验证保持。

服务端 `query.training.reuse_prepared_validation`、`query.training.compiled_transform` 默认 true，false 保留参考路径，仅服务器配置可设；profile 记录实际值。客户端保留 sample/batch/buffers 三种显式模式。不存在失败后静默回退。

## 实验设计

五组为 baseline（两个服务端开关 false、客户端 batch）、validation（仅验证复用）、transform（仅预编译统计）、arrays（仅 buffers）、all（全部开启）。这是三个单项对照与一个组合对照，不是完整八组 factorial，不能单独估计所有交互项。

Electricity370，两模型 PatchTST/PatchTSMixer，50 steps、batch2、L96/H24、seed20260929、标准 HF RandomSampler、GPU0；实际每轮51次训练 fetch/102样本。固定精确源版本1790670177863、prepared_compact、1GiB/16,384缓存条目、四路读取/四workers/256inflight。每组重启 daemon 后运行冷、暖轨迹；冷仅清空 decoded cache，不清空 OS/IoTDB缓存。两次重复，第二次反转组别顺序，共40次B和4次新本地A。

首次 prepare（包括独立副本和统计编译）与逐结果释放仍计入训练 wall，未把优化后的工作移出计时。本地 reader 的 setup 在训练计时之外，沿用前轮口径；不把这里的 train wall 称为对称的端到端启动总成本。

加固前的16次完成预跑保留于服务器 `execution-cost-ablation-20260930`，主动中断后正常恢复挂载；**不进入最终统计**。正式实验使用 `execution-cost-ablation-final-20260930`，同一最终二进制运行所有组。

## 验证与结果

正式版本 **978项 C++测试、18项 Python测试全部通过**。40次B均通过与对应A的训练/验证sample轨迹及loss比较（既有容差1e-6）；2,040条训练请求profile全部匹配，无记录到的重叠请求。50个部署源码文件及运行脚本哈希与本地一致。

下表为跨两模型、两次重复均值，每个单元格4次运行，单位秒。所有降幅均相对同轮baseline；未将上一轮历史结果混入。

| 组别 | 冷缓存 wall | 冷降幅 | 暖缓存 wall | 暖降幅 |
|---|---:|---:|---:|---:|
| 原实现 | 7.436 | 0.0% | 6.759 | 0.0% |
| 仅验证复用 | 7.212 | 3.0% | 6.373 | 5.7% |
| 仅预编译标准化 | 7.013 | 5.7% | 6.177 | 8.6% |
| 仅 buffer 转换 | 7.012 | 5.7% | 6.065 | 10.3% |
| 全部开启 | 6.071 | 18.4% | 5.287 | 21.8% |

全部开启后，冷缓存 **7.436→6.071秒（−18.4%）**，暖缓存 **6.759→5.287秒（−21.8%）**。本轮本地A均值 **2.195秒**，暖缓存仍约为其2.41倍，尚未与本地FS持平。

| 模型 | 冷：原实现→全部 | 暖：原实现→全部 |
|---|---:|---:|
| patchtst | 7.451→6.170 | 6.676→5.313 |
| patchtsmixer | 7.421→5.971 | 6.842→5.261 |

## 阶段归因

以下为暖缓存整轮均值，使用单项组隔离对应改动。fingerprint为创建阶段内部计时，不能与其父项重复相加。

| 阶段 | 原实现 | 对应单项组 | 变化 |
|---|---:|---:|---:|
| planner＋assembler 创建 | 0.468 s | 0.090 s | −80.9% |
| 执行期 fingerprint（嵌套项） | 0.212 s | 0 | 已在prepare验证并固定 |
| 标准化 transform | 0.549 s | 0.0149 s | −97.3% |
| 客户端数组转换 | 1.120 s | 0.500 s | −55.4% |
| 客户端验证（arrays组） | 0.431 s | 0.430 s | 基本不变 |

全部开启组创建0.093 s、标准化0.0154 s、数组转换0.495 s。首次prepare为0.081 s（baseline 0.085 s）；独立副本与编译均已计时。未将这几项当作额外独立收益重复相加。

暖缓存全部开启组的服务端总时间约2.298 s，prefetch仍约0.575 s，IPC编码约0.574 s。未改动的阶段也有波动，因此冷缓存单项wall降幅小于其对应阶段降幅并不矛盾；不能用本轮少量短程重复作统计显著性或完整epoch吞吐结论。此次不是完整factorial，也不对交互项单独归因。

所有40次B的每轮IPC均为85,791,200B，控制请求合计139,538B。冷缓存均为120次payload调用、5,205 ranges、109,117,440B、6,660次decode；暖缓存均为0；所有运行0eviction/redecode/decompression。未以改变tile、缓存命中、读取数据量、mask或采样轨迹获得加速。

## 最终版本与证据

二进制SHA256：`750b320c33b65a2704b761f7a52757f195c06e2dec0b92a8e6eb2cab7fa7c670`。所有正式组使用此版本，以显式开关选择参考或优化路径。baseline也包含共同的独立副本与PID防护，并非直接运行旧二进制。旧版二进制保留用于挂载恢复fallback；正式运行未使用。

运行结束恢复原配置、关闭profiling，挂载正常，daemon PID **649146**，实际运行最终优化二进制。未执行git写操作。

- [逐次指标](../results/arrow-execution-cost-20260930/results.json)
- [均值、min/max与分阶段统计](../results/arrow-execution-cost-20260930/summary.json)
- [数据、配置、源码与环境恢复核验](../results/arrow-execution-cost-20260930/validation.json)
- [源码哈希](../results/arrow-execution-cost-20260930/source-hashes.json)
- [原始训练报告、comparison、profile与测试日志](../results/arrow-execution-cost-20260930/execution-cost-evidence.tar.gz)

证据包不含运行配置/凭据或mount日志；被排除的预跑独立留在服务器，未混入本包。

## 复现入口

- [五组 runner](../../tests/benchmarks/arrow_training/run_execution_cost_ablation.py)
- [汇总与完整性检查](../../tests/benchmarks/arrow_training/summarize_execution_cost_ablation.py)
- [客户端转换与独立 oracle](../../tests/benchmarks/arrow_training/test_window_conversion.py)
- [prepared、source 与 fork 回归](../../tests/unit/query/training/arrow/test_TrainingQueryModule.cpp)
- [标准化逐位和错误路径对照](../../tests/unit/query/training/arrow/test_TrainingViewTransform.cpp)
