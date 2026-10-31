# Electricity 缓存扩容与并行读波次消融（2026-09-30）

本次实现只改变共享缓存容量与训练 adapter 的读波次调度，继续使用公开基线的标准 HF 随机采样。普通 Arrow typed 查询和 GRIB 的取数方式保持原设计。已完成两轮冷/暖 50-step 消融。

## 实现与边界

- 挂载默认 `query.cache.max_entries=16384`，字节预算仍为 1 GiB；保留条目上限以限制未按字节计费的控制元数据。库级 `QueryRuntimeConfig` 默认值不变。
- `query.training.read_parallelism` 支持 1–4，默认 1。完整驻留的 lookahead 使用独立 provider 并行取数，共享 canonical tile key、decoder 和 single-flight cache。超过驻留预算的 pending 波次继续串行读取与释放 lease。
- 默认 4 个 cache worker、256 个在途片段；挂载要求每条 lane 至少 64 个在途片段。实验 IoTDB session pool 固定为 8。
- 按窗口原顺序组装输出，错误或取消时 join 已启动任务；不输出部分 batch，不取消其他消费者的共享加载。
- Profile 记录实际配置以及源读取 `active/peak` gauge。累计 worker 读取时长可能重叠，不能与 wall time 相加或直接相减。

## 实验口径

服务器 `root@192.168.99.49`，工作目录 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline`，运行目录 `ablation-cache-parallel-20260930`。Electricity 共 370 通道，原始 channel-major Arrow 存储；tile 为 4096 个 Float32，input=96、target=24。精确 source version 为 `1790670177863`，SHA-256 为 `536c3c72afb04f09daadf860ef021719bb4f90515c10a6710bd35d78de18fa0f`。

PatchTST、PatchTSMixer 均在 GPU 0 上独立运行；HF Trainer 标准 RandomSampler、seed=20260929、batch size=2、50 steps，validation=4 个窗口。Trainer 预取使训练实际产生 51 次窗口请求、102 个样本；每次服务端请求通过不可变结果路径与客户端计时对应。每种模型每轮先执行普通文件系统 A，再执行各组 IoTDB-FS B；A 可在同一轮的各组之间复用，B 每次均与它校验。

四组仅改变条目上限与并行度：1,024/1、16,384/1、16,384/2、16,384/4。所有组固定 1 GiB、4 workers、256 in-flight，避免并行组同时改变 wave 容量。旧报告使用 64 in-flight；本次串行对照仍每波最多 64 tile，以本次重新测量的对照为准。

每组先重启 daemon 清空 decoded cache，随后运行 cold，再在同一 daemon 中重放 warm。**这里的冷/暖仅指 decoded cache；未清空 OS 或 IoTDB 缓存。** 两轮采用相同 seed，第二轮反转配置顺序。它们检验重复性和顺序影响，不代表多个采样 seed 或完整 epoch 的统计结论。所有运行均开启相同服务端 profile。

## 实测结果

两轮算术平均，单位秒；每个表格单元为两次独立 50-step 运行。

| 配置 | PatchTST 冷 | PatchTST 暖 | PatchTSMixer 冷 | PatchTSMixer 暖 |
|---|---:|---:|---:|---:|
| 1,024 条目／1 路 | 24.568 | 24.234 | 24.724 | 24.352 |
| 16,384 条目／1 路 | 15.537 | 13.709 | 15.826 | 14.124 |
| 16,384 条目／2 路 | 14.758 | 14.061 | 14.949 | 13.870 |
| 16,384 条目／4 路 | 14.478 | 13.936 | 14.431 | 14.085 |

普通文件系统 A 的两轮均值：PatchTST **2.162 秒**，PatchTSMixer **2.296 秒**。

- patchtst: 仅扩容降低墙钟 **36.8%**；扩容＋2 路降低 **39.9%**；扩容＋4 路降低 **41.1%**（1.70×）。在已扩容基础上，4 路再节省 **1.059 秒 / 6.8%**。
- patchtsmixer: 仅扩容降低墙钟 **36.0%**；扩容＋2 路降低 **39.5%**；扩容＋4 路降低 **41.6%**（1.71×）。在已扩容基础上，4 路再节省 **1.395 秒 / 8.8%**。

### 缓存与取数证据

以下计数在所有对应配置、模型与重复轮次中一致；只统计 51 个训练请求，验证集独立于该计数。

| 配置及状态 | payload 调用 | 物理范围 | payload 字节 | tile 解码 | 驱逐 |
|---|---:|---:|---:|---:|---:|
| 小缓存冷 | 571 | 30,140 | 555,237,376 | 33,889 | 32,866 |
| 小缓存重复 | 571 | 30,140 | 555,237,376 | 33,889 | 33,889 |
| 扩容后冷（1/2/4 路相同） | 120 | 5,205 | 109,117,440 | 6,660 | 0 |
| 扩容后暖（1/2/4 路相同） | 0 | 0 | 0 | 0 | 0 |

扩容后 payload 字节与 tile 解码减少 **80.35%**，调用减少 **78.98%**。训练末尾 cache-owned 字节由 16.83 MB 增到 109.19 MB（十进制），完整训练轨迹仍远低于 1 GiB；这是受管 fragment 内存，**不是进程总 RSS**。扩容组没有驱逐；重复轨迹的暖缓存为零读取、零解码。新建 lane 只增加共享 layout 的命中/等待，不增加 payload 字节、范围或 tile 解码。

### 阶段计时与并行证据

以下为每种配置的四次冷运行（两模型×两轮）均值，单位秒。阶段之间有包含关系：prepare、prefetch、IPC 不再与包含它们的 server total 相加。worker read 是并发时长之和，单独列出。

| 配置 | 训练准备 | 预取 wall | IPC | 服务端总 wall | worker read 累计 | 源读取峰值 |
|---|---:|---:|---:|---:|---:|---:|
| 1,024 条目／1 路 | 4.826 | 11.167 | 0.665 | 19.206 | 9.653 | 1 |
| 16,384 条目／1 路 | 4.666 | 2.533 | 0.658 | 10.346 | 1.857 | 1 |
| 16,384 条目／2 路 | 4.720 | 1.651 | 0.649 | 9.536 | 2.001 | 2 |
| 16,384 条目／4 路 | 4.715 | 1.285 | 0.650 | 9.182 | 2.284 | 4 |

`payload_reads_peak` 确认代码同时进入了 1/2/4 个源读取调用；底层 IoTDB SQL 使用连接池中的独立 session。并行组读取量完全相同，预取 wall 缩短，但 worker 时长之和略增，说明不能套用理想线性加速估算。暖缓存组预取已无 payload I/O，模型端到端时长没有稳定的并行收益。

### 结论与后续方向

**缓存扩容是主要收益，2/4 路并行是冷读阶段的增量收益。** 本次实现保留 16,384 条目的挂载默认值；并行能力已开放，默认仍为 1，不在单实验测量后直接改变多实验共享服务的默认资源占用。对本次单实验冷读，4 路最快；2 路可作为更节制的并发配置。

暖缓存仍显著慢于普通文件系统 A，主要剩余工作已不是 tile 重读。本次 profile 显示每次训练运行的 snapshot/view/coverage 等准备持续约 4.7–4.8 秒；下一步应优先复用已准备的 snapshot/view/index 与注册声明，再拆解客户端注册、IPC 解码及其他固定开销。继续缩小 tile 或增加读取并行度无法消除零 payload I/O 时的差距。这里完成的是 50-step 消融与正确性复验，尚不是完整 epoch、DDP 或 A13 整体性能验收。

## 验证与复现

Linux 构建 `iotdb_fs` 与 `unit_tests` 成功，955 个 C++ 测试通过。新增测试覆盖源读取实际 1/2/4 路并发、结果与 provenance 等价、暖缓存、低容量回退、逻辑预算、取消/失败后重试和服务端配置；Python profile 汇总的 8 个测试通过。

复现入口：[run_cache_parallel_ablation.py](../../tests/benchmarks/arrow_training/run_cache_parallel_ablation.py)，配置与执行示例见 [benchmark README](../../tests/benchmarks/arrow_training/README.md)。脚本逐次核对样本轨迹与 loss，检查全部 51 个请求 profile 无缺失/重叠，并在结束时恢复原配置、关闭 profile。


## 证据归档与恢复状态

- [32 次运行指标](../results/arrow-cache-parallel-20260930/results.json)含每次 wall、阶段累计、I/O/解码/驱逐与内存 gauge。
- [完整性验证](../results/arrow-cache-parallel-20260930/validation.json)记录 32/32 A/B 校验、1,632 条训练 profile 归因、二进制/runner SHA-256 和恢复状态。
- [原始证据压缩包](../results/arrow-cache-parallel-20260930/cache-parallel-evidence.tar.gz)含 36 个训练报告、32 个 comparison、16 个原始服务端 profile、runner、unit-test 日志及汇总；不包含配置文件、凭据或 mount 日志。远程原始报告与逐请求汇总仍保存在运行目录。

实验结束后 daemon PID 为 `499247`，恢复 `runtime/public-arrow-ab.properties`，profile 环境变量已移除；挂载使用本次新二进制，未改动数据版本。当前默认缓存为 16,384 条目、1 GiB、4 worker、256 in-flight，训练 read parallelism=1。未进行 git 写操作。
