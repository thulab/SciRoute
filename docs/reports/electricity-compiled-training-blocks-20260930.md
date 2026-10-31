# Electricity 64/128 点训练块：生产接线与验证

## 实现范围

保留 64、128 点 step-major 派生训练块，接入现有 Unix socket → TrainingQueryModule → 公共 decoded cache → IPC → FUSE 路径。普通 Arrow typed 查询仍按原物理 tile 模型取数，原始数据不被改写。

- **紧凑通道映射**：共享不可变 channel table 不再在服务器逐 row 展开。coverage 驻留索引保存 row 公共位置、共享表及 `(row, channel)` 引用；仅查询命中结果或显式诊断调用才展开 spans。快照隔离外部可变别名，index 的复制与惰性诊断展开同步。
- **预编译上下文**：prepare 编译序列、时间覆盖、anchor 区间、layout 与 List 索引。execute 保留独立分页、预算、取消和调度状态，复用编译结果。所访问的 source 仍检查 namespace、精确版本、revision、格式与大小，前端继续授权。布局编译有界，超过优化预算时使用原按需路径；未被窗口访问的已删除 shard 不影响其他窗口。
- **整块窗口组装**：`value_layout: "step_major"` 显式绑定 flat List，校验 `steps × declared_n_channels`。跨窗口需求按块聚合，完整块进入共享缓存；缺失压缩块支持多范围合批。完整有序投影按时间行批量复制，子集/重排按物理列位置复制。保留 mask、逐通道 provenance、逻辑预算和 null 拒绝退款。

内部 List 索引工厂只用于已经验证且不可变的 layout，避免预编译逐 batch 重验全 layout 的二次复杂度。公开工厂仍执行完整验证。

## 验证环境与边界

服务器 `192.168.99.49`，工作目录 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline`。沿用完整 Electricity 140,256 × 370 数据、L96/H24、冻结的 51 次请求/102 个样本。缓存 1 GiB、16,384 entries、4 个 worker、4 路读取。冷表示 daemon decoded cache 冷启动，未清理 OS/IoTDB 缓存。

C++ 单元测试 **994/994** 通过；Python prepared client、派生构建与读取测试 **11/11** 通过。新增覆盖 64/128 点边界、370 通道压缩合批、跨视图缓存复用、重排/子集、null/missing、拒绝退款、取消、并发分页、快照可变别名、索引复制竞争及编译预算回退。

生产回放逐请求与原始 Arrow 独立 oracle 比较标准化 Float32 位模式及 validity，全部一致。50-step Trainer 验证另列；这些短跑不能代表完整 epoch 稳态吞吐。

## 首轮生产回放

本轮每一遍重新注册 prepared 对象，第二遍仅复用 decoded cache；首次准备计入请求耗时，Python reader 构造单列且不计入轨迹。字节量使用十进制 MB。

| 布局 | 首次准备 s | 冷轨迹 s | 暖缓存、重新准备 s | 冷 payload MB / 调用 | 暖 payload 调用 |
|---|---:|---:|---:|---:|---:|
| 原始 channel-major | 0.136 | 4.616 | 3.868 | 109.117 / 120 | 0 |
| channel-major 128 LZ4 | 3.738 | 13.859 | 13.367 | 16.915 / 50 | 0 |
| step-major 128 LZ4 | 3.600 | 8.327 | 7.777 | 19.802 / 50 | 0 |
| step-major 64 LZ4 | 7.642 | 12.987 | 12.550 | 15.250 / 51 | 0 |

128 点 step-major 相对原始布局减少约 **82% payload 字节、58% 调用**；64 点再省字节，但块数由 164 增至 254，元数据与 provenance 更大。

| 冷轨迹阶段（51 请求合计） | 原始 | step-major 128 | step-major 64 |
|---|---:|---:|---:|
| planner 创建 s | 0.0024 | 0.0023 | 0.0025 |
| window plan s | 0.068 | 0.107 | 0.119 |
| prefetch s | 1.059 | 0.917 | 1.217 |
| assembly s | 0.099 | 0.042 | 0.052 |
| IPC encode s | 0.638 | 0.626 | 0.666 |
| 授权 s | 0.449 | 0.599 | 0.633 |
| 输出 IPC MB | 85.79 | 98.09 | 108.98 |

这些阶段是嵌套 profile 中选取的互不重叠阶段；不要再与包含它们的 `window_execute`/`publish_route` 相加。payload 指标来自 runtime，不包含 snapshot/layout 准备读取。

**整块组装和物理 I/O 已改善，但当前短跑端到端仍未胜过原始布局。** 冷阶段较小的块需要更多 coverage 元数据：128/64 的 coverage prepare 分别约 1.89/4.28 秒，module prepare 约 0.90/1.77 秒；客户端首次 channel-table 规范化另需 0.56/1.13 秒。驻留索引虽已紧凑，首次构建仍临时展开每个 row/channel 的 span。不能将驻留内存节省等同于首次准备已经消除。

暖缓存下 128 点块的 assembly 仅 0.039 秒，但 IPC、每通道 provenance、授权和客户端解码仍占有明显时间。这说明块布局收益已经穿过取数层，端到端剩余成本主要在准备和结果协议；不能据此宣称整个生产路径已达到原型 reader 的 0.5 秒。

## 复用同一 prepared ID

第二轮生产回放保持同一 reader 和 prepared ID，暖遍确实不再提交声明、构建 coverage 或编译 module；所有暖遍 payload 读取和解码为零。

| 布局 | 冷遍（含首次 prepare）s | 同一上下文暖遍 s |
|---|---:|---:|
| 原始 Arrow | 4.568 | 3.650 |
| step-major 128 LZ4 | 8.328 | 4.523 |
| step-major 64 LZ4 | 12.751 | 5.002 |

因此预编译的复用边界已打通，但在相同暖缓存条件下，当前 step-major 仍没有端到端收益。仅依据节省的 payload 字节推断训练总时间会改善是不成立的；当前仍需缩减首次 span 构建和小块带来的结果 metadata/provenance 成本。

## 50-step PatchTST 训练

两轮复验，第二轮反转 64/128 的测试顺序；每个冷臂重启 daemon。暖臂在 Trainer 计时前以同一 reader/prepared ID 预热完整冻结轨迹。所有比较均通过 train/validation sample trace、SHA256、模型/采样配置、train loss 与 eval loss 检查。

| 路径 | Train wall 均值 s | Fetch 均值 s |
|---|---:|---:|
| 普通文件系统原始 Arrow | 2.411 | 1.314 |
| IoTDB-FS step-major 128 cold | 9.568 | 8.329 |
| IoTDB-FS step-major 128 warm | 5.451 | 4.195 |
| IoTDB-FS step-major 64 cold | 13.924 | 12.575 |
| IoTDB-FS step-major 64 warm | 6.068 | 4.711 |

Reader 构造在 Trainer wall 外，冷臂首次 prepare 在 wall 内；暖臂的 prepare/prefill 在 wall 外。原始报告的 reader 累计 metrics 包含这段预热，不能把其中 `preparation_seconds` 再从暖训练 wall 中扣除。训练 fetch 由训练迭代器独立计时，表中暖 fetch 不含预热。

本轮只对 PatchTST 做训练数值复验，不代表其他模型或多客户端吞吐。128 点保留较少块数和 provenance，当前比 64 点更适合继续优化生产链路；两个块长均保留支持。普通文件系统训练仍更快。

## 复现与证据

- `replay_derived_typed.py --blocks 64 128 --layouts step_major`：完整生产回放；增加 `--reuse-context` 可让暖遍保留同一 prepared ID。
- `run_compiled_block_training.py`：默认两轮、反转块顺序的 50-step PatchTST 训练验证；冷臂重启 daemon，暖臂在计时外预热相同轨迹和 prepared 对象。
- `summarize_compiled_block_replay.py`：按 immutable result path 关联客户端和服务器阶段，拒绝重叠或缺失 profile。
- 本地证据目录：`docs/results/electricity-compiled-blocks-20260930/`。

派生构建时间、额外存储与原型对照仍见 [初始派生实验结果](electricity-derived-layout-results-20260930.md)。本次只验证生产训练适配器接线，不宣称自动派生选择、跨版本刷新或首次 coverage 构建优化已经完成。

原始证据归档 SHA256：`d71cc2fc7b8b81245fc22dfea9d6c07dd0efea908084eec0b9dc4d0a4b23f017`。源码文件哈希与服务器实际构建版本逐项一致；挂载最终恢复为 PID 920083，原配置、profiling 关闭。
