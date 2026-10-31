# 普通文件系统与 IoTDB-FS：50-step 训练 smoke（2026-09-29）

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

沿用首轮 A/B 接线（服务器归档：`docs/public-training-ab-wiring-20260929.md`）的隔离数据库、挂载、公开数据、`L=96,H=24,S=1` 窗口清单和仅训练区间拟合的标准化。运行前确认 `fuse.iotdb_fs` 挂载与注册 socket 可用、GPU 0 空闲，ETTm1 与 Electricity 的 exact source version 分别仍为 `1790670158165` 和 `1790670177863`。本次通过 `run_public_ab_smoke.py --max-steps 50 --gpu 0 --tag long50-20260929` 顺序运行本地 Arrow（A）与 IoTDB-FS typed windows（B），两侧使用相同 seed、batch size 2、Trainer 随机训练/顺序验证采样。ETTm1 验证 8 个窗口，Electricity 验证 4 个窗口。

四组均完成 **50 个优化步**。Trainer 实际读取 51 个训练 batch、102 个窗口（含预取），A/B 的样本 ID 序列及训练和验证 loss 各组完全一致；没有查询超时或结果预算失败。原始逐后端报告位于服务器 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/manifests/*-long50-20260929-{local,iotdbfs}.json`，机器可读汇总见 50-step 结果（服务器归档：`docs/public-training-ab-long50-20260929.json`）。

| 数据 / 模型 | A 训练秒 | B 训练秒 | A 验证秒 | B 验证秒 |
| --- | ---: | ---: | ---: | ---: |
| ETTm1 / PatchTST | 1.042 | 4.140 | 0.019 | 0.242 |
| ETTm1 / PatchTSMixer | 0.986 | 5.321 | 0.013 | 0.590 |
| Electricity 370 / PatchTST | 2.227 | 148.502 | 0.052 | 1.979 |
| Electricity 370 / PatchTSMixer | 2.159 | 147.969 | 0.048 | 1.894 |

Electricity 的 B 侧训练各发起 51 次查询注册、读取 102 个窗口；PatchTST 的 `registration_seconds` 累计 143.614 秒，PatchTSMixer 为 142.748 秒，约占各自训练墙钟的 96%–97%。该指标**不是纯注册准备时间**：socket 在返回路径前会同步执行窗口查询、编码 IPC 并发布结果。随后计量的 `materialize_seconds` 主要是客户端对已发布路径的 FUSE `stat`，不能据此认定数据已在注册阶段以外单独 materialize。B 侧结果 IPC 累计约 85.8 MB。ETTm1 的相同 51 次 socket 调用仅需约 2.5–3.3 秒。受控复测显示，Electricity 的主要额外时间来自 370 通道随机窗口触发的 Arrow 解码片段缓存未命中与驱逐；完整声明的重复解析/索引也增加约 0.1 秒/请求，但不是 143 秒的主要来源。证据和边界见注册耗时诊断（服务器归档：`docs/public-training-registration-diagnosis-20260929.md`）。

本次证明较长随机训练路径的正确性和稳定性，**不证明 IoTDB-FS 性能合格**。50 steps 仍远少于完整 epoch，验证窗口也很少；Electricity 的高验证 loss 是该固定短训配置的结果，不能解释为模型质量比较。下一轮应优先给解码缓存按实际工作集配置条目数或改进淘汰/批量读取，再复测 370 通道的随机访问；同时复用不可变 snapshot/view，避免每批重复提交与验证。之后进行多次 A/B/B/A、loader-only、完整训练及稳态性能验收。
