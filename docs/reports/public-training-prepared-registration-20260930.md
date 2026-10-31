# Electricity：准备对象复用与通道表规范化（2026-09-30）

## 实现与实验口径

本次把 snapshot、coverage index、view、固定 limits/options/execution 组成不可变准备对象。首次 `window_prepare` 返回独立 `prepared_id`；后续 `window_execute` 只发送 ID、窗口坐标和 max_windows。ID 不参与稳定的 sample identity。每次执行保留当次 peer 授权、精确源版本和实际读取的 layout 校验，不再重新准备 snapshot/index/module。

首次声明使用 `snapshot.channel_tables`，row 只引用 `channel_table_id`。通道 length 省略时继承该 row 的显式 declared_length；因此 Electricity 的17个完整分段和末尾992步的短分段共享同一370通道表。服务端恢复相同展开模型，fingerprint、值和缺失策略不变。当前优化传输声明与对象复用，没有进一步改变服务端 rows/spans 的内存模型。

对象按 UID 隔离，有 TTL、容量和元数据字节预算。客户端读取并关闭 IPC mapping 后显式释放结果 route，reader close 释放准备对象；对象退役后仍有在途 lease 时不提前退款。细节见 [协议与资源边界](../contracts/arrow-prepared-training-registration.md)。

三组使用同一新二进制：

1. `full`：每个 minibatch 提交原始完整声明，作为优化前路径。
2. `prepared`：完整声明只准备一次，其后 ID＋坐标。
3. `prepared_compact`：准备对象复用，并压缩首次通道声明。

固定 Electricity370、源版本1790670177863、L96/H24、batch2、seed20260929、标准 HF RandomSampler、50 steps、validation4、GPU0。挂载采用16,384缓存条目、1GiB、4读波次、4workers、256inflight。两模型各两次重复；第二次反转方案顺序。每个 model/arm/repetition 重启 daemon，随后在同 daemon 顺序运行冷、暖两条相同轨迹；“冷”仅表示 decoded cache 清空，不清空 OS/IoTDB 缓存。

每次训练新建 reader，暖缓存组也重新 prepare。首次 prepare（含客户端通道表整理）lazy 地发生在第一个 Trainer fetch 中，**计入 train_wall_seconds**；每次结果释放也计入。整个 reader 的 close 在训练/评估结束后，不计入训练墙钟。基线和复用组均保留执行期公共验证，没有引入校验绕过。

每次50步实际产生51个训练 fetch、102个样本。每个重复另跑本地 Arrow A 控制；比较数据版本/产物、sampler、训练与验证样本轨迹及 loss。计时是短程 smoke/消融，不是稳态整 epoch 吞吐。

## 验证与复现

- Linux完整构建；967项 C++测试、11项Python客户端/profile测试通过。
- 新测试覆盖 owner UID、TTL、预算与退休 lease、失败响应清理、精确源变更、当前权限检查、通道表等价/错误、首批lazy准备，以及超过1,024个顺序结果的释放。
- 执行阶段仍读取所选源 layout：该读取用于源与物理布局校验，不能与被消除的 coverage 构建读取混为一谈。
- 复现脚本：[run_prepared_ablation.py](../../tests/benchmarks/arrow_training/run_prepared_ablation.py)。
- 服务器输出目录：`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/prepared-ablation-20260930`。

## 50-step 墙钟结果

下表为两次重复的均值，单位秒；首次 prepare 已包含。

| 模型 | 缓存 | 原始完整声明 | 仅准备对象复用 | 复用＋通道表 | 最终方案降幅 |
|---|---|---:|---:|---:|---:|
| patchtst | 冷 | 14.492 | 9.002 | 8.902 | 38.6% |
| patchtst | 暖 | 13.998 | 8.039 | 8.276 | 40.9% |
| patchtsmixer | 冷 | 14.691 | 8.924 | 9.099 | 38.1% |
| patchtsmixer | 暖 | 14.023 | 8.208 | 8.042 | 42.7% |

对象复用稳定消除了约5–6秒的重复准备与控制链路工作。通道表规范化相对“仅复用”没有稳定的整轮墙钟优势：两次重复中有正有负，影响仅发生在首次 prepare，不能据此声称再次提升了稳态训练吞吐。它的明确收益是首次声明缩小、初次准备更快，以及减轻帧大小限制。

本地 A 的均值为 PatchTST 2.210秒、PatchTSMixer 2.368秒，IoTDB-FS 仍有明显差距。

## 声明字节与准备耗时

每轮训练51次窗口请求；以下是紧凑UTF-8 JSON字节，不包含4字节帧头或响应。服务端 request_bytes 与客户端逐项核对一致。

| 项目 | 原始完整声明 | 仅复用 | 复用＋通道表 |
|---|---:|---:|---:|
| 一次 prepare | 0 | 730,706 B | 107,320 B |
| 51次窗口执行声明 | 37,273,030 B | 12,022 B | 12,022 B |
| 51次结果 route 释放 | 0 | 20,196 B | 20,196 B |
| 训练期间控制请求合计 | **37,273,030 B** | **762,924 B** | **139,538 B** |

整个 reader 的最终 window_release 在训练和评估结束后，未混入训练期间统计。首次声明缩小 **85.3%**；包含准备与逐结果释放的训练控制请求总量下降 **99.63%**。原先37.27MB是51次累计，现为0.140MB，约为本轮18.12MB有效Float32值的0.77%。返回IPC仍约85.79MB/轮，编码schema和数据量没有改变。服务端首次 prepare 均值由仅复用时的 **0.126秒**降至 **0.070秒**；这部分优化不会在每个 minibatch 重复获得。

优化前51次请求的 prepare_total 累计均值为 **4.766秒**。复用后 snapshot_prepare、coverage_index_prepare、module_prepare 不再出现在 execute profile 中；反复发生的 JSON 解析等 prepare_total 约 **0.0015秒/轮**。首次完整准备另记上述0.070秒，已计入墙钟，没有把它移出计时。prepared ID查找与轻量路由操作仍在 server_total 内。

## 与读取/IPC的关系

三组冷缓存训练完全相同：120次payload读取、5,205个range、109,117,440B、6,660次tile decode；暖缓存均为0新增读取/解码，全部运行0驱逐/重复解码。这不是通过扩大缓存或改变采样、tile尺寸获得的收益。

最终方案客户端 Arrow表→NumPy＋验证仍约 **3.00秒/轮**；服务端window_execute约 **2.28秒/轮**（冷暖混合均值），IPC编码约 **0.57秒/轮**。执行阶段fingerprint计时仍约0.22秒，而完整声明路径为约0.65秒，符合“移走准备期重复计算、保留执行期校验”的设计。这些阶段有嵌套关系，不能全部相加；客户端ipc_decode字段也不等于纯IPC反序列化。下一步若继续优化，应先分析客户端转换和执行组装，不能再把约3秒客户端处理归因于IoTDB范围读取。

## 证据

24次B、4次A运行全部通过轨迹与loss校验；1,224条训练请求profile全部匹配，无记录到的重叠请求。全量原始报告、comparison、profile与测试日志已归档（不含配置凭据）。

- [逐次指标](../results/arrow-prepared-registration-20260930/results.json)
- [两次重复汇总与min/max](../results/arrow-prepared-registration-20260930/summary.json)
- [核验结果与二进制/源码哈希](../results/arrow-prepared-registration-20260930/validation.json)
- [原始证据包](../results/arrow-prepared-registration-20260930/prepared-registration-evidence.tar.gz)

## 最终展开防护与复验

主消融结束后补充了通道表的复制字节预检：模板和全部row引用的channel/字符串按保守预算合计不超过64MiB，先扫描全部引用再展开，防止小JSON通过大标签反复引用放大到GB级内存。原有数量与最终module元数据预算继续有效。新增测试使用128KiB标签×520个引用，在读取任何layout之前拒绝。最终版本完整968项C++测试通过；11项Python测试结果仍适用于未变更的客户端/profile实现。

为明确二进制边界，主消融仍保留原哈希及24次B记录。最终防护版本另跑两模型完整声明与最终方案的50-step冷缓存复验（4次B＋2次新A控制）：

| 模型 | 完整声明 | 复用＋通道表 | 降幅 |
|---|---:|---:|---:|
| patchtst | 14.852 s | 8.930 s | 39.9% |
| patchtsmixer | 14.692 s | 8.871 s | 39.6% |

4次B全部通过，204条训练请求全部匹配，声明字节与120次读取/6,660次解码的冷缓存数据量保持一致。结论与主消融相同；复验不混入前表两次重复均值。

- [最终版本复验指标](../results/arrow-prepared-registration-20260930/final-guard/results.json)
- [最终源码/二进制哈希及核验](../results/arrow-prepared-registration-20260930/final-guard/validation.json)
- [最终复验原始证据](../results/arrow-prepared-registration-20260930/final-guard/prepared-guard-evidence.tar.gz)

最终挂载恢复原配置，profiling关闭；已验证mountpoint正常，daemon PID **559882**。未进行git写操作。
