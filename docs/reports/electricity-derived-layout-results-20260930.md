# Electricity 派生布局：时间分块有效，生产训练链路尚未受益

已对完整 Electricity 构建并摄取18个独立 Arrow 派生文件：channel-major 与 step-major、128/512/2048/8192点、none/LZ4的16个初始候选，以及step-major/LZ4的64/256点两个调参候选。原始文件、版本、窗口语义和标准化统计未修改。

结论有两个层次：**同一实验读取路径下，时间分块显著降低冷读取和解码需求，step-major降低宽窗口复制成本；但仅把现有生产训练查询改指向分块文件，没有端到端收益，细分块还触发现有声明容量限制。** 实验读取器不是已经接入生产的A–E前端，不能混淆这两组结果。

[方法与边界](../deliverables/electricity-derived-layout-experiment.md)。所有回放使用原50-step训练的51次请求/102个窗口，L96/H24。初始矩阵与调参各做两次独立冷启动及每次十遍暖轨迹，逐请求验证全部有效Float32位模式与mask。冷仅为应用缓存冷，未清理OS/IoTDB缓存。

## 相同读取器下的布局收益

全370列，IoTDB direct range实验读取器，每条完整轨迹耗时为51次请求之和：

| 布局 | 块长 | 压缩 | 冷读秒 | 暖读秒 | 冷读取MB | 范围API调用 | 解码块数 |
|---|---:|---|---:|---:|---:|---:|---:|
| channel-major | 8192 | none | 1.501 | 0.0555 | 109.14 | 18 | 9 |
| step-major | 8192 | none | 1.451 | 0.0058 | 109.12 | 18 | 9 |
| channel-major | 128 | none | 0.615 | 0.0145 | 31.43 | 50 | 164 |
| step-major | 128 | none | 0.571 | 0.0065 | 31.18 | 50 | 164 |
| channel-major | 128 | LZ4 | 0.647 | 0.0141 | 17.32 | 50 | 164 |
| step-major | 128 | LZ4 | 0.570 | 0.0067 | 19.95 | 50 | 164 |
| step-major | 64 | LZ4 | 0.522 | 0.0060 | 15.48 | 51 | 254 |
| step-major | 256 | LZ4 | 0.704 | 0.0056 | 27.89 | 49 | 117 |

MB为十进制。调用是backend范围API，不是全部Thrift往返。64/256调参属于后续独立执行批次，不能从两次冷样本判断小幅差异的统计显著性。

- **分块收益**：相同step-major、相同none压缩，8192→128点使冷读取1.451→0.571秒，约2.54倍；传输109.12→31.18MB，约下降71.4%。尽管调用从18增到50，字节和解码量下降仍占优势。
- **转置收益**：8192点暖缓存时，channel-major→step-major使0.0555→0.0058秒。两者均零读取，主要反映连续窗口复制与strided gather的不同成本。128点的暖差距较小，0.0145→0.0065秒。
- **压缩并非总能进一步加速**：128点step-major的none/LZ4冷耗时几乎相同，虽然LZ4减少约36%字节。通道优先更易压缩，但时间优先更便于组装；需同时考虑CPU和I/O。
- **第二轮调参**：64点降低到15.48MB，但需254次块解码。当前短轨迹下64/128均值得保留，未证明64是全epoch或多客户端最优点。

8,192点对照仍是派生的一行/batch文件；原始文件是两行/batch，二者不可混称同一个物理布局。原始本地reader另列，其逐通道数组转换较慢，不能把替换该reader的收益全部称为IoTDB存储收益。

## 与相同布局的普通FS比较

128点step-major/LZ4，同一个decoder/cache/assembler：

| 存储路径 | 冷轨迹秒 | 暖轨迹说明 |
|---|---:|---|
| 本地mmap | 0.172 | 全部命中相同客户端数组缓存 |
| 本地普通read | 0.135 | 同上 |
| IoTDB直接范围请求 | 0.570 | 同上，无新增payload读取 |
| 普通IoTDB-FS挂载read | 1.549 | 同上 |

mmap/read两次冷样本存在波动，不能将二者差异推广为普遍结论。IoTDB direct在冷读上仍慢于相同布局本地FS；暖缓存相近来自同一客户端缓存，不是数据库比本地内存更快。

前7列查询也必须读取派生块中的370列，128点step-major/LZ4的IoTDB冷耗时约0.543秒，本地同布局约0.156秒；原始本地reader仅约0.030秒。全通道派生布局不适合替代所有窄投影路径，后续需变量分组或保留原始访问路径。

## 现有生产typed训练链路：仍然变慢

这里使用现有TrainingQueryModule、prepared_compact、四路读取、公共decoded cache、标准化、授权和完整窗口IPC/FUSE前端。下列为独立的一遍冷/暖轨迹验收，不能与实验direct读取器按层直接相减。

| 实际生产查询的数据布局 | 冷轨迹秒 | 暖轨迹秒 | 冷payload MB | 冷payload调用 |
|---|---:|---:|---:|---:|
| 原始8192点行、none | 4.971 | 4.190 | 109.12 | 120 |
| 2048点channel-major/LZ4 | 5.308 | 4.596 | 44.58 | 34 |
| 512点channel-major/LZ4 | 7.380 | 6.526 | 31.48 | 85 |
| 128点channel-major/LZ4 | 注册失败 | — | — | — |

128点产生1096行、405520个通道声明，协议展开阶段拒绝：`expanded channel bytes exceed 64 MiB`。未放宽生产限制掩盖问题。

profile显示细分块增加了原查询模型的准备和逐请求成本：

| 暖轨迹阶段，秒 | 原始 | 2048点 | 512点 |
|---|---:|---:|---:|
| 首次prepare，客户端计时 | 0.074 | 0.182 | 0.642 |
| planner创建，51次合计 | 0.091 | 0.246 | 0.772 |
| prefetch，51次合计 | 0.566 | 0.623 | 1.466 |
| query授权，51次合计 | 0.456 | 0.618 | 0.629 |
| IPC编码，51次合计 | 0.647 | 0.500 | 0.470 |

以上阶段并非都相互独立，不应再与父级总耗时相加。三个暖轨迹的payload读取均为零。代码仍每次创建planner的序列/行结构，并在assembler打开source时读取、解析完整layout；细分块使这部分元数据放大。派生路径比原路径多一级目录，也增加授权工作，不能把全部差值归因于布局。

每条轨迹IPC从原始85.79MB增加到2048点的88.25MB、512点的90.21MB，更多跨块provenance仍进入结果。生产压缩路径每个缺失buffer分别读取，尚未实现实验读取器的整组多范围message合批。

## 50-step训练验证

保持原PatchTST模型、HF RandomSampler、seed20260929、batch2、L96/H24、GPU0和标准化统计，运行两次重复，共14次训练（2次原始本地对照＋12次派生路径）。所有派生运行的训练/验证sample轨迹与loss均通过既有1e-6比较；观察到的train loss全部为0.890525131225586。以下为两次均值：

| 路径 | 冷训练秒 | 暖训练秒 | 冷/暖训练fetch秒 |
|---|---:|---:|---:|
| 原始本地FS＋既有reader | 2.236 | — | 1.085 / — |
| 本地FS＋128点step-major/LZ4实验reader | 1.263 | 1.184 | 0.170 / 0.027 |
| IoTDB＋128点step-major/LZ4实验reader | 1.770 | 1.094 | 0.601 / 0.027 |
| IoTDB＋8192点step-major/none实验reader | 2.723 | 1.100 | 1.606 / 0.027 |

冷训练8192/none→128/LZ4约下降35.0%，这里同时改变分块和压缩，单独的布局效应由前面的同codec回放隔离。完全暖缓存两种IoTDB布局的fetch都约0.027秒：小分块没有继续改善模型计算。实验IoTDB暖wall略低于同布局本地FS，但fetch相同且仅两次重复，不能声称IoTDB优于本地内存路径。

这组prototype可以快于原始本地reader，因为它也替换了逐通道转换与窗口组装；公平的同布局本地FS冷训练仍快于IoTDB。它没有生产typed前端的逐请求权限、调度、结果IPC和跨进程共享缓存。reader setup计时单列，暖运行先按精确训练轨迹预填充，预填充不进入Trainer计时；初始化与预热不得在完整启动成本中忽略。本轮只覆盖一个模型的50-step短程验证，不代表完整epoch或DDP吞吐。

[训练逐次指标](../results/electricity-derived-20260930/training-summary.json)。

## 构建成本与适用边界

128点step-major/LZ4文件162.62MB，约为原207.61MB的78.3%；原始文件保留，因此这是额外存储。一次源解码约2.45秒、构建/写盘/fsync约1.88秒、完整逻辑及hash校验约2.82秒；摄取约3.70秒、挂载完整回读校验约2.92秒，另有源hash验证约0.49秒。源解码在多个候选构建中共享，不能重复相加。构建阶段未测峰值RSS，不宣称这是流式有界内存builder。

派生价值最明确的是有限时间覆盖的冷随机宽窗口、重叠窗口与可复用物理块。相同step-major大块和小块在完全暖缓存时均约毫秒级，完整epoch读遍所有训练数据后，小块的字节节省也会收敛。不能假设每个epoch都重复获得一次冷短轨迹的倍率。

下一步若接入生产，应首先把通道表与时间覆盖分离存储、预编译并复用planner/layout，并让一个物理训练块一次服务所有变量；真正step-major需要独立训练物理binding。仅增加更多小Arrow batch，会放大当前逐行逐通道元数据结构。跨进程公共缓存、取消/预算/调度和权限生命周期仍需生产实现，实验socketpair读取器没有替代这些契约。

## 证据

- [18个候选中的初始16个artifact身份及构建指标](../results/electricity-derived-20260930/artifacts.json)
- [初始矩阵汇总](../results/electricity-derived-20260930/replay-summary.json)
- [64/256点后续调参](../results/electricity-derived-20260930/tuning-summary.json)
- [生产typed逐请求客户端指标](../results/electricity-derived-20260930/typed-replay.json)
- [生产typed服务端阶段与范围指标](../results/electricity-derived-20260930/typed-stages.json)

服务器实验根目录为 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/`。派生目录为 `electricity-derived-20260930` 和 `electricity-derived-tuning-20260930`，挂载内目录 `/electricity-derived-v1/`；保留原始source版本1790670177863。另一个早期构建目录 `electricity-derived-v1` 使用了错误的来源路径标签，已排除，未作为本轮实验manifest输入。

验证完成后恢复原配置并关闭profiling，挂载检查通过，daemon PID810203；生产二进制SHA256仍为`750b320c33b65a2704b761f7a52757f195c06e2dec0b92a8e6eb2cab7fa7c670`。C++范围桥接目标编译成功，8项布局/读取器测试与3项既有训练比较测试通过。

[完整白名单证据包](../results/electricity-derived-20260930/electricity-derived-evidence.tar.gz)包含86个文件：全部原始回放、typed profile、训练报告/比较、artifact索引和实验源码；不含配置凭据、daemon/bridge日志或大体积Arrow数据。包SHA256：`220b1793011c47705594c2d5ed9fbc105d0afc168bf675547de48310c9c26fba`。[逐文件与二进制哈希](../results/electricity-derived-20260930/electricity-derived-provenance.json)。
