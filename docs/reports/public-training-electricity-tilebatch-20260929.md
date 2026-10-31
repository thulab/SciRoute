# Electricity Arrow 训练合批：50-step A/B 复验

2026-09-29 在 `root@192.168.99.49` 的 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline` 运行。仅使用公开 Electricity 370；IoTDB-FS 数据源固定为版本 `1790670177863`，源 SHA-256 为 `536c3c72afb04f09daadf860ef021719bb4f90515c10a6710bd35d78de18fa0f`。本地 Arrow（A）与 IoTDB-FS 训练窗口（B）继续使用相同的窗口清单、训练区间统计、`L=96,H=24,S=1`、batch size 2、seed `20260929`、GPU 0、Trainer `RandomSampler`、50 个优化 step 和 4 个顺序验证窗口。未人工构造连续窗口或改变采样顺序。

| 模型 | A 训练墙钟 | B 训练墙钟 | 此前 B 训练墙钟 | 相对此前 B | A/B 样本及 loss |
| --- | ---: | ---: | ---: | ---: | --- |
| PatchTST | 2.110 s | 24.327 s | 148.502 s | 6.10× | 训练 102 个、验证 4 个；轨迹与 loss 完全一致 |
| PatchTSMixer | 2.157 s | 24.514 s | 147.969 s | 6.04× | 训练 102 个、验证 4 个；轨迹与 loss 完全一致 |

两次新实验的训练 sample trace SHA-256 均为 `c86427f37c1bc3d781aa1d8bfc1acd46b38ba554164bd8ee12d366f0fd00d188`，验证 trace 为 `163e5879b2e86b9cbc247328b9782b6be89fb4fe5851dd4bc1814112a9b08f75`。PatchTST 的训练/验证 loss 分别为 `0.890525131225586` / `157436.671875`；PatchTSMixer 为 `0.9005426788330078` / `157291.34375`，每项 A 与 B 相同。训练墙钟包含模型、数据获取和客户端解码，不能与服务端分段时间相加。

对 PatchTST 的 51 个训练请求，旧逐 tile 路径与新合批路径在同一 profiling 口径下比较如下。新 PatchTSMixer 的 51 个请求产生相同的物理调用、范围数和返回字节数；两组的服务端 profile 均为 **51/51 可归因、0 排除**。

| 服务端训练阶段指标 | 旧逐 tile | 新训练合批 | 变化 |
| --- | ---: | ---: | ---: |
| payload 调用/SQL 请求 | 35,671 | 571 | 减少 98.40%，约 62.5× |
| 请求内物理范围总数 | 35,671 | 30,140 | 减少 15.50% |
| 返回 payload 字节 | 584,433,664 | 555,237,376 | 减少 5.00% |
| 片段解码次数 | 35,671 | 33,889 | 减少 5.00% |
| 重复解码计数 | 8,263 | 5,623 | 减少 31.95% |
| payload 读取累计耗时 | 121.915 s | 9.473 s | 约 12.9× |
| 服务端请求墙钟总和 | 142.034 s | 19.132 s | 约 7.4× |
| IPC 编码累计耗时 | 0.672 s | 0.690 s | 近似不变 |

新路径平均每个 payload 调用承载约 52.8 个范围。训练组装器在一个有界窗口组中按规范 tile key 去重，跨窗口合并同 tile 需求；缓存只认领缺失 key；Arrow provider 把直接相接或重叠的字节范围精确合并，再把不连续范围放入一次 IoTDB 多 `READ_OBJECT` 请求。每波最多 64 个范围、8 MiB payload，超出时按波次取数并释放 lease；普通 Arrow typed/GRIB 仍走原单片段路径。真实 IoTDB 已验证一条 SQL 返回多个不连续范围，完整 CTest **952/952** 通过。

这里最显著的收益是**物理调用合批**，不是大幅减少 tile 解码：标准随机采样的两个训练窗口通常相距很远，而且默认 1,024 条 cache entry 仍频繁驱逐 Electricity 的 370 通道 tile。新路径只使返回字节与片段解码各减少约 5%；B 的训练墙钟仍约为 A 的 11.5 倍。缓存条目容量、跨请求预取/重排和注册声明复用需分别消融，不能归功于本次合批，也不应为得到好看的数据而改变标准 sampler。此前 B 墙钟来自[同配置 50-step smoke](public-training-ab-long50-20260929.md)，并非交错 A/B/B/A 稳态测量；本次仍是短训验收，不代表完整 epoch 吞吐。

原始 comparison、客户端报告、两组逐请求服务端 profile 与 JSONL 保存在服务器的 `public-arrow-baseline/manifests/UCI_Electricity_370-{patchtst,patchtsmixer}-tilebatch50-20260929-*.json` 和 `runtime/logs/electricity-tilebatch-query-profile-20260929.jsonl`。打包件为 `tmp/electricity-tilebatch50-results-20260929.tar.gz`，SHA-256 `ef282c071f317ff83d240f8f50be43cb6cd905994650a66154ec78f933a54bb3`。复验后已关闭 profile 并恢复同一新构建的挂载。
