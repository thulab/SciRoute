# ERA5 D1：50-step 训练与 1／2／4 视图供给实验

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-07，在 `root@192.168.99.49` 的
`/data_01/zzy/iotdb-fs-exp/era5-grib-baseline` 完成 D1 的固定 trace
训练与 loader-only replay。机器可读的 25 组原始摘要、阶段时间、资源采样
及数值一致性核对见结果 JSON（服务器归档：`docs/era5-d1-50step-result-20261007.json`）；
服务器上的逐 step 日志及每秒资源采样保留在
`results/experiments/d1-50step-20261007/<group>/`。这完成了本阶段的
50-step 与多视图**探索性供给测量**，尚非 EG01b 的完整训练 split epoch
或含 F2 的 EG02 完整验收。

## 数据、合同与正确性

D1 使用 2018 年 1 月 UTC 00/06/12/18 时次，20 个通道、2,480 个 GRIB
message。single-levels 为 1,029,987,648 B（496 messages），
pressure-levels 为 4,119,950,592 B（1,984 messages）。两文件已写入独立
IoTDB-FS namespace；精确版本分别为 `1791373952775` 与
`1791374038664`，普通路径和精确版本路径的完整 SHA-256 均与原文件相同。
绑定在服务器的 `results/setup/d1-iotdb-bindings.json`。

内容 snapshot 为
`f8dd5e6a1ad2922f33bf590868c00ab1aee26227f66496b334b1ce059fa0c585`。
V0/V1/V2 各有 train/validation/test 1,312/288/288 个合法样本，V3 为
1,264/240/240。训练统计仅在 train 时段及 patch 空间并集上拟合：East
Asia 12.53 秒，North America 12.80 秒；同区域视图复用同一统计 artifact。
本地 GRIB 与 Q2 精确版本查询对 V0–V3 首样本、V0 验证/测试起点，
以及各 split 末端和 patch 边缘共 **15 个 D1 样本**逐值比较，X/Y/mask
均 bitwise 一致。Z1 的 V0/V2 首样本也与本地 GRIB bitwise 一致。

同一份源 GRIB 转成 Z1 `[time,channel,y,x]` 基础数组耗时 164.49 秒，
其中源 SHA 核验 13.24 秒、转换写入 140.95 秒、容量扫描 10.27 秒；
生成 4,063,541,345 B、178,560 个值 chunk。它是可复用的预处理和额外
存储成本，不并入下面的训练 wall time。

训练使用 RegionalCNN-v1、batch=2、50 steps、FP32、AdamW、固定随机
trace、单进程 DataLoader (`num_workers=0`)。V0/V1/V2/V3 分别对应
East Asia 20 通道 6h、North America 20 通道 6h、East Asia 8 通道 6h、
East Asia 20 通道 24h。每个 view 使用独立进程与 GPU；F1 的各进程私有
解码缓存合计 4 GiB，Q2 的 daemon 共享解码缓存为 4 GiB。成功训练的
**27 个 view-run** 均完成 50 steps；同一 view 在各后端与并发组中的每
step 样本 ID、loss、初始及最终权重 SHA-256 全部相同。V0 首/末 step
loss 为 `0.6548661589622498` / `0.13335290551185608`。

## 训练 wall time 与多视图吞吐

下表包含进程启动、Python/模型初始化、取数和训练；每个 view 读取
100 个样本。J=1 是三轮配对重复的中位数，J=2/4 目前各一轮成功运行。
聚合吞吐用全部成功 view 的样本数除以整组 wall time。

| 后端 | J=1 V0 wall（s，中位数；范围） | J=2 wall（s）/ 聚合样本·s⁻¹ | J=4 wall（s）/ 聚合样本·s⁻¹ |
| --- | ---: | ---: | ---: |
| F1 本地 GRIB | 24.65；24.54–26.42 | 33.80 / 5.92 | 42.97 / 9.31 |
| Z1 派生 Zarr | 11.48；10.89–11.61 | 11.04 / 18.11 | 13.48 / 29.68 |
| Q2 IoTDB-FS typed | 99.62；99.48–100.55 | 113.83 / 1.76 | 133.37 / 3.00 |

同 trace 的纯供给 replay（不做 GPU forward/backward）对应 J=1/2/4
wall time：F1 **22.11/29.67/39.81 秒**，Z1 **9.14/9.30/9.83 秒**，
Q2 **96.36/110.05/129.07 秒**。Q2 的 50-step wall time 与 replay
非常接近，说明本实验主要是数据供给受限；这不代表更大模型仍有同样
比例。按 J=1 三轮中位数，Q2 wall time 为 F1 的 4.04 倍、Z1 的 8.67 倍。
这是一项基线观察，不是 IoTDB-FS 获得加速的证据。

J=4 的 V0 自身训练 loop 为 F1 35.87 秒、Z1 6.68 秒、Q2 126.46 秒；
J=1 第 3 轮分别为 19.47、6.06、94.86 秒。聚合吞吐提高的同时，
单视图可能变慢。J=2/4 只有一轮，且 Q2 的 mount cache、OS page cache
和路由状态不是严格相同的冷态，暂不对并发收益给统计显著性结论。

## 成本分解与资源

J=1 第 3 轮 V0 是 Q2 解码缓存冷重启后的配对测量；下面的阶段均为
该 view 的累计 wall time。嵌套阶段不能相加为总 wall time。

| 阶段或量 | F1 | Z1 | Q2 |
| --- | ---: | ---: | ---: |
| 50-step loop | 19.47 s | 6.06 s | 94.86 s |
| 样本 fetch | 17.80 s | 4.48 s | 92.89 s |
| H2D＋前后向＋优化器 | 0.98 s | 0.95 s | 1.02 s |
| step p50 / p95 | 0.350 / 1.021 s | 0.107 / 0.125 s | 1.887 / 2.221 s |
| 首 batch 就绪（从进程启动） | 4.14 s | 3.44 s | 5.58 s |
| 客户端 PSS 采样峰值 | 5.34 GiB | 1.28 GiB | 1.36 GiB |

F1 的 100 样本触发 2,632 次 GRIB message 解码、3,368 次私有缓存
命中；解码累计 15.68 秒、源读取 1.85 秒、编码源字节 5.47 GB。
Z1 读取约 665 MB 压缩 chunk、24,000 次底层 read，读取累计 4.47 秒。
Q2 发出 600 个 typed 查询，产生 **4.337 GB Arrow IPC**，对应训练值
393.216 MB，约 **11.03 倍**。Q2 客户端累计 socket 提交至结果可用
74.14 秒、IPC mmap/Arrow 读取 10.26 秒、NumPy 转换 8.02 秒。
当前 FUSE bridge 在 socket 回复前会执行授权、查询、GRIB 解码、IPC
编码和发布，因此 `registration_ns` 是**提交至结果可用**，不能解释为
“纯注册”耗时。Q2 daemon 的 PSS 采样峰值在该轮约 7.57 GiB，须与
客户端分开核算；每秒 GPU utilization 采样均值约 0.2%，只是粗粒度
供给受限佐证。`/proc/<pid>/io.read_bytes` 本轮为 0，表示没有观测到该
进程直接物理磁盘读，**不能**据此推断 IoTDB 的磁盘或网络字节为零。

## 故障、界限与下一步

首次 Q2 双视图训练 `q2-j2-r1` 在 V0/V1 第 35/34 step 左右因
`query route registration limit exhausted` 失败，原始失败日志保留。
将 ERA5 专用 mount 的 frontend route 上限由 1,024 调至 8,192，
仅重启该 mount 后重跑 `q2-j2-r2` 成功；这只是 50-step 运行容量，
没有解决长期训练的路由生命周期问题。Q2 第 2/3 轮单视图前也分别
冷重启 ERA5 mount 以清解码缓存；停机审计为 2.928/1.385 秒。

本轮的 Q2 由 Python harness 每个样本提交 6 次 GRIB typed 查询并在
客户端组装窗口；尚未接入原生 GRIB 训练 adapter 的跨样本需求合批、
window IPC 或服务端按视图调度。F2 本地共享解码服务尚未实现，因此
**不能**把 F1 与 Q2 的差异归因于共享缓存，也不能宣称 EG02 的共享
复用目标已验收。完整训练 split 供给 epoch、Q0 无缓存消融、更新与
checkpoint 恢复亦未运行。后续优先减少 Q2 的 600 次查询与 11 倍 IPC
放大，并补齐 F2 后再做公平的共享机制比较。对 Q2 另用相同 trace 的
10-step 冷/暖服务端 profile 拆分了取数、编码、结果发布与目录授权，见
[瓶颈分析](../knowledge/era5-d1-q2-bottleneck-analysis-20261007.md)。
