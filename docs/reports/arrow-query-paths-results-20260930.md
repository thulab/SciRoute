# Arrow 非训练 A–E 实验结果（2026-09-30）

已完成 20 个用例、2 个 campaign、6 条路径（A–E 加 A-read），共 **2,640 次查询**。全部值、null mask、index 与原生 PyArrow oracle 精确一致；B/C/E 冷读的 payload bytes、ranges、calls、decode count 完全一致，所有暖读 payload calls 为 0。生产代码未修改。

**结论：局部取数机制可以推广到 Arrow，但 GRIB 论文中的端到端速度优势不能不加条件地推广。** 少列冷读相对普通 IoTDB-FS 文件读取有优势；370 列冷读受到串行范围调用限制，反而更慢；暖缓存下本地原生 Arrow 明显领先，普通 query 路径仍存在可观开销，因此差距不只来自训练优化器。

方法与复现见 [实验说明](../deliverables/arrow-query-paths-experiment.md)。A 是本地 mmap；A-read 是相同原生 reader 的普通读取；B 是本地 backend 上的生产查询引擎；C 是 IoTDB 上的同一查询引擎；D 是普通挂载文件加原生 reader；E 是实际 socket→typed IPC→FUSE。这里“多列”为同一个 `target` 字段中多个 child path 对应的值列，没有测试任意多个顶层字段的联合绑定。

## 暖读完整矩阵

单位 ms。每 campaign 10 次去掉最高/最低取均值，表中再对两个 campaign 的均值取平均；没有把两个 campaign 的 20 个样本合并后去极值。暖读仍重新 prepare/execute，E 重新注册；A/A-read/D 命中客户端 record batch，D 此时不发生源文件 FUSE 数据读取。

| 数据集 | 用例 | A | A-read | B | C | D | E |
|---|---|---:|---:|---:|---:|---:|---:|
| ETTm1 | c1-boundary | 0.0010 | 0.0011 | 0.2982 | 9.7526 | 0.0025 | 45.4844 |
| ETTm1 | c1-full-row | 0.0324 | 0.0319 | 0.3581 | 9.8198 | 0.0339 | 46.9729 |
| ETTm1 | c1-small | 0.0010 | 0.0011 | 0.2940 | 10.2274 | 0.0011 | 44.5835 |
| ETTm1 | c1-tile | 0.0162 | 0.0162 | 0.2663 | 9.8005 | 0.0168 | 45.3372 |
| ETTm1 | c7-boundary | 0.0047 | 0.0049 | 0.5072 | 9.8960 | 0.0091 | 47.6521 |
| ETTm1 | c7-full-row | 0.2719 | 0.2081 | 1.1964 | 11.0320 | 0.2102 | 50.6294 |
| ETTm1 | c7-small | 0.0047 | 0.0049 | 0.3841 | 10.2660 | 0.0050 | 47.3529 |
| ETTm1 | c7-tile | 0.1163 | 0.1039 | 0.7074 | 10.1097 | 0.1063 | 49.2813 |
| Electricity | c1-boundary | 0.0019 | 0.0011 | 0.8363 | 12.7444 | 0.0011 | 50.7297 |
| Electricity | c1-full-row | 0.0317 | 0.0331 | 0.9104 | 11.3431 | 0.0333 | 50.4416 |
| Electricity | c1-small | 0.0010 | 0.0011 | 0.8474 | 11.7171 | 0.0011 | 47.5654 |
| Electricity | c1-tile | 0.0160 | 0.0166 | 0.8106 | 11.0287 | 0.0178 | 49.0497 |
| Electricity | c370-boundary | 0.2464 | 0.2843 | 13.5213 | 26.4978 | 0.2530 | 84.0691 |
| Electricity | c370-full-row | 21.9335 | 16.9525 | 67.6662 | 76.4186 | 18.1786 | 239.5422 |
| Electricity | c370-small | 0.2544 | 0.2710 | 8.7716 | 23.2156 | 0.2620 | 75.6632 |
| Electricity | c370-tile | 9.7822 | 6.0872 | 27.2172 | 40.5205 | 5.8778 | 146.9948 |
| Electricity | c7-boundary | 0.0048 | 0.0050 | 0.9781 | 11.2971 | 0.0051 | 49.2578 |
| Electricity | c7-full-row | 0.2173 | 0.2114 | 1.5719 | 12.3353 | 0.2098 | 50.9316 |
| Electricity | c7-small | 0.0048 | 0.0049 | 0.9102 | 11.0826 | 0.0052 | 48.7683 |
| Electricity | c7-tile | 0.1049 | 0.1108 | 1.1602 | 11.4078 | 0.1069 | 49.3369 |

## 应用缓存冷读与选择率

以下为 Electricity 两次冷读原值，单位 ms；没有清理 OS/数据库缓存。A–D 只重置 reader/decoded cache；E 每用例重新启动 daemon。因此这些数值不是存储冷启动或可靠的尾延迟估计。

| 用例 | A | A-read | B | C | D | E |
|---|---:|---:|---:|---:|---:|---:|
| c1-small | 0.17 / 0.03 | 21.09 / 8.60 | 2.71 / 1.22 | 16.63 / 13.94 | 429.29 / 413.61 | 70.66 / 55.07 |
| c7-boundary | 0.03 / 0.04 | 7.19 / 8.61 | 2.07 / 2.97 | 58.97 / 59.80 | 658.29 / 413.51 | 107.87 / 110.88 |
| c370-small | 1.38 / 0.33 | 7.63 / 9.43 | 37.83 / 34.82 | 1337.61 / 1379.28 | 405.03 / 412.10 | 1519.49 / 1501.99 |
| c370-full-row | 21.42 / 27.56 | 24.88 / 46.51 | 119.57 / 136.06 | 2714.86 / 2735.50 | 441.00 / 466.86 | 3088.96 / 3008.75 |

- 单列 128 值：C 约 15.28 ms、E 约 62.87 ms，D 约 421.45 ms；相对普通挂载读取分别约 27.6×、6.7×。但 A mmap 只需 0.027–0.171 ms，不能用 D 代表最强本地基线。
- 370 列各 128 值：C 约 1.36 s、E 约 1.51 s，D 约 409 ms；完整列组触发大量独立范围请求，即使请求字节更少，也没有延迟优势。
- 本地 mmap 不等于完整读取 record batch：Arrow 可以引用映射缓冲区，只触及输出需要的页。native message body 大小不能被当作 A 的实际磁盘读取量。

## 相同物理需求，不同调用成本

Electricity 首个 native batch 的逻辑 body 是 24,251,512 B（两行）。B/C/E 的 payload 计数一致：

| 用例 | 有效 Float32 值字节 | payload 请求字节 | calls / tiles | payload 放大 |
|---|---:|---:|---:|---:|
| c1-small | 512 | 16,384 | 1 | 32× |
| c7-boundary | 3,584 | 229,376 | 14 | 64× |
| c370-small | 189,440 | 6,062,080 | 370 | 32× |
| c370-full-row | 12,124,160 | 12,124,160 | 740 | 1× |

370 列 small 的 C 第一轮 cold payload read 总时间为 1,238 ms，占 1,338 ms wall 的约 93%；740 tiles 的 full-row 为 2,495/2,715 ms，约 92%。这与 GRIB message 一次定位读取的调用结构不同：物理数据确实被裁小，但独立 tile 请求被串行放大。没有使用训练 adapter 的合批读取来掩盖普通 Arrow 查询行为。

暖读全部 payload 命中缓存，仍有以下成本（Electricity，两个 campaign 的分项去极值均值再平均；分项分别裁剪，不能严格相加重建 wall）：

| 用例 | B prepare | B execute | C prepare | C execute | E 注册全程 | E 打开/读取 IPC | E 数组复制 |
|---|---:|---:|---:|---:|---:|---:|---:|
| c1-small | 0.578 | 0.248 | 7.609 | 4.052 | 38.637 | 8.504 | 0.220 |
| c370-small | 2.416 | 6.059 | 9.492 | 13.370 | 52.547 | 9.737 | 12.890 |
| c370-full-row | 2.637 | 42.175 | 10.308 | 43.457 | 181.904 | 12.195 | 43.088 |

1. **固定元数据/规划成本仍在。** 每次 B/C 查询读取两次完整 layout（规划与 execute 验证），Electricity 共 135,920 B，ETTm1 共 18,696 B；即使 payload 为 0，C 仍约 10–13 ms 起步。B 同样会处理这些布局，但没有数据库传输，单列 small 约 0.85 ms。
2. **E 的额外时间主要不能归结为 IPC 编码。** 单列 small 的服务端 typed_prepare 约 13.00 ms、query_authorize 约 7.50 ms，typed_encode 只有约 0.158 ms；后者含懒 cursor.next，但 execute 初始布局验证发生在该 timer 之外。注册前还进行 catalog scan/授权，结果打开也触发授权。`authorize_typed_sources_as` 即便 root 也会加载精确源 inode 和父目录，不能假定 root 注册没有数据库开销。
3. **宽结果仍有计算和发布成本。** 370 列 small：B execute 约 6.06 ms、E 数组转换约 12.89 ms；full-row 的 E typed_encode（含 cursor 执行）约 104.21 ms，result_publish 约 29.38 ms，输出 IPC 为 12,313,368 B。单列 small IPC 只有 3,248 B；这两个场景不能采用同一个固定开销解释。
4. **不能按 timer 名称重复求和。** `publish_route` 包含 typed_prepare、授权、execute、编码和发布，属于父阶段；它不是单独的 result-store 发布成本。E−C 还含 live catalog、授权、语言/客户端、IPC、FUSE 等不同因素，不能全部标为 FUSE。

## 初始化、证据与限制

A–D setup 均包含建立一次 IoTDB session、读取并核对 frozen manifest/layout；A/A-read 的这个 DB 依赖是实验身份核对成本，不是原生 Arrow 的必需成本。native 索引只扫 framing metadata 并 seek 跳过 body。ETTm1 的 D setup 为 53.8/57.6 ms，Electricity 为 173.7/191.7 ms；其余路径 setup 约 13–17 ms。setup 与 cache reset 单独保留在原始 JSONL，不计入表内重复查询。

此次单客户端/root，只有无压缩 List<List<Float32>> 公共数据。两轮之间部分大结果的本地复制时间有波动，例如 A-read full-row 为 11.95/21.96 ms；保留两个 campaign 和全部原值，不将两轮平均解释为严格置信区间。E 始终在各 campaign 的 A–D 之后运行；只在 A–D 内反转路径顺序，不能视为完整随机化设计。

- 环境：192.168.99.49，Linux 6.8.0-139，32 vCPU Xeon Gold 6130 / QEMU，`/data_01` 为 ext4；Python 3.12.3、Arrow/PyArrow 21.0.0、NumPy 1.26.4。
- 验证：C++ release benchmark target 构建通过；4 个 Python 契约/汇总测试通过；2,640 次现场精确比较及跨路径工作量校验通过。没有改动 core/adapter/FUSE 生产逻辑，因此未重跑整套生产单元测试。
- [机器可读汇总](../../tests/benchmarks/arrow_training/manifests/arrow-query-paths-20260930/summary.json)。
- [完整 JSON/JSONL 原始证据压缩包](../../tests/benchmarks/arrow_training/manifests/arrow-query-paths-20260930/evidence-json.tar.gz)：包含原始 A–D、E 服务 profile、oracle、全部 measurements 和恢复记录；不包含配置凭据及 daemon 日志。
- 服务器输出目录：`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/arrow-query-paths-20260930`。
- 最终已恢复原配置，profile 关闭，挂载验证成功，daemon PID `708805`。

论文可据此增加一个有边界的结论：Arrow 同样支持精确结构定位和局部物化，其收益依赖选择率、原生读取能力和物理范围的数量；GRIB 中避免扫描/消息解码的优势，不自动等价为所有 Arrow 查询优于本地 mmap。下一步若优化普通 Arrow 路径，应分别处理重复元数据验证与多范围串行访问，不需要先引入训练特有机制。
