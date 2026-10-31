# Arrow 压缩、过滤与并发验证结果（2026-09-30）

完成 **144 个实验单元、22,176 次查询、9,504 个同步 burst**，A–E 加 A-read 全部通过精确数值、mask、投影顺序和绝对 index 校验。另有 336 次 ZSTD 四客户端预验证；C++ benchmark 构建及 7 个 Python 测试通过。生产 core/adapter/FUSE 代码没有改动。

本轮证明压缩、residual 过滤和并发共享解码均能正确工作，但它们的性能效果不同：**压缩改善宽投影的冷读，却显著放大小切片的解压需求；过滤目前减少输出而不减少读取；并发提高吞吐但并非线性，宽结果在 E 前端四路时已出现收益停滞。**

[方法、计时边界与复现](../deliverables/arrow-query-extended-experiment.md)。A=本地 mmap 原生 Arrow，A-read=本地普通读取，B=本地 backend 上的生产 query engine，C=IoTDB 上相同 query engine，D=普通 IoTDB-FS 文件加原生 reader，E=实际 socket/IPC/FUSE 前端。所有 cold 均为应用/解码缓存冷 burst，没有清理 OS/IoTDB 缓存；cold 仅一次，不能作为稳定的尾延迟估计。

## 压缩改变了字节量和读取粒度

所有变体保持完整逻辑内容、schema、batch 分割一致，并经过挂载完整回读 hash 校验。文件体积（十进制 MB）：

| 数据集 | none | LZ4 | ZSTD |
|---|---:|---:|---:|
| ETTm1 | 1.956 | 1.119 | 0.975 |
| UCI_Electricity_370 | 207.613 | 93.676 | 82.290 |

Electricity 单客户端 cold burst 完成时间（ms）：

| 用例 | codec | A | A-read | B | C | D | E |
|---|---|---:|---:|---:|---:|---:|---:|
| c1-small | none | 0.34 | 23.64 | 2.90 | 17.84 | 477.57 | 68.16 |
| c1-small | lz4 | 30.44 | 40.83 | 73.05 | 235.74 | 211.90 | 288.79 |
| c1-small | zstd | 45.62 | 52.03 | 86.85 | 218.58 | 231.13 | 306.85 |
| all-tile | none | 14.94 | 22.09 | 67.19 | 1455.82 | 465.42 | 1631.38 |
| all-tile | lz4 | 41.50 | 25.84 | 94.91 | 202.36 | 272.83 | 389.33 |
| all-tile | zstd | 52.66 | 55.58 | 122.46 | 214.50 | 230.50 | 401.98 |

`all-tile` 输出 370 列 × 4,096 点。C 从未压缩 1,455.82 ms 降为 LZ4 202.36 ms / ZSTD 214.50 ms，约 **7.19× / 6.79×**；E 从 1,631.38 ms 降为 389.33 / 401.98 ms，约 **4.19× / 4.06×**。这不是只由文件变小造成的：压缩路径读取的字节反而比未压缩局部投影更多，但范围调用从 370 次变成 1 次。

| Electricity 用例 | codec | payload bytes | payload calls | decoded bytes | decompressions |
|---|---|---:|---:|---:|---:|
| c1-small | none | 16,384 | 1 | 16,384 | 0 |
| c1-small | lz4 | 11,054,787 | 1 | 24,248,320 | 1 |
| c1-small | zstd | 9,700,044 | 1 | 24,248,320 | 1 |
| all-tile | none | 6,062,080 | 370 | 6,062,080 | 0 |
| all-tile | lz4 | 11,054,787 | 1 | 24,248,320 | 1 |
| all-tile | zstd | 9,700,044 | 1 | 24,248,320 | 1 |

单列 128 点只有 512 B 有效 Float32 数据：未压缩取一个 16 KiB tile，压缩却必须解压整个 **24,248,320 B** leaf buffer（同 batch 两行、全部通道），约为有效值字节的 47,360 倍。C cold 因而从 17.84 ms 增到 235.74/218.58 ms。压缩并非所有局部查询的通用加速方式。

上述 calls 是 QuerySource payload 范围接口调用数，不等价于底层全部 Thrift 网络往返数。暖缓存下所有 codec 都没有新的 payload 读取/解压；宽投影 C warm 从 none 42.44 ms 降到 LZ4 32.61 / ZSTD 34.18 ms，但 A 仍是必须保留的强本地基线。

## 过滤正确，但尚不裁剪物理读取

阈值从有变化的通道冻结为 Float32 中位数；ETTm1 和 Electricity 实际保留率分别为：

- ETTm1：2027/4096 = 49.49%。
- UCI_Electricity_370：1824/4096 = 44.53%。

Electricity 同范围、同七列投影的配对比较；warm 为十个 burst 去极值均值，ms：

| codec | 过滤 | 返回行 | C warm | E warm | cold payload bytes | cold calls |
|---|---|---:|---:|---:|---:|---:|
| none | 无谓词 | 4096 | 12.31 | 63.14 | 114,688 | 7 |
| none | 部分保留 | 1824 | 14.39 | 64.55 | 114,688 | 7 |
| none | 全部排除 | 0 | 13.23 | 63.46 | 114,688 | 7 |
| lz4 | 无谓词 | 4096 | 10.64 | 59.01 | 11,054,787 | 1 |
| lz4 | 部分保留 | 1824 | 11.78 | 63.19 | 11,054,787 | 1 |
| lz4 | 全部排除 | 0 | 11.68 | 62.99 | 11,054,787 | 1 |
| zstd | 无谓词 | 4096 | 11.79 | 58.34 | 9,700,044 | 1 |
| zstd | 部分保留 | 1824 | 11.79 | 60.97 | 9,700,044 | 1 |
| zstd | 全部排除 | 0 | 12.15 | 60.86 | 9,700,044 | 1 |

即使结果为零行，也读取并解码相同候选数据，符合当前 residual WHERE 契约。hidden-filter 用例只输出 child 0，却以另一 child 过滤，确认过滤列绑定存在、输出不泄露该列，且 index 保留准确源位置。空结果保留列 schema。

这与 GRIB 的 message 属性索引过滤不同：本轮不能支持“Arrow 值过滤也像 GRIB 属性过滤一样提前减少 I/O”的论文表述。

## 并发：共享加载有效，吞吐有上限

下面是 Electricity 的暖 burst QPS（弱扩展，每波分别完成 1/2/4 请求，并非固定工作量强扩展；不代表持续饱和吞吐）：

| 场景 | codec | 路径 | 1 client | 2 clients | 4 clients | 4/1 QPS 比 |
|---|---|---|---:|---:|---:|---:|
| c1-small | none | C | 81.49 | 131.99 | 192.00 | 2.36× |
| c1-small | none | E | 16.59 | 27.68 | 38.49 | 2.32× |
| c1-small | zstd | C | 85.44 | 124.71 | 169.13 | 1.98× |
| c1-small | zstd | E | 16.74 | 29.18 | 35.90 | 2.14× |
| all-tile | none | C | 23.56 | 34.79 | 60.63 | 2.57× |
| all-tile | none | E | 6.42 | 9.69 | 9.68 | 1.51× |
| all-tile | zstd | C | 29.25 | 40.21 | 62.45 | 2.13× |
| all-tile | zstd | E | 6.88 | 10.16 | 9.62 | 1.40× |
| c7-disjoint-batches | none | C | 79.91 | 122.04 | 204.09 | 2.55× |
| c7-disjoint-batches | none | E | 16.29 | 27.62 | 41.51 | 2.55× |
| c7-disjoint-batches | zstd | C | 90.18 | 129.90 | 170.22 | 1.89× |
| c7-disjoint-batches | zstd | E | 17.70 | 26.89 | 38.45 | 2.17× |

- 共享同一批数据时，B/C 的 cold bytes、payload calls、decode count、decompressions 在 1/2/4 客户端下保持相同；不同 batch 时，calls/decode 数随客户端数线性增加。整波统计验证了共享加载，而非重复解码后恰好得到相同结果。
- ZSTD 单列共享 cold：4 客户端合计仍只有 1 次读取/解压；不同 batch 的四客户端则为 4 次。这不意味着四个独立 batch 可以只解压一次。
- E 的所有四客户端用例在服务器 profile 中观察到 4 个重叠请求，确实走了并发执行。E 的重叠 runtime_delta 为全局差值，**没有相加冒充总量**；精确并发物理工作量来自 B/C 的波次计数。
- 未压缩 all-tile 的 E QPS 在 2/4 客户端为 9.69/9.68，p95 从约 230.70 ms 升至 503.24 ms；ZSTD 对应 QPS 为 10.16/9.62。增加并发没有继续改善宽结果交付。其原因可能同时包含客户端 Python/GIL、逐列转换、编码/发布及授权，不能由这些总时延直接归因于单一 FUSE 层。
- B/C/E 所有暖查询的 payload 范围读取和解压计数均为零；A/A-read/D 的暖查询均命中原生 batch cache。并发收益和限制因此也包含元数据验证、缓存访问、CPU 物化及交付阶段，而不仅是数据库读取。

## 证据、恢复与可复现范围

本轮使用 ETTm1 batch 0、Electricity batch 2；后者为前七列首个有变化的 batch。不同 batch 用例分别采用 ETTm1 0–3、Electricity 2–5。新路径比上一轮多一级目录，增加 E 的授权路径成本。因此不要把上一轮 batch 0 或旧目录的绝对耗时与本轮直接相减。

无谓词七列 4,096 点的匹配对照作为独立执行批次补齐，其余七类用例在主批次执行；最终合并后对全部八类用例重新做了完整覆盖及跨路径/codec 校验。两批次配置和二进制相同，但执行顺序不是随机化设计。本轮每 cell 一个 cold、十个 warm，只作一次完整矩阵验证；尚未给出跨运行置信区间。

- 完整矩阵：[CSV](../../tests/benchmarks/arrow_training/manifests/arrow-query-extended-20261001/latency-matrix.csv)、[JSON 汇总](../../tests/benchmarks/arrow_training/manifests/arrow-query-extended-20261001/summary.json)。
- [原始 JSON 证据包](../../tests/benchmarks/arrow_training/manifests/arrow-query-extended-20261001/evidence-json.tar.gz)：全部请求/波次、每次 E server profile、setup、cases、oracle、冻结 source manifest 和恢复记录。
- [代码/二进制与证据 hash](../../tests/benchmarks/arrow_training/manifests/arrow-query-extended-20261001/provenance.json)。
- 服务器归档目录：`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/arrow-query-extended-combined-20261001`；目录后缀为 run label，实际执行日期为 2026-09-30 UTC。
- 原配置已恢复，profile 关闭，挂载检查通过；最终 daemon PID `762995`。
- 配置凭据及 daemon 日志未归档。原始实验产物放在仓库现有忽略的 manifests 目录；文档和脚本可正常评审，若提交证据需按仓库实验产物策略处理。

此次验证支持将“结构感知局部访问、可复用解码和并发共享”作为跨格式能力；速度结论应同时给出选择率、压缩 buffer 粒度和输出宽度条件。当前证据不足以声称 Arrow 压缩总是更快、值过滤能减少源数据读取，或四路前端必然优于两路。
