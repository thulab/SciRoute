# HDF5 跨 batch chunk 复用（2026-10-06）

`Hdf5SlabIterator` 按行数和输出字节划分矩形，不考虑 chunk 边界。GEOS `/T` 的 91×144 空间 chunk 与默认 65,536 行 batch 不对齐：单层全经度 1152 点时，完整 91 行 chunk 带有 104,832 个值，不能放进一个 batch。旧 Provider 在 `reserve_output` 和每次 `load` 清除全部 fragment lease，随后按输出行顺序读取，导致同一个 chunk 在相邻行或 batch 中重复访问。缓存命中时也有额外 lookup 和拷贝路径；被驱逐时会重新读取、解码。

现在 chunked dataset 按物理 chunk 顺序遍历一次，将每个 chunk 与当前 slab 的交集散写到现有的行主序输出缓冲区，再进行原有的科学值转换和坐标生成。batch 的行数／字节上限和 IPC 输出形状不变。Provider 跨 batch 只保留与下一个 slab 相交、且与输出及工作内存合计不超过 `max_buffer_bytes` 的 lease；读新 chunk 前再按预算驱逐。预取游标跳过已经组装的保留 chunk，使后续缺失 chunk 仍能形成合批读波次。连续存储的 tile 路径保持原有行复制方式。

回归测试用可验证的二维原生值，强制 runtime decoded cache 仅保留 1 个 entry、每 batch 2 行。4 行共享同一 chunk 带时，2 个 chunk 的实际读取／解码从 8 次降至 2 次；单行横跨 65 个 chunk 时从 260 次降至 65 次。另有跨 chunk 带预取及 128 B 输出／工作内存预算测试，分别验证读波次和有界 lease 淘汰。

公开 GEOS `/T` 的[请求](../hdf5-chunk-batch-results/D-wide-chunk.json)取一个压力层、纬度 `[200,400)`、全经度 `[0,1152)`，共 230,400 个值，默认上限下分为多个 batch。修改前的[原始计数](../hdf5-chunk-batch-results/D-wide-pre-chunk.jsonl)与修改后的[原始计数](../hdf5-chunk-batch-results/D-wide-chunk-major-aligned.jsonl)如下；两轮结果的行数、mask 和 FNV64 值摘要相同。

| 指标 | 旧冷轮 | 新冷轮 | 旧暖轮 | 新暖轮 |
| --- | ---: | ---: | ---: | ---: |
| 实际 chunk 解码 | 24 | 24 | 0 | 0 |
| decoded cache 命中 | 24 | **0** | 48 | **24** |
| payload 范围 | 4 | 4 | 0 | 0 |
| backend payload 调用 | 3 | 3 | 0 | 0 |
| 查询耗时 | 55.855 ms | 53.329 ms | 8.307 ms | 7.125 ms |

此公开负载的 24 个 chunk 都放得进 256 MiB 共享缓存，故修改前已经没有重复物理解码；收益表现为消除 batch 内重复 lookup，未减少唯一 chunk 数或后端 I/O。两版没有交错计时，耗时只作观察，不视作可归因的加速比。首次实现曾把冷轮 backend 调用增到 17 次；补齐预取游标后恢复到 3 次，最终版本才保留。较小缓存或更宽单行的重复解码问题由上述强制驱逐回归测试覆盖。
