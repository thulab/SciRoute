# Arrow 压缩、过滤与并发实验

本轮扩展 [A–E 局部读取实验](arrow-query-paths-experiment.md)，使用相同生产 query core、Arrow adapter、Unix socket 和 FUSE 路径。没有修改生产查询语义、压缩格式或缓存策略。原有未压缩文件保持原样，新文件在 `/query-ext-20261001/` 独立摄取。

## 与 GRIB 的对应范围

沿用 GRIB 的结构定位、局部结果物化、完整消费和 10 次去极值统计。GRIB 属性过滤（shortName/level/step）能够下推到 message 索引；本轮 Arrow `value > threshold` 是解码后的 residual 行过滤，不具有同样的物理裁剪语义。当前论文材料也不足以说明已经做过同口径的 GRIB 并发实验，因此过滤和并发属于新增测量维度，不能冒称完全复现 GRIB 的执行链。

Arrow IPC LZ4/ZSTD 以 buffer 压缩。当前 adapter 对压缩目标字段缓存完整 record batch 的 Float32 leaf buffer，可能覆盖多个 list 行和全部通道；未压缩数据继续按 4,096 元素 tile 缓存。两者输出契约相同，解压/缓存粒度不同，指标必须同时记录压缩读取字节和解码字节。

## 冻结数据与矩阵

`prepare_arrow_query_variants.py` 从冻结的 PB00 源文件生成 none/LZ4/ZSTD 变体，逐 batch 检查完整 schema、逻辑值和 batch 边界，独立记录完整逻辑 hash、文件 hash、摄取耗时、挂载读回 hash、Arrow 身份和精确 version。生成和摄取均在查询计时外。

每数据集采用首个前七列存在变化的 batch，ETTm1 为 batch 0，Electricity 为 batch 2；各 codec 使用相同坐标。Electricity 原实验 batch 0 的前七列在选定范围内为常数，不能据其做部分过滤性能测试。本轮结果与上一轮 batch 0 数字不能直接混算。源文件逻辑数据和存储分块未改变。

八类用例，均投影绝对 `index`：

| 用例 | 输出列 | 范围 | 谓词/并发坐标 |
|---|---|---|---|
| c1-small | child 0 | [37,165) | 无谓词 |
| c7-boundary | child 6…0 | [4032,4160) | 无谓词，跨 tile |
| c7-tile | child 6…0 | [0,4096) | 无谓词，过滤的同范围/同投影对照 |
| all-tile | 全部 child，逆序 | [0,4096) | 无谓词 |
| c7-filter | child 6…0 | [0,4096) | 选定通道值大于冻结 Float32 中位数 |
| c1-hidden-filter | child 0 | [0,4096) | 相同谓词，过滤列绑定但不投影 |
| c7-reject-all | child 6…0 | [0,4096) | 值大于该范围最大值，返回带 schema 的零行结果 |
| c7-disjoint-batches | child 6…0 | [37,165) | 不同客户端读不同 batch |

阈值在计时前从逻辑值计算并冻结，三个 codec 的逻辑内容和阈值一致；实际选择率保存在 oracle。null、NaN、±Inf 比较为 unknown，不保留。过滤后 index 可以稀疏，校验其准确位置，不将输出行号冒充源位置。

两数据集 × 三 codec × clients=1/2/4 × 八用例 = 144 个实验单元。每单元跑 A/A-read/B/C/D/E 六路径，每路径一个冷 burst 和十个暖 burst；每 burst 有 N 个客户端，每客户端一个请求，共 22,176 次查询、9,504 个 burst。

## 并发、缓存与计时

- B/C 在同一进程共享 QueryModule/QueryRuntime，总预算 1 GiB，16,384 entries、4 cache workers、256 inflight。C 使用 N 个数据库 session。
- A/A-read/D 每客户端独立原生 reader 和 batch cache，每客户端预算 1 GiB/N；本矩阵 batch 均能容纳，避免借增加客户端扩大总预算。OS/mount raw cache 仍可能共享。
- E 用单个 Python 进程的 N 个线程发起独立 socket 请求；daemon 有 4 个 typed worker。客户端的 GIL 和每列数组转换可能限制 E 的并发，不能只归因于服务器。
- 同一数据用例测试并发请求的共同加载/解码复用；disjoint-batches 测试独立物理数据请求。不能把相同查询并发的收益推广为所有随机请求的收益。
- 这是**弱扩展的同步 burst**：N 增大时每波请求数增大。报告波次完成时间、N/wall 的 burst QPS 和每请求 p50/p95，不把波次耗时直接当作固定总工作量的加速比，也不宣称稳态饱和吞吐。
- C++ 创建线程和等待全部线程就绪不计时；释放屏障到最晚请求结束为 wave wall。E 使用线程池屏障，同样以最晚结束时间计 wave。数组完整复制计时，所有哈希放在该波所有查询结束后。
- 每 case 清空 A–D 的应用 decoded/reader cache；E 每 case 重启专用 daemon。没有 drop OS/IoTDB caches，“cold”仅表示应用/解码缓存冷 burst；同一个 burst 后发请求可能共享前一个请求的加载。
- E 每次新注册，检查 profile 有 typed_prepare/typed_encode；同 case 最多 44 条路由。返回内容相同不等于执行缓存命中。
- B/C 的 backend/runtime 计数在整波前后采样。E profile 的 runtime_delta 为进程全局差值，N>1 时保留原记录但不求和、不据此声称精确 payload 总量；所有 warm delta 都为零时可以验证该波没有新的 payload I/O。E 单请求线程内阶段耗时仍可报告；嵌套阶段不重复相加。

A/B/C/E 的输出 schema、投影顺序、稀疏 index、Float32 字节和 null mask，与独立 PyArrow oracle 精确比较。所有 codec 和并发档的同一逻辑请求还做跨路径 hash 比较。统计为每 cell 十个 warm burst 去掉最高/最低再平均；本轮只有一次完整矩阵，cold 一个 burst，p95 和冷读数值均为探索性样本。

## 复现

先用 `prepare_arrow_query_variants.py --help` 生成冻结 manifest，再显式构建 CMake target `arrow_query_paths_probe`。在专用挂载上运行：

```sh
python tests/benchmarks/arrow_training/run_arrow_query_extended.py \
  --manifest /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/manifests/arrow-query-variants-20261001.json \
  --mount /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/runtime/mount \
  --socket /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/runtime/public-arrow-ab.query.sock \
  --binary /path/to/build/bin/iotdb_fs --fallback /path/to/build/bin/iotdb_fs \
  --probe /path/to/build/bin/arrow_query_paths_probe \
  --daemon-pid CURRENT_PID --output NEW_OUTPUT_DIRECTORY --repeats 10
python tests/benchmarks/arrow_training/summarize_arrow_query_extended.py NEW_OUTPUT_DIRECTORY
```

runner finally 恢复原挂载配置并关闭 profile。原始证据只归档 JSON/JSONL 白名单，不包含私有配置或 daemon 日志。新实验目录比上一轮 source 多一级父目录，会增加 E 授权遍历成本；不得将本轮 E−上一轮 E 解释为压缩本身的成本。
