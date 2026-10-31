# Arrow 非训练局部查询：A–E 实验

## 目的与边界

验证 GRIB 的结构定位、局部取数机制能否推广到 Arrow，并区分 adapter/core、数据库范围访问、普通 FUSE、typed IPC 前端的成本。实验不使用 TrainingQueryModule、snapshot/view、窗口组装、预取合批、标准化或训练器；单客户端串行提交，无谓词，只使用固定 source/version、物理 batch/row、child path 投影和 index 半开区间。

此实验验证跨**格式**泛化，不人工构造跨文件窗口。不能预先将训练端到端差距归因于 Arrow 引擎，也不能预先要求 IoTDB-FS 胜过本地 mmap。

## 路径与结果契约

| 路径 | 实现 | 用途 |
|---|---|---|
| A | 本地 Arrow stream，初始化时建立 batch offset 索引，mmap、原生 Arrow 解码 | 强本地基线 |
| A-read | A 的同一 C++ reader，显式改用 ReadableFile | D 的配对读取方式对照 |
| B | 生产 QueryModule + Arrow adapter，read-only 本地 payload backend | 隔离查询框架与 tile 访问成本 |
| C | 相同 QueryModule + Arrow adapter，IoTDB backend，直接消费 cursor | 加入数据库 manifest/layout/payload 访问 |
| D | 与 A-read 相同 reader，读取普通 IoTDB-FS 挂载源文件 | 普通文件访问对照 |
| E | Unix socket typed 注册 → 查询/IPC 编码/发布 → 不可变 FUSE 结果 → PyArrow | 实际前端端到端成本 |

普通源文件 `op_open` 设置 `direct_io=1`，不能使用 libfuse 2 mmap；D 明确使用 ReadableFile，不静默降级。typed 结果可 mmap。A–D 共用 C++ 消费器；E 使用 Python/PyArrow 客户端，须单列客户端转换时间，不能把 E−C 全部称为 FUSE 开销。

每条路径都完整消费选中列，生成自有 Float32 数组、每值一字节的 validity 和 UInt64 index；不以零拷贝切片创建结束计时。null 值归零，仅在有效位中记录缺失。计时后按请求列序计算 SHA256（每列 float32 little-endian 后接 byte validity），另对 index 计算 SHA256，与本地 PyArrow oracle 精确比较。逆序投影验证列顺序。

B/C 使用同一个已发布 manifest 的静态 exact catalog；C 的 prepare/execute 仍访问实际 backend 验证 manifest 和 layout。B layout 是本次运行初始化加载的内存快照，payload 为本地 pread。E 保留生产 live catalog 和授权。因此 B/C/E 不是只差一层传输的严格加法分解。

## 数据、用例与缓存

使用现有 PB00 未压缩 Arrow stream，不重新摄取或改写数据：

| 数据集 | 通道 | source version | 文件大小 |
|---|---:|---:|---:|
| ETTm1 | 7 | 1790670158165 | 1,955,584 B |
| UCI Electricity | 370 | 1790670177863 | 207,613,480 B |

所有用例使用 batch 0、row 0、`target[channel][step]`，该行 8,192 steps。通道数 ETTm1 为 1/7，Electricity 为 1/7/370，按逆序投影。

- small：`[37,165)`，128 值，位于单 tile。
- boundary：`[4032,4160)`，128 值，跨两个 tile。
- tile：`[0,4096)`。
- full-row：`[0,8192)`，用于观察选择率变化。

tile 固定 4,096 elements；B/C/E 共享预算均 1 GiB、16,384 entries、256 inflight、4 cache workers。普通 Arrow 查询保留串行片段读取，没有启用训练 adapter 的合批/并行波次。原生 reader 最多缓存一个 record batch，限制 1 GiB；本用例只访问一个 batch，不会因缓存策略差异驱逐。

每用例一个应用/fragment 冷读，随后 10 次暖读；两个 campaign，第二次反转 A–D 内的路径和用例顺序，E 始终在 A–D 后运行。暖读每 campaign 去最高/最低取均值，与论文 GRIB 的统计方式一致；冷读只有两个独立样本，保留原值，不作高置信推断。没有清理 OS/IoTDB 缓存，不能称磁盘冷读。

A–D 各用例重置 reader/QueryRuntime cache；D 的挂载级 raw cache 会保留，用例之前只按 campaign 重启 daemon。E 每用例重启 daemon，每次测量重新注册并立即消费结果，验证 profile 中确实执行 typed_prepare/typed_encode，避免完整结果缓存替代查询。每 daemon 仅 11 条注册，规避注册表/结果存储容量干扰。相同内容的 result ID 可以相同，registration ID 必须不同。

## 计时与复现

A–D 报告初始化、prepare、execute、结果复制和完整 wall；execute 包含每次 cursor.next 的懒执行/解码，consume 单独计输出数组复制。索引初始化只扫描 IPC framing metadata 并跳过 body，不进入重复查询，但单独报告，不把完整扫描藏进 per-query “局部字节”指标。

E wall 从 JSON 序列化前开始，到注册、IPC mmap/read、数组复制和关闭结果结束；服务器 `typed_encode` 包含懒 cursor 执行和 IPC，不能解释为纯编码。哈希比较均在计时之外。记录 payload/layout 实际请求字节及次数、cache 命中/解码、服务阶段、IPC 大小。mmap message body 大小只代表逻辑触及的 batch，不是磁盘实际读取字节。

构建 `arrow_query_paths_probe`，在专用空闲挂载上运行：

```sh
python tests/benchmarks/arrow_training/run_arrow_query_paths.py \
  --work-dir /data_01/zzy/iotdb-fs-exp/public-arrow-baseline \
  --mount /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/runtime/mount \
  --socket /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/runtime/public-arrow-ab.query.sock \
  --binary /path/to/build/bin/iotdb_fs --fallback /path/to/build/bin/iotdb_fs \
  --probe /path/to/build/bin/arrow_query_paths_probe \
  --daemon-pid CURRENT_PID --output NEW_OUTPUT_DIRECTORY
```

runner 从指定 daemon 继承数据库连接环境，配置副本权限 0600，finally 恢复原配置及挂载并关闭 profile。归档只复制白名单 JSON/JSONL，不收集配置和 daemon 日志。源文件 hash 与已冻结摄取 manifest 一致才运行。

## 与 GRIB 论文的关系

保留同源数据、结构化局部读取、完整消费输出、10 次去极值统计。当前 GRIB 历史 harness 的 source reader 每次重新扫描文件，部分 FUSE 结果可能在计时前 stat 时物化；本实验提供可复用索引的更强原生基线，并把 E 物化纳入计时。不能直接拼接两组绝对延迟，或用旧 harness 的 selected-message 字节计数代替真实 I/O。

只有结果一致、局部请求未读取整个源文件、成本随投影/范围按预期变化，才能支持“结构访问机制可泛化”；是否有端到端速度优势必须按缓存状态、选择率和访问路径分别报告。本阶段不覆盖压缩、dictionary、多客户端、语义谓词和历史版本检索，也不声称全部论文工作负载已经跨格式复现。
