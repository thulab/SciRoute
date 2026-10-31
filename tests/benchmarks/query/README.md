# 公共查询调度器辅助混合负载探针

这是无 IoTDB 的辅助机制实验：使用生产 ingestion、NativeHdf5 / Arrow 适配器、同一 QueryRuntime、控制 socket、公共调度器、结果桥与 IPC 编码。HDF5 由真实 NativeFileStore 安装并打开本 cell 私有目录中的不可变副本，metadata/layout/catalog 与 Arrow payload 为内存 fixture。每个请求选择一个源，不能当作 FM1 的 N=128 主实验。

构建器复用已经验证的 unit_tests 生产对象与锁定依赖，不改原构建目录。调用者须保存原构建的源码身份及与本次产品源码的一致性证明：

    python3 tests/benchmarks/query/build_query_task_mixed_probe.py \
      --source-root /absolute/new/source \
      --build-root /absolute/verified/build \
      --output-root /absolute/new/mixed-build

输出 build-manifest.json 和 build.log，记录编译/链接命令、复用对象与 archive 的 SHA256、编译器及探针二进制 SHA256。output-root 必须不存在。构建只写新目录。

固定命令行：

    query_task_mixed_probe HDF5_FILE DATASET OUTPUT_DIR WORKERS SCENARIO WARM MEASURED CAMPAIGN ARROW_VALUES

- HDF5_FILE：封印样本的原始只读路径。读取后由生产 ingestion 建立内存 manifest/layout/catalog，再在 OUTPUT_DIR/private-native-root 中用 NativeFileStore 的完整 hash/fsync/原子安装流程建立同字节副本；全部准备在计时前。原始文件不修改。辅助副本使用独立 fixture namespace/descriptor/seal ID，不冒充封印 corpus 的 owner identity。
- DATASET：精确 dataset 路径，或 auto，按名称递归选第一个 64×64 Float32 / Float64 / Int16 数据集；跳过软链接和外部链接。
- OUTPUT_DIR：必须不存在，保留所有历史失败和慢样本。
- WORKERS：公共池 1/2/4。队列固定 64，批大小等于 worker 数，片段池默认固定 4。
- SCENARIO：hdf5（8 个 HDF5 请求）、arrow（8 个 Arrow）、mixed（先 4 个 HDF5，再 4 个 Arrow）、mixed-held（同一混合负载，外部线程持有实际原生 HDF5 库互斥锁 100 ms）。
- WARM / MEASURED：建议 2 / 10；每 wave 固定 8 个请求。
- CAMPAIGN：重复实验标签。
- ARROW_VALUES：建议 4096，生成真实嵌套 Arrow IPC，独立确定性值序列。

每个配置使用新进程。客户端只有一个执行线程：打开 8 个连接、先发送全部请求，再 poll 响应。协议每个连接只容纳一个请求，不能在单连接发送 8 帧。只有诊断采样线程及受控持锁线程在客户端旁路运行，均不执行查询。自然负载不人为增加 sleep 或 I/O 延迟。

受控负载先观察到 4 个 HDF5 请求全部留队（queued=4, resource_blocked=4, active=0），再发送 Arrow；每个 wave 都要求 4 个 Arrow 的 owned 结束时刻在持锁释放标记前。标记在原生操作回调返回前写入，因此早于真实解锁，判断是保守的。100 ms 控制成本单独报告，不与自然负载混作吞吐提速证据。

所有返回值均为成功取值结果，不能用授权错误充当查询。HDF5 独立 oracle 在调度前用原生 H5Dread 读取原始文件、Arrow oracle 为生成公式。计时区间只做 socket 接收、IPC 解析及独立 owned 值/mask/ordinal 数组复制。全部 8 个结果拥有独立数组后，wave 结束；然后统一逐值检查行数、列数、值、全部 null mask 与 Arrow ordinal，并计算值与 mask 的 FNV-1a 指纹，单列 oracle_validation_ns。oracle 校验和 hash 不延迟后续请求、不进入 T。

HDF5 published reader 的 open_native_file、native_file_cache_identity 和 reuse_native_file 全部委托真实 NativeFileStore，不提供未校验的 native handle，不放宽文件身份、能力、descriptor 或缓存重绑定检查。manifest 记录副本 SHA256，并在计时前通过真实 handle 完整读取逐字节确认与原始输入一致。保持 native_session_cache_enabled=true 及共享 runtime 暖缓存，不能把暖态 cache hit 宣称为每次都发生了原生解码。

| 文件 | 内容 |
|---|---|
| manifest.json | 辅助实验范围、source_count=1、worker/队列/批/片段池/缓存配置、输入、样本尺寸、重复次数 |
| requests.jsonl | 每请求 submit/receive/owned 单调时钟、观测延迟、字节数、行数、oracle 校验及指纹、完整成功 response |
| waves.jsonl | wave 完整 owned 时长、8 / 时长的 QPS、持锁起止标记、Arrow 解锁前完成数、调度计数前后快照 |
| scheduler.jsonl | 100 μs 目标采样间隔的 active/queued/blocked/资源等待与 bypass 统计；实际以单调时间为准 |
| profile.jsonl | 生产查询 profile；可由 response.path 与 profile.path 关联，使用精确执行区间计算重叠度 |
| failure.json | 若失败，保留终止错误；进程返回非零，之前已完成的原始行不会删除 |

receive 是单线程 poll/owned 转换观察响应的时刻，含客户端消费顺序影响。执行延迟和实际并行度须同时依据生产 profile 区间，不能仅用 100 μs 采样峰值推断。累计 peak_active 不可当作逐 wave 新峰值。吞吐以所有请求数除完整 wave 时长，慢样本不排除。
