# 查询并行瓶颈诊断：先查 GRIB，再解释 HDF5 混合负载

2026-10-25。本文复算上一轮冻结记录，并补充独立观测开关诊断；原始 4.3 结果不替换、不扣除观测成本、不剔除慢样本。按用户最新要求，优先定位 GRIB。当前生产代码和共享数据库配置均未修改。

## GRIB：已经确认的边界

上一轮 N128/burst8 中，720 条 GRIB 完整测量查询对应 720 次发现和 1440 次 typed 取值。所有 typed 测量 profile 都满足：

- `native_field_cache.alias_hits/content_hits/hits=1`；`misses/waits/loads/hashes/evictions=0`。
- 一次取值输出 4096 行、一个 batch，两次 cursor.next。两个取值仍按原客户端顺序执行。
- 记录窗口内各项 IoTDB 和公共 fragment runtime 差分均为零；这里只检查每窗口是否为零，不累加重叠共享计数。

`loads` 在调用 native decode 前递增，因此本组暖取值没有重新调用 ecCodes 解码。不能把 cursor 增长称为 ecCodes CPU 增长，也不能因为 `waits=0` 排除 mutex 等待：这个计数表示 native singleflight 的 pending 状态，不测锁等待。

每轮请求均值再取三轮中位，单位 ms；inclusive timer 不可相加：

| 环节 | S1 | S2 | S4 |
| --- | ---: | ---: | ---: |
| 发现服务端执行 | 1.099 | 1.128 | 1.414 |
| 第一条 value 服务端执行 | 0.898 | 1.176 | 1.544 |
| 第二条 value 服务端执行 | 0.963 | 1.026 | 1.283 |
| 第一条 value cursor | 0.189 | 0.385 | 0.575 |
| 第二条 value cursor | 0.211 | 0.351 | 0.440 |
| 第一条 value IPC encode | 0.127 | 0.128 | 0.109 |
| 第二条 value IPC encode | 0.140 | 0.108 | 0.107 |
| 第一条 value profile 结束→下一条 value 入队 | 4.527 | 6.306 | 7.424 |
| 最后一条 value profile 结束→客户端 owned | 4.364 | 5.617 | 5.827 |

四路减少排队，却放大了部分执行和交付包络。以下使用每配置全部 240 条完整测量查询的均值，列可以相加；不能与原报告的 p50 配对效应混算：

| 格式/配置 | 完整 T | 执行前等待 | profile 执行 | control 剩余 | control 外 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GRIB S1 | 34.723 | 17.449 | 3.129 | 8.605 | 5.540 |
| GRIB S4 | 29.026 | 6.491 | 4.240 | 11.648 | 6.647 |
| HDF5 S1 | 28.221 | 16.530 | 3.076 | 5.046 | 3.569 |
| HDF5 S4 | 26.496 | 12.419 | 3.158 | 7.350 | 3.570 |

`执行前等待` 从完整 frame 的 queued_at 到 profile start，还包括 resource hint 解析和开始前的 runtime/IoTDB 快照，不能全部视为纯队列等待。control 剩余包括 socket/framing、响应解析、协程恢复和未计时服务端前尾段；control 外包括请求构造、FUSE/IPC 和 owned copy 等，不能全部命名为某一种锁或客户端 CPU。

S4 GRIB 的 profile 执行只占完整均值 T 的 14.6%；执行前等待占 22.4%，control 剩余和 control 外分别为 40.1%/22.9%。即使消除某个 cursor 子阶段，也不能将该局部收益直接外推到完整 T。

## GRIB 的具体竞争路径与待证伪假设

### 热缓存仍跨第二个线程池

DenseGrid cursor 构造时无条件 `schedule_grid_decodes()`。即便 alias/native field 已经 ready，仍把 cache lookup、费用重放和 render 提交给独立 `GribDecodeExecutor`，再由查询 worker 在 `future.get()` 等待。其默认线程数按 hardware_concurrency 选 2–4，不按此次两核预算推导。

因此路径为“公共查询 worker→GRIB worker→公共查询 worker”，包含分配 packaged task、排队、唤醒、共享 future 发布和调用线程恢复。`typed_cursor_execute` 包的是 cursor.next，任务又可能在 cursor 构造时提前开始，所以它既包含 future 等待，也会随 setup/生产者调度发生计时归属移动，不是独占解码时间。

核对点：`GribFormatQueryAdapter.cpp:1476`、`:1558`、`:1679`，executor `:667–696`。独立探针应记录 submit→worker start、生产者 wall/线程 CPU、future.get 等待、生产者完成→调用线程恢复，并记录 fallback/ready-before-get，才能把“有额外线程池”推进为定量归因。

### Native cache 的共享锁

alias lookup/LRU 和 field lookup/2Q LRU 共用同一 mutex；warm hit 仍修改链表并复制 shared_future/shared_ptr。loader、future.get、费用重放及 render 都在这把 cache 锁外，不能称为整段解码串行锁。

另有明确可缩短的临界区：`GribDecodedFieldCache::report()` 持 cache 锁向请求私有 profile map 写六个 gauge，可能在共享锁内分配 map 节点/字符串。这部分会进入结束 next 的 cursor timer。是否是主要瓶颈须分别测 alias、field、report 的 wait/hold，不能从 cache hits 推出锁开销很小，也不能从代码中有锁推出锁就是主因。

核对点：Adapter `:396`、`:460`、`:325–332`。

### 热 render/输出复制和发布

热命中仍有窗口值分配、Float64→Float32 转换和 batch slice。它们可能受 CPU/分配器竞争影响，但当前没有内存带宽饱和证据。重复结果也没有在编码之前合并：每配置、每轮、每个 value 位置有 80 个 registration route、仅一个 result digest，却执行了 80 次编码和发布。现有 store 的 digest 去重发生在编码/哈希之后；桥的 singleflight 按 registration_id，而非已授权的冻结查询语义键。

发布位于 cursor/IPC 之后，可以影响 typed 总时长，不能直接解释 cursor timer 增量。本轮没有证据证明还在大量触发先前已修复的 publication race。

### 公共交付与观测尾部

`serve_request` 在 `QueryRegistrationSocket.cpp:1837` 记 profile end，之后同一 query worker 继续做响应写入、失败回滚、profile JSON 构造、`profile_mutex_` 和逐请求 flush，最后才返回。任务槽和资源预约仍由 scheduler 持有到返回及 capture 清理结束。这些工作不在 server_total 内，但会影响其他请求；“profile 在计时区间外”不等于没有间接扰动。

客户端虽固定八个在途且没有新增查询 worker，唯一 asyncio loop 中仍同步执行 FUSE memory_map、Arrow stream 读取和 NumPy owned copy；这期间其他协程不能继续处理响应或提交下一段依赖请求。当前只定位到边界，未将 control 残余全部归因于客户端。

## GRIB 专用线程/锁探针：定量结果

已完成 3 轮×1/2/4 路，共 9 cells、7920 个任务（7200 测量＋720 预热），另有 18 次独立 seed。每任务取原 N128 两个命中 source 中的一个 selected message，交付 4096 Float32 值与 mask；所有输出对原封印 oracle SHA 通过，全部测量保持 alias/content hit、零 payload source 读取、零新增 native decode。客户端主线程将八个独立任务一次 submit_batch 到真实公共调度器，没有新增客户端查询 worker。

此探针隔离 Adapter，不含目录发现、Owner/IoTDB、FUSE、IPC 或结果发布；仅 taskset CPU2/3，不使用原完整 FM1 的独占分区和 DataNode 共享 quota。重新链接同一冻结 build 的生产对象，只替换插桩 Adapter 对象；没有改原调度、cache、render 或 executor 策略。时钟和 atomic/profile 计数有观测成本，数值用于判断机制，不能替换完整 N128 性能，也不能当作已实现优化收益。

以下单位 **µs**，每轮任务均值再取三轮中位；嵌套区间不可相加：

| 阶段 | 1 路 | 2 路 | 4 路 |
| --- | ---: | ---: | ---: |
| query worker 总 wall | 142.57 | 158.14 | 229.03 |
| query worker 总线程 CPU | 99.01 | 98.56 | 99.16 |
| cursor next wall（含 owned 拷贝） | 85.82 | 89.65 | 143.44 |
| cursor next 线程 CPU | 43.84 | 41.40 | 40.40 |
| executor submit→job start | 14.91 | 16.99 | 33.24 |
| executor job wall | 24.89 | 24.95 | 26.46 |
| executor job 线程 CPU | 24.67 | 24.29 | 23.95 |
| job 内 render 线程 CPU | 17.22 | 16.78 | 16.61 |
| future.get wall | 49.76 | 48.93 | 103.82 |
| future.get 线程 CPU | 7.44 | 6.25 | 5.53 |
| job done→get 返回 | 15.63 | 17.75 | 75.43 |
| alias＋field＋report 三处锁总等待 | 0.30 | 0.79 | 1.52 |
| 三处锁总持有 | 2.22 | 2.76 | 2.47 |

**结论已可从“可能有第二线程池开销”收紧为：热路径的线程交接/等待明显放大，计算量和 cache mutex 等待不足以解释该探针的主要 wall 增长。** Query worker 的 CPU 基本不变，而 wall 增长约 86µs；native job CPU 约 24µs、render CPU 约 17µs 均未增加。同一 cache mutex 在三处获取的总等待仅约 1.5µs，远小于 104µs 的 future wall。report 锁仍有可以缩短的临界区，但不是本探针首要瓶颈。

`job done` 是 callable 析构、packaged_task 兑现 promise 之前，故到 get 返回包括兑现、唤醒、调用线程恢复，以及已完成结果尚未被调用线程消费的间隔，不能全称为 scheduler lock wait。为排除“只是 get 调用较晚”，另按 ready-before-get 分层：4 路 2400 个测量任务中有 1980 个在 get 前尚未 ready；这些任务 future wall/线程 CPU 均值为 **127.75/6.31µs**，job done→get 返回为 **75.31µs**。1 路 2400 个任务均未 ready，后一间隔仅 **16.13µs**。这一差别在真正等待的任务中仍存在。

局部吞吐三轮中位为 5582/8881/9203 个单-message任务每秒；同轮配对 2/1 提升 56.13%，4/2 仅提升 **3.13%**，三轮 4/2 分别为 1.0313/1.0600/1.0169。新增执行槽已接近局部收益平台，不能把 4 个槽解释为 4 核持续计算，也没有测得物理双核完全饱和或某个内核锁的独占贡献。

因此下一步 GRIB 的优先顺序明确为：**先消除 ready native-field 命中的 executor 往返，再处理轻量临界区，最后按完整 T 继续定位公共响应/profile 与客户端依赖交付间隙**。当前已有内部 executor，并非本轮公共调度器新引入；优化热路径可以减少线程交接，不需要新增 Message 并行。

## 优化方案及语义边界

以下按本轮探针结果排序。本轮未修改生产行为，也未给未实现方案声明加速倍数。

1. **先让单 message 直接在已有查询 worker 上消费。** 当前取值每个请求只有一个 message，可在已完成原验证的 DenseGrid 单 message 计划上跳过 executor 提交，复用 `next_grid()` 已有同步 fallback 的 `decode_cached_grid` 路径；原 get_or_load、逻辑 payload 预留、逐来源 metadata 费用重放、取消检查和 render 全部保留，多 message 计划沿用原路径。这样不用新增一套 cache peek、费用重放和 pending singleflight 规则。单 message 冷/热查询都要做对照；不以 cache hit 跳过授权或计费，不共享请求预算/取消/失败结果。后续若扩展为通用的“ready cache 直接消费”，应只对 ready alias＋ready field 非阻塞取安全引用，保留精确身份与原费用 trace，pending/miss 继续原有有界路径。该方案减少线程交接，不新增查询内并行。
2. **把 profile map 写入移出 field-cache 临界区。** 锁内只取得一致的六个整数快照，解锁后写当前请求的 profile。原缓存键、命中、LRU、容量、费用和异常处理规则保持。是否值得先做，以 report-lock 实测占比决定；暂不直接分片锁或改替换策略。
3. **将诊断构造/输出从查询关键尾部移开，并精确通知。** 快照只保存不持有 native handle/Owner lease 的值，使用有界诊断队列；保留查询结果、当前授权、响应失败回滚、状态和 server lifetime，stop/drain 对日志另设水位。HDF5 提前释放逻辑资源必须在最后一个可能使用 native 的对象和失败清理结束之后，并主动通知 dispatcher。不能在最后 H5Dread 后直接解锁。
4. **按两核实际预算管理可运行任务，优化唤醒。** dispatcher 和指定 worker 使用分开的通知通道，提交/完成/资源变化通知 dispatcher，仅唤醒本批 Ready worker；保留队列容量、FIFO 可运行扫描、资源绕行和取消先于 deadline。不能把当前共享 CV 的 notify_all 直接改 notify_one。对 CPU 型暖请求可评估更小的有效 active 上限，保留 I/O 等待时其他可运行任务的推进；不为不可运行 HDF5 占槽，不给 HDF5 固定高优先级掩盖 Arrow 退化。2 路可作两核下的下一次对照，尚非已证明最优值。

render copy/move 候选在 10.22 已做过正确性及完整 R/T 对照，因稳定收益门槛未通过而回退；本轮不把它当作未经验证的新方案自动恢复。跨注册的物化复用也涉及完整冻结键、逐请求授权/计费、取消 survivor 和内存 pin，优先级应低于已确认的短路径交接与诊断尾部。

所有方案保持 typed_batch 单任务及内部顺序，不引入 GRIB Message 拆分，不改 IoTDB 线程池，不合并原来独立 RPC 的预算和错误原子性，不通过增加客户端查询 worker 获得结果。真正实现后的验收需先逐项对比冷/热、单 message/多 message、旧计划/新 head、预算耗尽、取消 survivor、错误 code/phase/message/source、publication race 与 stop/drain；不能只因数据结果相同就宣称语义相同。随后在同一个候选版本中重做 1/2/4 对照和完整 N128 时钟，保留所有轮次。局部交接节约不保证足以改善完整 T；后续还需区分响应/profile 尾部与事件循环中的 FUSE/owned-copy 停顿。

## HDF5 混合负载：79.62% 的来源

原始三轮 HDF5 p50 为 S1→S4：2.3178→5.2126ms、2.3139→4.1562ms、2.2010→3.3205ms。三轮比值中位数为 +79.61981084%，不是原生读取成本增加。

每配置 120 条 HDF5 测量请求的均值可加分解如下；该分解不能直接相加解释 p50 比值：

| 阶段 | S1 ms | S4 ms | 增量 ms |
| --- | ---: | ---: | ---: |
| submit→queued_at | 0.2107 | 0.2846 | +0.0739 |
| queued_at→profile start | 1.3541 | 2.4931 | +1.1390 |
| profile 执行 | 0.6388 | 0.8542 | +0.2154 |
| profile end→receive | 0.0290 | 0.1966 | +0.1676 |
| receive→owned | 0.1475 | 0.2704 | +0.1229 |
| 总计 | 2.3801 | 4.0989 | +1.7188 |

均值增量中，66.27% 在执行前等待；HDF5 cursor 均值仅从 0.1024→0.1207ms，source open 从 0.0080→0.0063ms。相邻 HDF profile 的串行交接间隙均值从 0.3135→0.6787ms，三轮均增加。gap 含响应/同步诊断及派发恢复，不能全归为 dispatcher 空转或 flush。

容量为 1 的 HDF5 逻辑资源覆盖整个控制任务，而非只覆盖原生调用。四路可以使 Arrow 越过受阻 HDF5 并推进，却不能增加 HDF5 原生服务容量；两类任务还共享 CPU、公共状态和交付。原探针先发 HDF5，也使 S1 的串行先行优势成为比较背景，不能把改回 HDF5 固定优先当作消除所有竞争。

## 观测开关独立对照

为检验“全由 profile 或采样导致”，使用相同冻结生产对象重新链接仅改变观测开关的辅助程序，3 轮、1/4 路、HDF5-only/mixed、5 条件，共 60 cells；每 cell 10 预热＋100 测量波次，每波 8 请求，合计 52800 请求（48000 测量）。全部值/mask/ordinal 与原辅助 oracle 一致。

该诊断只用 taskset 将进程及线程限制在 CPU2/3，没有重新建立原主实验的独占 root 分区/总 quota，也未调整 DataNode；不替换 FM1 或原辅助数字。条件顺序轮换、worker/场景顺序交替，采用同轮配对。

| 混合负载观测条件 | S4/S1 HDF5 p50 变化 | S4/S1 Arrow p50 变化 | S4/S1 整波吞吐变化 |
| --- | ---: | ---: | ---: |
| 文件 profile＋100µs 采样 | +75.36% | −37.72% | +15.46% |
| /dev/null profile＋100µs 采样 | +81.50% | −35.85% | +11.97% |
| 关闭 profile＋100µs 采样 | +57.74% | −36.31% | +18.90% |
| 文件 profile＋不采样 | +72.06% | −35.67% | +5.53% |
| 关闭 profile＋不采样 | +64.36% | −35.48% | +10.56% |

同为 S4/mixed，关闭整个 profile、保留采样，相对文件 profile 的 HDF5 p50 配对下降 19.17%、吞吐上升 17.89%；仅写 /dev/null 的对应变化仅 −0.44%/+0.31%。因此是 profile 构造/计时/同步输出整体有成本，不能把主因简化为磁盘落盘。关闭采样没有稳定改善，当前不支持将 sampler 认定为主因。关闭两种观测后 HDF5 相对 S1 仍明显变慢，也反驳“79.62% 完全来自观测”的解释。

纯 HDF5 在无 profile/无采样下 S4/S1 p50 仍为 +11.99%，吞吐 −7.69%，说明存在与 Arrow 无关的额外调度/共享状态成本。整进程 voluntary context switches 的三轮中位数从 4825→13031，但这些计数包含启动、预热、oracle 和输出，不是调度器的独占开销，只提供进一步测唤醒路径的线索。

## 验证、环境与证据

- 主实验实际 `cpuset.cpus.partition=root`，CPU2/3 exclusive、两核父 quota；不能把早先 isolated 禁用负载均衡的问题当作本轮原因。
- 现有数据离线复算通过 1440 条 burst8 完整测量查询、180 波，以及 1440 条 GRIB typed profile；辅助分解绑定 181 个原始输入 SHA，逐请求整数分解和均值闭合通过。
- 60 组新观测对照的 52800 个 owned 输出全部匹配原 oracle，原产品 `09229a…`、原辅助程序 `5b4314…` 和科学输入摘要前后不变。DataNode PID 2269698、配置 SHA `f69f408d…`、原 cgroup 和所有观测到的线程亲和性集合保持。
- 原冻结产品的 1981 项单测来自上一轮完整构建；本轮没有修改产品并重复宣称新全量验证。新诊断程序只复用生产对象，验证的是观测条件及输出，不是已实现优化的性能。
- 一次诊断准备脚本因路径参数类型错误在构建/采集前退出，保留初始 source-r1；修正后在 fresh source-r2 构建，没有重采或筛选性能样本。

本地目录 `experiments/cross_version/work/query-task-bottleneck-20261025/`：

- `n128_analysis.py/n128-analysis.json`：完整查询可加均值分解、服务端子阶段及依赖交付间隙。
- `grib_existing_analysis.py/grib-existing-analysis.json`：GRIB 热缓存事件、两个 value 位置、重复 registration/result digest。
- `aux_analysis.py/aux-analysis.json`：HDF5/Arrow 原始时间线分解与串行交接间隙。
- `prepare_diag.py/run_diag.py/analyze_diag.py`：观测因子辅助程序及独立验收。
- `diagnostic-evidence.tar.gz`：11174621B，SHA256 `9a83f5dc6a7bacce093d0d41ec778ca05f1390c49227a90471922eb0d291b55f`，395 个文件大小与 SHA 全部通过；不含 ELF、NativeFileStore 副本、原始科学文件或私有配置。
- `diagnostic-analysis.json`：全部 60 cell、每轮数据和配对比值；`diagnostic-local-audit.json`：下载验收。

GRIB 计时证据在 `grib_diag/`：`grib_task_probe.cpp`、`prepare_grib_diag.py`、`run_grib_diag.py`、`analyze_grib_diag.py`、`grib-diagnostic-analysis.json`。输入由原 case/manifest 提取，`grib-input-binding.json` 绑定两个 raw SHA、message offset/length 和值/mask oracle；构建清单封存 original/instrumented Adapter SHA、1022 文件源码 inventory、全部复用对象/库及实际命令。原产物和源码前后未变。

`grib_diag/grib-evidence.tar.gz` 为 1184944B，SHA256 `ff343aef6e8429fa5cdb5d2606794713a2d83edab1d16573d9370992814a3078`；51 个成员大小/摘要全部通过。本地独立验收 7920 个任务、全部 warm/measurement grid、cache 事件、单次 job/future/lock 次数与 oracle；`grib-followup-metrics.json` 另存吞吐配对及 ready-before-get 分层。当前共享 DataNode/config/cgroup/affinity 保持，未部署诊断 binary 为产品。
