# V03：发现重验复用验收

日期：2026-10-16。开发基线 `d1742e6`。关联[实施计划](../deliverables/version-directory-content-sharing-plan.md)、[实现契约第 9 节](../contracts/version-directory-content-sharing-v1.md#9-v03请求上下文准入和共享预算)、[机器验收摘要](../../experiments/cross_version/results/v03-acceptance-20261016.json)、[运行时契约](../../experiments/cross_version/version_directory/specs/version-directory-v03-runtime.json)。

**状态：V03 完成。** 659 项统一回归、32 科学版本、9 项真实非 root 权限、11 项真实后端 ReadContext 检查和默认 strict 原 256 版/1,024 typed 单元全部通过。固定 N=128 三组各 180 条观测完成；V03 两格式温热发现的已观测 SDK 查询提交均为 0，discover-values 主指标优于冻结 A。原服务配置、许可证与节点身份文件核验通过，本轮挂载无遗留。未执行 Git 写操作。

## 实现与已确认边界

V03 保持 V02 `namespace-record/2` 与 content-build/2，不升级持久格式。内部 ReadContext 保存初选全部候选及 Exact/Permission/Liveness、去重当前祖先依赖，残余谓词未命中和零命中结果也必须授权。Latest 仅初选一次；新 execute、发现返回、最终结果注册和新 open 在 owner 提交边界准入。已打开 IPC 的 read/fstat 保持固定字节。

变更窗口最多 256 次提交、65,536 个依赖键或 8 MiB；缺口、超限或恢复后对全部冻结候选和祖先作有界重验，不重新选择 Latest。执行租约仅覆盖在途执行。共享解码 job 在入队时捕获租约，等待者取消后仍持有到真实 I/O 与源捕获结束，并在缓存锁外释放；完成缓存不永久持有源租约。

用户另行确认两项修正：64 MiB 元数据 ledger 从扫描到发布共享，源身份分别计费，缓存命中重放费用且超限 sticky；独立 execute/open 建立新预算，长期 ReadContext 不持有账本。共享 producer 记录中立费用 trace，每个等待者独立重放，trace 内存计入 runtime 临时/驻留预算。写入串行锁与目录准入锁拆开，不可变对象准备及查询远端读取/解码在准入门外；最终 head 确认和 journal 对账窗口串行，确认不明仍冻结。结果哈希、碰撞扫描、淘汰和大对象析构在门外。

## 构建、源码与正确性证据

最终实际依赖构建：**659 tests / 53 suites，0 failures/errors，24.937 s**。覆盖 Owner/ContentArtifacts、规划、共享解码缓存、前端 socket/结果发布、格式与训练路径。服务器 `experiments/cross_version/work/v03-verification-20261016` 保存构建、命令、日志、XML、二进制与最终 source-test binding；机器摘要逐项记录路径和 SHA-256。

| 绑定项 | SHA-256 / 结果 |
|---|---|
| 最终 r3 源码归档，817 文件 | `4b869634551fb68270c5036bce358677f94e6d1130f84683fffbc12d98ca8c36` |
| 实际 daemon | `5e23a4a6a9b24e91d7d8b0f99227b752668d943d0e5f7c2ebf686130eea2e6f8` |
| 最终测试 XML | `601b08a885e70314e31e0f1a30017932db915631c3d8e56205825fca4e3df4e0` |
| 最终本地/服务器源码与实验契约核对 | 489 文件，0 差异；其中 475 个生产代码/测试/CMake 文件与 r3 归档一致 |

r3 冻结后只修正权限测试的宿主挂载面，并添加运行时说明 JSON；二者在最终摘要与单独 receipt 中列明。文档和结果随后补全，不声称当前全部文档仍等于最初 817 文件归档。生产 C++、测试与 CMake 在最终构建后未变。

真实正确性报告 `results/v03-live-r4-20261016.json`，新 prefix `vd_v03_correctness_f735da5f12bb41d9`。复用原 raw corpus：每格式、每场景一个文件各 8 科学版本，共 32 版；首次与 owner 重启后各 128 typed 单元的值、掩码/provenance 和全文件 SHA 通过。9 项真实非 root 权限检查通过，旧 V02 head/journal 不变。本轮新增 owner 进程重启复验，DataNode SIGKILL 证据沿用 V00/V02，不称为新增数据库崩溃实验。

| 本轮正确性运行成本 | 观测 |
|---|---:|
| HDF5 发布中位数（16 科学版） | 2.064160117 s |
| GRIB 发布中位数（16 科学版） | 1.881189100 s |
| owner 新进程恢复 | 14.593506114 s |
| 发布后 / 重启读取后 daemon RSS | 66,032 / 43,720 KiB |
| 发布后全部 objects BLOB | 556 行 / 475,105 B |

objects 包含旧根、seal 和不可达中间对象，不是唯一可达字节或数据库物理压缩率，未执行 GC。

`results/v03-context-20261016.json` 的 11 项真实后端/HDF5 检查通过。直接 Store 读 head 校准记录 1 次 SDK 查询、140 B BLOB，证明计数可观测；热准入/manifest 的 6 次调用、热发现/最终准入的 4 次调用以及无关提交后的准入，两条 SDK 路径均 0 查询。257 次提交后记录 history_gaps=1、dependency_rereads=8、2 源/2 祖先重验；本次对象命中缓存，SQL 仍为 0，不将其解释为跳过全依赖检查。新版本不替换旧 exact；未命中候选 chmod、祖先 chmod、冻结源删除分别拒绝新准入，暖缓存仍受 source/catalog 与累计元数据预算约束。

并发排序由确定性 Owner Store hook 和 frontend mock 补充，不声称覆盖真实 FUSE 全部操作与窗口的笛卡尔组合。native ReadContext probe 使用 HDF5；GRIB 热发现 SQL 另由下述 N=128 profile 验证。

## 固定 N=128 准备、比较与成本

原 raw/FS/SQLite 在原位置复用，未新生成或复制原文件、重建索引。固定 V02 二进制在新 prefix `vd_v03_n128_20261016` 重摄取 128 HDF5 + 128 GRIB 文件。V02/V03 共用这些精确版本；legacy 使用原版本和持久布局，该组织差异必须随比较披露。准备报告 `results/v03-n128-prepare-20261016.json` 完成：恢复核对 256 个完整文件 SHA、两格式独立数值 oracle 和输入摘要。

| 计时外准备成本 | 观测 |
|---|---:|
| HDF5 / GRIB 发布中位数（各 128 文件） | 2.247039471 / 2.320100069 s |
| V02 准备后新进程恢复 | 114.807079351 s |
| 发布后全部 objects BLOB | 5,920 行 / 42,637,216 B |

固定 N=128、K=2、D=4、depth=2、seed=20261013、tree_width=4。runner SHA `be728432fc2b01f810af167a5d07308211a50cf980a478a0dfcf4e229ce65155`，native SHA `49f7f7127e6a7c73bab304f9ec230f46e540ebf26b3d3e40fed05b3a6e398864`。每组 3 campaigns × 5 repeats，两格式、三种 arm、两个阶段共 180 条完整观测；每格式/阶段使用全部 12 个 repeat>0 温热样本，不筛选。本段三组依次运行 legacy、pinned V02、V03，共 540 条；数值 oracle 和 profile 收集仍在冻结 client 时钟外。

原 helper 的 90 秒 readiness 不足以覆盖冷恢复，内存 wrapper 仅延长到 900 秒，保存唯一替换 diff、原/执行函数与依赖 SHA。runner/helper 文件和查询时钟不变；每组记录 7 次实际 daemon 挂载及二进制/配置摘要。

下表均为 client discover-values 的毫秒中位数；discovery/value/total 各自取中位数，不能把列相加。

| 组 | HDF5 discovery | HDF5 value | HDF5 total | GRIB discovery | GRIB value | GRIB total |
|---|---:|---:|---:|---:|---:|---:|
| 同轮 legacy A | 121.142246 | 24.745959 | 148.615696 | 54.546417 | 13.042997 | 68.576998 |
| pinned V02 owner | 8026.623136 | 14454.919555 | 22470.964499 | 14574.805129 | 7343.000909 | 21863.007469 |
| V03 owner | **12.962150** | **8.366024** | **21.565456** | **33.428187** | **5.777850** | **39.101750** |

冻结历史 A 始终保留 HDF5 **106.811351 ms**、GRIB **57.404444 ms**；不以更慢的同轮 legacy 替换门槛。V03 相对该固定门槛分别下降 **79.81% / 31.88%**，两格式 performance gate 均通过。V02 列诚实保留直接 Store 重验的秒级成本，不能用其旧 query_calls=0 宣称无 SQL。

V03 每格式的 discover 与 discover-values 两个阶段各 12 个 discovery request，均具有完整 Store 计数，既有 query_calls 与新增 version_store_query_calls/head_reads/object_reads/blob_bytes 的 min=max=0，独立 manifest/marker/verification calls 也均为 0。结论只覆盖该稳定温热发现范围，不外推 payload 冷读无 SQL。V02 缺少 Store 仪表，相关值及总查询数保留 null/unavailable。legacy 精确固定二进制无新 Store 路径，其已观测发现查询为 HDF5 3 / GRIB 2 次。

实际 daemon 的 executeQueryStatement 提交数由两条互斥 SDK 路径相加，不等于 wire RPC。新版 profile 的 server_total 包含 response JSON 与最终 catalog admission，旧版仅 register_json；这些辅助计时范围不同，主 client total_ns 范围保持不变。每条 profile、阶段计时、缓存命中、实际读取、launch 和 SHA 均保存在各组 complete-results/report.json、warm-analysis.json 与 startup-receipts.jsonl，并由最终 binding 引用。

| 组 | 7 次新进程挂载中位数 s（范围） | 结束前 daemon RSS 范围 KiB |
|---|---:|---:|
| legacy A | 0.922927（0.778640–1.267129） | 29,560–49,696 |
| pinned V02 owner | 124.108653（101.664963–160.010793） | 34,524–48,656 |
| V03 owner | 104.981050（102.959160–135.123082） | 38,736–58,936 |

新进程恢复仍逐版本强制深验，V03 不是冷恢复优化。这里进程缓存为空，但未清数据库或 OS 缓存；没有随机交错 A/B 或统计显著性结论。三组是 V03 整体增量，不能逐项归因给 LRU、ring 或解析缓存，不能外推多版本扩展性或全冷读。

## strict、原服务恢复与留档

`results/v03-default-strict-20261016/publication.json` 完成，实际 daemon SHA 与上述 V03 相同；未设置 version-directory 配置，原 256 版/1,024 typed 单元、全文件和 raw 审计通过。默认仍 strict。

`results/v03-post-restore-20261016.json` 完成。四个维护窗口的 original/restored 事件与配置摘要交叉核对；最终配置 SHA `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，原 `.env`、许可证和 system_info 摘要不变。只读 SHOW 确认实际 ASYNC / enable_partial_insert=true。在此配置下实际 V03 owner 启用被明确拒绝，journal 前后逐文件不变。

ConfigNode PID 2645019、class/cwd 正确，启动早于 V03 且检查前后身份稳定；没有事前 cmdline 摘要，故不声称做了跨全程 cmdline 哈希比较。最终 DataNode PID 2386091 与最后 restored 事件一致，进程 class/cwd/start ticks 检查通过。本轮所属挂载精确核对为 0；未自动清理其它挂载。没有启动隔离实例、获取新证书或创建账号，所有操作沿用授权原实例和既有账号。

前置失败也保留：首次驱动从服务器旧工作副本导入 v02_validation 失败，未进入服务控制；随后 r3 权限 oracle 被宿主 0700 父目录阻断，科学文件和 owner 重启检查此前通过且原服务已恢复。r4 只将测试挂载面放入独立 0755 临时目录，让非 root 到达 FUSE，控制目录/日志仍私有；确认卸载后归档与清理本轮临时目录。另一次本地 worker 费用回归暴露预算丢失，修复并增加 producer/等待者费用和取消租约测试后，才冻结最终 r3 二进制。失败证据不覆盖、不计入成功报告。

64 MiB 请求 budget 是逻辑需求账本，不是 RSS 限制。Owner 与 ContentArtifacts LRU 各有 64 MiB 持有量上限，current-root snapshot 另有 64 MiB 编码/trace 估算边界；活跃旧上下文、借用对象、临时解析、IPC 和数据库内存另外存在。已有取消/调度超时保留，不声称 IoTDB SDK 全链可抢占 deadline。payload 仍每版本独立；payload 共享、物理 GC 和显式迁移分别由 V04、V05、V06 继续交付。
