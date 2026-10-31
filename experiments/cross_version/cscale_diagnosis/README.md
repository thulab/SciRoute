# 固定 N512 的 HDF5 暖发现诊断

本入口只运行已封印的 `FM1-hdf5-N512-V1-C128-S0-base-latest` 和 `FM1-hdf5-N512-V1-C512-S0-base-latest`。二者必须绑定同一完整 N512 语料、namespace、head、owner journal 和受管原生文件位置；不新增 case，不发布数据，不重建 FS-index，不新增位置绑定。

A 是产生原始 2026-10-21 结果的 `3d58c7d56740fbb2a2b962e8b3558b5c12ee65703545e93ecb15f69a85d27cb3`。B 仅在同一 A 源码增加诊断，实际 binary SHA、完整 source closure、测试 XML、编译器与依赖绑定在采集前核验。标准 build binding 可补充原始不完整收据缺失的绝对路径，但须指向同一原始 SHA 的产物并保留原始收据来源；不得改写原始历史证据。B 的 binding 必须包含 `diagnostic_overlay`：`protocol` 等于本协议、`baseline_sha256` 等于 A、`instrumentation_only=true`、`performance_claim=false` 和完整唯一的 `changed_product_files`。产品源差异必须与此清单完全一致，且禁止增删产品文件。

四 campaign 的产品顺序为 AB、BA、BA、AB。每个产品每 campaign 只挂载一次，两个 C scope 在同一 mount 中运行。每 mount 为 5 个暖身 scope pair + 30 个正式 scope pair；每 pair 两次发现，起始 scope 逐 pair 交替，各 campaign 起始顺序为小、大、大、小。总计 8 次 mount、560 条发现记录（80 暖身、480 正式）。不自动重试，不按延迟追加轮次，不剔除慢值。暖身阶段的第一个请求可能构建 domain；全部暖身原样保留。

A 保留原始客户端 Td 的两个外部时钟：构造 request → control/call → 构造发现 rows → scope 检查。B 在上述边界增加客户端分段时钟；服务器诊断 profile 随原始记录保存。profile、独立 oracle identity 检查和资源采样均在 Td 外。HDF5 JSON 发现返回的完整 dataset locators 与已封印 K=2 oracle 比较；本协议没有 typed value 查询，因此不宣称重新验证值字节、完整 T 或 native-cache/batch 收益。B 的时间只用于归因，不能作为优化 A/B 结果。

正式 A/B 均须逐条证明 domain/index 命中、两次权限 proof 命中以及 SQL/payload/layout 零调用。B 正式行额外要求全部声明的诊断次数和费用计数明确存在并精确匹配，trace 导出／导出错误／捕获溢出／active collector／prelude 捕获错误均为零；首次 prepared replay 之后普通 observe/observe_unique 不得新增节点或费用（post-first node/charge delta 均为零）；缺字段不当作零。原有 stage 和 `cdiag.total.*` 为包含计时；`cdiag.self.*` 为独占分段，其总和必须逐条精确等于 `cdiag.total.serve`，且该 root 不超过原 `server_total_ns`。

B 单独设置 `IOTDB_FS_CSCALE_TRACE_DIR`，A 显式移除此变量。仅最初暖身导出每 C 的真实不可变费用批次；正式行禁止导出。四份唯一批次和五次事件必须包含 selection、manifest、catalog、proof，以及同一 proof 的重复别名。费用 trace 协议为 `iotdbfs-cscale-real-fees/2`，另外保留首次 prepared replay 前的真实 ledger `prelude_fees` 和 `prelude_consumed_bytes`，不假设请求账本最初为空，不凭空补入 head seed。Prelude 每项必须是唯一的正费用普通 metadata identity，禁止 reserved demand 域或零值，费用之和须等于捕获的 initial consumption。导出使用 `identity_encoding=latin1-bytes`：每个 JSON 字符对应一个原始字节，高位字节和 NUL 必须无损保留，拒绝超过 U+00FF 的字符，身份大小按 Latin-1 原始字节统计。每个 B mount 后先从真实 prelude 开始独立重放 fee union（按 identity 取最大值、排除零值新节点，预算字节扣除两个保留的非字节 demand 域）；批次 inserted nodes 仅计算在 prelude 之后新出现的节点，逐条核对全部正式行的 initial nodes/consumption、visited entries、inserted nodes、final ledger nodes 和最终 budget consumption；原始 trace 与 SHA 收据保留。跨批次 raw fee 值之和不称为实际收费字节。

首次 `collection-r1` 因发现“初始 ledger 为空”的测量假设错误而停止，保存 80 行后完整恢复；不将部分结果作为正式完成数据。原 runner r2 文件及 SHA 在本轮 work 的 `harness-r2-before-prelude/` 保留。修正仅用于新登记的 `collection-r2` 全套 560 行重采，不改写原失败证据，不按延迟挑选样本。

主要统计为每 campaign 同 mount 两个 C 的正式 Td 中位数，再计算 C512/C128 和该比值除以 4。四个配对比值均列出，最后取四轮中位数；不借用历史轮次填补缺失值。A 是缩放性能证据，B 是诊断证据。资源和服务恢复使用现有 NativeABRunner 的 supervisor、balanced root v2、原 JVM 内存与 sync_window。CPU 为 guest vCPU 2/3，有共享两核 quota/query 2 GiB；不能称为独占物理核。启动时间另存，不计入暖发现 Td。工作目录、完整执行闭包、原始资源时序和恢复收据保留服务器，汇总只提取白名单文件。

参数与 `NativeABRunner` 相同：`--repo --work --binary --baseline-binary --build-binding --baseline-build-binding --library --inspector --source-collection --current-collection --tier c512`。先在独立 preflight work 运行 `--prepare-only`，再用同一 work/参数运行 `--verify-prepared`，两者为零科学查询、零服务/资源修改。正式 work 必须全新。正式远端启动由主任务统一执行。本 README 或脚本登记本身不会启动实验。
