# 已编译 selector 与候选域短引用复用方案（已确认，已完成首版）

2026-10-23 设计，2026-10-24 完成实现、回归和 N8/N128 实验。结果见 [实现和实验报告](../reports/compiled-selector-handle-20261024.md)。用户已确认公共句柄复用及首版 HDF5 接入并要求运行实验。设计核查基于当前 HEAD `fac8688283e6de07108cf9aeef1c9638f1f95746`。实施前重新冻结实际基线及源码，不把历史二进制当作当前源码的替代。

## 历史核查与版本边界

| 已有工作 | 实际复用内容 | 与本方案的区别 |
| --- | --- | --- |
| 10-23 第一阶段 N 扩展优化 | `PathScopeSet` 的规范化、验证状态和 identity 在对象副本之间共享；候选数组共享，prepare 借用 | 新 JSON 请求仍重新编译完整 selector；报告明确没有 `scope_id`。大 N 性能验收未通过，不构成本方案已经失败的证据 |
| 10-18／19 热候选域及后续共用热池 | 同一 head 下的候选、属性索引、read context、主体证明跨请求共享 | 外部仍提交完整 selector，命中前仍构造／比较长域键 |
| 10-23 借用不可变批次＋阶段摘要 | 请求间共享不可变费用批次、阶段差额；请求账本独立 | 没有对外 selector ID，也没有消除 selector 输入与域键开销 |
| selector-R4、shared-candidate、batch-parse 等试验 | 目录解析后投影、请求内所有权或解析复用；部分候选未通过性能验收或回退 | 不属于 HDF5 `dataset_catalog` 的对外句柄协议 |
| 09-30 Arrow `window_prepare/execute/release` | 已有公开 prepared ID、TTL、UID、lease 与生命周期管理 | 保存的是冻结 snapshot／coverage／训练 module；不是可随 head 更新的 HDF5 selector 句柄，性能结果不能外推 |

未找到已实现、实验或回退过 HDF5 `dataset_catalog` 对外 selector／scope 句柄的证据。源码中 owner writer-lock 的局部 `scope_id` 以及 `dataset_catalog_prepare` 计时器都不是此类协议。

来源：[第一阶段报告](../reports/nscale-phase1-20261023.md)、[热域与 selector 历史](../reports/hdf5-route3.md)、[共用热池设计](shared-cross-format-hot-query-plan.md)、[训练 prepared 合约](../contracts/arrow-prepared-training-registration.md)。

必须修正上一轮分析的版本对应：摘要实验 `fee-stage-summary-product-20261023/build-binding.json` 中的 `QueryPlanning.cpp` SHA 为 `44c76f910821ba0da14f1e37e9949d042fe89bc9505dc58e7e12e0123371a638`，当前文件为 `be652e780747169a43ca5420804abf7afd0cbc19dc0093438dcbedd494121cc7`；selector header、socket 和 main 也不同，预算摘要 header 相同。已有时延仍是其冻结版本的有效记录，但不能把它精确归因到当前 `PathScopeSet` 的编译次数。后续对照必须从当前实际代码构建 A，明确包含此前已采纳的内容。

## 推荐结构：外部 selector 句柄，内部 resident domain 短引用

外部句柄保存“如何选源”，不保存“上一次选中的版本就是以后所有请求的版本”。

1. **外部不可变模板**：复用 `SourceSelector/PathScopeSet`，保存规范化路径、版本选择、格式、HDF5 `dataset_path` 和固定 `max_sources`。保存其已编译 identity，而非每次重建。模板不保存谓词结果、请求预算、授权结论、值数组、完整执行计划或强引用大候选域。
2. **内部域短引用**：保存当前缓存的 `slot + generation + full head + payload kind`。引用由 backend／原 `HotQueryCache` 创建，客户端不能提供；它只是缓存定位提示。
3. **暖执行**：取得模板 lease，读取 fresh owner view；在原 cache 锁内校验短引用、完整 head、namespace、payload kind 和 resident membership，更新 LRU，再取得本次执行的 strong pin。命中时不重建完整路径域键。
4. **失效执行**：head 更新、短引用 generation 改变或域淘汰时，使用已编译模板走现有完整发现入口，按当前版本选择规则重新得到候选域；必要时更新短引用。这一回合允许构造长键及进行冷建工作。

`weak_ptr.lock()` 成功不足以认定命中：对象可能只是被其他执行持有的退休域。必须核实它仍是 cache 的当前 resident entry。slot 删除／复用递增 generation，禁止溢出回绕造成 ABA。

采用该结构后，**head 更新使内部引用失效，外部 selector 句柄仍可继续表达 Latest**；下一次执行选择新 head 下的 Latest。namespace incarnation、backend 身份或 daemon 实例改变，则外部 ID 失效。请求执行期间发生提交，仍按当前 admission／冲突规则处理，不自动返回先前答案，也不新增隐藏重试。

## 首版接口与范围

首版仅接入当前支持热域的 owner HDF5 `dataset_catalog`，公共注册表和短引用机制不解析 HDF5 属性字段。暂不扩展 GRIB、时间范围、file predicate 或其他尚未认证的热域语法。既有普通查询入口继续承担原有完整功能；不增加协议别名或隐式兼容回退。

示意（省略原有 sources 的完整结构）：

```json
{"kind":"catalog_selector_prepare","request":{"sources":{},"format_id":"hdf5","max_sources":1024}}
{"kind":"dataset_catalog_by_selector","selector_id":"opaque-id","predicate":{},"max_datasets":65536,"max_catalog_bytes":67108864}
{"kind":"catalog_selector_release","selector_id":"opaque-id"}
```

prepare 可包含原有可选 `dataset_path`；成功返回独立 `selector_id` 与 `idle_ttl_ms`。它仅验证静态声明并保存编译结果，不访问 catalog，不执行假谓词，不提前保存授权或构建结果。静态声明错误在 prepare 报告；首次 execute 仍完成真实发现、权限和预算检查。

执行时固定 paths、version selection、formats、format_id、dataset_path、max_sources；改变它们需要新 prepare。尤其 `max_sources` 已参与现有域身份与选择失败语义，不能只在旧域上补一个大小判断。

predicate、max_datasets、max_catalog_bytes 按每个 execute 提交并验证，沿用现有检查阶段和上限，不提升额度。首版只接受原热域已经支持的完整、正确类型、非 null 六属性等值 AND；未支持的 prepared 请求明确报错，客户端可显式使用普通接口，服务端不暗中改协议。

ID 是随机、不透明、实例内有效的注册标识，绑定 UID、daemon、backend／namespace incarnation。每次仍使用 socket 当次真实 UID/GID/补充组构造 principal；同 UID 的组变化不沿用旧凭据，必须重验实际主体。其他 UID、未知或过期 ID 返回不泄露存在性的统一失效错误。ID 不是授权票据，不是 FUSE 路径，不参与科学 case 或稳定结果身份。

## 权限、版本与预算不变量

- 每次 execute 独立创建现有请求预算；prepare 的工作不抵扣后续逻辑费用，也不保存已经计费的 ledger。
- 获取域后继续同一热发现执行函数：selection fees → 原 authorize/admit → manifest fees → catalog byte 限额／catalog fees → 属性索引 → 结果数量限额与编码。原错误先后和计费成功前缀保留。
- 公布响应前继续第二次真实 admission。发现后 typed 取值继续现有精确 path/version 和来源验证。
- fresh owner view／admission 保留冻结状态和提交冲突检查。权限或祖先权限改变、新版本、删除、liveness 改变均按既有 head/proof 失效规则处理；无匹配项也不跳过全候选授权。
- selection／proof／manifest／catalog 的既有批次在每个新账本中继续按原阶段摘要计费，实际 principal 对应正确 proof；ID 查找不构成预算豁免。
- collector 活动、摘要超界、顺序不匹配或预算预检失败时，沿用已有 collector 退出及原序逐项回放，不吞掉权限、预算和存储错误。
- 首版不单独实现一份 handle discovery；普通入口与 handle 入口只在输入绑定、域定位阶段不同，随后进入同一个执行函数。

## 配额与生命周期（已采用默认值）

沿用唯一一个 **64 MiB 热池、16 域、64 主体证明**，不借用训练 registry 的独立 256 MiB 配额。为 Handle 增加独立计数，不将它伪装成 Domain 或 Proof。

| 新限制 | 默认 |
| --- | --- |
| 存活 Handle 总数 | 64，包含已退休但仍有执行 lease 的条目 |
| 每 UID | 16，同样包含退休 lease |
| 单 Handle 保留字节 | 1 MiB |
| 全部 Handle 保留字节 | 8 MiB，是原 64 MiB 的子限额，非额外额度，也非预先占满 |
| idle TTL | 5 分钟，使用单调时钟；另有显式 release、daemon stop 清理 |

登记前先为静态模板、编译对象、长 identity、容器容量、registry node／ID／管理块及新增短引用索引预留可核算的内存；编译附加临时空间也必须有界，分配失败不发布部分句柄并退款。普通帧解析沿用既有 1 MiB 帧限额，帧限额不能替代 retained-object 配额。共享编译对象的同一实际分配只计一次，多个对象或不同 ID 的管理块分别计费。

Handle 只保留小域引用；大域仍由原 cache reservation 管理。本次执行 pin 计在原域预算，不能复制域或重复记同一分配。TTL、release 和 eviction 先取消注册所有权；保留的执行 lease 直到实际对象析构后才退还字节及数量。prepare 响应发送失败撤销新 ID；query 响应失败清理本次资源，不误撤销仍可继续使用的 ID。

注册表锁只用于取 lease／更新小状态，不能持锁进入 owner、cache、proof；大对象在锁外销毁。并发 miss 继续现有 single-flight。旧请求不能覆盖较新 head 的绑定；短引用发布需比较 head／generation。停止时先停止并等待 worker，再清理 registry/cache。

## 复杂度目标及实际边界

令 P 为显式路径数、X 为候选 dataset 数、K 为返回数、U 为费用身份数、r 为摘要阶段数、q 为请求 overlay 数。

- prepare：一次路径输入、规范化、去重、排序和 identity 编译，约 `O(P log P)` 次字符串比较；保留空间与路径总字节数成正比。
- 同 head、同 resident domain 的 execute：固定长度 ID 查找和有界域定位，不再解析 P 条路径、编译 selector 或构造长域键；服务端主要剩 `O(log X + K + q r log U)`，r≤8、q≤128。准确常数和多阶段修正以实现及计数验证为准，不宣称所有工作 O(1)。
- head 更新／域淘汰后的 execute：允许回到原域定位、构建、授权证明及费用处理成本；不重新编译已保存的 selector。quota 不足时保留原普通发现的正确性回退，不能伪造空匹配。
- 完整客户端 d 还含结果解析和来源检查。如果保留 `path in scope` 的列表检查，仍可能有 `O(KP)`；实验两模式必须采用相同检查，不能只给句柄组换成更快验证再称作产品收益。必要时将共同验证改为预构造只读集合，但须对两组对称生效、计入准备、披露基线变化。
- 首次 prepare、首次域构建、失效重建单列；报告重复 Q 次的 `T_prepare + ΣT_query` 及摊销盈亏点。不能把首次准备移出计时后宣称整体加速。

## 确认后验证与实验

先实施通用句柄注册／资源 lease、resident 短引用，再接入 HDF5 原热执行函数及显式客户端协议。基线冻结为实施时实际当前版本（包含阶段摘要），不复用旧费用逐项重放 A。先核验候选的普通入口与基线行为一致。

回归至少覆盖：普通／句柄结果和稳定顺序相同；同一句柄换谓词；空匹配；实际主体／组变化、源及祖先撤权；新版本／删除／冻结；执行和最终 admission 之间提交；预算临界、三类计数与 sticky failure、collector 中途出现；max_sources 固定及两项动态限额；过期／未知／跨 UID／跨 daemon／namespace ID；slot ABA、域逐出但仍被其他请求 pin；并发执行／release／失效；OOM／分配故障、配额先预留、退休 lease 继续计数与最终退款；prepare 响应失败／stop 清理。

性能先用既有封印 N8、N128 两组，不扩 N512、不重新发布语料或建立 FS-index。推荐在同一候选 daemon 中交替“完整显式请求／句柄请求”，沿用相同客户端生命周期、相同判定谓词与 owned values/masks 终点；FS-index 控制保持原科学工作量，记录实际环境、源码／二进制与恢复证据。每档四轮平衡首序 AB/BA/BA/AB，每模式每臂 5 warmup＋10 formal。具体采集闭包须在运行前预登记。

准备、首次执行与暖执行分开记录；完整 T=d+q、d/q、相对 FS-index 的 R/Rd/Rq、均值及块内中位数配对结果同时报告。保留每条记录及负载标记，不因慢值剔除，也不以诊断旧 A 的波动替代候选 B 的验收。

机制计数要求：暖 handle 命中时 selector compile=0、输入路径项=0、long-domain-key bytes=0、short-ref hit=1、全批次费用遍历=0；原授权、proof、两次 admission、实际预算与结果身份均仍可核验。首次／失效回合必须记录 ref miss 原因、域构建和费用回退。

只消除局部处理而完整 T 没有稳定收益，则保留证据，单独报告局部机制通过，不能据此提升为已接受性能基线。用户已确认；实现、验证和实验记录见后续报告，未完成的验证不记为通过。
