# 公共 q：已验证请求与 planning recipe 复用方案

日期：2026-10-24。**状态：用户已确认阶段 A，正在实施与验证；阶段 B 尚未启用。** 下文保留确认时的设计和验收条件，实际实现协议见 [prepared-query-template](../contracts/prepared-query-template.md)；审计资料位于 [public-q-reuse-design-20261024](../../experiments/cross_version/work/public-q-reuse-design-20261024/)。

## 1. 推荐决定

建议分两阶段：先实现 **A：公共不可变请求模板句柄**，GRIB/HDF5 共用解析、声明所有权、句柄配额和执行入口，执行仍走完整原 planner；再独立设计／实现 **B：Adapter 认证的无源能力规划配方（source-free recipe）**，只有证明预算、错误前缀与 collector 等价的格式才启用。

A 的“已验证”仅指**语法、结构和限额声明**通过验证，不代表源存在、字段／类型匹配、拥有权限或版本仍有效。首版保留下层结构复检。B 也不能整体缓存当前 `PreparedQuery`：它持有实际 `QuerySource` 和 `ReadContext`，不是纯编译值。

本次待确认的首轮实施范围是 A，包括 GRIB 两条独立 single 请求、HDF5 一个两项 batch 的客户端接入和 N8／N128 对照。B 是后续扩展设计，**不随 A 自动启用**。本方案不将渲染、解码、FUSE／Arrow 交付、并行化或 q 费用摘要混入 A。

## 2. 历史检查与这次的增量

详细收据和源码核对见 [历史审计](../../experiments/cross_version/work/public-q-reuse-design-20261024/history-audit.md)。当前审计绑定 `f09f3742246be3402aaf7dec764f4a7d7d357740a9fb916e30b3b2ca9bed5774` 对应的 528 文件清单；不是用旧版本性能替代新基线。

| 历史事项 | 实际状态 | 对本方案的约束 |
| --- | --- | --- |
| 10-23 `typed_batch` 已解析请求复用，B=`b26c2aed…` | 删除 batch 子 JSON 序列化／递归解析；N8 完整 T −5.74%，N128 +18.33%、3/4 块慢；已回退 | 直接重合，必须叫重试。首次几块解析只省约 31–47μs；资源干扰不能把回退改写成成功 |
| 不可变 `PathScopeSet`、selection/source 借用 | 当前保留；N-scale phase1 的独立性能未通过，不等于源码已回退 | 它们是基础，不能重复计算为新收益 |
| HDF5 capabilities→prepare 的 `PlanningState` | 同次请求已复用 binding；命中前仍检查 request key | 不能声称当前两阶段各自完整 bind 一次 |
| 10-12 frozen-typed 固定 `PreparedQuery` | 曾做单 exact 源／版本的私有摊销实验；公开实验协议已移除 | 不是“从未试过 plan 复用”；当时源与权限不变的假设不满足当前公共语义 |
| 训练 prepared validation／module | 已保留，私有 trusted factory 与深不可变所有权 | 可借鉴封闭构造和生命周期，不能借用训练收益证明 GRIB/HDF5 收益 |
| 公共 d selector 句柄、GRIB 定位提示及费用摘要 | 当前保留；普通取值 q 仍完整规划 | 本轮扩展到 query-specific 静态声明，不重复宣称 d 已有收益 |
| GRIB render／移动输出、HDF native 三项微优化 | 各自性能验收不足，已回退 | 本轮不恢复，也不与公共 q 合并 |

因此，新增功能是**跨请求的显式静态请求复用**，以及以后有条件的规划配方复用。此前 [共用热查询方案](shared-cross-format-hot-query-plan.md) 的 `QueryDomainCache` 保持 query-independent；本方案将 query-specific 模板放在公共前端 registry 中，未来 recipe 也不塞进候选域索引冒充无关查询的 artifact。

## 3. A：公共不可变请求模板

### 3.1 接口与首版能力

下列名字为待确认的新协议，无兼容别名：

```json
{"kind":"query_template_prepare","declaration":{"kind":"typed","request":{"sources":{},"subset":{},"limits":{}}}}
{"kind":"query_template_prepare","declaration":{"kind":"typed_batch","items":[{"kind":"typed","request":{}}]}}
{"kind":"query_template_execute","template_id":"<opaque-id>"}
{"kind":"query_template_release","template_id":"<opaque-id>"}
```

其中 `{}` 仅表示原合法声明的占位，不能按示例空对象执行。prepare 返回模板 ID 和 idle TTL；execute 返回现有单／批 published route 结果；release 幂等。execute 拒绝所有覆盖字段：路径、版本表达式、predicate、projection、slices、输出类型／插值、physical-array 和全部 limits 都在 prepare 时固定。

首版规则：

- 每条路径 scope 只能 Exact；版本选择仅 Exact 或原格式已支持的 Latest。Latest 保留为表达式，每次执行重新解析，prepare 不冻结 winner。**HDF5 value 仍按原契约要求单 Exact 路径＋Exact 版本，不能借模板放宽为 Latest。**
- batch 固定整个原 `typed_batch` 声明，1–64 项，沿用 Exact 路径／版本及去重源上限 1,024。不会用若干 single ID 临时拼装新 batch；重复项保留原顺序和次数，不能去重结果。
- 不接入自定义 `hdf5_binding`；普通 `typed` 原入口仍支持原能力。新端点明确拒绝该项，不把其 post-authorization 语义检查提前到 prepare。
- 其他声明使用原纯 parser／validator；不新增格式白名单或 schema 探测。file predicate 等原有纯声明冻结后仍由原选择器在执行时处理。不改变普通 `typed`／`typed_batch` 已有语言或错误顺序。
- GRIB 固定组使用两个 single 模板、两次串行 execute；HDF5 固定组使用一个两项 batch 模板。不能通过 GRIB 自动 batch 改变独立预算和失败语义。

prepare 只读取控制帧、创建不可变声明和管理状态；**不扫描 catalog、不调用 authorizer／Adapter capabilities、不读 layout／payload、不建立 source、plan 或结果**。prepare 成功只说明声明可登记。不存在、撤权、坏 schema 等仍在实际执行的原阶段失败。

### 3.2 所有权和公共执行入口

模板持 `shared_ptr<const ImmutableTypedDeclaration>` 或等价深不可变值：已编译 `PathScopeSet`、const predicate AST、所有字符串／数组和静态 limits。构造器仅供原 parser／结构 validator 的私有 factory 使用，带验证契约版本；外部不能传入 `validated=true`，也不能保留可写别名、ptree 借用指针或闭包。

公共执行 seam 接受只读声明和**当次真实 peer**。它承担解析之后的原顺序：

```text
模板 acquire / kind / incarnation 检查
→ fresh catalog selection（完整候选及 Latest 原语义）
→ actual UID/GID/groups 授权
→ 原 execution admission / lease
→ 完整原 QueryModule::prepare（含字段、schema、能力和预算处理）
→ 当次执行读预算／wrapper／cursor 执行和编码（batch metadata／累计预算仍共享）
→ 原 publication admission、route 暴露和后续 FUSE new-open admission
```

该路径每次生成当次 `CatalogSelection`、`ReadContext`、临时 `PreparedQuery`；它们不能回存模板。模板不保存授权证明、peer groups、native 句柄／session、账本、collector、cursor、数组或结果。

普通 single 可以委托这个 seam；**普通 batch 首轮保留目前的 serialize／recursive 调度**，避免不经独立验收恢复旧 b26 优化。模板 batch 直接使用其冻结声明。下层 `register_typed`／core 对内部调用者的原结构复检先保留，不把静态证书扩展成跳过所有校验的凭证。

### 3.3 错误顺序、batch 与 collector

single 和 batch 原来就有不同的静态校验先后，factory 分别保留：single 的 HDF registration strict 在 generic validation 前；batch 预声明是 generic validation 在 registration strict 前。不能为共享函数统一这两种顺序。字段／类型和 Adapter 能力验证仍在执行期授权之后；首版不做源相关的“提前验证”。

batch prepare 先按原输入顺序完成全部预声明，再发布一个模板；execute acquire 并 pin 整个模板，之后才开始 item0。维持原整批 metadata scope／`QueryBatchBudget`，严格串行执行、同一 sticky failure、失败后不处理后项、撤销此前所有 routes/pins；超帧和响应发送失败仍清理本批结果。release／TTL 到期不能破坏已 pin 的输入。它并不让尚未完成权限／执行准入的工作绕过后续检查。

异常映射应在 batch scopes 仍活跃时完成；若先析构 scopes 再 catch，可能看不到已发生的 sticky budget failure。保留原错误 code／phase／成功收费前缀和实际执行项计数，不伪造已被省掉的 parse/compile 次数。

A 未跳过任何 planning 读和收费，所以当前 active／nested／late collector 都沿原 planner 收集；不另抓一份 sorted fee trace，也不额外补收一遍规划费。模板 prepare／release 的管理计时单列，不把它当作执行逻辑费用豁免。

### 3.4 权限与版本新鲜性

ID 只授予对静态声明的使用权。registry 按实际 UID 隔离；同 UID 的 GID／groups 改变、祖先撤权、源删除／更新、无匹配项的权限和错误检查都走当次完整原路径。不能从 d 返回的 locator 或模板 ID 推导读取授权。

模板绑定 daemon/module/backend 实例代次；namespace incarnation 在已有配置或当次 fresh owner 路径核对，不为 prepare 补读源。backend/module/namespace 被替换后不把旧 ID 静默转向新服务；返回原失效类状态或撤销 lookup。正常 full-head 更新不让静态模板失效，也不能冻结 Latest；具体版本及最终发布仍由原 admission 保证。

新增 `query_template_execute` 对未知、过期、已释放、其他 UID 和错误 kind 使用统一无可用句柄错误；新增 release 对这些情况返回未释放，不泄漏存在性，也不能误删另一类句柄。既有 catalog 端点安全判型后保留原错误 code／phase，不顺带改写其现有错误契约。已 acquire 的声明 lease 仅保证内存寿命，后续权限／发布准入仍可拒绝。

## 4. 同池配额和生命周期

**d 和 q 使用现有同一个 `PreparedSelectorRegistry` 实例**，共同受 64 个全局条目／每 UID 16 个、模板子配额 8MiB、单项 1MiB、5min idle TTL，以及唯一 `HotQueryCache` 64MiB 总池限制。不为 q 再开一组额度；64MiB 是此池预算，不是整个进程 RSS 上限，现有 native cache 不变。

现 registry 存 `shared_ptr<const void>`，旧 consumer 直接转换为 catalog 类型。实施必须统一所有 producer／consumer 为一个 tagged envelope，例如：

```text
DatasetCatalogTemplate | FormatCatalogTemplate
| TypedSingleTemplate | TypedBatchTemplate
```

GRIB catalog 和 q 都使用 `QueryRequest`，需要不同 wrapper 表示 kind。所有 execute／release／rollback／lost-response cleanup 均安全判型，旧 catalog consumer 也要改，不能只保护新 q。类型检查和删除通过 expected-payload／generation 条件原子操作避免并发误删，不能检查后再无条件按 ID erase。

配额在编译／深分配前保守预留，含 registry/ID/UID 表、控制块、paths 编译值、AST、字符串、vector capacity、batch 所有项及必要管理状态；checked arithmetic。1MiB 帧不等于 1MiB 驻留，保守上界超额即拒绝新模板，普通查询仍按原能力执行。prepare 不创建 catalog `Binding`，也不应让 q 声明登记能力意外依赖 catalog binding 回调。

pending builder、过期／release／clear 后仍有 in-flight lease 的 retired 对象继续占全局、UID、bytes 和池配额，最后 lease 释放才退账。prepare 响应未送达则 retire ID；clear 不允许未完成 builder 复活登记。停止时按原服务顺序结束连接／workers，再清理 registry；大对象析构和 Adapter／source 调用在 registry 锁外。模板仅保存代次／弱实例标记，不靠强持运行中的模块阻止关停。

## 5. B：后续公共 plan 复用的可实施边界

### 5.1 公共机制与格式职责

B 放在公共 `QueryModule::prepare` 的原位置，**实际 execution admission 之后**，由 `FormatQueryAdapter` 可选导出／重绑接口接入；Core 不按 GRIB／HDF5 字段分支。成功普通 prepare 后才机会式导出，不后台预编译或缓存失败。

| 公共部分 | GRIB 自行认证 | HDF5 自行认证 |
| --- | --- | --- |
| 完整 key、source-free 所有权、共享配额、fresh binding、有序费用效果、checkpoint 和 collector 退出 | parsed index、message ordinals、predicate/output/residual schema 等纯值 | dataset/directory、binding／prepared selection、decoder/config 等纯值 |
| 当次 catalog/auth/source/context/budget/cursor、跨源 schema/order 校验、最终 admission | 重新核对 source/version/revision/layout、kernel/mode，执行仍付 alias／payload 需求和 render | 重新核对 semantic/profile/layout/mode/transport，仍建立当次 native session／cursor及执行 range 需求 |

Adapter 必须声明 artifact 不含 `QuerySource`、planning/read context、principal proof、admission/lease、账本／collector、native session、cursor、结果、请求 callback。`const AdapterPlan` 的类型外观不构成这项证明。借用 immutable index/directory/decoder 必须另有完整驻留计费 pin，不能默认原缓存淘汰后仍替它收费。

### 5.2 命中 key 和失效

key 至少包含验证／recipe 契约版本、UID／policy namespace、module/backend/namespace incarnation、完整 owner head、原 selector/version 意图、按原顺序的全部 exact 候选、每源 revision/format/size/verified-layout 身份、完整 typed fragment 及**全部 limits**、Adapter 实例／配置／kernel／mode／transport 身份。hash 仅加速，使用无歧义编码并保留完整等价检查；不因 size 一样、adapter ID 一样或只比较 head sequence 就命中。

仍先选择和授权全部候选，包括最终无匹配项；新 head、权限证明失效、缺 verified identity、配置变化或未认证 artifact 均不能跳过原检查。B 不缓存授权。失效／不支持／超配额在产生重放效果前选择普通 planner；最终 publication/new-open admission 保留。

### 5.3 预算和 collector 是启用门槛

现 `PreparedQueryMetadataTrace` 是 `identity→max(bytes)`，不能表示整个 planner 的真实操作顺序，也漏掉 `observe_unique`、`QueryReadBudget` 的 range/layout/payload、整批 reserve 等效果。只重放最终 map 会改变首错位置和成功收费前缀，不能用于证明 B。

B 必须有分阶段 **ordered planning effects**，在当次账本按原顺序执行：稳定 identity 的 max 观察、fresh unique demand、range/layout/payload reserve、整批累计／status checkpoint 和必要 fresh-source 校验。一个 `reserve` 包含的内部 unique／batch 副效果只记录一次，不能与子事件重复收费。缓存冷／热状态改变的规划需求也须由认证契约定义，不能把一次 cold trace 当所有 warm 请求的替代。

collector 已活跃时在进入重放前走普通 planner。若重放部分 prefix 后 collector 才出现，**禁止从 prepare 开头重跑**，否则可能重复 unique/range/batch 费用。需要公共 planner 明确每源 capabilities→validate→prepare 等 checkpoint：cached 段只用无 callback 的公共计费原语；在可回调边界检查 collector 链／epoch，出现时从未完成阶段继续原流程，保留当次 context 和已收费 prefix。无法提供这种安全续接、或 opaque hook 可在段内安装 collector 的 Adapter，不得进入 B。

按逐事件重放先证明正确性，再考虑摘要加速；不在 B 首版同时叠跨阶段 summary。配额不足、捕获不完整、构建失败只放弃可选 artifact；若已向当前账本重放了效果，就必须继续已证明的路径或按原失败返回，不能清账重来。

### 5.4 B 的容量和并发

每模板最多保留一个完整 key 的 recipe bundle。模板＋key＋effects＋recipe 及其传递闭包／pins 仍受原 1MiB 单项、8MiB 子配额和 64MiB 池约束；不提高额度。当前 registry 只有 insert 固定预留，没有安全延迟增长 API；实施 B 前需事务式 child allocation account，同时约束 pending、resident、retired 与 UID 账目，不能只调 pool grow。

旧 head recipe 在 in-flight 时仍计费；替换需先预留新对象，超额即不缓存。被 pin 而延寿的共享 artifact，要么有有效独立计费 pin，要么保守计全闭包，允许保守重复计费而不漏账。

构建／Adapter 回调在锁外；同完整 key 最多一个 producer，首版其他请求不等待，走自己的普通 planner。leader 失败／取消不负缓存、不传递其预算给 waiter。发布核对完整 key／generation 未失效；release/clear 后 builder 不得复活条目。上述设计及 Adapter 证明须另行审查，不能因 A 验收通过默认开启 B。

## 6. 复杂度与预期收益

令 L 为请求字节、P 为 paths 数、E 为 predicate AST 节点及静态声明工作量，H 为共享 registry 条目数（≤64），m 为同一模板实际执行次数。现解析／selector 构建约 `O(L + P log(P+1) + E)`，另含字符串／slice／输出声明尺寸；A 将它从每次执行移至一次 prepare。模板 acquire 的有序 map 查找为 `O(log H)`，不是整个管理路径 O(1)：现 accept loop 的 reap 另有 `O(H)` 暖扫描，批量过期可至 `O(H log H)` 加析构。保持既有有界清理，不为无瓶颈证据的此项扩大首版；保留的下层结构复检仍可能随 E 等增长。

```text
原生命周期：Σ [解析/selector构建 + 完整规划/预算/执行/交付]
A生命周期：prepare + Σ [ID解析/查找 + 原结构复检/完整规划/预算/执行/交付] + release
```

A 不降低候选选择／权限／费用依赖 N 的复杂度，不消除 GRIB `Θ(D·E)` 消息筛选、输出 `Θ(R)`，也不消除 HDF5 binding／describe／逻辑 range／输出工作；不能承诺 q 对 N 的斜率下降。B 才可能避免跨请求重复 message predicate/binding，仍保留 fresh identity、逻辑预算、输出和准入成本。

当前固定组 q 的单项 P=1、K=2、GRIB D=4；历史 batch 解析只省数十μs。最近 HDF N128 q≈4.046ms，typed prepare≈0.372ms，native.read≈0.212ms，control 外交付包络≈1.764ms；这些计时有嵌套，不能直接相加。**A 不覆盖交付包络，也不保证 lifecycle 回本。** GRIB 正常块 typed_prepare≈0.417ms；GRIB/HDF5 这两个 typed_prepare 计时从公共 planner 开始，都属于 A 保留的工作，不能当作 A 可节省时间。A 主要省此前 Socket parse／AST／selector 构建，其独占成本另列。详见 [q 模型报告](../reports/q-model-grib-hdf5-20261024.md)。

显式模板优于首轮自动 exact-wire memo 的理由是：后者仍发送／哈希／比较完整 L 字节、需额外 UID/wire 副索引、空白变化 miss；收益证据不足。显式模板可清楚测量 prepare／execute／release，且后续承接 B；其 API 和管理成本也必须计入验收。

## 7. 确认后的实施与验收计划

### 7.1 顺序

1. A 公共 seam、深不可变声明、tagged registry 与管理协议；普通 endpoints 保留原行为，特别是普通 batch 不恢复 b26 调度改动。
2. GRIB/HDF5 客户端按上述原 single／batch 形状显式使用模板；功能、权限／版本／预算／collector、配额和生命周期门槛通过后再计时。
3. 固定封印 N8／N128 新增同轮 A/B，对比当前 f09 机制基线与模板版；不重建 corpus、不引入 N512／新组。
4. A 独立验收后，决定是否继续 B；先完成 source-free／有序效果／late collector 证明，再逐 Adapter 启用和单独实验。

### 7.2 修改位置

| 位置 | A 的改动职责 |
| --- | --- |
| 公共 `QueryRegistrationSocket` | 原顺序的 single/batch 静态 factory、解析后执行 seam、新 prepare/execute/release dispatch、typed payload envelope、发送失败清理 |
| 公共 registry／控制响应／client parser | 同实例配额与安全判型、模板状态返回、ID／route 生命周期；旧 d consumer 一并安全接入 |
| `TypedQueryFrontend`／service | 接收只读声明或其当次副本，沿原验证／admission／planner／执行路径；不接内部 frozen `register_prepared` 快路 |
| GRIB/HDF5 实验客户端 | 根据真实 d 结果管理 single/batch 模板、失效重建、prepare/release 完整计时，数据／输出／调用预算边界不变 |
| `QueryModule` 与格式 Adapter | A 不跳过原 prepare，不改 Adapter planning contract；B 才新增 opt-in recipe／effect／checkpoint seam |

### 7.3 必须覆盖的正确性

| 类别 | 验收内容 |
| --- | --- |
| 声明／结果 | 普通与模板的 schema、值、mask、provenance、源/项/行顺序一致；禁止任意 override；single/batch 各自首错顺序保留 |
| 权限／版本 | 异 UID、同 UID groups 改变、祖先撤权、未命中候选、删除/新版本/Latest、full head、module/backend/namespace 替换；各次原准入不减少 |
| 预算／collector | 精确边界及少一单位、ordered require/unique/range/layout/payload/batch 事件和失败前缀一致；active/nested/late collector；不能仅比最终费用 map |
| batch | 所有预声明优先于 item 授权；累计预算、重复项、空匹配、1/64项、后项失败、异常 sticky status、frame/send failure、全 routes/pins 回滚 |
| 配额／寿命 | d/q 错 kind execute/release、随机ID碰撞、并发 release/TTL/clear/stop、pending/retired 仍计费、last lease 退款、lost prepare response、超配额无未收费旁路 |
| A 机制计数 | 执行不重新做 Socket 声明 parse/selector 构建；实际 planner/授权/admission/逻辑费用及源打开需求不变；不把 QuerySource open 说成物理文件 open |
| B 另设门槛 | 每类事件首错、callback 中 collector 安装、安全 checkpoint 续接、verified-layout/config mismatch、未认证格式 decline、事务增长／传递 pins、失败 producer 不污染他人 |

### 7.4 性能口径与主线条件

只复用 `experiments/fixed_matrix/catalog.json` 已确认 GRIB/HDF5 N8／N128、原封印语料/case/oracle；新 HDF run 用与 GRIB 同一当前源码构建，不拿历史 HDF 50f 与当前 f09 作跨版本 A/B。d 使用当前已有发现句柄、输出与 q 终点不变；两臂保留同期 FS-index 控制、资源、进程寿命和服务恢复证据。按 AB/BA 平衡顺序、多独立块对照，所有首轮、暖轮、慢块和失败都保留。

客户端必须根据**实际 d 结果**准备 q 模板，不能从 oracle 预知 targets。prepare 至多记一次：若放在首个 q 中，lifecycle 不再重复相加；若独立计时，报告不含 prepare 的 warm q，同时报告包含实际准备／释放／失效重建的 lifecycle，且两臂前置发现／预热一致。源版本或完整声明改变时，旧模板不能隐式改写，显式重建成本可见。

分别报告 prepare、首执行、暖执行、失效／release、server parse/prepare/setup/encode、完整 owned q、T=d+q 和生命周期总时长；FUSE/Arrow/owned 边界仅用真正独占计时归因，不能相减后冒认某一组件。设 `T_life=prepare+Σexec+release`（各部分互不重复），与相同实际 m 次普通查询总耗时比较；不额外增加重复次数制造摊销收益。

若模板只改善局部计数而完整 q/T 无可重复收益，或实际生命周期不回本，则不以性能优化名义进入主线，保留原基线和全部候选记录。不能凭 normalized ratio 单独采纳，也不能删除共同慢块后挑选收益；资源异常的全量与预先定义的诊断分组同时报告。

## 8. 本轮交付与待确认项

本轮只新增方案与审计文档；没有产品／测试变更、编译、性能采样、服务操作或 Git 写操作。需用户确认的具体决定是：**先按 A 的整单静态模板、共用配额及 GRIB single／HDF batch 接入实施，再做既有 N8／N128 对照；B 留作独立门槛，不同时上线。**

补充审计：[frontend 顺序与 registry](../../experiments/cross_version/work/public-q-reuse-design-20261024/frontend-design-audit.md)、[core／Adapter 与预算／collector](../../experiments/cross_version/work/public-q-reuse-design-20261024/core-plan-design-audit.md)。
