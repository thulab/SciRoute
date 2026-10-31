# 公共层候选与身份复制检查

日期：2026-10-22。代码基点：`c4de27a`。初始检查为静态审计；随后用户授权实施 P1 三项消重，实施状态见末节。GRIB 渲染候选 C 不纳入主线。下述收益是可删除的工作量，不是实测延迟收益。

## 结论与变量

存在值得优先处理的重复复制与容器构造；可以先利用已有不可变对象，不必新增缓存池或并行算法。优先级为：取消请求内重复复制／未使用集合，再调整跨请求候选数组所有权。

本文用 C 表示**本次入口选中的精确文件版本数**，M 表示准备后命中的源数。它们不是总语料 N，也不等于实验中命中的 dataset/message 数 K。N128 的发现可以有 C=128，后续逐文件 exact typed 取值通常 C=1；不能把一次完整查询里的每个 typed 请求都按 C=128 估算。

## 已确认的重复工作

| 位置 | 当前行为 | 可优化方向 | 范围与优先级 |
| --- | --- | --- | --- |
| `src/query/core/QueryModule.cpp:1062` | `success(*selected_snapshot)` 再复制整份 `CatalogSelection` | 同步 prepare 借用调用者的 const selection；只有自行 select 时持有本地结果 | P1；一次 O(C) manifest 容器及其拥有字段复制 |
| `src/query/frontend/QueryRegistrationSocket.cpp:1482` | publish_route 每次将全部候选构造成 `set<tuple<path,version,format>>` | 延迟到授权 callback 确实需要成员验证时再构造；owner 常规路径无需这份集合 | P1；一次 O(C log C) 插入及字符串／树节点分配 |
| 同文件 `:1778`、`:1635` | typed 和 GRIB format_catalog 授权前从 manifests 重造 source refs | 在同一冻结候选域的 owner 路径引用 `read_context->sources()`；strict 保留独立拥有的列表 | P1；一次 O(C) 身份字符串复制及 vector 扩容 |
| `src/query/frontend/TypedQueryService.cpp:165` | 先从 prepared sources 构造 M 项 refs，随后被 `read_context->sources()` 的 C 项复制覆盖 | owner 使用已持有的 context 提供 source 视图；strict 才生成独立 refs | P1；删除 O(M)+O(C) 冗余构造，需一起整理结果对象的 sources 访问契约 |
| `src/IoTDBStructuredBlobBackend.cpp:3492` | 热缓存命中仍按值返回 `domain->selection` | selection 的候选数组改为共享不可变存储，并保留容量计费与生命周期 pin | P2；每次热命中一次 O(C) 深复制；涉及公共数据结构 |
| `src/query/core/QueryModule.cpp:1119`、`:1165` | 未命中语义域时复制 observed，最后又复制本地 manifest 入 vector | 局部末次使用改 move；有 context 时避免先复制立即被替换的 observed | P3；冷／不支持索引请求的局部优化，不是热域主要收益 |

`QueryResult<T>::success(T value)` 按值接收，因此以上两个 selection 返回点确实会复制，不是仅传引用。`CatalogSelection.versions` 是 vector，manifest 包含拥有的 path、format 和 metadata；权限快照、read_context、hot_state 字段本身已是 shared_ptr，只增加引用计数。owner 候选可能是 metadata skeleton，不能把每次复制都描述成完整文件元数据复制，更不包含文件 payload。

owner 授权在 `src/main.cpp:1494` 直接进入 context 的 admit；上述初始 refs 在当前生产回调中没有被逐项消费。不过接口允许独立授权回调观察完整列表，因此推荐传已有列表引用，而非传空列表或省略全候选授权。

## 已有复用与格式差异

| 路径 | 已有复用 | 本轮机会 |
| --- | --- | --- |
| GRIB `format_catalog` 发现 | `IndexedCandidateDomain` 热命中后，`QueryModule.cpp:1167` 已引用 domain manifests，按匹配源序号准备，并复用 Adapter planning state | 此前的热 selection 返回、prepare selection 复制与初始 refs 重建仍存在；不经过 publish_route，因此没有该处授权 set 成本 |
| 通用 typed 注册／执行 | selected 从注册传入 prepare，结果桥接处多为 move；owner 新 open 已通过 context 准入 | 仍有 selection 复制、publish_route set、service refs 构造与覆盖 |
| HDF5 `dataset_catalog` 热发现 | `IoTDBStructuredBlobBackend.cpp:2955` 起直接引用 domain sources、selection、manifests；只为匹配结果构造输出 | 没有同样两次整域 selection 复制；不能把 GRIB 发现收益直接套用到这里 |
| HDF5 两种正式模式的取值 | `native-hdf5` 与 `iotdb-indexed` 共用 typed 前端 | 都可受益于 typed 优化，但实际规模取决于 exact 请求及批量项数；不是把 HDF5 文件内索引重新缓存 |

`QueryReadContext::sources()` 已返回 const vector 引用（`QueryReadContext.h:52`），在 backend 冷 select 时一次生成并随热域复用（`IoTDBStructuredBlobBackend.cpp:482,515`）。这是复用身份数组的现成基础。`FuseTypedQueryBridge.cpp:208` 已仅在没有 read_context 时复制缓存 refs，owner 新 open 无此复制；不应重复宣称优化。

strict 授权确实仍构造 manifest 查找表、父路径集合和源去重集合（`src/main.cpp:1500` 起），但 owner 在此前已返回。它们不是当前 owner 热查询反复承担的工作，首轮不建议顺带重写 strict 授权。

## 不可改变的边界

1. **候选域完整性。** context 必须保留全部冻结候选及未命中项，不能用 prepared 的 M 项替换 C 项；选择顺序、格式与精确版本绑定不变。旧计划不重新选择 Latest。
2. **成员关系不等于权限证明。** path/version/format 的存在只证明属于同一域；不得替代完整 head、namespace/incarnation、主体及祖先权限的有效性检查。每次执行、发布、新 open 继续准入。
3. **借用必须持有所有者。** prepare 同步局部引用可依赖调用栈；结果缓存、后台执行、跨请求数组必须拥有 shared_ptr/context pin，不能保存悬空 span、string_view 或 selection 裸指针。
4. **原额度不变。** 共享数组、索引及仍被执行引用的退役对象继续计入现有 64 MiB 池；保持 reservation/QueryDomainCache 的存活计费，不引入引用环。请求元数据费用仍按现有语义重放，不能因为少复制而跳过逻辑费用。
5. **不直接清空结果接口。** MaterializedResult.sources 是显式接口；owner 消费者可以改为经保活的 context 访问，但需检查调用方并完善访问契约和测试，不能只删除赋值假设所有消费者都忽略它。
6. **strict 仍可独立工作。** 无 owner context 时保留源列表、成员验证、代际及新 open 授权，不将 owner 证明套到 strict 路径。

## 审计阶段建议的实施与验收顺序

先做 P1 请求内消重，单独验收；再做 P2 共享不可变候选数组，便于区分收益。首轮无需新建共享 membership 哈希池：owner 的 publish_route 集合可以不构造，strict 可以按需构造。对剩余确实频繁的成员查询，再评估复用冻结域查找结构。

功能验收应覆盖：全部候选授权及未命中项、撤权／删除／新 head、旧计划精确版本、不同主体、空结果、严格模式、缓存淘汰后在途引用、预算边界、错误顺序和失败清理。应确认上述构造次数确实消失，而不是将其搬到另一层。

性能验收仅复用已冻结 FM1 语料与 case，先 N8 探针，再独立 N128；记录环境与实际资源。分别观察 GRIB 发现和两格式 typed 取值，主判据仍为同轮完整查询 R/T，d/q 辅助定位。沿用修正的资源执行方式，避免首轮启动状态与 A/B 顺序混淆；局部复制减少不足以证明完整查询加速，明显减慢则回退。此检查没有新运行、没有新增实验数据或性能主张。


## 经用户授权的 P1 实施

本轮只实施 prepare 同步借用、身份数组复用及授权集合按需构造；未实施 backend 热返回的共享 manifest 表示，没有扩大缓存池、变更持久格式或客户端协议。

| 改动 | 所有权与语义 |
| --- | --- |
| `QueryModule::prepare` | 有输入 selection 时仅在同步调用期间借用；自行 select 时持有本地对象。返回计划仍拥有 reader/plan 并保活 context，不保存 selection 地址 |
| Socket typed／format_catalog 授权 | owner 借用同一 context 的全部 source refs；无 context 时构造自有列表。自定义 authorizer 仍收到全部候选并实际执行 |
| `TypedQueryService::MaterializedResult` | 自有字段明确为 `owned_sources`；`sources()` 在 owner 下返回保活 context 中的数组，在 strict 下返回自有数组。保留原 move-only 结果句柄，不添加兼容别名 |
| `FuseTypedQueryBridge` | 移动 strict 自有数组并独立保留 context；owner 缓存命中／新 open 仍按原规则准入 |
| `publish_route` | membership 集合推迟到授权 callback 实际消费时构造；strict 身份不属于已授权集合时仍执行原重新授权逻辑 |

费用轨迹重放、规划／执行预算、粘性失败检查、执行／最终发布／新 open 准入均未删减。未将命中源集合当作全候选集合。新增 profile 计数区分借用／构造的 refs 数量、membership 构造次数和条目数，仅用于核对机制。

针对性回归覆盖：selection 释放后计划执行、owner 来源视图在 move 和外部引用释放后存活、未命中候选仍保留／可触发拒绝、每次注册仍调用 authorizer、strict 身份变化触发重新授权，以及 owner 不构造未使用 membership。原有预算耗尽、撤权、失败清理和新 open 测试一并回归。

独立源码快照与构建／测试证据目录：服务器 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/shared-candidate-reuse-20261022/`。本地对应 `experiments/cross_version/work/shared-candidate-reuse-20261022/` 保存输入摘要、补丁及验证收据。使用现有 Arrow/HDF5/IoTDB SDK 依赖、Release 干净构建、nice 10、CPU2/3、-j2；不覆盖历史构建、不安装替换运行服务、不操作数据库或语料。

验证结果：产品及完整单元测试可执行文件的 Release 干净构建通过；**1,833 项单元测试通过，0 failure、0 error、0 disabled**，模块依赖方向检查通过，`git diff --check` 通过。构建约 665 秒，测试约 38 秒（验证耗时，不是科学查询计时）。源码快照包含 1,002 个文件，核对当前文件全部与冻结摘要一致；独立代码审查无阻断发现。

- 源码清单 SHA-256：`80ceb293bb76098bf212c4003cba4c8764087ef4946527de7e26d30bf33c226f`。
- 产品二进制 SHA-256：`d52c9b1edfb618efa27834e4464248995bbe0976a8b4d4298ad8a434d82bdd20`。
- 单元测试 XML SHA-256：`a225c93a6620ef2c92a75e1fad818d1e2728a2ccf99d53505ffb10152e4ffa53`。
- 收据：`verification.json`、`build-status.json`、`all-unit-tests-status.json`、`all-unit-tests.xml`；完整构建／测试日志保存在上述服务器目录。

实现阶段未包含性能采集。随后已按用户确认的4.3四个固定组，与最新已接受基线完成480条A/B验收；详见 [公共候选复用验收报告](shared-candidate-ab-20261022.md)。功能及复用机制通过，但完整R/T未显示稳定收益，因此保留B候选，不更新已接受基线A，也不将本轮记为稳定性能提升。服务器运行服务未替换，GRIB渲染C仍未恢复。

2026-10-22 后续状态：用户批准单独验收批量解析优化后，上述B的9个源／测试文件及完整补丁已归档至 `experiments/cross_version/work/batch-parse-reuse-20261022/previous-candidate/`，并从工作产品代码移出；恢复内容逐一核对已接受A收据。历史实验与服务器独立B构建保留。
