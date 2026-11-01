# 公共不可变 typed 请求模板

Stage A；协议版本由当前 daemon 定义。模板保存静态声明，不保存 `PreparedQuery`、权限证明、源能力、账本或结果。设计依据为 公共 q 复用方案 (local archive: `docs/deliverables/public-q-request-plan-reuse-plan-20261024.md`)。

## 协议

`query_template_prepare` 的 `declaration` 是一份完整原 `typed` 或 `typed_batch` 请求：

```json
{"kind":"query_template_prepare","declaration":{"kind":"typed","request":{"sources":{"paths":[{"kind":"exact","path":"/data/a"}],"versions":{"kind":"exact","exact":7}}}}}
```

成功返回 `kind=prepared_query_template`、不可预测的 `template_id` 和 `idle_ttl_ms`。prepare 仅进行原语法／结构校验和管理配额预留；不扫描 catalog、授权、读取源、规划或创建结果。字段／schema／类型／Adapter 能力错误仍在执行时出现，登记成功不是可读权限或源有效性证明。

```json
{"kind":"query_template_execute","template_id":"<id>"}
{"kind":"query_template_release","template_id":"<id>"}
```

execute 返回现有 `published_route` 或 `published_routes`。release 返回 `kind=released_query_template` 和布尔 `released`；重复 release 返回 false。三个操作均通过控制 API，path-only registration convenience API 不接收管理操作或模板 execute（execute 可能返回多条路径）。无旧协议别名或隐式兼容回退。

## 声明边界

- 路径 scope 必须 Exact。single 版本为 Exact 或原格式支持的 Latest；Latest 在每次执行重新选择。HDF5 value 仍受原模型单 Exact 路径／Exact 版本约束。
- batch 冻结完整 1–64 项，全部 Exact 路径／版本，最多 1,024 个去重源；原输入顺序和重复项保留。不能用若干 single ID 动态拼 batch。
- 不支持自定义 `hdf5_binding`。原普通入口保持原能力。
- 整个 request 和全部 limits 固定；execute 只接受 kind 与 ID，拒绝 predicate、projection、slice、output、source、limit 等覆盖字段。
- single 和 batch factory 分别保留原 HDF strict／generic validation 顺序。动态字段／schema 校验不提前到权限检查之前。

GRIB 客户端仍执行原两个独立 single；HDF5 仍执行原整个 batch。普通 `typed_batch` 的子 JSON 往返路径保留，模板 batch 才直接使用冻结声明。

## 执行语义

每次使用实际 socket UID/GID/补充组重新进行 catalog 选择、完整候选授权、原 execution admission、完整 planner、当次执行读预算及新 cursor、最终 publication admission 和后续 new-open admission。模板 ID 与 registry lease 仅用于声明所有权和生命周期。

batch 使用原整批 metadata scope 和 `QueryBatchBudget`，严格串行执行。任何失败停止后项，撤销此前全部 route/result；响应超帧／发送失败同样回收。模板 release/TTL 不破坏已 acquire 的声明 lease，后续原权限和新鲜性检查仍可拒绝。

规划的原预算／range/layout/payload/unique 费用和 active/nested/late collector 路径均保留；不重放或汇总整个 plan 的费用，也不从头重新收费。

模板绑定当前 module/backend/catalog/storage 实例与已有 namespace 身份；不把旧 ID 静默转向替换后的服务。正常 head 变化按原路径重新选择／准入，Latest 不冻结。stop 清理所有 ID。

## 同池配额与释放

与 d selector 共用一个 registry：64 全局条目、每 UID 16 条、8MiB 模板子限额、单项 1MiB、5 分钟 idle TTL，且共享同一 64MiB 热池。深分配前保守预留所有模板、AST、paths、字符串和管理开销；帧可接收不代表模板一定符合驻留上限。pending 与 retired/in-flight 继续占配额。执行route及从route单独保留的不可变request指针，均持有原模板lease；删除ID／route或stop不使仍被共享的AST/compiled paths提前退账，最后引用销毁才退款。

全部 d/q payload 使用统一 tagged envelope，execute/release 安全判型。删除同时匹配 ID、UID、对象及共享所有权，避免 ID 复用误删。新模板 prepare 响应发送失败撤销登记；后台构建不能在 clear 后复活 ID。新 q execute 对未知／过期／已释放／其他 UID／其他 kind 返回同类无可用句柄错误；既有 catalog endpoint 的错误契约保留。

## 计时与收益声明

prepare、首次 execute、暖 execute、失效重建和 release 分列；完整生命周期包含所有管理成本，各项只计一次。模板只省控制层重复声明构造／解析／selector 编译，不省完整 planner，也不承诺 N 阶数下降或短生命周期回本。

`query_template.*` 和 `selector.compiles` 等诊断计数表示真实机制。模板 batch 仍报告 `typed_batch_items` 与每项 profile；它们不代表跳过原收费或把源打开次数等同物理文件打开次数。
