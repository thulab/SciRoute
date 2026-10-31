# Prepared training registration

2026-09-30。目标是让标准随机采样的 minibatch 复用已准备的 snapshot、coverage index、view 和训练 module，并避免每次发送完整声明。用户已批准该协议；现已实现准备对象、按 ID 执行与规范化通道表。实验记录见 [三组消融](../reports/public-training-prepared-registration-20260930.md)。

## 控制接口

```json
{"kind":"window_prepare","snapshot":{},"view":{},"limits":{},"options":{},"execution":{}}
```

准备时验证完整声明、冻结精确源版本并创建不可变训练 module，返回独立 `prepared_id`。响应应明确携带 `prepared_id`、snapshot fingerprint 和 idle TTL，不能把 ID 冒充 FUSE path。

```json
{"kind":"window_execute","prepared_id":"...","request":{"coordinates":[],"max_windows":2}}
```

执行仅接受 ID 和窗口请求，返回既有不可变结果 FUSE path。snapshot/view/limits/execution 配置不能由后续执行覆盖。现有一次性 `window` 操作保留为独立查询方式，并用作不复用的实验对照。

```json
{"kind":"window_release","prepared_id":"..."}
```

客户端结束读取器时显式释放；已取得执行 lease 或已打开结果 handle 的操作按原有生命周期完成。过期 ID 必须明确返回失效，不能静默重新选择 latest。

## 生命周期与边界

- ID 使用进程内随机不可猜测 token，绑定 owner UID 和该 daemon 的 storage/runtime；不参与 sample ID 或稳定 fingerprint。
- 每次执行使用当次 socket 的 UID、GID 和补充组授权。对实际访问的 shard 保留精确 path/version、namespace、manifest revision、payload/layout size 校验，不扫描并重建全部 coverage，也不替换为 latest。
- 注册表限制总条目、单对象及总元数据预算，并有 per-user 配额、idle TTL 与显式释放。准备过程的并发和临时内存也需受限。
- 计费覆盖 snapshot 的 rows/channels/strings/timestamps/manifest metadata、coverage spans/map、view/statistics 及容器容量。JSON 帧大小不能替代这些对象的内存预算。
- Frontend route 和执行可能比 prepared ID 存活更久。通过持有内存 charge 的 lease（可用 aliasing shared_ptr）传递 module，最后一个持有者释放前不退还预算；退休但仍驻留的对象也算入容量。
- prepare 响应发送失败撤销刚创建的 ID；execute 响应失败只清理该结果 route；不撤销其他 minibatch 需要的准备对象。
- 每次 execute 仍会产生结果 route。需要明确客户端消费后的结果释放，或在结果不可再打开时退休 route，避免长 epoch 在 5 分钟 TTL 前触及 1,024 个前端注册上限。

## 验证与测量

先用相同二进制和配置分别测一次性完整声明与 prepared ID；固定 source/version、标准 HF sampler、seed、窗口轨迹、缓存容量和并行度，比较冷/暖 50-step。首次准备必须 lazy 地发生在第一个 minibatch 内，或将其耗时明确加回端到端时间；不能把准备移出计时而宣称提速。

Profile 应将 `window_prepare` 和后续 execute 分开记录，同时保留 operation、prepared_id 和发布结果 path，以便无歧义关联。记录完整声明/执行请求字节、客户端序列化、socket round trip、首次准备、每次执行查找/授权、payload、IPC 与 loss/轨迹一致性。

当前执行仍调用通用 planner/assembler 的全量校验，约有两次完整 snapshot fingerprint 计算。是否进一步引入仅能从已验证不可变对象构造的内部执行状态，应根据细分 profile 决定；不能简单删除公开 API 的校验。

必测身份隔离、TTL、并发 execute/release、源版本删除或变化、失败响应清理、退休 lease 仍计费、释放后的预算恢复、长 epoch 的 route 生命周期，以及多次 execute 不再重建全量 snapshot/index。

## 规范化声明

`snapshot.channel_tables` 保存共享通道表，例如：

```json
{"channel_tables":[{"id":"channels-0","channels":[
  {"field_id":"target","original_ordinal":0,"physical_ordinal":0}
]}],"rows":[{"channel_table_id":"channels-0","declared_length":8192}]}
```

示意省略 row 的 source/batch/row/steps 等必需字段。表内 channel 沿用原通道字段；省略 `length` 时必须继承所在 row 的显式 `declared_length`，显式 length 则保持原值。因此最后一个短 row 不需要复制整张表。row 的 `channels` 与 `channel_table_id` 互斥。未知/重复表 ID、重复通道、缺少继承长度均拒绝。每行最多 4,096 通道，展开后总计最多 1,000,000；模板与各行复制的 channel/字符串另有 64 MiB 保守解析预算，先预检全部行，再展开，防止小声明放大内存。共享表只压缩传输表示，不改变 snapshot fingerprint、语义或服务端展开模型。

## 已实现的资源边界

默认最多 64 个驻留准备对象、每 UID 16 个、单对象 64 MiB、总预算 256 MiB、idle TTL 5 分钟。元数据预算是保守的容器容量估算，不等同于进程 RSS。snapshot/index/module 构建阶段单并发；进入该阶段前的 JSON 解析仍由有界 socket worker 处理，JSON 帧另受既有长度上限限制。控制端执行仍采用公共 planner/assembler 校验。

每个准备对象最多跟踪 1,024 个结果 route。客户端关闭 IPC mapping、复制完成后发送：

```json
{"kind":"window_result_release","prepared_id":"...","path":"返回的完整 FUSE 路径"}
```

该操作只释放此 UID、此准备对象产生的对应 route。释放/过期整个准备对象也会清理其 route；已打开 handle 保持有效。客户端 close 显式 `window_release`。ID 过期不会自动重新准备或切换源版本。

C++ `submit_query_control` 返回明确的响应类型；路径客户端 `submit_query_registration` 仅接受产生结果路径的操作。Python 基线默认 `prepared_compact`，也可显式 `--registration-mode full` 或 `prepared` 做对照。
