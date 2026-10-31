# 有界异步 result-fetch：实现与正确性验证

2026-10-25。实现及正确性验证完成；本文不主张吞吐或查询延迟改善。已验收的性能结果单列于 [N128 探针报告](result-fetch-n128-probe-20261025.md)。

## 协议与实现

注册 socket 新增显式请求 `{"kind":"result_fetch","version":1,"path":"已发布的绝对结果路径"}`。请求继续使用 uint32 网络序长度加 JSON。成功响应为同样封帧的 `{"ok":true,"kind":"result_fetch","version":1,"size":B}`，紧接恰好 B 字节的既有 Arrow IPC。该操作只打开已发布的确切不可变结果；不重新选择版本、执行查询或重建已淘汰结果。原 FUSE 接口和注册响应保持可用。

主要实现文件：

- [BoundedResultSender.h](../../include/iotdbfs/query/frontend/BoundedResultSender.h)／[实现](../../src/query/frontend/BoundedResultSender.cpp)：专用 poll I/O 线程、非阻塞发送、唤醒管道、预约和清理。
- [QueryRegistrationSocket.h](../../include/iotdbfs/query/frontend/QueryRegistrationSocket.h)／[实现](../../src/query/frontend/QueryRegistrationSocket.cpp)：请求识别、当前 socket principal、公共调度器上的授权、exact-result 打开及生命周期交接。
- `experiments/fixed_matrix/result_fetch_client.py`：现有单事件循环异步接收；每条查询仍依次执行 catalog、typed、fetch、owned copy，再提交下一次 typed。没有新增客户端 worker、线程执行器或查询内拆分；Arrow 解码与 owned copy 仍在该事件循环执行。

## 三个独立边界

| 边界 | 生命周期及上界 |
| --- | --- |
| 公共查询执行槽 | 负责解析与新一次授权；成功交接后按原调度器顺序释放 task captures、资源和执行槽。I/O 线程不持 native reader／producer 执行 lease |
| 结果 pin 与交付额度 | 最多16个 fetch，从进入公共任务队列前预约，持续覆盖授权排队和发送；传输持不可变结果 handle、连接和 request completion pin，发送完成或失败后才清理。结果 pin 继续计入原 TypedResultStore resident budget |
| 数据暂存 | 服务端每个传输最多64KiB数据块及4KiB响应头；每轮每个可写连接发送至多64KiB。客户端按已校验长度接收，单结果不超过64MiB、读取块64KiB，并沿用原查询在途数限制；原始 IPC 与 owned 数组仍有各自内存占用 |

释放执行槽不等于释放结果预算；服务端发送完成也不等于客户端 owned 数组已就绪。完整查询计时仍延续至后者。

## 保留的语义

每次 fetch 使用当前 socket 凭据和 `FuseTypedQueryBridge::acquire_exact`，检查注册、确切 result ID、版本／head、权限及 owner 依赖；执行新的 metadata admission 和费用重放，保留 sticky budget 失败。授权完成后只传递不可变字节，已打开的结果遵循现有 open-handle 语义，不在每个数据块重新授权。

交接前错误使用原结构化 QueryStatus 响应；交接后若断开、取消或超时，则终止本次传输并释放其 pin，不把后续 JSON 拼进部分 IPC，也不撤销其他调用者共享的已发布 route。无进展超时为3秒，与原 socket 发送超时一致；持续进展会重置计时。注册响应写失败的既有回滚保持原样。

shutdown 先停止传输并 join I/O 线程，再等待 outstanding request pins 清零，之后清理注册表。连接／completion／handle 析构在 sender 状态锁外进行；排队中的任务不能在 sender 停止后安装孤立传输。原调度器的取消、deadline、资源争用和 drain 路径保持。

## 实际验证与证据

| 检查 | 结果 |
| --- | --- |
| Linux r4 Release 产品与全量单测 | 构建完成；**2010/2010** 通过 |
| Linux fetch focused／压力 | **20/20**；10轮共 **200** 次通过 |
| macOS sender 初版 | ASan＋UBSan 10轮 **140/140**；TSan **14/14** |
| macOS sender 测试观察修正后 | 慢读测试压力 **100/100**；ASan＋UBSan **14/14**；TSan **14/14** |
| 本地 owner／授权 socket 回归 | **16/16** |
| Python 客户端及离线检查 | 客户端 **26/26**；另外两组离线检查 **6/6、16/16** |

覆盖实际 socket 部分写、慢读时其他连接进展、额度争抢、取消／断开／超时、stop、resident pin，以及 owner principal／依赖、费用重放、sticky 预算和已开始传输不持 producer lease。macOS 检查不替代 Linux 产品和全量验证。

Linux r3 首次 focused 有一项测试失败：快连接的 `completed` 计数已更新，但其锁外析构尚未归还 reservation，测试立即期望 `reserved==1`。r4 仅将该测试改为等待同一快照满足 `completed==1 && reserved==1 && active==1`；生产代码未因此改变。其他测试观察点已检查。原失败日志保留；早期测试 fixture 构造修正也保留在 r1 记录中，未用通过结果覆盖失败历史。

证据根目录为 `experiments/cross_version/work/result-fetch-20261025/`：`implementation-inputs-r4.json` 绑定输入及必测集合；`local-checks/` 与其 `r4-local-checks/` 保存命令、源码 SHA、日志及 XML；`r1-test-fixture-correction.json`、`r3-focused-failure.log`、`r4-test-race-correction.json` 保存修正历史。多轮测试 XML 只含末轮，完整次数依据逐轮日志。

远端最终产品目录为 `experiments/cross_version/work/result-fetch-20261025/product-r4/`，binary SHA256：`4e10d45ab1b64f63d6f397f42970f8420f845781b3207a3a2007ac9b6bfb4545`。对照基础为已接受的定向唤醒版 `4caa446dc2dadbec9227fbdafd9062425a3ae56910c05c96352e341e98e40cb7`。构建、正确性与性能是分别验收的证据，本文仅覆盖前两项。
