# 服务端分段计时与合并发送

2026-10-25。实现及 Linux／本地正确性验证已完成。性能收益以另行验收的 [N128 探针报告](result-sender-n128-probe-20261025.md) 为准，本文不根据代码变化推断提速。

## 架构与边界

`BoundedResultSender` 以 `sendmsg/iovec` 合并剩余长度前缀、JSON header 和不可变 IPC body。部分写后按实际返回字节数推进跨段偏移；每轮每个连接的全部 iovec 合计最多64KiB，header 仍最多4KiB。`TypedResultStore::Handle::slice` 返回同时持有 payload 和 accounting state 的只读 pin，字节只在 scoped callback 内使用，删除原 body 临时 vector。普通 socket 发送仍涉及内核复制。

`query.result_fetch.sender_threads` 可取1或2，默认1。每个 reactor 有独立队列、锁和非阻塞唤醒管道，每个 reservation 固定归属一个 shard，取消只唤醒该 shard。**全局最多16个 fetch**，覆盖公共任务队列中的授权和整个发送过程，不随 sender 数增加。公共查询槽按原顺序释放；结果 resident budget、连接和 request completion pin 独立保留。未增加客户端 worker，也未引入查询内并行。

每次 fetch 继续执行确切 route/result、当前 socket principal、版本／head、依赖权限和 metadata fee admission；sender 只访问已经授权的不可变字节。错误前置响应、断开／取消／3秒无进展超时、共享 route 不因 fetch 失败而撤销、注册写回失败回滚均保留。stop 先 join sender，再等 outstanding request guard 清零，最后清理注册表。连接、payload 和 admission 的释放不持 sender 锁。

实现和协议见 [BoundedResultSender](../../src/query/frontend/BoundedResultSender.cpp)、[TypedResultStore](../../src/query/frontend/TypedResultStore.cpp)、[socket 接入](../../src/query/frontend/QueryRegistrationSocket.cpp) 和 [result-fetch 合约](../contracts/result-fetch.md)。

## 分段诊断

`IOTDBFS_RESULT_FETCH_TRACE_JSONL` 按需记录 accept、transport 开始、frame 就绪、公共任务提交／开始、exact acquisition／授权开始和结束、sender 入队／出队、首次／全部成功发送返回以及清理尾点。增加发送线程 CPU、调用数、字节数、EAGAIN 和部分写计数；最多64个调用区间同时记录 requested bytes、返回值和 errno。缓冲最多8192条，path 最多4096字节；超界或分配失败只丢弃诊断并计数，停止并 drain 后统一输出，普通 query profile 不增加 fetch 行。profile 的 `runtime_config.result_sender_threads` 来自实际传入 sender 的配置。

`last_send_ns` 表示内核接受全量字节，不表示客户端收到或 owned copy 完成。成功传输的 `cleanup_ns` 只表示 sender 自有引用已释放，其他 task capture／缓存仍可能持 pin；错误行的该字段仅为错误处理尾点。`task_start-submit` 含少量提交准备开销，`admit_end-admit_start` 包括整个 exact acquisition，不是纯授权 CPU。调用区间重叠不能证明物理 CPU 并行；线程 CPU 也含诊断时钟的小量开销。主性能探针关闭详细 tracing，诊断另跑。见 [接入只读审查](../../experiments/cross_version/work/result-sender-20261025/local-checks/socket-review.md)。

## 正确性验证

证据根目录：[result-sender-20261025](../../experiments/cross_version/work/result-sender-20261025/)。

| 检查 | 当前证据 |
| --- | --- |
| macOS sender＋store r6 | 普通 **37/37**；ASan＋UBSan **37/37**；TSan **37/37**，见 `local-checks/*-r6.{log,xml}` |
| 新增8项 r5 随机压力 | 100轮，共 **800次通过**；包含1000次双 shard cancel／stop／restart 循环，见 `local-checks/stress-r5.log` |
| 本地实际 frontend／Unix socket | **9/9**；新增3项20轮随机重复，共 **60次通过**，见 [receipt](../../experiments/cross_version/work/result-sender-20261025/local-checks/result-sender-timing-socket-receipt.json) |
| Linux Release、配置入口、全量／focused／压力 | Release 构建通过；全量 **2021/2021**、focused **46/46**、10轮 **460次**、启动配置 **16例** 通过；证据见 `product-r1/build-report.json` |

覆盖 prefix/header/body 边界和小轮预算、实际部分写、慢读不阻塞其他连接、全局额度、取消／超时／停止、共享结果驻留 pin、诊断失败隔离、trace 延后输出及既有授权／sticky metadata 回归。macOS 结果不能替代 Linux 产品验证。

早期本地检查保留了超时、取消和部分写失败，以及 stop 阻塞采样。采样定位到 Darwin `sendmsg` 仅使用 `MSG_DONTWAIT` 时仍会阻塞；r5 在成功 handoff 后由 reactor 显式设置 `O_NONBLOCK`，避免影响即时拒绝时的原错误写回。r5 正常路径通过上述800次压力。r6 仅补充线程部分启动失败时，在关闭 wake fd 前同步 drain 所有停态 shard；普通／ASan＋UBSan／TSan 的37项均已重新通过，未重复800次压力。线程创建失败本身未故障注入。失败历史、最终六文件 SHA 和细节保存在 `local-checks/notes.txt`、`hang-sample-r1.txt`、`base-tests.*` 和 `final-r6-source.sha256`。

socket 首次在本地 sandbox 内因无法创建监听 socket 而9项失败；允许实际 Unix socket 后9项及60次重复通过，原记录保留。没有数据库连接、挂载或服务重置。本次冻结输入及必测集合见 [implementation-inputs.json](../../experiments/cross_version/work/result-sender-20261025/implementation-inputs.json)；对照为已接受的 result-fetch 产品 `4e10d45ab1b64f63d6f397f42970f8420f845781b3207a3a2007ac9b6bfb4545`，新产品 binary SHA256 为 `5862f9335e413503d772199e0d87156e658b3db9e01549ed25b34f5ed10335bb`，源码清单与二进制、测试 XML 同时绑定于完成的 Linux build report。
