# FS 公共查询任务调度器实现与验证

## 实现范围

`QueryRuntime` 新增格式无关的 `QueryTaskScheduler`，为独立 FS 控制请求提供固定工作线程、有界 FIFO 队列和就绪任务批量派发。控制 socket 的四个 transport worker 完成对端认证与 frame 接收后异步入池，随后继续收包；查询执行不再占用 transport worker。

默认配置：

```properties
query.tasks.worker_threads=4
query.tasks.max_queued_tasks=64
query.tasks.max_batch_size=4
```

未指定 batch 时取 `min(4, worker_threads)`。worker 为 1–256，等待容量为 1–65536，batch 为 1–worker。等待容量不含已分配给 worker 的任务；队列满返回结构化 `ResourceExhausted`。

dispatcher 每次派发最多 `min(空闲槽位, batch 上限, 就绪任务数)`，不设置等待凑满窗口。C++ `submit_batch()` 原子接纳多个独立任务，逐项返回 future；不合并查询结果或 SQL。

本轮没有拆分单个查询、GRIB Message 或查询空间，没有调整格式内部已有并行度，也没有改动 IoTDB 查询线程池。一个 `typed_batch` 或 prepared batch 仍作为一个任务，其 items 按原顺序共享预算、错误次序和整体回滚。一个同步客户端始终只有一个在途请求时，不会因池容量为 4 就自动产生四个查询。

## 语义与生命周期

- 请求 metadata/profile scope 在实际执行线程中建立和恢复。版本选择、权限检查、执行 lease、查询预算和结果发布继续经过原查询路径；不复制四份预算或重做四个独立版本选择。
- 泛型任务入场前检查共享取消信号与单调时钟 deadline；运行中采用协作取消，原样返回任务状态和错误详情。异常转换为 `Internal/Execution`，后续任务继续运行。
- socket 沿用现有取消边界：没有新增断连取消协议，也不将客户端响应超时当作服务端 task deadline。已接纳请求继续运行，未交付结果使用原回滚路径清理。
- server 停止时先结束接收线程，再等待本 server 的请求清理，最后清理注册表。它不停止其他调用者共享的 runtime。runtime 先排空任务池，再销毁缓存与 admission 服务。
- profile 增加任务池配置及 `query_tasks.queue_nanoseconds`。原有执行时长不包含排队时长；排队单独记录。

具体接口和限制见 [契约](../contracts/query-task-scheduler.md)。

## 验证记录

本地证据目录为 `.tmp/query-task-scheduler/`。Linux 独立源码与构建目录为：

```text
root@192.168.99.49:
/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/query-task-scheduler-20261024-r1/
```

参与构建的源码包 SHA-256 为 `4544473477922ca2c26a1e0fc1077693a7ba918c5fd26cc150e2b10377ac6b28`，基于工作区 HEAD `04db6383f5587c58a90d80f87d195ffadb654ea3` 加已有 owner reader pool 改动和本轮改动。包内所有文件由 `source-manifest.json` 记录哈希；后续仅补充文档，产品和测试源码未变。

Linux 使用仓库正式 CMake 构建目标 `iotdb_fs`、`unit_tests`，Release、GCC 13、锁定 Arrow 21.0.0 和线程安全串行 HDF5 1.14.4。构建只写独立目录，不替换运行中程序，不修改数据库配置，不挂载或改写实验语料。

| 检查 | 结果 |
| --- | --- |
| Linux 产品构建 | `iotdb_fs` 与 `unit_tests` 成功 |
| Linux 全量单元测试 | 1946/1946（160 个测试套件） |
| Linux 任务池启动配置 | 41/41，无数据库连接或挂载 |
| Linux owner reader 配置回归 | 12/12，无数据库连接或挂载 |
| macOS 新增定向测试 | 20/20：18 项核心调度 + 2 项 socket 集成 |
| macOS 较大前端/缓存/版本回归 | 416/417；唯一失败为本机 HDF5 不满足线程安全串行 1.14.4 的既有要求 |
| 最终源码 ASan/UBSan | 18/18，无报告 |
| 最终源码 TSan | 18/18，无报告 |
| 模块依赖静态检查 | 通过 |

socket 集成测试通过条件变量屏障确认 1/2/4 个请求实际同时进入执行，额外两个完整请求已排队；并逐字节比较各并发度下的 Arrow IPC 输出。还检查了满队列的结构化错误、停止排空和停止一个 server 后共享 runtime 仍可用。核心测试验证实际批次上限、单任务即时派发、原子批次拒绝、FIFO、取消、过期、错误详情、异常隔离、TLS 与请求 capture 清理。

保留了前序失败记录：本地旧预编译 GoogleTest 与当前 C++ 运行库混用导致测试框架异常，改用同源码工具链构建 GoogleTest 后定向测试通过；沙箱阻止 Unix socket 的首轮记录保留，放行本地 socket 测试后通过。Apple arm64 不支持 LeakSanitizer，最终 ASan 使用 `detect_leaks=0`，address/undefined 检查仍启用。

本轮没有运行正式性能实验，不据上述并发正确性检查主张吞吐量或端到端提速。未提交或推送 Git。
