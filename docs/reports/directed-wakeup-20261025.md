# FS 公共调度器定向唤醒：实现与正确性验证

2026-10-25。实现、macOS 调度器验证与 Linux Release 产品／全量单测均完成。本轮没有部署候选版、重启服务、改变数据、增加客户端 worker 或引入查询内并行；未复跑 N128 性能，尚不主张吞吐或延迟改善。

## 改动

生产改动仅 `src/query/core/QueryTaskScheduler.cpp`，另新增四项调度器回归及更新 `docs/contracts/query-task-scheduler.md`。其余 owner reader pool、资源感知调度、GRIB 单 message 直接执行与查询路径保持原源码；独立构建从已接受的上一版源码快照出发，严格核验仅上述三个文件不同，原快照未改。

- dispatcher 改用独立 `dispatcher_ready` 条件变量；每个固定 Slot 增加一个仅由对应 worker 等待的条件变量。线程数量、公共配置和 stats 定义均保持。
- 两个派发分支（普通队列／Parked 重新取得资源）在设 Ready 后只通知该槽。共享 scheduler mutex 继续保护状态与所有等待谓词；worker 晚于通知进入等待时仍能观察 Ready，不依赖通知积累。
- submit、完成与资源重新分类只通知 dispatcher；不再广播唤醒所有查询 worker。资源不可用时仍按原 10ms 间隔探测外部释放、取消和 deadline。
- 正常 shutdown 仍停止接纳并 drain，dispatcher 在 pending／active／parked 全空时设置退出谓词，再逐一唤醒全部 worker。部分构造失败时设置同一退出谓词并唤醒已启动 worker；重复 shutdown 仍由 shutdown mutex 串行化。

没有将原共享条件变量简单替换为 `notify_one`：不同 worker 有不同 Slot 谓词，必须先拆分等待通道，才能确保唤到实际获派任务的线程。Slot vector 在启动线程前按固定长度构造，之后不 resize／移动，条件变量地址稳定。

## 保留的语义

| 边界 | 保持方式 |
| --- | --- |
| 版本、授权、计费和预算 | task 本体、TLS／read／profile scope、版本选择、admission、费用重放与预算控制没有修改；任务仍只在公共 worker 执行 |
| 队列与资源 | 原有 atomic batch 接纳、容量上限、资源预约、部分预约回滚、bypass 与 Parked 交换逻辑未改 |
| 资源释放与完成 | task captures 析构 → reservations／resource requirements 清理 → 持锁更新终态及 Idle → 解锁 → 原 promise 完成 → 通知 dispatcher；未提前退款或兑现 Success |
| 取消与期限 | 保留 task 前 Cancelled 优先于 DeadlineExceeded，运行中合作取消、原状态传递，以及资源等待中的有界探测 |
| 错误 | 原 QueryStatus 的 code／phase／message／source／operation／retriable 继续传递；标准／未知异常翻译和 ResourcesChanged 校验未改 |
| shutdown | 不丢弃已接纳任务，不提前拆除所依赖的资源；保持原 drain 语义和重复调用行为 |

## 实际验证

| 检查 | 结果 |
| --- | --- |
| macOS，真实 GoogleTest，无 stub | scheduler／resource 41/41 通过 |
| macOS 新回归压力 | 4 项 × 200 轮，800 次通过；XML 仅末轮，总轮数由运行退出码记录 |
| macOS ASan＋UBSan | 41/41，无 sanitizer 报错；Apple arm64 未启用 LeakSanitizer，不据此宣称泄漏检测通过 |
| macOS TSan | 41/41，无数据竞争报告 |
| Linux GCC13.3 Release 产品 | `iotdb_fs`、`unit_tests` 构建成功 |
| Linux scheduler／resource | 41/41 通过 |
| Linux 全量单测 | **1990/1990**，无 failure、error、skip 或 disabled；原1986项完整保留，加4项新回归 |
| Linux 新回归压力 | shuffle，4项 × 100轮＝400次通过；完整日志逐轮验证100次PASS，XML单列为末轮4项 |
| 独立并发审查与证据复核 | Ready／wait同锁、通知早于wait、两个派发分支、正常／部分构造shutdown均已审查；下载包逐文件SHA、确切三文件差异、全量测试身份及压力日志独立核验通过 |

新增四项门闩回归：

1. 三个 worker 被门闩占用时，唯一空闲 worker 连续完成32次任务，验证不会把通知交给其他槽。
2. 四个 worker 连续执行8轮完整门闩批次，验证所有槽都能反复进展。
3. 无任务、可能尚在启动期的四路池执行 shutdown，重复8次，验证全部worker退出和停止接纳。
4. 四路池同时含 Running=3、Parked=1、资源等待队列=1 时 shutdown；释放资源后全部future完成，capture、旧／新resource均释放。

既有测试继续覆盖HDF5资源争用时Arrow可进展、多资源预约回滚、取消／deadline、资源重新分类、错误详细字段、lease／capture早于future释放、TLS恢复、共享runtime及drain。Linux全量还验证了上一版GRIB单message的冷／热执行、metadata上下文、取消survivor、异常状态与publication race。

测试中的5s等待只作为挂死保护，不作为性能断言。线程创建失败未做故障注入；其清理路径由代码审查覆盖，不能声称已执行该故障用例。macOS独立测试不替代Linux产品构建，也不证明实际FUSE挂载路径的端到端性能。

## 身份与证据

| 项目 | SHA256 |
| --- | --- |
| 上一接受版 binary | `4d8153bcb554574a4dc7be1e0abceb36087fd91ce27191b0267ca1f1cefce3ea` |
| 本次 Linux binary | `4caa446dc2dadbec9227fbdafd9062425a3ae56910c05c96352e341e98e40cb7` |
| 本次完整 source manifest | `948ad42b8966ac36d60011528fc5cbe563642a827576ed78f7b5e51e31bd7ffc` |
| scheduler 源码 | `fb0899aea9f0e23be7b6e36c7d927ba4ca5d9172b2fd96ec88f37647d83a415c` |
| 测试源码 | `bf30a2c32b89de65ccfb139fcdcad6a85791004dcaf2527d7d34fd6e2e51f348` |
| Linux 证据归档 | `c9199ccf383aaf140bcdb07fe852a11b0fa8c5a2bae3ef5b734041215cf69d87` |

证据位于 `experiments/cross_version/work/directed-wakeup-20261025/`：

- `before/`／`before-sha256.json` 保留本次修改前的三个文件；`implementation-inputs.json` 固定overlay、旧版身份和必测用例。
- `evidence.tar.gz`／`package-receipt.json`／`evidence/` 保存Linux构建／全量／focused／stress日志、XML、源码清单及三个修改文件；不包含binary、服务配置、凭据或数据库内容。
- `accept_evidence.py`／`local-evidence-audit.json` 独立核对下载包、原1986项与新4项的精确集合、日志和本地源码等于实际构建源码。
- `local-checks/` 保留macOS sanitizer命令、输入SHA、结果、日志与XML；`report-receipt.json` 绑定本报告和相关证据。

Linux产品在远端新目录 `experiments/cross_version/work/directed-wakeup-20261025/product-r1/`；没有安装到运行服务，也没有改变服务器 affinity／cgroup／数据库设置。因此本轮不需要服务恢复操作。后续性能验收应使用此新binary身份和同条件交错对照；本轮正确性结果不代替性能结论。
