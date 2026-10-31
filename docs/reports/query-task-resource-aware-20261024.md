# 公共查询调度器资源感知实现与验证

## 行为

公共调度器现在在派发前非阻塞预约任务声明的资源，按提交顺序扫描并跳过暂不可运行项。未取得全部资源时归还部分预约，任务留队，执行线程可用于后续可运行请求。逻辑资源 Lease 允许跨线程归还，不转移 `std::mutex` 所有权。

本轮实际接入 Native HDF5：进程内各 runtime 共享容量 1 的逻辑预约，另以同线程 `try_lock/unlock` 探测真实 HDF5 library mutex。已有 native 库锁及其全部句柄保护路径保持不变。Indexed HDF5 不进入 native 库，不申请此资源。

普通 typed、format catalog、batch 的格式提示只观察 JSON 结构；显式 listed 且不含 HDF5 时不申请 native 资源，all、混合或无法确定时保守申请。没有提前调用 select/prepare，没有冻结额外版本视图，没有提前收费或授权。typed_batch 仍作为一个任务按原次序执行 items、共享预算并整体回滚。

prepared template/selector 使用不续 TTL、不清理过期项的只读 inspect，并只保存 weak ownership。实际 acquire 在原 registry mutex 内、TTL 续期前核对 ownership。观察时为 Arrow/不存在，执行时同 ID 已被复用为 HDF5 时，内部观察失效会重新分类资源，并在进入查询 I/O、计费和发布前返回等待。

重分类保留同一个 task/future，归还旧资源，转为 Parked，不占执行线程。若 pending 已满，可交换已有队列单元给后续可运行任务，不额外扩容：pending 不超过配置上限，active + parked 不超过 worker 数，总接纳数不超过二者上限之和。原 future 仅最终完成一次。missing、wrong UID、过期和已有客户端错误继续沿原路径处理；观察失效本身不产生新增客户端错误或续 TTL。

## 验证设计

核心调度器使用同步门闩验证实际状态，不以耗时推断并行：

- 1/2/4 worker 下四个等待 HDF 资源的任务留队，后续无该资源需求的任务先完成。
- 多资源申请失败回滚、资源容量、跨线程 Lease、异常/取消/过期清理、整批拒绝及 shutdown drain。
- pool=1、queue=1 且队列已满时，资源重新分类仍允许后续 Arrow 标记任务运行；同一个 future 和请求 capture 保留到最终完成。
- Registry + Scheduler 组合测试复用同一 ID：释放资源后执行新 payload；若等待期间达到原 TTL，则返回原 `SourceNotFound`，不会因观察或重分类续期。

socket 集成测试使用真实 Arrow 物理数组查询，校验 index/value 四行数值以及与串行基线相同的 IPC 字节：

1. 一条 HDF 请求在授权门闩持有预约，其余 HDF、混格式 batch 和含非法项的混格式 batch 留队；Arrow 普通查询和 Arrow-only batch 在释放前完成，原预算、授权、非法声明错误逐项比较 code/phase/message。
2. 外部持有逻辑 HDF 预约时，Arrow template 的 prepare/execute/release 和纯 HDF 声明准备/释放可继续执行。
3. Indexed HDF5 不被 native 资源预约限制。
4. 外部线程通过 `with_hdf5_ingestion_source` 打开有效 HDF5 内存文件并实际持有 library mutex；公共查询池仅 1 个 worker，四个 HDF 请求全部留队、active=0，Arrow 真实解码仍在解锁前完成。测试先用 HDF5 core driver 创建含四个数值的数据集和完整文件镜像，再经有界读取回调打开，不伪造文件签名或放宽校验。

## 结果与证据

本地证据：`.tmp/query-task-resource-aware/`。Linux 验证目录：

```text
root@192.168.99.49:
/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/query-task-resource-aware-20261024-r1/
```

首轮源码包 SHA-256：`fe2db7d3a261250b66babd08a377e9bac0ae8ec53df836284ff8f1efc3bd1073`，`source-manifest.json` 记录 1014 个文件身份。最终源码包 `source-final.tar.gz` SHA-256：`f6ba2a7ed25c6823c42d469cc7d6262a4eb21489bc451c08f24bc5f2a0eaf69f`，`source-manifest-final.json` 记录 1015 个文件，远端构建前逐文件核对无差异。本地在归档时也逐文件核对一致，归档后仅补充本文结果。

两份源码包之间只修正 `test_QueryRegistrationSocket.cpp` 的有效 HDF5 文件夹具，并新增本文；产品代码没有变化，身份差异见 `final-overlay.json`。构建基于现有 owner reader pool 和公共任务池工作区，未丢弃原有改动。Linux 使用 GCC 13、Arrow 21 和锁定的线程安全串行 HDF5 1.14.4；产品 `iotdb_fs` 和 `unit_tests` 首轮构建均通过，最终增量只重编修正后的测试文件并重新链接。

| 检查 | 结果 |
| --- | --- |
| 核心常规 GoogleTest | 37/37 |
| 核心 ASan/UBSan | 37/37，无报告 |
| 核心 TSan | 37/37，无报告 |
| 本地整合定向测试 | 67/67，含 registry 原有与新增测试、三组 socket 测试 |
| Linux 产品构建及完整单元测试 | 构建通过；1980/1980，无禁用用例，45.974 秒 |
| Linux 真实 native HDF5 锁争用定向测试 | 1/1；单 worker、4 个 HDF5 等待任务时，Arrow 在解锁前完成 |
| 模块依赖和 diff 静态检查 | 通过 |

本地 HDF5 不满足锁定的线程安全串行 1.14.4，首次 native mutex 集成测试在进入门闩前被生产 policy 拒绝，原始失败保留在 `focused.log/xml`。Linux 首轮为 1979/1980，唯一失败是该测试误用了空 HDF5 源；`Impl` 构造时立即打开文件，因此在门闩前被 `HDF5 EOA exceeds source EOF` 拒绝。已将夹具修正为有效内存文件，并保留首轮源码包和日志；产品代码不变。没有放宽产品校验或跳过 Linux 用例。

`.tmp/query-task-resource-aware/verification.json` 汇总 XML 计数、源码身份、sanitizer 身份和 Linux 原始证据哈希。`linux/` 保留 `configure.log`、`build.log`、`build-incremental.log`、最终 `unit-tests.log/xml`、定向 `native-contention.log/xml`、首轮 `unit-tests-initial.log/xml` 和源码核对文件。已核对三种核心构建的源码前后哈希均稳定且仍匹配当前文件；源码版本、预算、取消和错误路径由完整回归及新增定向用例共同覆盖。本轮不将测试时长作为性能比较。

开发中发现并修复了“模板 ID 复用导致资源分类与执行对象不同”的竞态；修复包含弱所有权原子核对、原 future 重新调度与满队列交换测试。另保留资源容量测试早期过强断言的失败记录：执行槽包含完成清理阶段，不能用它代替同时持有资源的任务数。最终 sanitizer 证据标记为 `final-parked-reclassification`。Apple arm64 不支持 LeakSanitizer，因此关闭 detect_leaks，ASan/UBSan 仍启用。

## 范围

这是完整独立请求的保守资源预约，不拆分单个查询。Native HDF5 的预约覆盖整条请求，包括可能命中缓存的请求；未来可在保留上下文语义的前提下细化到执行阶段。

native probe 是瞬时观察，库外调用可能在探测后抢先持锁，最多一个已预约 HDF 请求仍可能等待。单 worker 在这一竞态下不能保证其他任务立即执行；没有宣称完全消除所有锁等待。外部资源永久不可用、无取消/截止时间时，drain 也无法保证有界结束。通用多资源组合不承诺无饥饿。

没有调整 IoTDB 查询线程池、owner reader pool 连接数或格式内部并行度。没有替换运行中服务、修改数据库配置、挂载或改写正式实验语料。本轮验证可运行性和原语义，不产生正式吞吐量或端到端提速结论；未执行 Git 提交或推送。
