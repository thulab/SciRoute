# GRIB 渲染候选在可负载均衡独占分区中的复测

状态：资源执行方式已修正并保留；三轮 216 条完成，独立审计及原服务／资源恢复通过。C/A 有正向中位数信号，但未满足预登记的三轮同向 R 改善，渲染候选暂不恢复主线。

用户要求先修正资源执行方式后重新测试，并在 C/A 的完整 R 优势可复现时考虑纳入主线。本次修正实验基础设施，查询产品继续使用同一封存 A/B/C；独立 N128 语料、谓词、取值交付、缓存协议和采集顺序不变。

## 资源修正

| 项目 | 新协议 |
| --- | --- |
| CPU 分区 | `cpuset.cpus.partition=root`，exclusive/effective CPU2/3，启用分区内调度负载均衡 |
| 总资源上限 | 父组 `cpu.max=200000 100000`；query 2 GiB；原数据库 15,232 MiB heap、2,176 MiB direct 另列 |
| 参与者 | 客户端、IoTDB-FS、FS-index、原 DataNode 共享；不为某一臂单独增加资源 |
| 身份 | 新资源 journal／identity 协议；旧 isolated 结果不并入新协议统计 |
| 验证 | 所有入口与逐查询样本严格验证 root、exclusive/effective、leaf 边界与 quota；原恢复机制保留 |

Linux 6.8 文档说明 root 分区形成独立调度域，exclusive CPU 不供分区外普通 cgroup 使用；isolated 额外禁用自动均衡并排除 unbound workqueues。本修正不声称隔离宿主调度、IRQ、内核工作队列或共享 I/O：[官方文档](https://www.kernel.org/doc/html/v6.8/admin-guide/cgroup-v2.html)。

## 采集前固定设计

| 项目 | 设置 |
| --- | --- |
| case | `FM1-grib-N128-V1-C128-S0-base-latest`，独立 N128/V1/D4/K2/S0/Latest |
| corpus | `dab7d4518c470b922948fc45b741c71170e7e4e844344c1e5c1fc72a37b80daa` |
| case ID | `44b55fbc3ac22950980905804b8833263d9e5d6db15cfaa724deb889daaf83f0` |
| A | `f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087`，原渲染 |
| B／C | `a55236145f10a3f7512fa780c47bd564c9ab27e8e301b1243f3635d129a62601`，同候选，几何开关关／开 |
| 查询 | 原 format_catalog 发现＋逐命中 typed 取值；完整非插值 64×64，仅值投影，40,960 B owned 值／mask |
| 轮次 | 三 campaign：ABC／BCA／CAB；两臂交替，每臂每配置 2 暖身＋10 正式，共 216 条 |
| 计时 | d 为精确发现交付，q 至 owned 数组；T=d+q；oracle／profile 在计时外 |
| 主比较 | 每轮 C/A 的完整 R，再汇总三轮中位数；同时看原始 T 和 Rd／Rq |
| 次比较 | B/A；C/B 为同算法波动参照，不解释为本 case 的几何缓存收益 |
| 新目录 | `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/grib-render-balanced-20261022/formal-r1/` |

不重新生成／发布语料或构建 FS-index。旧 N8 只作为同一产品正确性与机制资格引用，不加入新资源协议的性能样本。完整保留暖身、慢值、失败及资源记录；不因结果自动追加、不事后选择更快 B/C。原始记录留服务器，只同步派生统计及 SHA 清单。

先运行单次资源机制探针：同 query leaf 内两个独立 CPU 工作进程，均允许 CPU2/3、不逐核 pin，同步后各工作 2 秒；无科学查询、不重启数据库。通过条件为两个进程 CPU 时间之和大于共同 wall 窗口，且 CPU2/3 均有 user＋system tick 增长，同时资源边界与恢复通过。探针不加入查询计时，不要求真实查询均匀占满双核。

## 采纳判据与边界

修正资源及独立恢复验证通过是前提。若 C/A 的三轮完整 R 均小于 1、汇总改善，且完整 T 没有明确反复退化，则结合 C/B 同算法波动与实际资源证据评估优势是否可信。主指标按用户要求为 R；旧 isolated 结果不用于补足新协议的方向或样本数。信号小于明显同算法波动、完整 T 反复大幅退化或资源验证失败时，不自动采纳。

若满足采纳条件，只恢复已封存七个产品／测试文件，验证与原构建输入和测试证据绑定，并补齐原缓存契约；不加入新的渲染方案，不重发布数据，不执行 Git 写操作。否则继续保留回退状态并说明原因。

## 实施与验证结果

公共资源代码改为 exclusive `root` 分区，active／pre-exec／周期观察／逐查询采样一致校验新边界。journal 升级为 `iotdbfs-cgroup-ratio/2`，资源身份升级为 `exclusive-balanced-shared-two-logical-cpu/2`；旧 journal 不静默兼容。原总 quota、内存边界、DataNode 配置与恢复流程保留。

55 项离线测试通过，覆盖资源建立与失败恢复、旧协议拒绝、错误 partition／exclusive／leaf 配置拒绝、资源观察、固定矩阵与两套 GRIB runner。部署前保存服务器实际原件，新旧可执行闭包逐文件核验；只有 `resource_limited/resources.py`、`resource_limited/run.py` 和 `fixed_matrix/run.py` 三个执行模块变化。A/B/C 产品、四个冻结客户端 helper、科学身份与原索引均未变。

资源机制探针两个工作进程合计 CPU 时间 3.99049 s，共同 wall 2.00219 s，实际约 **1.99306 核**；CPU2／3 各有 200／201 个 user＋system ticks 增量，均允许 {2,3}、未逐核 pin。探针不查询／重启数据库，结束后恢复原 cgroup 和全部线程 affinity，独立审计通过。

两次独立进程只读预检均通过，143 文件执行闭包一致；唯一正式 run 完整保留 216 条（180 正式、36 暖身）。未重试、追加、筛除慢值或更换配置。

## C/A 与消融结果

以下为同 campaign 配对后再取中位数，小于 1 表示改善；列中位数不再相除重建另一指标。旧 isolated 数据不参与汇总。

| C/A | 完整 R 倍数 | IoTDB-FS 完整 T 倍数 | Rq 倍数 |
| --- | ---: | ---: | ---: |
| campaign 0 | 0.745821 | 0.732606 | 0.650584 |
| campaign 1 | 0.874145 | 0.894228 | 0.983181 |
| campaign 2 | 1.013105 | 1.034540 | 0.990799 |
| 三轮中位 | **0.874145** | **0.894228** | **0.983181** |

完整 R 中位改善 **12.59%**，完整 T 中位改善 **10.58%**，Rq 中位改善 **1.68%**。第三轮 R 增加 1.31%、T 增加 3.45%，未达到预先确定的三轮 R 均小于 1；这不是已证明候选无效，而是本轮尚不能确认稳定复现。

| 次要比较 | 完整 R 倍数中位 | 完整 T 倍数中位 | 解释 |
| --- | ---: | ---: | --- |
| B/A | 0.896101 | 0.884295 | 保留次要消融，不事后改选 B |
| C/B | 0.981779 | 1.011232 | 同一完整网格算法的运行波动参照 |

C/B 三轮 R 倍数为 0.981779／0.975498／1.049269；其范围仍约 −2.45% 至 ＋4.93%。本 case 没有几何映射查找／构建，不能据此宣称几何缓存加速。

还有一项需要保留的运行状态信号：A 的第三轮完整 T 相对第一轮下降 28.56%，B／C 对应变化分别为 −1.89%／＋0.88%。C/A 的优势随轮次缩小；这与基线运行状态变化相容，但没有证据将其唯一归因为数据库预热、CPU 调度或渲染实现。完整 R 的正向中位数不应掩盖这种顺序／状态敏感性。

## 实际资源与恢复

| 项目 | 实测 |
| --- | --- |
| 周期采样 | 共 781 帧；独立生命周期 active 窗口 780 帧，另 1 帧属恢复阶段并保留 |
| 查询前后采样 | 432 帧全部符合新 root／exclusive／quota／内存规则 |
| 活动包络 CPU2 user／system ticks | 16,009／2,732 |
| 活动包络 CPU3 user／system ticks | 15,682／2,635 |
| IoTDB-FS 正式查询采样包络 query CPU PSI some | 1.371% |
| FS-index 正式查询采样包络 query CPU PSI some | 0.074% |
| 整个活动包络 parent CPU PSI some | 5.517% |
| 整个活动包络全机 I/O PSI some | 18.087% |
| 整个活动包络 quota throttle | 9,941 μs，非零，原样记录 |

真实正式查询的采样包络中，两 CPU 均出现工作量；旧协议 CPU3 用户／系统 ticks 全零的现象未重现。例如 IoTDB-FS A/B/C 各 30 个正式采样包络内，CPU2 user＋system 分别为 23／15／14 ticks，CPU3 为 29／22／23。它们是 host CPU 累计计数，不能精确归属某个进程，也不证明每条查询都并行。

所有资源包络含 oracle／profile／采样等计时外工作，PSI 不能直接作为 d/q 独占等待时间，也不能从完整 T 中扣除。新旧资源协议的 CPU 等待观测可作机制参考，性能样本不混合。

独立 live 恢复通过：最终 DataNode PID 2960058，228 个当前线程恢复原 affinity，原 cgroup、heap／direct 和配置恢复；配置 SHA 为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`。实验 cgroup、挂载和活动 worker 均已清理。

## 主线决定与证据

**保留资源修正；暂不恢复渲染候选的七个产品／测试文件。** 新协议已证明能实际利用双 CPU，未来实验应使用明确的新资源身份。候选有值得继续验证的 R 改善信号，但本轮未满足事前复现门槛；不改变判据、不追加到出现有利结果、不换选 B。后续应先厘清 A 随轮次下降的运行状态影响，再决定是否采纳简单渲染改动。

服务器根为 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/grib-render-balanced-20261022/`：

- `registration.json`：采集前登记，SHA `440ed0faad5a68a724f86ee1d8bf5cd0c96f4731006fef7c6a333142abf2c4b1`。
- `resources-probe-r1/`：无科学查询的机制探针与恢复原件。
- `formal-r1/`：216 条原始查询、环境、资源、源代码封印、原服务恢复原件。
- `deployment/`：55 项测试证据、服务器原件备份／上传清单、预检、启动和完成收据。
- `audit-evidence/independent-audit.json`：完整独立审计，SHA `cccdf265f160ec952facb0d22d700d9f49dd128beae95b50743cc293c1ceb2fc`。
- 本地仅保留允许的 `audit-brief.json`、`resource-derived.json`、`server-original-sha-manifest.json`、`live-restoration.json` 等派生摘要及 SHA 清单，原始科学行／资源／profile 留服务器。

审计过程中保留了两个工具边界修正记录：预检收据新字段名称适配；周期 active 窗口按独立 `created→restore_database_planned` 事件界定。最后一帧非 root 发生在最后 query 后约 28.90 s、恢复计划事件后约 69 ms，属于正常恢复；该帧未删除。旧拒绝、工具版本与修订封印均保留，未修改科学行、时钟、采纳判据或统计函数。
