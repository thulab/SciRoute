# 有界资源与查询时间比值复测方案

2026-10-21。状态：已完成144次查询并验证原服务、配置和资源恢复；现版R为6.971／13.013／3.446，未出现R≤1。详见[正式报告](../reports/resource-bounded-ratio-retest-20261021.md)。

## 目标与主指标

主指标为同一语料、查询case、实现block内暖完整查询的中位数之比：

`R = median(T_IoTDB-FS) / median(T_FS-index)`。

同时保留逐对时间比、两端绝对时间、首请求、所有原始观测和资源指标。R小于1表示该端点下IoTDB-FS更快；接近1只作描述，不凭小样本宣称统计等价。未达目标的场景如实报告，不另造数据、缩小谓词结果或更换FS计时边界以获得优势。

按后续定位要求，将相同完整查询拆为发现 Td 与取值交付 Tq；报告 `Rd=median(Td_IoTDB-FS)/median(Td_FS-index)` 和 `Rq=median(Tq_IoTDB-FS)/median(Tq_FS-index)`。两者与 R 使用相同 block 和测量样本，并列两端绝对时间；逐条校验 `T=Td+Tq`。阶段中位数未必相加为总中位数，R不能写成Rd+Rq。GRIB保留C++／NumPy交付差异，不用Rq直接推算IPC独占成本。已有144行仅新增离线派生分析，原始结果与身份不改写。

历史最小R按相同科学场景及引擎/缓存/交付协议分组，由同一block的两臂重算，不能拼接跨轮最小耗时。历史缺资源记录者明确标为未控制资源的参考，不能作为严格因果A/B。HDF5本地/FUSE原生prototype与正式数据库VFD引擎分列。

## 已冻结资源边界

- 两个逻辑CPU，CPU集合在预检后固定并绑定run identity。优先使用可核实的cgroup v2 isolated cpuset partition；仅taskset不算独占。无法核实隔离则不启动正式计时。
- 本轮客户端、IoTDB-FS、FS-index和原DataNode共享两核CPU预算，不允许每臂各自额外获取两核。原数据库仍复用原实例、账号和许可证。
- 查询进程组（客户端/daemon/native worker）合计2 GiB内存上限。DataNode保留原`-Xms/-Xmx15232M`和2176M direct-memory设置，另记内存峰值；不能宣称全栈只需2 GiB RAM。原实例堆约15 GiB，不在本轮擅自缩堆。
- 临时预留两核意味着其他共享任务在该窗口少使用这两核；不暂停其它任务，不修改其优先级。结束恢复原CPU集合/cgroup配置并归还资源。
- 不人为运行负载填满CPU来“固定利用率”。固定的是可用资源上限；CPU利用率、PSI、调度等待、CPU时间/墙钟、cgroup节流及内存均记录。
- 当前owner启动硬要求SYNC/partial=false且可能进行schema检查和staging清理。沿用已有原实例维护与恢复流程，不能把本轮称为ASYNC下严格只读。无独立实例、新许可证、科学版本发布或索引重建。

## 首批矩阵

| 场景 | 语料/查询范围 | 目的 |
|---|---|---|
| GRIB小文件控制 | 独立N128/V1/D4，Latest，K_files=2，S=33484 B，完整两个64×64字段 | 复核39/82 ms漂移，比较同场景R |
| HDF5控制 | 既有N512/B4/C128/V1/D4，Latest，K=2，64×64 ROI | 使用完整native oracle且明确候选域，复核历史R |
| GRIB既有大S/多V候选组 | 复用原N8、约8MiB、identical/V8，等值谓词选择每版2个message，科学版本All | 检查解码内容复用在既有场景能否追平/优于FS-index；不是无谓词全文件遍历 |

全部复用现有原始文件、真实发布版本与索引；大S场景必须先补齐真实完整语料/查询身份和oracle证据，不能因有利而放宽准入。独立N128 HDF5缺历史native mask证据，本轮不能用旧假设填入正式value+mask case，也不能把N512四桶写成N128。

先做资源/身份/结果预检，然后使用现有保留基线与新版二进制；不新增产品源码版本。首批每cell固定少量warmup和10对暖测量，两臂交替先后；挂载恢复及索引准备单列。不因某次比值有利而追加/截停样本。大矩阵扩展由首批的正确性、资源稳定性与R共同决定。

## 比较与交付边界

GRIB历史FS-index内部钟交付C++ owned数组，IoTDB-FS交付NumPy owned数组；该差异必须保留。若另做对齐的二进制数组交付端点，需独立命名/身份，不能给FS加JSON数值序列化来制造优势。route3 HDF5两端已交付NumPy owned数组。

验收交付包括资源配置、前后恢复记录、corpus/case/run IDs、原始配对查询、R与绝对时间、同场景历史最小R及其局限。无R<=1场景时明确报告当前未实现优势。
