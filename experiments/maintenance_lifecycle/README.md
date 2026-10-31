# M1：三路增量发布生命周期

2026-10-31。承接 `docs/deliverables/fs-index-incremental-maintenance-plan.md` 的 M1。此目录只包含可复核运行器；生成输入、私有日志、计时与结果在独立服务器目录，不能把预检或未完成轨迹计入正式结果。

## 冻结负载

GRIB/HDF5，各 8 个逻辑文件，每文件 4 个 64×64 单元。先发布 8 个 v1，再按文件顺序各发布 v2/v3，共 16 次更新。每次更新后分别运行 q=1/10/100 个查询；q≥10 时每十个查询的第一个选择每路径 v1，其他选择 Latest。q=1 仅 Latest。六属性谓词、两命中单元、Float32 values＋uint8 mask 完整 owned 输出 40,960 B。科学日期继承 FM1 的 20261013，发布版本与科学时间分离。

2 格式×3 q×3 路径×3 campaign＝54 条轨迹，1296 次完整文件发布、31,968 个正式查询。每个 campaign 内三路顺序轮换，各组拥有独立存储状态；Python 客户端进程复用。全部输入在正式计时前生成并以原生 reader 建立完整 oracle。对应三路使用逐字节相同输入，不同 campaign/q/ordinal 的实际数值不同。本组是所有单元内容改变的完整文件新版本发布，不是单字节 patch 或内容重复发布。

FS-scan 同样使用持久化 source/head/version 目录，但不登记 unit 属性；查询扫描所选版本。FS-index 使用 schema v2 维护，每次只提取新发布的一份文件。IoTDB-FS 使用 owner 版本目录：GRIB format_catalog＋普通 typed 取值，HDF5 dataset_catalog＋typed_batch／native-hdf5。发现先选版本再过滤属性；v1 在 IoTDB-FS 中用初始 8 次发布的精确、已核验且不与更新重叠的时间窗表达，FS 用每路径 ordinal=1。两者都选择同一批科学文件版本。

## 时钟与持久化

总时钟直接测量：独立目录初始化/私有 daemon 启动→初始发布→16 次更新与查询→显式 checkpoint/FLUSH→关闭。查询时钟包括发现、原生读取或 IPC、owned 数组复制，不含 oracle hash 和 profile 分析。发布时钟包括完整复制、文件同步、关闭与取得已提交公开版本身份；FS 的 copy/hash/目录 fsync/提取/SQLite COMMIT 在内部收据细分。循环、轻量记录和分配成本保留在实测总时间中，不通过相加阶段中位数构造总时间。

FS 为不可变文件＋文件/目录 fsync＋SQLite WAL/FULL；IoTDB owner 启动门禁验证实际 WAL=SYNC/partial_insert=false，原生文件由 NativeFileStore 同步后发布。FS 显式 TRUNCATE checkpoint 必须 busy=false 且剩余 WAL=0；IoTDB 用既有 SDK helper 对**本次独立数据库**执行 FLUSH。FLUSH 不等于 SQLite checkpoint 的内部工作量，也不保证所有后台合并工作已结束。二者对齐的是已确认持久化、可见且可重开的版本，以及显式公开收尾调用，不声称相同故障模型或完成断电实测。

客户端、FS-index、FS-scan、私有 FUSE、SQL helper 和 DataNode 共享独立父 cgroup 的 CPU2/3、两核总配额；query 子组限 2GiB、swap=0，database 子组不另限内存。这恢复了旧 FM1 的 CPU 总预算结构。DataNode 不重启，移动前保存其原 cgroup/所有线程 affinity，外部 supervisor 每5秒验证资源，结束或普通信号中断后恢复。DataNode 既有内存不会完整迁移到新 memory cgroup，故以 /proc RSS/HWM 单列，不把新组 memory.current 当成数据库全量内存；JVM 28GiB 堆仍与旧15GiB不同，不能宣称全部环境逐项相同。输入和审计也在 query 组内，memory.peak 不是纯查询或逐 trace 峰值。所有轨迹独立于旧数据计算比值。

第一轮客户端两核而数据库不受限的 `m1-formal-01` 已失败并保留，仅作诊断，不纳入正式汇总。它在第1025个 typed 注册遇到默认1024槽上限；同一失败 namespace 的只读重放精确复现512个双结果查询后失败。公开配置 `query.frontend.max_registrations=4096` 后1600个双结果查询完成，owner state hash不变。修订正式配置固定4096槽，容纳预声明最大3200个注册，不中途重启、不删除失败、不改变产品代码/TTL/内存上限；这不是无界连续服务能力的证明。
数据库进程及数据库一次性创建是共享基础设施；数据库创建、原始输入生成、实验父目录/launcher、结果审计不在每 trace 的总时钟中。每 trace 的 owner namespace/schema 初始化及 FS store 初始化计入。没有全局清缓存、后台压力生成、数据删除或现有服务重启。

## 正确性与恢复

每查询在总时钟之后验证完整源身份、版本、unit、数值/掩码摘要及 owned 状态。每 trace 关闭后重新打开，校验 24 份 exact 历史文件的完整 SHA-256，以及 Latest/v1 的完整查询结果。查询/发布 profile 保留原始文件。所有成功 trace、失败和预检目录保留，不自动挑选最快样本重跑。`measured-before-audit.json` 区分已计时和已验证，只有 `trace.json complete=true` 才可进入统计。

本实验不验证持续并发发布（M2）、超缓存/网络负载（M3）、权限等价、多 writer、任意尺寸单元或 Arrow 维护。旧 4.3/4.4 的静态查询证据仍独立有效；维护结果不能追溯替换旧实现名称。

正式完成后的既有publication profile核验发现：每条IoTDB-FS轨迹24次业务发布对应48次后端发布（8次空创建、16次截断空提交、24次真实内容提交）。本协议对齐业务发布与查询时的科学版本，不对齐中间后端版本事件；不能声称三路具有相同并发写入可见性。`publication_summary.py`核验非空提交version与业务收据一致并汇总阶段。收据`copy_ns`含目标打开/截断，`file_fsync_ns`含完整导入和版本提交，均不能按字段名误解为纯文件复制或单次磁盘同步。详细解释见生命周期报告。

## 运行与分析

`python -m unittest experiments.maintenance_lifecycle.test_protocol` 检查历史占比和版本先于谓词的 oracle。服务器需要已验证 Release `libfs_index_native.so`、现有产品、h5py/numpy/pyarrow 及 v00 SQL helper；连接凭据仅从已授权 daemon 内存读取，不进入参数和公开产物。

`run.py --pilot` 跑 6 条 q1 预检；去掉该参数跑 54 条完整轨迹。必须指定新的 `--work`；不复用失败目录，不恢复部分正式运行。正式通过 `supervise.py --work <new-root> --pid-file <existing-runtime-pid> -- <run.py arguments>` 启动；管理器在配额外、worker 在 query 叶节点。具体部署路径、源码/二进制摘要和执行命令记录在报告及服务器 `frozen.json`/`source-closure.json`。

## 静态复审后的解释约束

本机 Claude Opus 5.5 对初版协议静态复审：负载、计数和总时钟无须中止重测，但要求修正解释和统计。三轮是独立输入、namespace 与文件目录，**不是独立 DataNode/数据库进程**；18 条 IoTDB-FS 轨迹共用一次新建的实验数据库，存在累积 WAL/后台状态和顺序混杂，不能声称三个统计独立样本或用它们做显著性检验。循环轮换的前驱关系不完全平衡；分析必须单列 FS-scan 和 FS-index 区间内的 DataNode CPU/IO，不能隐藏后台资源残留。

查询统计除 Latest/v1 外，另外分“更新后的第一个请求”“v1 后第一个 Latest”“其余请求”。q1 的首查为 Latest，q10/q100 的首查为 v1；不同 q 的同名 kind 中位数不能直接作为刷新代价的因果比较。IoTDB 发布中取得公开 version 的目录查询也可能预热元数据，故需结合发布成本解释首查。FS select 的 discovery 时间含刷新但不是刷新独占时间；现有响应只记录 generation，不能据此虚构 refresh_ns。

IoTDB query profile 在响应路径同步写普通日志（不逐条 fsync），其开销包含在客户端时钟内；FS 原生内部计数与它并非相同观测开销。启动/卸载由现有 private_mount 轮询（50ms）和 wait 完成，墙钟真实包含这些服务管理成本。基础 profile.properties 与实际 server profile 原样归档。client.validate 对身份、数量、值/mask、owned buffer 不符均抛错，重开检查返回值未单独断言不会静默接受错误；分析器仍必须拒绝带 failure 的 report，并检查 supervisor 恢复成功。

只报告 FS SQLite/WAL/SHM 和原生文件尺寸。IoTDB 的 native_file_bytes 不包含数据库 TsFile/WAL，不能由它得出三路完整空间比较。分析修订在独立路径执行，不改正在采集的源快照。

q1 与 q10/100 的历史查询比例不同，各 q 单独作三路比较；不从三点直接拟合精确摊销/回本阈值。首次发布的原始输入生成使用独立原生解码 oracle；这是受控合成小文件维护负载，不是新真实数据集或任意科学格式的维护结论。
