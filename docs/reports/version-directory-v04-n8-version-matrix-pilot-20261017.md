# V04 N=8 单版本／跨版本多文件完整矩阵试跑

日期：2026-10-17。**试跑完成，原服务恢复。** 192 个配置、2,880 次完整查询全部通过运行器的独立数值／mask／精确版本校验。独立审计逐项复算 192 个 cell 与 108 组比较，全部一致。

**决策：本轮不将 V04 提升为全格式共同开发主线，保留 V03 为查询性能参照。** GRIB 的 All 查询在 identical 与 single_unit 两场景都满足对无索引 FS-scan 的三轮完整耗时、绝对增量及相对优势条件，值得作为限定场景继续研究；HDF5 未通过。V04 对 FS-index 和 V03 的 V8 联合门禁均未通过，不能把相对 FS-scan 的收益归因于 payload 共享。V04 功能与本轮数据继续保留；没有启动 N=32/128 或 V05，也没有改动生产源码。

## 冻结协议与实现

N=8、K=2、D=4、depth=2，V=1/2/4/8，两种格式、identical/single_unit、Latest/Exact(v1)/All，四系统为冻结 V04 r3、冻结 V03、native FS scan、native FS SQLite index。每 cell 为3个独立campaign，各5次查询；每cell每campaign排除repeat=0，留下12个温热观测，独立复现单位只有3个campaign。第一项只是该cell在该轮的首请求，不称为全冷请求。

协议见 [spec](../../experiments/cross_version/version_matrix/specs/n8-version-matrix.json)，执行入口为 [n8_matrix.py](../../experiments/cross_version/version_matrix/n8_matrix.py)、[准备模块](../../experiments/cross_version/version_matrix/n8_matrix_prepare.py)与[版本化 native worker](../../experiments/cross_version/version_matrix/native/version_matrix_native.cpp)。新 worker 复用原 multi_file 的 HDF5/ecCodes 读取与 owned buffer 函数；原 runner、native reader 和生产文件未改。新增12项纯运行器回归通过；正式 native 使用 HDF5 1.14.4/thread-safe/no plugins、ecCodes 2.34.1、SQLite 3.45.1。

每个 owner system/scenario/V 使用独立 namespace，共16个，每个含两格式各8条稳定路径；合计960个科学版本绑定。它们引用相同的原pilot序列和独立数值oracle。V=1就是各序列v1，不与N=128旧报告直接相减。真实版本号逐路径记录，ordinal不冒充row_time；create/truncate产生的空历史保留，格式筛选排除空快照。每个namespace在新进程重读所有科学版本完整SHA及未过滤的科学版本目录，16V个文件、64V个单元均符合冻结集合。

FS 为每scenario/V/format复制完整历史，16份历史树共480个普通文件，480个独立单链接inode，且与原corpus不共享inode；用户态read/write复制，不用hardlink、symlink或reflink。一份SQLite索引覆盖该树全部V，不按查询重建或用oracle预筛命中。Latest和Exact先定位版本，再执行六属性谓词；Latest scan不被迫遍历历史，All逐版本保留结果。独立预验96个FS组合通过，所有文件与16份索引前后SHA一致。

## 完整查询计时与判定

主指标为发现加全部命中单元读取到owned buffers。native时钟含版本选择、遍历或SQLite、原生读取解码及本进程owned arrays；IoTDB-FS时钟含控制／注册、发现、选中值请求、IPC及客户端owned copy。沿用既有边界：native的hash/JSON/pipe在主时钟外，worker往返另存；IoTDB-FS的hash/provenance校验和profile等待在时钟外。二者不是等RPC或完全相同进程传输边界实验。

Latest/Exact命中2单元，All命中2V单元，单元各4096 Float32及mask。Exact(v1)需要按8路径的真实版本号分别发8次discovery；Latest/All各1次subtree discovery。HDF5按每个命中path/version/dataset单独typed读取；GRIB Latest/All合并命中路径，Exact逐命中path精确读取。这些请求构造与实际开销均保留；一个完整应用操作包含多个独立RPC及其现有独立预算账本，不声称单RPC延迟或统一客户端ledger。

对每个独立campaign，T为该cell四个repeat>0完整耗时的中位数；Δ=T(V)−T(1)，G=T(V)/T(1)，S=T_control(V)/T_V04(V)，R=S(V)/S(1)。同时列pooled12样本中位数用于描述，不把它当配对因果效应。强门禁要求三个campaign分别同时满足：V04完整总耗时更小、Δ更小、R>1；不同格式／场景／selection不合并投票。没有统计显著性或跨机器泛化声明。

下面为V=8的pooled完整耗时中位数，单位ms；三轮明细、V=1/2/4及全部比较保留在audit.json，阶段中位数不可相加。

| 场景 | 格式 | 选择 | V04 | V03 | FS-scan | FS-index |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| identical | hdf5 | latest | 13.379 | 9.381 | 3.020 | 0.931 |
| identical | hdf5 | exact | 27.621 | 26.449 | 3.113 | 0.900 |
| identical | hdf5 | all | 98.273 | 78.213 | 26.227 | 4.753 |
| identical | grib | latest | 11.933 | 9.746 | 25.721 | 1.359 |
| identical | grib | exact | 41.440 | 26.461 | 24.889 | 1.398 |
| identical | grib | all | 42.989 | 32.843 | 189.621 | 8.353 |
| single_unit | hdf5 | latest | 11.828 | 20.213 | 3.075 | 0.748 |
| single_unit | hdf5 | exact | 22.755 | 29.597 | 3.402 | 0.798 |
| single_unit | hdf5 | all | 72.740 | 104.024 | 25.045 | 5.389 |
| single_unit | grib | latest | 12.143 | 11.907 | 26.234 | 1.223 |
| single_unit | grib | exact | 40.705 | 43.695 | 27.479 | 1.208 |
| single_unit | grib | all | 49.018 | 52.432 | 219.081 | 8.549 |

## V=1→8 的跨版本增量

All查询的pooled端点差和相对优势如下。正的R变化不自动代表绝对更快，更不代表每轮一致；通过与否以独立campaign门禁为准。

| 场景 | 格式 | Δ V04 ms | Δ FS-scan ms | Δ FS-index ms | R 对scan | R 对index | 三轮对scan联合门禁 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| identical | hdf5 | 88.396 | 23.119 | 3.842 | 0.848 | 0.524 | 未通过 |
| identical | grib | 32.032 | 162.151 | 7.218 | 1.759 | 1.875 | 通过 |
| single_unit | hdf5 | 63.154 | 21.936 | 4.715 | 1.061 | 1.055 | 未通过 |
| single_unit | grib | 39.852 | 195.232 | 7.486 | 1.718 | 1.504 | 通过 |

V8的36个联合比较仅2个通过，均为GRIB-All对FS-scan：identical三轮R分别2.937/2.132/1.210，single_unit为2.115/1.850/2.007。pooled完整速度比分别4.411×与4.469×。这满足用户所提条件的**GRIB无索引历史全量查询子场景**，不能抹去该正结果。

HDF5-All没有通过：identical的V04完整耗时及增量均较大；single_unit的pooled R=1.061，三轮R却为1.045/0.931/0.740，且完整耗时均慢于FS-scan，不能只引用合并比例宣称优势。FS-index在全部V8 cell的三个campaign都绝对快于V04。

相对V03不能笼统声称“所有模式更慢”：single_unit/HDF5/Latest三轮V04为7.947/20.324/10.944ms，V03为11.522/21.743/21.043ms，确有三轮完整绝对收益；但第三轮R=.877，版本增长优势不一致。V8相对V03没有cell通过全部联合条件。GRIB-All的V03 pooled为32.843/52.432ms，V04为42.989/49.018ms，方向因场景／轮次而异；因此相对FS-scan的优势尚不能识别为V04 payload共享的独有收益。

## 空间账本与增长来源

下表两格式合计，单位为实际字节。唯一引用payload只计科学版本引用的块；all objects包含namespace COW目录历史、控制对象、科学／空版本seal及可能不可达中间对象。FS另列文件逻辑字节、实际块分配及SQLite。**这些不是相同物理磁盘口径，不能相除得出数据库磁盘压缩率。**

| 场景 | V | 逻辑科学payload／FS文件字节 | 唯一引用payload | 全objects BLOB | FS文件allocated | SQLite字节 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| identical | 1 | 948,448 | 948,448 | 1,683,599 | 983,040 | 49,152 |
| identical | 2 | 1,896,896 | 948,448 | 2,386,551 | 1,966,080 | 57,344 |
| identical | 4 | 3,793,792 | 948,448 | 4,379,207 | 3,932,160 | 90,112 |
| identical | 8 | 7,587,584 | 948,448 | 10,212,059 | 7,864,320 | 135,168 |
| single_unit | 1 | 948,448 | 948,448 | 1,683,599 | 983,040 | 49,152 |
| single_unit | 2 | 1,896,896 | 1,896,896 | 3,343,751 | 1,966,080 | 57,344 |
| single_unit | 4 | 3,793,792 | 3,793,792 | 7,250,807 | 3,932,160 | 90,112 |
| single_unit | 8 | 7,587,584 | 7,587,584 | 16,912,459 | 7,864,320 | 135,168 |

identical的V8块引用比为12.5%，即payload引用字节较逐版本独立文件减少87.5%；但V1→8的全objects增长8,528,460B，其中namespace目录catalog-page增长8,206,068B，占96.22%。它不是HDF5 dataset catalog；内容catalog/layout/proof/metadata相关对象字节在本矩阵保持不变，解析元数据共享确实生效。

single_unit没有获得payload块复用：所有科学文件最大85,072B，均小于1MiB，每文件恰一块；改一个dataset/message就改变整个块。它只能检验本批小文件，不能否定大科学文件的局部块共享。

V8每namespace有128科学版本和128空快照，科学roots在identical为16、single_unit为128；所有空快照共用一个空map但保留128个独立seal。它们是可查询Exact历史，不是未发布staging垃圾。staging_payload_rows均为0，未做GC或按历史保留策略mark/reachability分类，不能将全部目录页或非payload字节称为可回收空间。V03 objects账本不包含其独立payload存储，故未拼造V03总空间。

## 准备成本、服务恢复与限制

执行总wall为5058.502s（约84.31分钟）。960次科学发布的fsync/close计时合计：V03 1314.911s，V04 1486.215s；不含绑定／最新SHA／初始化／对象inspector。48次正式查询owner挂载的恢复startup合计1731.668s；这些均在查询时钟外单列，不能解释为免费准备。

仅使用原数据库、账号和许可证，16组为原库内新表前缀，未创建新数据库。主维护窗口有2次正常DataNode SIGTERM，控制器未停启ConfigNode；原配置恢复后只读SHOW ALL CONFIGURATION证实ASYNC/partial insert=true，DataNode PID3759841，配置SHA仍为`ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，全部本轮挂载已释放。受保护原文件的核对和操作事件保留；没有隔离实例、激活证书或Git写操作。

准备期另做V2的12项真实backend预验（不计正式2880次）；首次误用system Python因缺h5py在import阶段退出、未连接数据库，改用原venv后通过，失败记录保留。正式主运行没有崩溃／自动重跑或丢弃失败campaign。FS96预验也单列，不能增加正式样本数。

本轮是N8、准备后OS页温热、三campaign的pilot。各轮进程新建、系统与数据组顺序轮换，未完整覆盖四arm的所有位置；不宣称随机化或冷盘。不同profile需新namespace，科学字节／逻辑版本／oracle相同而物理组织不同，不是同一物理数据库布局上的纯二进制A/B。未同步采集逐请求CPU／调度证据，不将轮次波动全部归因代码或环境。固定历史A与既有N128报告保持原值，本轮不替代它们。

**后续取舍：** V04可保留为内容共享功能和GRIB无索引All查询的候选；当前证据不支持统一切主线，也不足以证明去重导致查询提速。若继续研究，应分别确认GRIB相对FS-index／V03的收益、HDF5完整读取成本，以及目录COW／历史保留空间。扩大规模或新设计须单独冻结，本轮没有擅自实施。

## 来源与复核

服务器根目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/n8-version-matrix-pilot-20261017/`，本地忽略work保留凭据无关副本。`command.json`、`source-binding.json`、两次overlay／构建记录、正式native版本、12项回归日志、`report.json`、`run/queries.jsonl`、`run/analysis.json`、48次启动／配置、16组head/journal／对象探针以及控制事件共同绑定执行来源。

独立的`audit_results.py`以标准库重算全部指标，11项合成回归通过；`audit.json`逐字段匹配192cells与108comparisons。IoTDB原始记录保留冻结runner的oracle成功与owned行数，不保存全部解码输出数组；完整值hash是在执行时校验，不能冒称事后从数组重新计算。`analyze_storage.py`重新hash原字节并计算1MiB ContentId／extents，核对真实引用图、独立seal、480FS文件／16索引；`storage-analysis.json`、native/backend预验及`post-restore.json`分别留存。

- 完整运行 `report.json` SHA-256：`12d65779e6870a13b3f1ca182733397983f18a048194ae9fa7d236c0808bf460`。
- 查询审计 `audit.json` SHA-256：`12095a8a9a14d85f4cdcb2b349cca03bde207946ecc2fe680e45b5af49a093af`。
- 空间审计 `storage-analysis.json` SHA-256：`43ee9d8450eda82f6db099b49e968937605523127d95f4a113dd2cd4a51c6e41`。
- 冻结来源 `source-binding.json` SHA-256：`18d272b0f23f3b8f1bac9dccf402487fac12d789968e39ab26af6a2715e2979d`。
- V04 daemon：`83b90b15181f3311e5b70bf8dfbd334295b3e5422bf1ded388a9948ce2144305`；V03 daemon：`5e23a4a6a9b24e91d7d8b0f99227b752668d943d0e5f7c2ebf686130eea2e6f8`；native：`0a4ecf77baf6f20423a2f2c1153e1dc598cbc95a978b83c0d6b73a0856c5a9f0`。
- 原V04 r3源包：`6271c8633637bcdee83eec1d6966ab38f609e1c19cbca502b3e074dcca96e642`；本轮全部191个生产文件与它逐项一致，新增的是实验运行器／native／测试，不含已撤回的根校验复用或head热路径补丁。

最终`seal_evidence.py`在原服务恢复后复核来源、全部观测、保护文件摘要和2次SIGTERM控制事件，封存458个凭据无关文件。`evidence.json` SHA-256为`184ea1f1c81bcfc0230a209a37704c351ce80932abd688a7806158fbab37f5cb`；`evidence.tar.gz`为6,155,726B，SHA-256为`56d2b459c23d3414c8dc384c8d7a62d4be05810b13b717c831b3269594aefcc4`。原始FS/corpus字节、完整source/build、私有mount日志和原配置备份保留服务器原路径，不重复打包；`.env`、license、system_info只保留摘要。最终文档在实验封存后独立绑定，不改写原证据包。

本地封存副本已逐文件核对458项SHA-256和大小，全部通过；完整query JSONL与独立分析均在该包内。
