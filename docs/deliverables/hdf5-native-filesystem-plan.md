# native-hdf5：数据库目录与文件系统数据通路

日期：2026-10-21启动，2026-10-22完成正式复测。状态：用户已确认“按此实施，先改查询通路”；代码、真实后端功能验收、先行两个固定case小探针，以及后续全部16个HDF5首轮case的正式三轮均已验收。旧实验与二进制证据保留原义，默认模式未改变。

## 1. 用户要求与目标

`native-hdf5` 改为：数据库目录完成跨文件属性谓词、路径范围和精确版本选择，数据读取交给普通文件系统上的完整、不可变 HDF5 文件及 libhdf5 原生文件内索引。取值路径不再读取数据库 payload，也不通过自身 FUSE 挂载路径。

“返回文件位置”是目录筛选的结果，不是把目录功能缩减为按文件名查路径。六属性等值 AND、其他现行谓词、Latest/历史窗、dataset 定位与既有错误语义继续适用。目录服务可复用现有冻结候选域、属性索引和授权证明。

## 2. 已核实的现状

- 改动前的正式 native-hdf5 中，Hdf5QueryKernel 创建 Hdf5SourceDriver，后者用自定义 VFD 调用 QuerySource::read_payload，实际字节来自数据库；本轮已改为 `open_local` 及受管文件能力。
- owner 的 file-descriptor/2 封印 payload_root、metadata/layout/proof，不含本地完整文件定位信息。本轮通过独立 Current 定位记录和格式中立文件能力补齐，不能用逻辑路径直接假定本地物理路径。
- 历史 catalog-native-local 原型使用实验 bindings[(path,version)]['source'] 找到完整本地文件；生产目录发现已经具备，但该实验映射不是正式定位协议。
- 现行 FS-index 已有六属性复合索引、版本索引、SQLite prepared statement、文件与 dataset 句柄缓存。不能把它当作逐文件属性扫描基线。

## 3. 已确认的实现边界

| 项目 | 建议 |
| --- | --- |
| 定位身份 | 新增 owner Current 保留键域中的 `iotdbfs-native-file-location/1` 不可变定位记录，绑定 namespace/incarnation、精确版本、descriptor/seal/payload root、字节数和相对位置；绝不按 Latest 在取值时重新定位 |
| 本地存储 | 显式配置受管 FS 根；根下保存只读完整版本文件。位置是相对根的定位信息，根可按部署配置，避免将实验绝对路径写入公共逻辑模型 |
| 发布顺序 | 文件复制/校验、fsync 和原子安装完成后，owner 才发布引用；提交失败保留可追踪孤儿，不让半成品可见；首版不新增在线 GC |
| 既有版本 | 提供显式补绑定流程，从既有科学文件校验完整字节与已发布内容身份；不重新发布科学语料。已有前缀若需持久格式迁移，先显式迁移，不隐式升级 |
| 缺失/改变 | 无定位、文件丢失、版本或身份不一致时明确失败；不在查询时下载数据库 payload、重建文件或回退旧 VFD |
| 取值 | HDF5 Adapter 使用有界本地 pread VFD 和 H5Dread；不读取数据库payload或自建 chunk 索引页。VFD仍是文件系统I/O，只在此保留预算/取消检查接点。文件访问能力由存储/目录层授予，不让通用 QueryCore 解释 HDF5 |
| 安全与结果 | 保留目录授权、精确版本冻结、execute/发布/新 open 准入，以及 dtype/ROI/科学绑定、mask 和 owned 输出契约；本地文件生命周期随执行租约管理 |
| 预算 | 每次本地VFD读取先通过请求的 reserve_logical_payload，再读取受管句柄；保留原VFD累计范围/字节限制及取消。首版不采用不透明sec2读取绕过原逐范围预算 |
| 缓存 | 原生句柄和库缓存应有界、可观测，执行持有也计入容量；与 FS-index 比较时固定缓存配置。具体共享生命周期须在实施时与请求租约/取消/预算分离 |
| 首版存储范围 | 先改变 native 查询数据通路；数据库原 payload 暂保留供 iotdb-indexed/POSIX。首次不删除既有数据，也不同时改为 metadata-only 摄取协议；存储重复成本单列 |
| 模式 | native-hdf5 改义，不保留旧数据库 VFD 的公开别名/自动兼容分支；iotdb-indexed 与默认值保持原样。历史 native-hdf5-VFD 结果单独标记 |

定位记录是显式的owner目录类型扩展，不改变namespace-record/3；旧二进制会拒绝未知Current键，补绑定后的namespace必须由新版及当前journal打开。不是任意manifest字符串、客户端raw路径映射或旁路JSON。

## 4. 对 FS-index 的性能判断

两端读取同一不可变文件、使用同一原生 HDF5 引擎、缓存和交付边界时，文件内 q 应趋近，但完整系统仍可能有准入和 IPC 差额。完整时间分解为：

`T_I = d_I + q_I`，`T_FS-index = d_FS-index + q_FS-index`。

要达到完整查询更快，必须实测 `d_FS-index - d_I > q_I - q_FS-index`。去掉数据库 payload 读取只是消除当前的额外成本，本身不证明目录比 SQLite 更快。当前正式 typed 路径仍由服务端读取本地文件并经IPC交付，尚未变成“发现后客户端自行H5Dread”的零IPC路径，不能承诺q与FS-index完全相等。

| 场景 | 可以检验的机制 | 结论边界 |
| --- | --- | --- |
| H1/H2：大候选域、少量命中 | 目录热属性索引直接返回命中；查清双方仍随 C 增长的工作 | 现版 IoTDB-FS 仍有候选/费用轨迹工作；FS-index 也使用复合索引，不能预设优势 |
| H4：版本选择 | 比较版本目录与 Latest/历史窗口的筛选、连接和状态复用 | 当前 FS-index 已有 latest 表和版本索引；大 V 不自动赋予 IoTDB 优势 |
| H3：扩大非命中内容 | 验证选择性发现与文件内原生索引不读无关内容 | 两端均可跳过；预期主要用于验证规模不敏感，而非必胜场景 |
| 后续更新/并发 | 统一目录维护、共享验证状态可能降低摊销成本 | 属于待检验假设。SQLite 也支持索引维护和状态复用；不能拿离线重建成本冒充其必要下界。新实验组仍需确认登记 |

冻结实验的 FS-index select 当前还统计全 scope 的 selected_sources；这可能引入 O(C×V) 工作。该工作是否是必要查询职责需要对等审计。不能专门选择放大该统计的语料来证明数据库索引优越，也不能只优化 IoTDB 端后宣称索引复杂度胜出。

## 5. 实施与验收顺序

1. 冻结第3节存储/发布边界，确定定位记录线格式、显式迁移与本地读取预算接点。
2. 实现存储层文件定位能力、受管完整版本文件发布/补绑定；QuerySource 仅暴露格式中立能力。检测缺失、替换、错误版本、失败发布和租约生命周期。
3. HDF5 Adapter 接入本地原生读取；验证生产路径零数据库 payload 查询、无自身 FUSE 回环、无自建 chunk 页读取。保留所有现有科学值/掩码等价检查。
4. 更新模式契约、主线说明和 runner 的实际后端标识；旧封印原始结果保持不变，已有数据库 VFD run 不改写成新架构。
5. 在既有 FM1 的 HDF5 N8/N128 case 上做有限功能与完整计时探针，不新造语料/候选域；明确新 run 的引擎与存储协议。后续完整采集继续复用固定 catalog/corpus/case，并记录新实现和文件绑定收据。

步骤1–5已完成，验收记录如下。正式部署仍需显式配置受管根，并为既有版本补绑定；没有更换默认挂载的产品二进制。

## 6. 验收与已知边界

| 项目 | 结果 |
| --- | --- |
| 构建与单元测试 | 固定服务器依赖构建；87项聚焦测试、1,782项完整单元测试均通过。最终内容哈希校验并强制重编译所有变化输入；不是把mtime未更新的增量结果当作有效构建 |
| 真实数据库 | 8项功能检查通过：旧版本补绑定／幂等、自动发布、恢复、六属性筛选、ROI值和mask、无定位／丢失／相同字节替换失败；冷／热取值SQL、Store和payload计数均为0 |
| 固定case探针 | 独立 `FM1-hdf5-N8-V1-C8-S0-base-latest` 与 `FM1-hdf5-N128-V1-C128-S0-base-latest`；共72条，12预热、60测量，单block，不替代三campaign正式FM1 |
| 语料身份 | 补绑定8＋128个精确版本，每版本Exact/descriptor/seal/POSIX身份前后相同；原科学发布、raw、FS-index、oracle与归档journal不变；新增位置目录head与当前journal单列 |
| 准备成本 | N8补绑定40.648秒、510,916字节；N128补绑定249.333秒、8,173,187字节。均在查询时钟外，数据库payload继续保留，故完整副本是新增存储成本 |
| 实际资源 | CPU2–3隔离、总配额2核；查询组2GiB，原DataNode堆15,232MiB／直接内存2,176MiB另列；536个周期采样、144个查询边界快照验收通过，覆盖查询期间100个周期点；无OOM、查询包络间CPU限额节流增量为0 |
| 实际占用 | 查询边界观察到的查询组峰值94,797,824字节，数据库memory.current最大12,718,628,864字节；峰值含准备和context setup，不是查询独占RSS。KVM宿主调度与共享存储未隔离 |
| 完成与恢复 | 原服务配置哈希、许可证／节点身份及资源恢复由控制器核验；原配置SHA为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728` |

完整时间与d/q比值见[总报告的新通路小探针](../reports/hdf5-route3.md#native-filesystem)。N8/N128的完整暖中位数分别9.563／12.233ms，相对同轮FS-index的R为8.486／8.778，**尚未证明优于FS-index**。文件内q仍包括服务端HDF5新句柄、预算／身份检查、typed组装和IPC；FS-index保留8个file／dataset句柄及raw cache，缓存生命周期未完全对齐。本轮未实施新的跨请求HDF5句柄缓存或客户端直接取值协议。

服务器证据根为 `experiments/cross_version/work/hdf5-native-filesystem-20261021/`：

- `reviewed-build-binding.json`、`reviewed-focused-tests.xml`、`reviewed-all-unit-tests.xml`：最终产品SHA `3d58c7d56740fbb2a2b962e8b3558b5c12ee65703545e93ecb15f69a85d27cb3`。
- `gate-r1/probe.stdout`：真实功能原始输出。R1控制器把`completed`误读为`complete`，因此原失败收据保留；退出码0的8项功能证据经独立校验后由R2复用，没有挑选更快样本。
- `gate-r2/acceptance-audit.json`、`timing/queries.jsonl`、`timing/probe-summary.json`、`resource-samples.jsonl`及恢复收据：72条完整采集与独立验收；`executed-source/`保存执行时脚本原字节。之后仅修正控制器完成标记／输出计数并增加8项离线回归检查，未改产品、原始时钟或重跑计时。
- 本地白名单证据副本为同一work目录下的`evidence/`；归档SHA `353172a3f67abe386ca94fb872b9f0ee8ca7d1a51fc99c4f724fc4f2352bd946`，162个内容哈希逐项核验，私有配置及许可证未导出。

**后续打开要求：** E2阶段这两个namespace已显式补绑定，后续第6.1节的正式复测已将当前journal接续复制到新`collection-r1/corpora/<corpus_key>/owner-active`。现在全部7份HDF5语料按最新`current-locations.json`打开；原`collection-r2/.../owner`和E2 journal保留归档，不可同时启动多个owner或重新从旧head补绑定。N8/N128受管文件根仍沿用E2的`native-files`，其余5份位于新run中，必须保留这些数据根。GRIB的7份原语料未迁移。

保留的失败包括最初mtime导致的旧对象增量测试失败、R1控制器字段误读；均未进入性能样本。完整源码测试和最终收据验证通过后停止追加实验。

### 6.1 用户追加授权：补齐HDF5固定首轮组

材料身份：`FM1-HDF5-filesystem-20261021`；类型为固定语料的实现复测；状态为完整采集、独立重算、全文件哈希和原服务／资源恢复已验收。唯一实验清单、源语料、case和统计协议仍取自FM1，非新的科学实验设计。

- 16个既有HDF5首轮case、7份封印语料；包含H1 N8/N128/N512、H2 N512内C8/C32/C128/C512、H3 S0/S1/S2、H4 V8 I/U及四种选择策略。18扩展不执行，不改变种子、谓词、ROI或命中集合。
- 新FS通路完整三campaign：每臂2预热＋10测量，共1,728条（1,440测量＋288预热）。N8/N128也按三轮重测，前述单block72条不拼入正式统计；不按速度选择重测行。
- 产品仍为上述`3d58c7d…`，FS helper、客户端、查询循环、原FS-index及oracle不变。新增`experiments/fixed_matrix/hdf5_filesystem.py`只管理复用、补绑定、资源与验收，不生成新语料或改产品。
- N8/N128从既有当前journal复制后沿用受管文件根；其余5份只新增位置绑定。原科学Exact／descriptor／seal／POSIX身份逐版本保持，原归档journal不变，当前head与新journal收据单列。
- 沿用两核／query2GiB及原数据库配置与恢复；worker硬上限12,600秒、资源keeper14,400秒，采样每秒。支持进程、实际资源与虚拟化／共享存储边界如实披露；失败目录保留，不自动重跑失败科学采集。
- 执行前清单为`experiments/cross_version/work/hdf5-native-matrix-20261021/execution-plan.json`。正式目录`collection-r1/`已验收，旧探针／旧FM1原始记录保持不可变。飞书原表先填入N8/N128并标注单轮，正式三轮完成后原位统一更新HDF5的16行及原有实验条件，不新增变更章节、不改GRIB或扩展行。

| 正式三轮验收 | 结果 |
| --- | --- |
| 完整样本 | 16case×3campaign×3arm×12=1,728条，1,440测量＋288预热；未拼入E2的72条，没有失败科学采集重试或慢样本排除 |
| 科学与通路 | 全部值／mask与原oracle一致；792个精确版本、736个受管完整文件SHA逐项通过；2,304个取值socket profile的SQL／payload／layout／Store查询全部0 |
| 统计 | R/Rd/Rq均先求同campaign中位数之比，再取三轮中位数；16组中位R为7.031–10.164，最小H-H4-I-V8-all-filtered仍为7.031，未显示优于FS-index；表内汇总T不能直接相除重建R |
| 实际资源 | 3,544周期样本，查询包络覆盖2,197点，3,456前后帧验收；query kernel峰值467.59MiB、DB 15.66GiB；全活动观察期父组平均0.622核、累计节流86.385ms，无OOM。含准备／恢复／审计，非纯T或单case独占 |
| 准备成本 | N512新绑定628.445秒；三轮owner恢复／挂载436.264／442.241／438.617秒，均在T外并在表中单列。N8/N128本轮幂等核验33.607／148.837秒；原科学发布与SQLite建设只引用旧准备收据，本轮未重建 |
| 恢复 | 原ASYNC/true、原配置SHA及许可证／节点保护由控制器验收；独立核对DataNode PID1696413、CPU0–31与实验cgroup删除，原科学发布／raw／index／归档journal保持不变 |
| 证据 | `collection-r1/acceptance-audit.json`、`analysis.json`、`current-locations.json`及原始流；查询SHA `61caa43c9177c944f2d26593a0117b8485faa0e0754437d11e18e3abc8ae01ad`，分析SHA `aa9f5b2c3b7d788307be5fac6376b0b48ed84a11f965a7411a5b848c6fa99ed3` |
| 本地封存 | 同work根的`evidence/`，427个成员SHA逐项核验；包SHA `d02c3f811193d16b9acf0e51ed3d010099cd541866c3a6429dfb6dbd9a8b055a`。Feishu补丁／回读另存`feishu/`；不导出私有配置和许可证 |

完整结果及每语料准备／三轮启动见[统一HDF5报告](../reports/hdf5-route3.md#native-filesystem)。先行单轮数字仅作路径验收，不能取较快的9.563／12.233ms替代正式结果；正式N8／N128的汇总T分别17.180／26.938ms，对应R为9.833／8.774。原生句柄缓存与IPC差额仍存在，本次未进行新的产品优化或回退。

## 7. 原生句柄复用与批量取值消融（2026-10-22，已完成验收）

用户要求先对齐句柄／chunk缓存，再合并命中dataset的请求并做消融，已明确回复“按方案实施”。实现已通过177项聚焦测试和1,800项完整单测；独立N8先行96条及正式三组864条均完成，逐查询oracle、独立统计、资源及原服务恢复全部验收。两批不拼接；以下边界不改写此前正式原始结果。

### 7.1 已确认的成本与可验证假设

- 当前暖取值仍为每dataset一次typed请求；driver持有请求的QuerySource、取消回调和累计trace，不能直接跨请求保存。
- FS-index保留8个file／8个dataset，metadata上限每file 2 MiB、raw chunk cache每dataset 256 KiB。现产品每driver metadata 8 MiB、raw cache为0，每次重新打开file；describe和read分别打开dataset。
- 假设H：复用原生句柄和chunk后，相同工作集的暖查询新增H5Fopen/H5Dopen及原生VFD读取减少，完整T和Rq随之降低。若只有打开计数改善而T明显增加，则不能判为优化成功。
- 假设B：一次socket请求承载多个精确取值项后，控制往返减少，完整T降低；本轮不预设Arrow编码或逐项计划成本已经消除。
- 假设HB：两项收益可以共同保留；以2×2消融检验，而非只比较最终版本与历史最快结果。

### 7.2 原生会话池

| 项目 | 建议实现边界 |
| --- | --- |
| 所属模块 | 池和libhdf5句柄在HDF5 Adapter内；通用Core不解释dataset、chunk或HDF5缓存 |
| 容量 | 同一挂载的全部Adapter共用一个池，最多8个file、总计8个dataset；metadata每file上限2 MiB、raw cache每dataset 256 KiB，对齐FS-index。池新增总计费上限64 MiB，包括管理信息及仍被执行持有／已淘汰未释放的对象；库内部filter scratch另受原执行契约及实测资源披露约束，不宣称该池上限是进程RSS上限 |
| 身份 | 使用namespace/incarnation、完整head、精确版本、descriptor/seal及完整受管文件定位身份；dataset以精确路径及相关原生配置区分。不能仅按逻辑路径或物理文件名复用 |
| 请求状态 | 每次独占租用会话时绑定本次已准入QuerySource、读取能力、取消回调及预算；归还时清除这些引用，空闲池不得保存旧ReadContext、主体授权证明或请求账本。重置本次trace及错误状态，不能将历史会话消耗累计到新请求 |
| 缓存命中校验 | 即使libhdf5直接从metadata／raw cache返回而没有VFD回调，也在本次操作前后验证新读取能力和完整文件身份；权限撤销、head变化和相同字节文件替换均不能被缓存绕过 |
| 预算 | 原metadata费用复用、输出／解码工作空间、取消和执行／发布／新open准入保持；每次实际VFD范围读取仍先reserve_logical_payload并接受本请求累计范围／字节限制。缓存字节常驻计入池，不将未发生的物理读取伪记成I/O；逻辑预算与物理计数分别披露 |
| 淘汰和并发 | LRU；同一原生会话一次只由一个执行租用，不共享可变VFD状态。繁忙／容量不足走现有有界一次性路径；该路径仍受执行预算约束。无资格或损坏直接失败，不能作为普通cache miss掩盖；不确定I/O状态丢弃会话 |
| 默认与对照 | 不改变默认iotdb-indexed模式。native模式提供有界缓存启停配置，使同一产品二进制可完成消融；关闭时保留原一次性通路，开关及实际缓存容量写入profile和run身份 |

实现补充：原生会话键还绑定精确dataset、ROI和slab工作流。池保存该选择的有界冷读取费用，暖命中时逐范围向新请求重放逻辑字节与范围费用；实际VFD命中已预付范围不重复扣费，新增范围仍计费。这样保留源预算及整批累计上限，不把零磁盘读取当作零逻辑需求。其他ROI使用不同条目，不回放无关选择的历史费用，也不缓存ROI答案。空闲池只保存无读取接口的`NativeFileDescriptor`，不能保留旧请求guard；所有原生Adapter实例共用挂载级池。

### 7.3 合并取值请求

- 增加格式中立的typed批量注册入口；一个控制请求最多64项，每项仍是已有typed request，使用发现结果冻结的Exact版本，禁止在取值时重新选择Latest。总控制帧继续不超过1 MiB。
- 首版在服务端顺序调用既有typed执行函数；不新增工作线程，不绕过owner／权限／预算，不为并行效果额外占CPU。原生HDF5锁的并行收益后续单独判断。
- 一个批请求共用本次元数据账本并执行聚合资源预检；保留现行源／extent／元数据／payload及结果限制，不通过分项规避总预算。每项仍执行必要准入，不能凭批请求缓存授权。
- 返回带稳定item序号的结果路径清单，保留每项原Arrow输出、来源及dataset对应关系；客户端逐项生成owned values/masks。首版优化控制往返，不假称已经合并为一个Arrow流或消除了所有FUSE输出。
- 任何项失败则整个请求失败，并清理本批已产生的临时结果／路由；不返回部分成功清单。无持久格式变更，无直接客户端raw路径或数据库payload回退。
- 超过64个命中的查询分成有界批次；本轮既有case最多16个命中，所有取值可在一次控制请求提交。

### 7.4 固定case与验收

| 固定case | N／C／V／D／K | S／版本选择 | 本轮用途 |
| --- | --- | --- | --- |
| H-H1-N8 | 8／8／1／4／2 | S0／Latest | 独立N8功能探针及正式小规模对照 |
| H-H1-N128 | 128／128／1／4／2 | S0／Latest | 独立N128对照，不以N512子域替代 |
| H-H4-I-V8-all-filtered | 8／8／8／4／16 | S0／相同内容、All后六属性筛选 | 多命中批量收益与8句柄容量边界；不是无条件全遍历 |

每个case均执行C0B0、C1B0、C0B1、C1B1。C0保持原每driver metadata 8 MiB／raw 0；C1包含句柄生命周期及原生库缓存容量对齐，故C因子不是单独的句柄开关。K为命中dataset数；ROI、完整文件实际大小、corpus/case及oracle身份全部沿用原注册记录。

| 维度 | 方案 |
| --- | --- |
| 消融 | C0B0无缓存／逐项；C1B0缓存／逐项；C0B1无缓存／批量；C1B1缓存／批量。四配置使用同一产品、同一科学case；FS-index helper与其缓存配置不变 |
| 首次功能／计时探针 | 仅既有独立H-H1-N8，四配置分别配对FS-index，单campaign；结果单独封存，不拼入三轮统计 |
| 正式小矩阵 | 既有H-H1-N8、独立H-H1-N128、H-H4-I-V8-all-filtered；复用current-locations.json对应的三份语料，不重新发布、补绑定或重建FS-index。不加入N512挂载以控制准备耗时 |
| 次数 | 每配置／case／campaign：IoTDB-FS与FS-index各2预热＋10测量，3campaign；正式共3×4×2×12×3=864条，保留全部样本。配置顺序均衡并预登记，FS-index在对应block内配对，不能从不同block挑最小值 |
| 资源 | 沿用CPU2–3／父组2核、查询组2 GiB及原DataNode配置；完整记录环境、实际占用、节流、失败与恢复，原数据库／账号／许可证不变 |
| 时钟 | 所有组从查询入口到客户端owned values/masks计T；d终点为冻结命中清单，q包含全部控制、执行及结果交付。profile收集和oracle验证保持在T外；缓存／批协议及实现闭包进入run身份 |
| 指标 | 以同block完整T的R为主，Rd/Rq和绝对d/q辅证；报告三轮中位及范围。H5Fopen/H5Dopen、命中／淘汰、实际VFD读取、预算、IPC及输出一致性用于解释，不能替代完整计时 |
| 正确性 | 固定oracle的全部值／mask／来源一致；权限变化、源撤销、文件丢失／替换、缓存命中取消／预算、并发租用／容量、批中途失败及清理都有回归检查。不得以减少校验获得速度 |
| 决策 | 优化后完整T若重复显示超出同轮波动的明显回退，关闭／回退对应优化；如波动无法区分则报告不确定，不择优重采。不要求先跑16组全量；扩大仍只用已登记case |

### 7.5 完成结果与保留决定

同一产品、同一封印case的完整2×2结果见[统一报告](../reports/hdf5-route3.md#native-cache-batch)。正式864条包含720测量／144预热，另96条探针独立保留；没有失败采集重试或慢样本剔除。

| case | C0B0 R | C1B0 R | C0B1 R | C1B1 R | 合用配对R变化中位数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 独立N8，K2 | 9.403 | 7.060 | 7.415 | 5.625 | −36.6% |
| 独立N128，K2 | 8.485 | 7.517 | 7.846 | 7.087 | −17.7% |
| N8/V8相同内容All-filtered，K16 | 7.990 | 7.209 | 5.761 | 5.788 | −27.6% |

N8／N128合用的R三轮均改善，但N8完整T仍有一轮+2.0%；N128完整T三轮均改善。V8合用R一轮+4.3%、完整T一轮+56.9%，不能称稳定加速；B1下额外开缓存的R中位数+0.5%，故该组保持缓存关闭、保留批量。K2暖命中均无新HDF5句柄和实际VFD读取，逻辑预算仍每查询36范围；K16工作集超8会话、无暖命中。默认缓存仍false，默认引擎仍iotdb-indexed；不通过扩池或另造组制造效果。

资源3978周期／1728前后帧通过，无OOM／配额节流；query内核峰值768.38MiB、DB11.80GiB单列，不能称全栈2GiB。全部取值1656份profile无SQL／Store／payload或自建chunk读取。36次恢复／挂载合计3575.831秒在T外；原DataNode、全部227线程CPU0–31、配置与cgroup均恢复，原许可证及节点保护通过。产品SHA `b7e7d995…`；证据根`experiments/cross_version/work/hdf5-cache-batch-20261022/`，公开包`b0e54adf…`的577成员通过；完整范围、构建失败留档与恢复边界见统一报告。实现验收时未更新飞书，也未追加其他13个HDF5组；用户后续授权的飞书4.3原位回填已完成并回读核验至版本702，补丁、其他47行不变校验及前后记录保存在同work的`feishu/`，未新增章节或改写原实验包。

## 8. 机制参考

- [HDF5 VFD 文档](https://portal.hdfgroup.org/documentation/hdf5/latest/_h5_f_d__u_g.html)：VFD决定字节从哪里读取；使用原生文件内索引不自动等同于本地文件读取。
- [SQLite Query Planning](https://www.sqlite.org/queryplanner.html)：多列索引可服务AND谓词，覆盖索引可避免额外表查找。FS-index具备谓词索引能力，不能因其名称含FS而当作目录扫描。具体本实验索引以fixed_native.cpp及封印EXPLAIN收据为准。

## 9. 2026-10-23：原生热路径重复成本审查

状态：只读代码审查与既有证据复核，未改产品、未访问远端、未追加实验或实时采样。用户要求暂停虚拟机上的大批量I/O优化，后续移至物理机；以下仅列CPU、分配和不可变状态复用候选，尚未授权实施新的缓存设计，也不宣称性能收益。

审查以当前正式A代码为准。上一轮batch解析复用B已回退，仍归档在`batch-parse-reuse-20261022/candidate-archive/`；不能把该候选当作当前实现，也不把它重新包装为本轮新增优化。

### 9.1 已复用的状态与实际调用链

正式取值链为：typed注册及授权 → QueryModule准备 → HDF5 Adapter binding/plan → native cursor → open_local租用会话 → describe → 每slab read → typed batch → Arrow编码／结果路径 → 客户端owned values/masks。

- capabilities与prepare共用同一PlanningContext，bind命中已有PlanningState直接返回；不是每次都完整重新编译binding。见[QueryModule.cpp](../../src/query/core/QueryModule.cpp)的1242–1262行和[Hdf5FormatQueryAdapter.cpp](../../src/formats/hdf5/Hdf5FormatQueryAdapter.cpp)的393–413行。
- 已验证layout目录跨请求共享；native模式不进入自建chunk页读取及`prepare_hdf5_selection`。当前原生chunk预检属于HDF5 API安全预检，不是IoTDB-FS自建chunk索引查询。
- 会话池已保留H5F、H5D、metadata和raw chunk cache；不能再将“增加句柄缓存”列为尚未实现的优化。
- 当前只构造实际投影的坐标；Adapter物理值没有无条件生成CF坐标或validity bitmap。无残余过滤且schema一致时，公共层已有直接移动batch的快路。

### 9.2 重复项、优化点与约束

下表行号对应本次审查的当前源码。B表示一个非空命中dataset产生的slab数，L表示dataset路径组件数；避免与语料D、命中K混用。

| 项目 | 当前重复及代码证据 | 候选优化 | 必须保留的边界 |
| --- | --- | --- | --- |
| dataset描述与内部路径检查 | Kernel:449先describe；Driver:990/995执行path_links＋inspect，每slab又在1026/1031执行；共1+B次。每次路径检查调用L组exists/link-info/object-info；Dataset:542只保存句柄／LRU／计费，没有Profile | 先在单次租用内复用成功的描述与硬链接路径证明；再考虑随精确dataset句柄有界保留 | 精确文件身份、新请求授权、取消、ROI／输出预算仍重验；不能仅凭数据库descriptor跳过本地文件验证；新增常驻Profile须计入原池额度 |
| chunk分配预检 | Driver:1075–1119每slab枚举覆盖chunk，调用H5Dget_chunk_info_by_coord；raw cache命中也执行。slab按行／字节切片，不保证chunk对齐，跨slab可能再次检查同chunk | 在同精确文件／dataset／selection／limits内复用成功的预检证明，优先局部／有界复用 | undefined fill、encoded chunk长度、EOF、候选数上限和取消检查不能消失；仍交给HDF5原生索引，不新增自建全文件chunk目录 |
| binding与选择计划 | 完整binding已在请求内共享，但state命中前仍构造QueryRequest并校验、序列化request_key；prepare和cursor分别按同projection构造LogicalSchema；不同请求可能再次编译CF binding，单次首次构建中compile后bind还会重复遍历验证坐标 | 减少纯派生请求工作；在不可变plan保存投影schema／列角色；CF validated binding单独评估 | layout proof／owner冻结观察、参数和配置身份仍重验；现有物理ordinal组不走CF编译，不能拿CF成本解释这些case |
| native未使用的identity | Adapter:676–688无条件生成indexed decoded-cache身份；Kernel native分支389–464直接返回，没有使用它 | 只在iotdb-indexed分支构造 | native仍须保留前面的精确源、layout身份及transport检查；不是删除安全identity |
| 文件／dataset会话 | Driver:798–800先新建临时State并复制options，暖命中831又换成resident State；session key含完整head、精确版本、dataset、ROI、slab和limits | 延后到miss才分配State；稳定身份片段可评估预计算 | 保留命中前取消和fresh请求能力绑定；不直接缩短key或扩大池 |
| 会话归还 | Driver:671–682每次sort＋merge完整fees，即使trace没有新增logical_charged范围；在全局library_mutex下执行 | 无新增费用时跳过sort／merge；有新增费用仍走原合并路径 | request-owned source／guard／reader／预算／取消回调必须清空；错误会话仍退役，费用remaining仍在下次租用重置 |
| 输出值数组 | Kernel:183–215先分配最终列，再调用Driver:1049–1061另分配subset并由H5Dread填充，随后Kernel:246–275逐run复制 | native物理Float32/Float64直接移动subset值数组；物理Int16使用Driver已生成的Int32，消除Int32→Int16→Int32往返 | descriptor、dtype、长度、来源、取消和输出预算检查仍保留；CF科学值需要单独转换，不能直接替换为原始dtype；不得复用仍被结果持有的可变数组 |
| 公共Arrow数组构造 | ArrowResultEncoder:193–201逐元素Append，267–295另建Builder；向量到Arrow仍复制 | 先评估批量Reserve／AppendValues；buffer借用属于后续更大设计 | 适用于GRIB/HDF5，保持validity、owned结果、writer生命周期和内存计费；不是HDF5 Adapter专属修改 |

会话复用的结构边界也需保留：当前“8 files”实际是最多8个精确工作流会话，并不保证覆盖8个不同物理文件。不同dataset／ROI即使来自同一文件也可能占不同entry，K16已有容量miss证据。若要将文件状态与selection费用分层，需要单独确认设计；不能为了命中率移除selection key，把另一ROI的冷需求或旧请求授权混入本次。

### 9.3 不能直接视为冗余的检查

非空、成功执行路径中，每dataset有3+2B次显式validate_local：租用一次、describe前后两次、每read前后两次；外层NativeFileStore::reuse及实际pread前后校验另计。其内含取消、fresh guard、fstat和fstatat，对应精确文件缺失／替换／改变检测。**可复用的HDF5内部路径／Profile证明，与必须观察当前文件状态的校验是两种职责。** 在缓存命中完全没有实际VFD回调时，后者仍须成立；本轮不建议直接删除这些边界检查。

逻辑费用也不能随物理I/O命中而免除。Driver:840–852逐冷范围重放本次预算，258–277匹配预付范围并对新增需求收费；它是新请求的预算保真。可以减少管理／排序开销，但不能直接跳过reserve。批量预算接口属于后续公共层设计，不纳入本次最小候选。

进程级环境／plugin策略和ErrorScope也在describe/read重复执行，但涉及HDF5运行时安全约束；当前不优先改成未经证明的一次初始化。

### 9.4 既有profile给出的量级边界

只引用已保存的`batch-parse-reuse-20261022/derived-audit.json`中正式A的四轮，各值是每轮10条正式query中位数的最小至最大，不合并不同query、不择优、不作为新的A/B证据。K2、两次会话租用／两个slab的batch合计，单位ms：

| 既有阶段 | 独立N8四轮A | 独立N128四轮A |
| --- | ---: | ---: |
| hdf5.native.describe | 0.148–0.176 | 0.183–0.275 |
| hdf5.native.read | 0.166–0.188 | 0.199–0.237 |
| typed_prepare | 0.279–0.334 | 0.379–0.445 |
| typed_ipc_array_build | 0.093–0.109 | 0.107–0.126 |

这些是嵌套wall阶段，不能相加为独占时间或可节省上限：read本身包含预检、H5Dread和数组构造，并不全可删除；typed_prepare也不是binding纯耗时。profile没有单独分离release排序、path_links、inspect、allocation预检和输出复制，当前不能给出各项独占毫秒收益。

同批观察中位数始终为2个session hit／rebind、4个dataset handle hit、10次显式session validation、2个slab、8,192个输出元素和36次逻辑范围费用；36是预算冷需求重放，不能解释成36次数据库查询。对物理Float32，Driver与Cursor重复值数组对应额外一份合计32 KiB的分配／复制；这只是已知数据量，不是完整进程峰值或速度收益。

这些证据支持先清理确定的重复，但显示HDF5内部局部阶段处于亚毫秒量级，不能承诺仅靠这些项就消除毫秒级IPC、结果发布和客户端owned交付差距。此前资源审查已确认环境非平稳，因此不从历史最小阶段时间推出优化百分比。

### 9.5 建议实施顺序（尚未实施）

1. 先清理无需新增缓存的三项：归还无新增fee快路、miss-only State分配、native不构造indexed decoded身份。变更小，适合先做正确性验证。
2. 原生物理值数组直接移动，消除重复分配／复制和Int16往返；先覆盖纯物理dtype，科学转换单独保持。
3. 复用dataset成功描述／内部路径证明，随后才评估精确selection的chunk预检证明；先单次租用范围，跨请求常驻需明确额度、失败及失效规则。
4. plan投影schema／列映射复用和公共Arrow批量构建，分别验收；不同时加入异步交付或大范围并发改造。

不重新引入上轮未采纳的批量JSON解析B，不扩大池或造新实验case。后续完整性能结论待物理机上按固定case和资源／环境协议验收；本轮仅完成检查与建议。

### 9.6 用户追加授权：三项CPU小优化与49服务器A/B

用户随后明确要求先实施归还快路、miss-only State分配、native跳过无用indexed身份，并在49服务器运行A/B。此次授权仅覆盖这三项的小规模固定组，不启动大批量I/O或其余缓存／数组优化。

| 项目 | 本轮登记 |
| --- | --- |
| A／B范围 | A为已接受f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087；B仅变更Hdf5SourceDriver.cpp和Hdf5FormatQueryAdapter.cpp；不叠加上轮解析B |
| 固定case | 独立HDF5 N8/V1/C8/D4/K2/S0/Latest、独立N128/V1/C128/D4/K2/S0/Latest；原corpus/case/oracle、SQLite、精确版本位置绑定 |
| 次序／样本 | 两规模各4轮AB／BA／BA／AB，每配置每臂5暖身＋10正式，共480条；先N8完整恢复通过，再启N128 |
| 查询／统计 | 两侧native-hdf5、相同cache/batch设置和客户端owned完整T；同轮R/Rd/Rq及B/A，同时保留两臂原始T；不择优、不拼接旧样本 |
| 资源边界 | 沿用guest vCPU2/3分区、共享两核配额、query 2GiB和原DataNode配置；记录实际环境／资源，不宣称物理核或频率隔离 |
| 正确性 | 新增费用保留／暖归还不累积、命中前取消及回调异常、mutable回调副本语义；完整权限、文件身份和预算回归继续执行 |
| 入口／证据 | experiments/cross_version/hdf5_native_cpu/；服务器work/hdf5-native-cpu-20261023/；构建／测试／科学采集／恢复分别留收据 |

候选实现及正确性验证已完成：Release完整构建、101项HDF5／batch预算专项测试和1,828项全量单元测试通过。固定组A/B已完成，但未通过完整查询性能验收；两个产品文件及对应测试已按摘要恢复到已接受A，候选和证据归档。未重新生成或发布语料，默认引擎与缓存容量不变。

本轮首次采集在原DataNode重启时因默认5%空闲比例限制中止，科学查询为0，原失败目录完整保留。启动前`data_01`已有约290 GiB空闲，约为3.92%，低于5%对应的约370 GiB；新构建和源码合计不足0.2 GiB，并非本轮发布新实验数据导致。盘点后未进行数据迁移。

用户明确选择将`disk_space_warning_threshold`持久设为`0.01`，保留约74 GiB的余量保护。已备份原配置、只追加该属性，依次重启同一原ConfigNode／DataNode；`SHOW ALL CONFIGURATION ON 1`返回0.01，DataNode为Running，原ASYNC／partial-insert、许可证／节点身份、15,232 MiB heap／2,176 MiB direct及原cgroup／CPU亲和性核验通过。该阈值作为A/B两侧共同环境记录，恢复时保持1%，不恢复旧5%。新采集单列在服务器`work/hdf5-native-cpu-20261023/retest-1pct/`，不拼接此前失败记录。

两组480条全部完成，320正式、160暖身，服务及资源恢复全部通过。四轮配对变化的中位数：独立N8完整T −3.93%、R +11.12%；独立N128完整T +39.45%、R +6.73%。N128末轮其他30个guest vCPU忙度从12.92%升至70.59%，两臂同步减慢；不能把观察到的T增长全部归因于候选。按完整查询验收规则保守不纳入主线，不剔除慢轮、不追加实验。方法、逐轮结果、环境边界与服务器证据位置见[本轮验收报告](../reports/hdf5-native-cpu-ab-20261023.md)。
