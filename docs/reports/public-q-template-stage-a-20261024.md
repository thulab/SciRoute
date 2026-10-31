# 公共 q 不可变请求模板：阶段 A 实现与验收

2026-10-24。阶段A实现、Linux全量回归、真实环境资格与主实验、独立审计和服务恢复均完成；阶段B plan recipe 未启用。**静态声明复用机制成立，四组正式样本的q均值下降4.41%–7.12%、完整T均值下降3.95%–5.82%；逐块归一化结果和完整生命周期并非一致改善，暂不足以承诺稳定全局收益或改变N的算法阶数。** 接口保留为显式可选，普通入口维持既有行为。

## 实现

公共 frontend 增加 `query_template_prepare`／`query_template_execute`／`query_template_release`，完整协议见 [合约](../contracts/prepared-query-template.md)。模板保存私有 factory 产生的深不可变静态声明，含编译 paths 和 predicate AST；single／batch 分别保持原静态校验顺序。整份 batch 是一个模板，保留重复项、顺序、累计预算与失败原子性。

execute 不接受字段覆盖。每次仍执行 fresh selection、实际 Unix peer 授权、原 admission、完整 `QueryModule::prepare`、独立执行读预算／cursor、原结果编码和 publication/new-open admission。下层结构复检保留；按现 frontend 值语义复制必要声明，已不可变的 paths／AST共享，不能称为消除了所有请求复制。GRIB 仍两条串行 single，HDF5 仍一个两项 batch。普通 batch 的 JSON 序列化／递归解析调度保留。

d/q 在原同一 registry 共用 tagged payload、安全判型、UID/TTL 和配额。条件 erase 同时匹配 UID、ID、对象地址与共享所有权，避免 release、丢响应与 ID 复用误删；prepare 响应和异常有回滚 guard。模板不需要 catalog Binding 回调，不保存 source／ReadContext／权限／账本／native session／plan／结果。执行路由的不可变 request 通过 alias holder 保留模板 lease；单独解析出的 request 指针也保留它，先销毁所借 AST/paths 再退配额。64MiB 总池、8MiB 模板子限额、64 总条目／每 UID16、单项1MiB／5min idle TTL 均不变；pending/retired leases继续计费。

产品变更限定六文件：registry、socket 和 TypedQueryFrontend 的 header／cpp；QueryModule、格式 Adapter、解码、渲染和输出交付不变。实验及本轮证据目录为 `experiments/cross_version/work/public-q-template-stage-a-20261024/`，服务器路径前缀为 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/`。未执行 Git 写操作。

## 历史与测量边界

旧 `b26c2aed…` 批内解析复用的 N128 完整 T +18.33% 是原实验统计结果，仍按当时口径保留；该变化**不能直接解释为代码导致的回归**。当时末轮两侧共同变慢及后台负载证据说明存在共同干扰的可能，也不能据此事后删除样本或认定旧优化已经有效。详见 [历史审计](../../experiments/cross_version/work/public-q-reuse-design-20261024/history-audit.md)。本轮静态跨请求模板与旧批内重试分开，普通 batch 不直接恢复旧候选。

主实验预先定义为**同一 candidate daemon 的 A 普通入口／B 模板入口机制对照**；f09 是冻结源码／构建／正确性基线，不是同步计时的另一个产品。不得据此宣称新 daemon 相对旧 f09 的跨二进制总收益。

资格仅两个既有 N8 格式组，480 行；主实验四个既有 GRIB/HDF5 N8／N128 组，960 行，640 正式行；两阶段不混样。每组四块，每块独立客户端、A/B 两个独立 FS-index context；mode/arm 相邻交替和平衡。主实验四组各占一次块内位置0–3，保留 block ordinal／repeat／前后资源及1Hz观察器，直接检查末段效应。

完整 owned 终点与 T=d+q 保持原定义。模板从真实 d 结果准备；首轮实际管理控制调用的 `total_ns` 单列，在q中扣除并在生命周期中加回（不是仅扣内部 `control_ns`），声明匹配／管理bookkeeping仍在q。prepare／release与row引用的同一ledger event只计一次，discovery管理由两模式共享并单列。全量均值／中位数、逐块raw B/A、normalized ratio、首/中/末和负载描述分开；主结果不删除慢块，不凭normalized ratio单独采纳。

## 已有验证与过程记录

- 最终 r4 在 Linux 完整 clean Release 构建通过，模块依赖检查通过；定向301／301、全量1913／1913通过，无跳过。129项必需回归在两套结果中各完整出现一次，包括Linux跨UID／同UID补充组变化及独立request配额生命周期测试。独立复核528文件源码闭包、6文件产品差异、所有二进制及测试XML哈希一致。
- 最终产品 SHA256 `3440025ea021505d5057db628cd241f2a099e82d0e39a57f7e571f9d2869e321`；构建收据 SHA256 `273233b3bcce71215358e82860c896bc30904f6116f04af20e7a97c6d612a539`；源码清单 SHA256 `7079762e575a6b7966d902284b147b1b79cd0ad1da84d214f17fe1c5b5792e7f`。
- Registry 16 项 ASan/UBSan 通过，含4项新增条件删除／ABA／TTL／并发回归。
- 生命周期修补后的本地聚焦33项通过，覆盖原Frontend、新single/batch模板和d selector；两项新增用例验证独立request指针及最后batch route释放前仍占配额。
- 本地 reduced frontend 23 项新模板／selector 回归通过；86项原接口中85项通过，1项因本机缺指定 thread-safe HDF5 1.14.4 在 ingestion 前置阶段失败，尚未进入 socket。
- Linux 与本机实验框架分别13项离线测试通过；包含取消／等待原 supervisor 恢复、完整网格拒绝和真实 discovery 构造模板。
- 受保护窗口两项owner/d探针均通过，原ASYNC/true配置、cgroup及全部线程CPU0–31恢复，并独立核验。N8资格采集480行／320正式、8mount、640profile、40生命周期事件全部通过；168个实际采集源与528个产品源分别核对。实际q selector编译GRIB为2→0、HDF为4→0，原注册／admission保留，正式d/q逻辑费用逐对相等。独立本地复核逐块数值和生命周期恒等式通过；资格样本不进入主实验统计。
- 初次冻结前检查发现新测试 fixture snapshot loader 返回类型不符；只修测试，产品哈希未变。初始源码／语法失败及修复说明保存在 `preflight-r1/`。该次没有产品构建或性能采样。
- 首个Linux新groups测试错误地把同一组同时设为主组与补充组，现有规范化会去除重复主组，导致290/291通过；修正为固定主组、只切补充组后291定向／1911全量通过。该中间候选随后因独立生命周期复核发现共享AST/paths可能提前退账而被替代，未进入性能采集；归档于preflight-r2/r3。最终r4增加声明alias-holder并重新完整构建／验收。
- 新 socket 集成覆盖静态 prepare 零源读取／授权、schema和权限检查后置、重新选择 Latest、独立请求预算、整批累计失败、collector观察和首错／回滚、d/q跨kind／共同UID配额、restart／丢响应。实际跨UID与同UID补充组变化在Linux专用用例验收；不能用macOS通过代替其结果。

## 主实验结果

以下只用四组联合主实验的640条正式行，每组／模式／臂40条，单位ms。A为普通入口，B为模板入口；变化为B/A−1。全部960条原始记录保留，含320条预热记录；没有慢块排除。每组只有4个独立配对块，不把40条重复测量当作40个独立实验。

| 格式／N | q均值 A→B | q变化 | T均值 A→B | T变化 | q中位数 A→B | T中位数 A→B |
| --- | --- | --- | --- | --- | --- | --- |
| GRIB／8 | 4.411→4.097 | −7.12% | 5.321→5.011 | −5.82% | 4.317→3.926 | 5.216→4.826 |
| GRIB／128 | 4.092→3.845 | −6.02% | 4.957→4.707 | −5.04% | 4.036→3.808 | 4.891→4.679 |
| HDF5／8 | 9.021→8.624 | −4.41% | 10.764→10.338 | −3.95% | 10.291→9.873 | 12.438→11.817 |
| HDF5／128 | 4.284→4.061 | −5.22% | 4.984→4.763 | −4.42% | 4.157→4.014 | 4.847→4.748 |

d不是本轮优化对象，其均值GRIB N8/N128分别0.910→0.915／0.865→0.862，HDF5分别1.742→1.715／0.699→0.703ms。B的q占T约81.7%–85.3%，故q的小幅改善传递为稍小的完整T变化。

### 逐块结果与不确定性

每格依次为campaign0–3的**块内q中位数B/A**；归一化逐块计算 `(IoT B/A)/(FS B/A)`，再给四块算术均值／中位数。归一化用于描述，不能当作已校正的因果效应。

| 组 | IoT原始四块B/A | FS归一化四块比值均值／中位数 |
| --- | --- | --- |
| GRIB N8 | 0.983／1.005／0.843／0.912 | 0.963／0.960 |
| GRIB N128 | 0.975／0.996／0.896／0.894 | 1.000／0.984 |
| HDF5 N8 | 1.003／0.998／0.880／0.873 | 0.916／0.909 |
| HDF5 N128 | 0.999／1.011／0.977／0.934 | 1.003／1.027 |

GRIB N128原始四块方向一致，但归一化四块分别1.179／1.104／0.864／0.851；HDF5 N128归一化中位数反而+2.72%。因此不能将pooled mean写成所有配对口径均稳定提高，也没有足够独立块证明固定百分比收益。上述主表是同一新程序两种入口的实测差异，仍不代表相对旧f09二进制的总收益。

### 机制及耗时位置

两N均确认GRIB每次q的selector编译2→0，HDF5为4→0，B实际复用两份声明；原注册次数GRIB2／HDF5 3、实际read/version admission均6，完整规划与两份结果物化仍在。q逻辑metadata consumed bytes逐条正值且A=B：GRIB N8／N128为35,284／139,559；HDF5为50,891／127,963。d也逐对严格相等，collector与异常路径另由回归覆盖。

N128的互不重叠q均值包络如下，单位ms；server/control均可能包含等待，不等同独占CPU。

| 格式／入口 | server | control内server外 | q内control外 | 合计q |
| --- | --- | --- | --- | --- |
| GRIB A | 1.593 | 0.606 | 1.892 | 4.092 |
| GRIB B | 1.401 | 0.570 | 1.874 | 3.845 |
| HDF5 A | 2.092 | 0.341 | 1.851 | 4.284 |
| HDF5 B | 1.908 | 0.301 | 1.852 | 4.061 |

N128累计q JSON parse计时GRIB 0.123→0.0148、HDF5 0.0869→0.00849ms；typed prepare仍约0.398→0.393／0.396→0.409ms，没有被跳过。其余嵌套stage不能与本表重复相加。B仍有约1.85–1.87ms位于control之外，且HDF5 execute setup、描述与编码继续存在，阶段A未解决这些主要剩余成本。

复杂度变化限于将重复请求的静态JSON／selector／AST工作移到prepare；execute仍复制必要值语义声明、校验和完整prepare。GRIB仍执行每源Θ(D·E)消息筛选与至少Ω(R)输出；HDF5仍逐item绑定、setup、读取和物化。固定组q并没有重新遍历N个文件，本轮不消除其目录新鲜性、费用身份或输出复杂度。两点经验q斜率β=(q128−q8)/120：GRIB A/B为−2.658／−2.094μs每新增路径，HDF5为−39.476／−38.026；负值混合了不同块的环境与负载差异，不能据此归因或推导渐近阶数，更不能声称N斜率降低。

## 管理费用与完整生命周期

下表为四块的**每块均值**，单位ms。q-life B包含prepare＋实际15次q＋release，T-life B同理；15次含5次预热。共享d prepare/release只计入实际paired mount一次，完整逐块/前缀记录在派生JSON中。没有将row引用与ledger事件重复加账。

| 组 | B prepare／release | A 15次q／B q-life | A 15次T／B T-life |
| --- | --- | --- | --- |
| GRIB N8 | 1.047／0.820 | 133.237／116.537 | 149.531／132.202 |
| GRIB N128 | 1.365／0.650 | 130.009／130.021 | 166.092／165.419 |
| HDF5 N8 | 0.875／0.762 | 195.896／185.954 | 225.400／214.697 |
| HDF5 N128 | 0.555／1.574 | 121.704／115.061 | 155.291／148.712 |

**GRIB N128实际15次q生命周期约持平，B略高0.0094%，没有取得与暖q均值相同的收益。** HDF5 N128 campaign1 release=5.128ms已保留，表中的1.574ms是均值，不是典型单次。首次q约96–150ms，会依AB／BA顺序落在不同mode；四块平衡顺序并不等于独立冷缓存。因此这些生命周期值是完整账务实测，不能据此宣称独立冷启动提速或推导冷态盈亏平衡点。

## 末段效应、负载与观察器

本轮不支持“最后campaign或最后位置普遍升高”。B q均值campaign3/campaign2的IoT／FS倍数：GRIB N8为0.886／0.801、N128为0.975／0.931；HDF5 N8为2.699／2.060、N128为1.040／1.263。最后全局block15是GRIB N8 campaign3，两侧均比上一轮快。HDF5确有两侧共同变慢的块，但不遵循通用尾轮规律。

HDF5 N8的B q中位数campaign0–3为10.976／9.959／3.759／9.864ms，FS为1.229／1.228／0.526／1.322ms；三个慢块位于序列位置1／0／2，位置3反而最快。慢不是单条离群值，也不是N大时特有现象。块内last5/first5 q均值上升的IoT A/B分别8／7块（共16），FS分别8／10块；N128 B的GRIB有3/4块下降，HDF5有4/4块下降，不支持普遍批内尾升。位置在单组内与campaign一一对应，不能单凭位置作因果解释。

60/64个正式cell的other30 endpoint CPU聚合达到分辨率阈值，忙度27.35%–36.05%，其余4个保留missing；未见末轮普遍暴涨，**不能据此称后台负载相等**。parent/query/database正式包络中的throttle与OOM计数均0。host aggregate可分辨的64cell steal均0，但CPU2单行最大有1tick，CPU2/3逐cell百分比全部因不足100ticks而missing；没有频率或物理核独占证据，不能排除细粒度虚拟化、共享资源和调度干扰。

观察器882次采样，单次capture wall／CPU均值约175.4／175.3ms，开销不可忽略。640正式行中114条确定与q重合、115条可能重合（其中1条歧义）。HDF5 N8 B在明确不重合诊断子层中，四块IoT q仍为11.266／8.304／3.893／10.843ms，FS约1.197／0.987／0.597／1.357ms，直接capture重合不足以解释整块差异；这不排除间接观察器影响。census完整进程CPU归因只有contained 5/17、boundary 10/32，其余不能作精确背景core匹配。上述子层仅描述，主结果没有删行或因果校正。

历史+18.33%仍是当时的观测结果；本轮证明了不同入口机制及当前平衡采集下的结果，不能事后证明旧批内优化正确，也不能把旧回归全部归因于末轮异常。

## 验收、恢复与留存

主实验16mount、16独立客户端、32 FS-index context、1280profile、80管理事件，科学／机制／费用／源码门禁全部通过。独立本地再次核验41归档成员、528个当前源码文件、129项必需回归和136条聚合／时钟／生命周期恒等式；原始科学行在服务器执行审计，没有假称本机重新读取raw。

最终DataNode PID1696795；配置SHA `f69f408de5c09edabae3a8293cf662327f794b908cbf98d5a02b3878fea154f4`，有效ASYNC/true、3项受保护身份、原cgroup和全部线程CPU0–31均核验。16mount／daemon、16查询客户端、32native context已关闭，3个临时资源组不存在；观察器正常退出。没有Git写操作或飞书表格更新。

- [主实验数值与全部块](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/public-q-template-stage-a-20261024/completion-export/comparison-derived.json)
- [主实验独立审计](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/public-q-template-stage-a-20261024/completion-export/audit-main.json)
- [负载／位置／观察器描述](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/public-q-template-stage-a-20261024/completion-export/load-derived.json)
- [根节点独立核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/public-q-template-stage-a-20261024/root-verification.json)
- [原服务恢复检查](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/public-q-template-stage-a-20261024/completion-export/gate-after-main.json)
- [复现归档](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/public-q-template-stage-a-20261024/completion-evidence.tar.gz)

主derived SHA256 `d2a8cb9f795d2111ff65ad3a8428719872fa9da1275aa96d1420833de342f5c7`；load `ac5a631ea45ae221146ccdc007da0238e9b5b44c3cdcca76dfe26c007bb5e4a9`；归档 `9dd4955aface151baee4c967626cd38b92e52338088cadfb5faa0b0c434a1c14`。原始科学记录、完整census和私有日志留在服务器同名工作目录。

阶段A保留为有语义回归保护的显式接口；本轮支持静态处理减少及有限暖态收益，不支持默认全量切换的固定收益保证。下一阶段仍应针对公共规划与owned交付的剩余成本分别设计验证，阶段B未在本轮擅自启用。
