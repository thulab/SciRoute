# q 查询模型：GRIB 复杂度与 HDF5/native read 成本差距

2026-10-24。完成当前源码调用链审计与冻结记录的离线再分析；未修改产品、重跑性能实验、改变服务或执行 Git 写操作。

**GRIB 暖 q 的主要线性自变量是消息数 D、谓词工作量 E 和输出点数 R；当前固定组没有重新扫描 N 个文件。HDF5 的现有暖态差距主要位于 native.read 计时之外：N128 q=4.046 ms，而 native.read=0.212 ms，仅占 5.24%。** 可优化空间集中在公共 q 的重复规划、逐项物化与结果交付；现有计时尚不能把交付包络精确拆成 FUSE、Arrow、owned copy 和调度各自的独占成本。

## 1. 版本、引擎与证据范围

GRIB 采用当前费用摘要程序 f09f3742246be3402aaf7dec764f4a7d7d357740a9fb916e30b3b2ca9bed5774 的既有联合主实验，480 行、320 正式行，数值来自前轮已验证 `grib-q-diagnosis-20261024/q-derived.json`。HDF5 使用最近合格性能程序 50f24526ed71ccb9da8e6b2c06e5de4fd0582e3c851a3e5bc3574916150a296a 的 `collection-hdf5-n8/n128`，共480行、320正式行；全部保留，A显式selector、B复用句柄，每N/模式/臂40正式记录。下列HDF主表使用B，A和全部campaign仍在派生文件。

**HDF没有在最新GRIB f09程序上重新正式采集。两格式数据不是同版本、同窗口性能比较。** 当前算法审计使用工作区冻结源码；HDF耗时限定为最近合格50f样本，不伪称最新程序重测。

HDF实际profile和每次launch均核验 `native-hdf5`、filesystem payload、native session cache=true。它通过自定义只读pread VFD读取受管本地文件，用libhdf5文件内索引/过滤器；不经数据库payload，也不经数据文件自身FUSE。结果仍经typed Arrow/FUSE交付。FS-index的q则直接用进程内libhdf5写客户端owned数组；SQLite索引只用于d。此处不能用iotdb-indexed chunk decoder解释耗时；其独立源码路径见附件审计。

固定组N=P=C从8到128，V=1、D=4、K=2、S0、Latest、输出40960 B。GRIB q为两个串行typed请求；HDF q为一个包含两个item的typed_batch，逐item顺序执行、两个独立Arrow结果路径。两者均结束于客户端独占Float32 values＋uint8 masks，profile/oracle在t2之后。

## 2. GRIB q 的算法模型

一般产品一个typed请求可以包含多个源和多个命中message，不要求每unit一个请求。用J表示q请求数，P_i/L_i表示请求路径项/请求字节数，C_i表示实际候选源数，D_ij表示每源message数，H_ij表示命中message数，E_i≥1表示完整谓词求值工作量（含空谓词函数常数、字符串/IN比较），R_ijh表示输出点数，G_ijh表示完整grid点数。F_i为实际费用观察/遍历数、U_i为ledger distinct identities，M为保留route历史数，Q为受配额约束的cache条目数。N为文件数、V为版本数，目录规模另记Nver≈N·V。

在parsed index/native field均命中、无collector/淘汰、普通成功的条件下，工作量可写为：

```text
Wq,warm = Σi=1..J [
  O(L_i + P_i log(P_i+1))                    # 解析/selector规范化
  + A_i + O(F_i log(U_i+1)) + O(M)           # 准入、账本、route清理
  + Σj=1..C_i Θ(1 + D_ij · E_i + H_ij)      # 重新选择message
  + Σj,h Wrender(G_ijh,R_ijh)                # cache命中仍render
  + Θ(output_cells + B_IPC_i + B_owned_i)    # 物化/编码/交付
]
```

式中省略了字符串比较长度乘数；A_i含实际principal补充组规范化、fresh head/owner admission、缓存定位等，典型有O(g log g＋log Q)。暖candidate/proof命中使用已捕获证据，不必每次重走目录B-tree。冷exact查找中，若a为需查的祖先/依赖key数、h_tree为树高、b为页内条目上界，比较工作可有O((P_i+a)·h_tree·log b)，平衡有界页树简化为O((P_i+a)log Nver)；宽域/all-version/subtree选择须另计实际扫描。F_i按全部实际观察定义，prepared-visit counter不是所有费用工作的完整计数。

| 环节 | 可确认的复杂度 | 当前固定组及含义 |
| --- | --- | --- |
| 暖message筛选 | Θ(D·E)／源 | q收到path/version＋predicate，未使用d返回的offset；仍扫描D条消息。K2合计8次message求值 |
| 暖全grid render | Θ(R)，全选R=G | native缓存返回共享F64字段，但仍生成新输出；每unit4096点，合计8192 |
| public DomainSlice | 最坏Θ(nx+ny+R) | 准确为Θ(S_axes＋R＋输出轴长度)，S_axes为实际带slice的输入轴长度之和；空结果也可能付轴扫描成本 |
| 规则私有crop／bilinear插值 | Θ(R＋输出轴长度) | 规则crop算术定位；bilinear每输出点4邻点，不遍历全部G；附带DomainSlice时另加轴扫描 |
| metadata费用重放 | O(F log U) | q尚未启用d的跨批次摘要；prepared visits仅104→111，逻辑字节不等同读取/复制字节 |
| cache定位 | ordered map为O(log Q)，另乘key比较 | 非无条件O(1)，且Q/字节预算有限 |
| route TTL清理 | O(M)／固定次数操作 | M是同mount历史，不是N；受TTL/配额约束 |
| 完整IPC/owned交付 | 至少Ω(R)与Ω(B_owned) | 输出必须形成；多次同阶拷贝和固定协议开销可减少 |

当前代入J=K=2、C_i=P_i=1、D=4、每源H=1、R=G=4096，主项是两次小规模准备/准入、Θ(8E)、Θ(8192点输出)及固定协议处理。没有Θ(ND)全候选遍历项。N仍可通过捕获的费用身份数、目录页/版本证据、cache工作集和资源争用间接影响q，不能据此宣称严格O(1)。

native field miss仍可能命中encoded-message cache；仅在encoded cache也miss时，另付完整encoded message的Θ(B_enc)读取/复制/哈希工作。native miss仍需让ecCodes形成完整G个F64值，小ROI的R≪G不会把冷解码自动降到R。这部分至少有Ω(G)物化/空间，另加外部ecCodes解码及实际发生的encoded加载工作，不能对所有packing无条件声称严格Θ(G)。冷parsed layout按字节/记录线性解析，没有D条message排序；selector/catalog的排序是别的环节。

峰值空间包括ledger O(U)、候选/消息plan、cached native Θ(G)、新render Θ(R)、batch副本与完整IPC Θ(B_IPC)，客户端全部owned结果累计Θ(B_owned)。GRIB native缓存现为16 fields/64MiB；不同缓存身份工作集超额、身份失效或淘汰可能使暖模型失效，重新支付miss路径。target_batch_rows只切render后的结果，当前不能把整R的render峰值空间降为单batch大小。result-store容量压力下若连续b次线性找淘汰项，可有O(bM_result)、最坏清空时O(M_result²)比较；当前暖模型排除淘汰，未据此认定它是实测瓶颈。

### 当前GRIB瓶颈及可优化性

已测N128正常块q≈4.355ms，候选费用重放≈0.0447ms（约1%）；其不包含全部费用阶段，不据此推断所有预算处理总成本。慢块与正常块的5.304ms差额，57.5%在control外，31.8%在server内；字段cache均hit2/load0。这说明当前应优先研究重复规划和交付边界，不能默认把q费用摘要排在首位。

1. **复用已验证的value plan／unit定位。** d已有selector handle，但未覆盖q；GRIB仍重编译selector并扫描D个message。服务端持有、完整绑定的unit hint/immutable plan可减少重复D·E与prepare；不能信任裸offset。当前D4收益可能有限，D大时此项模型收益更清楚。
2. **减少公共物化和交付成本。** native hit仍按task/future/线程池执行、等待后render；Float32路径目前存在double输出→float→batch slice→Arrow builder等多个线性过程。可研究warm-ready同步路径、直接构造带owner的输出buffer、批量Arrow构建及FUSE copy_into。它们主要降常数/峰值空间，输出Ω(R)仍在，尚无新A/B收益证明。
3. **GRIB批量化需先确认契约。** HDF已用公共typed_batch，GRIB尚未接入。现有batch共享整批累计预算并在失败时回滚本批结果；从两个独立typed改成batch会改变预算/失败原子性，不能当作纯无语义变更混入实验。即使批量，逐itemrender和两个结果交付也不会自动消失。
4. **随规模选择次级项。** public DomainSlice可研究规则轴算术/二分定位；route expiry可改有序到期结构/增量清理；F大时再研究q费用摘要。result store已有编码后的SHA/dedup，它只省长期存储，不省本次prepare/render/encode；提前复用typed结果属于新设计，不能误称已存在。

所有复用都须保留actual principal、精确版本/layout、head变化时重验/重绑、执行/发布/new-open准入、逻辑费用、预算不足的原序失败、collector逐项观察、取消及共享配额/lifetime。缓存授权结论、直接跳过预算或冻结Latest均不在方案内。

## 3. HDF5 q 与 native filesystem read 的实测差距

以下B的全部正式样本，单位ms；比值是均值之比，未删慢块。

| N | IoT q均值 | IoT q中位数 | FS q均值 | FS C++ read均值 | q／FS q | q／FS read |
| --- | --- | --- | --- | --- | --- | --- |
| 8 | 7.308 | 5.526 | 0.731 | 0.506 | 10.002 | 14.448 |
| 128 | 4.046 | 3.920 | 0.525 | 0.363 | 7.701 | 11.138 |

N128的完整q差为 **3.520ms**；中位数IoT q=3.920ms、FS q=0.487ms，二者比为8.056。FS read=0.363ms不是它的全部q，余下约0.162ms包括NumPy分配、row JSON、ctypes/C ABI解析和循环等。不能拿IoT完整q与纯磁盘读取比较。

N128 IoT q可进一步合法拆成四个互不重叠的均值包络：

| 区间 | ms | 占q |
| --- | --- | --- |
| native.read | 0.212 | 5.24% |
| server 内、native.read 外 | 1.726 | 42.65% |
| control 内、server 外 | 0.344 | 8.51% |
| q 内、control 外 | 1.764 | 43.60% |

native.read之外合计 **3.834ms**，约94.76%。主要耗时位于公共查询／native会话设置／物化及结果交付，而不是已计量read区间；这些包络也包含等待和调度，不能全部视为框架CPU。即使让这段read计时归零、其他工作不变，q也只减少约5.24%；这不是“全部HDF5工作”的上界，因为describe/session等工作在read计时外。

服务端还可用下列inclusive子阶段定位；**本表嵌套，不能纵向相加**：

| 阶段 | ms | 占完整q |
| --- | --- | --- |
| typed prepare／规划 | 0.372 | 9.19% |
| execute setup | 0.365 | 9.03% |
| native describe（setup 内） | 0.198 | 4.88% |
| typed encode（含 cursor/read/IPC） | 0.525 | 12.97% |
| IPC encode（typed encode 内） | 0.261 | 6.45% |
| assemble.copy（含 native.read） | 0.218 | 5.38% |
| native.read | 0.212 | 5.24% |
| native 库锁等待 | 0.001 | 0.02% |

prepare每次仍绑定源/layout与能力；execute setup含session租用/rebind、文件/head验证、冷range逻辑需求重放和describe；typed encode含cursor/slab、Arrow schema/build/write。`native.read ⊂ assemble.copy ⊂ cursor ⊂ typed_encode`，describe则在setup内。候选费用重放仅约0.034ms，native库锁等待约0.00064ms，不支持把这些已计量小项作为当前主要耗时。

`assemble.copy−native.read`仅约 **5.709μs**，其中还有锁获取/descriptor检查，不是纯拷贝计时。只消除subset→LogicalArray这一层副本，不能据本组承诺毫秒级收益。Arrow/FUSE/owned等其他复制另有边界，不能由此一并排除。

### 读取时钟的含义

FS read_ns从C++ read函数入口计，含frozen catalog身份、文件/dataset cache与文件校验、shape/type检查、H5Dread、finite检查和mask填充；不含row JSON的C ABI parse及Python分配。IoT native.read含descriptor、逐chunk preflight、hyperslab/内存分配、H5Dread和最终文件验证，但不含此前session/rebind、describe，也不含之后的LogicalArray/Arrow/客户端拷贝。**两者都不是纯H5Dread或物理磁盘计时；0.212＜0.363不能证明IoT的H5Dread更快。**

FS native stats逐context差分的范围是整次d＋q，不是全部q独占指标：stat_calls4包含d中的两次select校验和q中的两次文件校验，candidate_units/select_ns属d；read_ns/read_calls及file/dataset hits可从源码定位到q。本次差分无reset/missing，不能把累计read_ns直接均值化。

固定HDF工作量为2个dataset、8192点、8个几何chunk、2个slab；两N的session hits=2、rebinds=2、logical range replays=2、logical calls=36。metadata prepared visits仅110→117；没有重扫d的C128全域，单dataset按目录路径定位，不像GRIB重求D条message predicate。几何chunk数不等于物理read/解压次数；缺少的libhdf5内部cache/decompress子计时保留missing，不作零。

两侧都使用libhdf5，file/dataset各最多8、raw chunk每dataset256KiB；IoT session另受64MiB配额及selection/limits/fullhead完整key约束。metadata cache策略、文件能力及上层语义不同，不能声称等量功能。IoT多了namespace/权限/新鲜性、预算、lease及typed结果机制；FS只保留frozen catalog版本和本地文件校验。输出与owned终点相同，使端到端差距有意义；额外功能应保留，重复实现和拷贝次数仍可优化。

### 各块方向

| N | campaign | IoT q ms | FS q ms | FS read ms | q／FS q |
| --- | --- | --- | --- | --- | --- |
| 8 | 0 | 4.885 | 0.609 | 0.410 | 8.019 |
| 8 | 1 | 11.226 | 1.198 | 0.827 | 9.371 |
| 8 | 2 | 9.399 | 0.542 | 0.365 | 17.355 |
| 8 | 3 | 3.723 | 0.574 | 0.422 | 6.487 |
| 128 | 0 | 3.888 | 0.477 | 0.329 | 8.144 |
| 128 | 1 | 4.223 | 0.554 | 0.380 | 7.627 |
| 128 | 2 | 4.342 | 0.597 | 0.416 | 7.268 |
| 128 | 3 | 3.730 | 0.473 | 0.328 | 7.887 |

N128四块q／FS q约7.27–8.14倍，差距在各块均存在；N8较不稳定，四块均值也全部更高。HDF各块重复观察到的端到端差距与前轮GRIB“特定块变慢”是不同问题，不混用其因果解释。

## 4. HDF复杂度与优化优先项

native暖单item可写为 `O(A_warm + I log L + Rdem + Ru log Ru + H·chunk_lookup + Jio log Ru + W + Bphys + Z + Bipc + M)`：I为dataset路径深度、L为目录对象数，Rdem/Ru为冷保留逻辑range出现/去重数，H为覆盖chunk数，Jio/Bphys为实际VFD读取次数/字节，Z为libhdf5未命中chunk解码工作，W为选中元素数。完整q逐K项相加并交付owned输出。冷miss另加目录解析、file/dataset打开及实际存储/解压；libhdf5索引成本按其布局/缓存而变，不硬设恒定IO时间。

Rdem逐出现重收逻辑payload预算，Ru归还时仍排序；H为chunk preflight；W和Bipc是输出物化。它们的自变量均不直接等于全局N。固定K2/D4/W8192/H8/Cq1时，没有取值必然产生的O(ND)项。不同完整session key的活跃工作集超过8，或先耗尽8个dataset/64MiB等配额时，暖模型可能转为miss；K不直接等同session数。GRIB亦受16个不同native field/64MiB等配额约束，不能用本组K2外推任意热集。

建议先设计公共q的两类优化：

- **减少重复规划。** typed_batch先验证全部item后又逐项JSON序列化/递归解析，当前HDF selector compiles=4而QuerySource open调用=2（不是两次物理文件打开）；可复用immutable validated item/plan，同时保留预验证顺序、整批预算、首错和回滚。d句柄、session cache和batch均已存在，不能把它们再次列为新收益。
- **拆清并减少结果交付。** q−control约1.764ms是最大未细分边界；先独立计时FUSE acquire/getattr/open/read、Arrow读取、owned copy与客户端线程CPU，再评估copy_into、Arrow批量构建、结果路径操作和安全的buffer复用。现有包络不能证明这1.764ms全是拷贝或纯IPC，更不能直接给出预期提速百分比。

HDF三项微优化——无新增费用时跳过归还sort、仅miss分配State、native不构造indexed-cache身份——此前已经A/B并因未通过完整查询验收而回退，当前源码和回退收据匹配。本次没有这些点的独占计时，不把它们当作未试方案或直接恢复。其作用仍可能存在，但应在独占profile证实后再重试。详情见[历史报告](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/docs/reports/hdf5-native-cpu-ab-20261023.md)。

## 5. 经验N斜率与算法阶数的区别

| 数据窗口 | 口径 | N8 q ms | N128 q ms | 倍数 | β μs/新增路径 |
| --- | --- | --- | --- | --- | --- |
| GRIB 当前 B | 均值 | 6.228 | 7.007 | 1.125 | +6.492 |
| GRIB 当前 B | 中位数 | 5.311 | 4.725 | 0.890 | -4.886 |
| HDF5 最近合格 B | 均值 | 7.308 | 4.046 | 0.554 | -27.188 |
| HDF5 最近合格 B | 中位数 | 5.526 | 3.920 | 0.709 | -13.383 |

β=(q128−q8)/120，N=P=C同变。GRIB均值/中位数方向相反，HDF负斜率又受N8慢块影响，均不能据此推导渐近复杂度。当前源码模型真正指出的是：输出/消息/费用/历史状态这些自变量如何增长，而不是把噪声端点斜率硬解释为O(N)或O(1)。

## 6. 核验与复现

HDF新提取器从冻结raw/audit/report/launch/actual-clock-source绑定离线重算，10项分析测试通过；不读items子profile来重复累计batch时钟，native累计计数按context正确差分。根节点独立对照此前aggregate核验 **3504个数值**、**80条均值分解恒等式**，并核验当前 **528个冻结源码文件**未变。正式科学raw、完整census仍留服务器，仅新增安全数值／源码／说明。

- [GRIB逐环节复杂度与源行](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024/grib-complexity.md)
- [HDF5/native逐计时与源行](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024/hdf5-source-audit.md)
- [HDF5独立数值汇总](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024/hdf5-derived-r2.json)
- [字段/时钟定义](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024/hdf5-schema.md)
- [独立交叉核验](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024/root-verification.json)
- [本轮复现归档](/Users/alan671/Documents/FS-paper1/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024/completion-evidence.tar.gz)

HDF derived SHA256：`55231c9b14a9d8d2d67ec0e6d600c44e5e7f9f5598898e9e516caea110288225`。
GRIB复用derived SHA256：`c9c3efe32afff48ca2328fc19586e9f11880a305dd009266acb4e1f3fc18d433`。

新诊断目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/q-model-analysis-20261024`。未修改原始记录、旧报告、固定语料、产品源码或飞书表格。
