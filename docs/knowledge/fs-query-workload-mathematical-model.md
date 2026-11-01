# FS 查询适用性模型：大文件、多变量、有限采集与训练采样

日期：2026-09-28。状态：可用于实验设计的第一版数学模型；公式中的理想化假设不代表系统已有性能。用户澄清：本轮“有限采集”以**单次采集的 message 占文件总 message 的比例**表达，不展开卫星过境、到达过程或保留策略。本文补充 公开训练基线计划 (local archive: `docs/deliverables/public-training-baseline-plan.md`)，不修改现有查询代码。

目标是判断一个**文件布局＋采集接口＋训练工作负载＋缓存预算**是否适合FS，而不是仅按扩展名或文件大小分类。相同文件对不同训练任务可以得到不同结论。以下推导为本项目模型；格式事实来源在文末。

## 1. 最小参数集及量纲

| 层次 | 符号 | 含义 |
| --- | --- | --- |
| 文件 | F、B_f、M_f | 文件数；文件f编码字节数；文件内可标识基础单元数（GRIB用message） |
| 基础单元 | b_u、d_u、t_u | 单元u的编码字节、解码驻留字节、一次解码耗时；不同单元允许不等长 |
| 逻辑维度 | V_f、T_f、Z_f、E_f、R_f | 变量、时间步、层次、集合成员、空间tile等基数；不是强制每个文件都有这些维度 |
| 布局 | π_f、Γ_dec、Γ_acq | 逻辑坐标到物理位置映射；解码依赖映射；解码所需字节到实际采集块映射 |
| 有限采集 | m_a、α=m_a/M | 等大message简化下单次采集容量，及其占文件message数比例；或固定采集块大小，二者必须注明 |
| 非等长采集 | B_a、α_B=B_a/B_f | 单次编码字节容量及其比例；真实大message不能仅靠数量预算约束 |
| 查询 | m_q、v_x、v_y、L、G、H、S | 窗口需要的message数；输入/目标变量数；历史长度、gap、预测长度、候选窗口步长 |
| 训练 | P_s、n_b、N、E_p、J | 采样过程；batch样本数；候选窗口数；epoch数；并行实验数 |
| 缓存 | C_d、C_e、C_work | 解码缓存、编码缓存、组装/在途/锁定结果的预算；不得仅统计C_d |
| 系统 | λ、β、Q | 采集调用固定时延、有效传输吞吐、评估期间查询数；多链路分别测量 |

“一次可取message数”是容量上限时，实际一调用可以少取；记录实际占比 `α̂=m_actual/M`。若协议要求整块获取，则m_a是不可再细分的块大小。两个系统可以有相同α，却有完全不同的读取成本，不能混用两种模型。

“大”使用绝对B_f和相对指标共同描述：`B_f/B_a` 表示完整文件需多少采集容量；`D_active/C_d` 表示训练活跃解码工作集与缓存的比例。原文件大但训练只访问一个很小子集时，不能用全文件大小推断缓存失效。

## 2. 从多变量窗口到物理单元

### 2.1 统一逻辑请求

采用现有训练模型的target anchor a。对确定实体s、输入变量集合V_x、目标变量集合V_y：

\[
q=(s,a,V_x,V_y,L,G,H),\qquad
\mathcal D(q)=
\bigl(V_x\times[a-G-L,a-G)\bigr)
\cup\bigl(V_y\times[a,a+H)\bigr).
\]

H>0时，\(W_{span}=L+G+H\)是覆盖包络，不表示gap中的值也必须读取；H=0时目标为空，实际输入包络长度只有L。层次/空间范围等可继续加到坐标。float32且每坐标单值时，输出值字节为 `4(v_x L+v_y H)`，mask、时间、metadata另计。相同series的coverage、split和有效位决定哪些候选窗口合法。

单条完整连续序列、长度T、H>0、统一窗口参数且输入起点从0按S枚举时：

\[
N=\max\left(0,1+\left\lfloor\frac{T-(L+G+H)}{S}\right\rfloor\right).
\]

H=0且没有额外anchor限制时，只需输入覆盖，计数为 `max(0,1+floor((T-L)/S))`。存在缺失、不同split、ragged或多个实体时按实际合法窗口集合计数，不套单序列公式。shuffle只改变窗口读取次序，不改变这个集合；随机crop可以改变被访问的集合，另用P_s描述。

### 2.2 GRIB实例

取“一条message对应一个变量、时刻、层次、成员和tile”的简化剖面；完整矩形布局才有：

\[
M=V T Z E R.
\]

当输入/目标选择相同v个变量、z个层次、e个成员、r个tile，G不读取、历史和目标不重叠时：

\[
m_q=v(L+H)zer,\qquad
\rho_m=\frac{m_q}{M}
=\frac vV\frac{L+H}{T}\frac zZ\frac eE\frac rR.
\]

这给出“总message数—窗口message数—变量维度”的直接联系。单一时刻文件中T可能为1，一个时间窗口就需要跨多个文件；不能假定每个GRIB文件都沿时间覆盖很长。多field message、不完整维度组合、不同网格/时次的真实文件必须回到实际message目录计数，不能强行使用乘积。

message数选择率不等于空间点选择率：若空间裁剪需要先解码整个场，裁掉99%网格点也不意味着少解码99%的message。变量数V也应与变量×层次组合数分开报告。

### 2.3 Arrow和TsFile映射

| 格式 | 逻辑选择 | 物理解码单元u | 不能忽略的依赖 |
| --- | --- | --- | --- |
| GRIB | message标签＋场内空间范围 | 经decoder验证的message/field依赖闭包 | message目录、packing、bitmap、多field共享结构；裁剪可能仍需全场解码 |
| Arrow stream两层List | series row、channel、step区间 | 未压缩稳定value tiles；压缩时为实际压缩buffer等依赖单元 | schema/message目录、两级offsets、validity、dictionary；一压缩buffer可能跨多变量/多行 |
| TsFile | table/entity、measurement、time | 实际reader解码的value/time pages及所需依赖 | table/chunk/page索引、对齐时间列、codec状态；按实际reader确定粒度 |

TsFile的table数量是跨关系选择维度，不直接等同变量数；变量数通常对应所选关系内measurement维度。Arrow row数量也不等于内部时间步数。

统一定义：

\[
\mathcal U(q)=\Gamma_{dec}(\pi(\mathcal D(q))),\qquad
\mathcal A(q)=\Gamma_{acq}(\mathcal U(q)).
\]

U是解码所需单元集合，A是实际采集范围/块的并集。共享offset/time page只计一次，不能按变量重复相加。对编码bytes的区间并集计量，可避免相邻/重叠请求双计。

至少记录三个独立比例：

\[
\rho_m=\frac{m_q}{M},\qquad
\rho_{io}^{cold}=\frac{B_{payload}^{cold}(q)}{\sum_{f\in\mathcal F(q)}B_f},\qquad
A_{dec}=\frac{\sum_{u\in\mathcal U(q)}d_u}{Y_{required}(q)}.
\]

F(q)为实际包含所需数据的文件；Y_required为所需基础值的唯一逻辑字节，索引/metadata另报。A_dec使用统一数值表示时刻画解码放大，否则还包含dtype转换的膨胀，应拆开。小ρ_m未必导致小ρ_io或小A_dec；这些量也不包含热缓存收益。

## 3. 有限采集：同一个α的两种含义

### 3.1 可选择message的容量约束

假设message等大、独立可取、每次最多取m_a个，且API支持一次聚合离散message，无缓存时理想值：

\[
n_{acq}^{ideal}=\left\lceil\frac{m_q}{m_a}\right\rceil
=\left\lceil\frac{\rho_m}{\alpha}\right\rceil,
\quad B_{payload}^{ideal}=m_q b,
\quad n_{full}=\left\lceil\frac M{m_a}\right\rceil.
\]

调用数是理想下界；若接口每次只接收一个连续range，实际次数还取决于π下的离散extent数与合并策略。GRIB一次解码message大于B_a时，需要多次传输后才能解码，不能直接使用message数量式。

当ρ_m≤α时，窗口的数据量可以装入一次采集容量，**不代表物理上必然只调用一次**。可部分读取情况下，即使没有任何缓存命中，也可能相对整文件获取减少传输；与本来就支持相同range读取的原生reader相比则没有该项独有优势。

### 3.2 不可拆分的固定采集块

假设M可被m_a整除，有 `K_a=M/m_a` 个固定块。窗口触达K_q个块时：

\[
m_{read}=m_a K_q,\quad
\rho_{io}^{cold}=\frac{K_q}{K_a},\quad
A_{acq}=\frac{m_aK_q}{m_q}.
\]

公式只计等大message payload，不含索引；K_q由π与实际窗口决定。对于物理连续m_q条message、起点在块内的余数均匀、忽略整文件边缘截断：

\[
\mathbb E[K_q]=1+\frac{m_q-1}{m_a},\qquad
\mathbb E[A_{acq}]=1+\frac{m_a-1}{m_q}.
\]

若从M条message中均匀无放回选m_q条，则：

\[
\mathbb E[K_q]=K_a\left[1-
\frac{\binom{M-m_a}{m_q}}{\binom M{m_q}}\right].
\]

分子不可能的组合数按0处理。稀疏大集合下可近似为 `K_a[1-exp(-m_q/K_a)]`；`αm_q=m_q/K_a`刻画随机选择逐渐触达所有块的趋势。**随机选择连续时间窗口不等于随机选择message集合**：前者仍包含时间与变量布局结构，必须按π计算K_q，不能直接套无放回公式。

如果固定块是链路/存储不可规避的最小单位，FS也必须付出同样冷读成本；建索引不会使这个单位自动变小。若FS能通过重布局改变Γ_acq，则另计改写/转换和新增存储成本，不与保持原始B的方案混称。

### 3.3 节省发生在哪一条链路

分别标注卫星→接收端、NFS→FS、FS→训练worker等层次的 `B_l,n_l,α_l`。仅在请求筛选发生于瓶颈链路之前、源端支持选择性获取时，才可能减少该链路流量。

当前原始B完整摄取、字节保持的模式仍要接收/保存整文件。摄取后的文件内查询收益不能追溯计作减少首次卫星下传。后端只返回少量bytes也不证明磁盘层只读了同样数量；必须区分返回、网络和磁盘计数。本版只以α描述单次采集比例，β和λ作为成本测量参数，不推导卫星链路物理机制。

## 4. 训练采样与解码缓存

采样过程P_s输出有序请求 `q_1,...,q_Q`，包括epoch、job和worker调度。候选窗口集合相同，不同P_s也可能有不同缓存表现。有限采集α本身不约束训练活跃集合或复用距离，不能用小α推断高命中。

### 4.1 相邻窗口的潜在复用

仅在G=0、输入目标选择相同变量、W=L+H、相同实体且按时间顺序读取时，相邻窗口的逻辑重叠比例为：

\[
\omega_{logical}=\max(0,1-S/W).
\]

n_b个这样的等步长窗口所需的唯一逻辑值数为：

\[
v\,[W+(n_b-1)\min(S,W)],
\]

而各自独立物化需要 `n_b v W` 个值。该比值表示潜在基础值复用，不是直接的I/O/解码加速；真实复用按U集合并集计算，且最终每个模型输入窗口仍可能需要独立拷贝。

### 4.2 精确trace定义与LRU近似

将请求展开为带版本/codec身份的物理解码单元引用序列 `u_1,...,u_R`。第r次引用的解码缓存命中标志为h_r，实际解码成本与字节命中率为：

\[
T_{dec}=\sum_r(1-h_r)t_{u_r},\qquad
h_{bytes}=\frac{\sum_rh_rd_{u_r}}{\sum_rd_{u_r}}.
\]

数据命中和解码命中独立：编码缓存命中仍可能需要解码。首次引用miss；同一个正在解码单元的并发请求通过single-flight只产生一次工作，等待时间另计。

在所有单元均可装入C_d、无pin/bypass、固定大小身份、全相联按字节LRU的理想条件下，令RD_r为两次引用u之间访问过的**不同其他单元**的解码字节总量：

\[
h_r=\mathbf1[RD_r+d_{u_r}\le C_d].
\]

真实系统的pin、在途内存、不同准入/淘汰策略以实际trace模拟或计数为准。仅知道相邻时间窗口重叠，不能知道随机次序下RD。

### 4.3 随机采样

极简模型：N_u个等大物理单元，每次独立均匀引用一个，缓存装c个单元且已暖稳态，则任意只依赖过去的满缓存命中率为：

\[
h=c/N_u\quad(c\le N_u).
\]

这是独立单元引用模型，不是HF无放回shuffle整epoch的精确模型。它说明大活跃集合/小缓存时命中低；不能用“随机”一词直接断言命中为0，也不能把全文件单元数替代实际被访问集合。

更接近窗口batch的独立同分布模型：令p_u为一个随机窗口包含解码单元u的概率，各窗口之间独立（窗口内部单元可以相关），则：

\[
\mathbb E[D_{ind}]=n_b\sum_up_ud_u,\qquad
\mathbb E[D_{union}]=\sum_ud_u[1-(1-p_u)^{n_b}].
\]

两者分别表示无复用解码量和batch内理想去重后的解码量；差值是潜在节省。实现需要容纳/调度相关片段，恢复原样本顺序，不得用无限lookahead；shuffle无放回、局部crop等非IID过程用实际抽样概率或trace替换该式。

### 4.4 多实验共享

在一个可共享的时间范围内，实验j需要U_j，定义潜在重复解码比例：

\[
\chi=1-\frac{\sum_{u\in\cup_jU_j}d_u}{\sum_j\sum_{u\in U_j}d_u}.
\]

范围必须对应缓存可驻留时间或实际并发请求，不可用整轮实验集合替代驻留时间。χ>0只是上限机会；不同version、无法共享的预处理结果不合并。默认共享原始解码基础值，view标准化/输入输出包装分别处理。

比较 `Σ_j C_{d,j}=C_d_shared`，并计服务、worker、OS共享页的真实内存。若J个实验均独立均匀访问同N_u个等大单元、总容量c≤N_u、私有容量各c/J，理想暖态有 `h_private≈c/(J N_u)`、`h_shared≈c/N_u`。绝对提升仍可能极小；各实验访问不同集合时此推导失效。公开原生共享缓存也可获得这种收益，不能称为FS独有。

## 5. 跨文件窗口与文件筛选

### 5.1 连续时间分片

同一实体沿时间每P步切一个文件，查询连续W步、起点s。无gap且各通道相同分片、完整覆盖时精确文件数：

\[
K_f(s,W)=1+\left\lfloor\frac{s+W-1}{P}\right\rfloor
-\left\lfloor\frac{s}{P}\right\rfloor.
\]

起点余数均匀并忽略整序列两端时：

\[
\mathbb E[K_f]=1+\frac{W-1}{P},\qquad
\Pr(K_f>1)=\min\left(1,\frac{W-1}{P}\right).
\]

S与P有公因数或固定起点时，起点余数通常不均匀，须对实际anchors求平均。gap不读取、变量分别分片、缺失coverage时以实际U对应的文件集合计K_f；包络W_span公式只是可能的上界，不能多读gap文件后声称必要。

有限连续数据有F_seg个等长文件、全部合法起点按stride=1枚举，且1≤W≤P时，准确的跨界窗口数和比例为：

\[
N_{cross}=(F_{seg}-1)(W-1),\qquad
p_{cross}=\frac{(F_{seg}-1)(W-1)}{F_{seg}P-W+1}.
\]

该式保留两端效应，F_seg=1时自然为0。P<W≤F_seg P时每个合法窗口均跨界；更一般stride/coverage以精确K_f的指示函数在合法样本集合上求平均。

随机训练不会消除K_f>1；它改变缓存和调度。跨文件合法性由显式continuation决定，不由文件名/相邻目录/训练顺序决定。独立实体的多个窗口可以组成batch，但不能拼成单个窗口。

### 5.2 筛选与组装分别计量

文件目录有F个候选，metadata筛选保留F_c个，实际打开K_f个；记录 `F_c/F`、`K_f/F_c`、目录/布局读量及规划耗时，不预设索引必然O(log F)。同一逻辑样本的跨文件成本是各源读取＋coverage解析＋局部组装；对手必须产生相同样本，不允许因文件边界丢窗口。

跨界窗口数量少时，只提高它们的速度可能对整体训练影响很小；应报告它们的**耗时占比**，不能仅用样本占比代替。也不能通过不合理缩小文件制造边界比例后概括真实场景。

## 6. 查询收益与训练收益的判据

对实现x（FS或对照native），串行服务时间近似分解为：

\[
T_x(q)=T_{plan,x}+\sum_l\left(n_{x,l}\lambda_l+\frac{B_{x,l}}{\beta_l}\right)
+T_{dec,x}+T_{assemble,x}+T_{pack,x}.
\]

byte流量计真实miss后的读取；decoded hit不再次算一次物理读，编码命中只减少对应链路读。range合并可能降低n而增加B。T_pack包括转置、输出复制和IPC等必要工作；以上是无重叠时间预算，有流水线时应按资源瓶颈/关键路径测量，不能把CPU服务总时间直接等同wall time。

设I_FS、I_native为本评估范围的一次性摄取/索引/转换成本。相同样本、顺序、总内存和硬件条件下，FS查询有收益当且仅当：

\[
\frac{I_{FS}-I_{native}}Q
<\mathbb E[T_{native}(q)-T_{FS}(q)].
\]

右侧≤0时，增加epoch不能摊销出正收益；若右侧>0且增量setup>0，则 `Q>(I_FS-I_native)/ΔT_query` 是回本条件。没有对方成本、实际调用次数和解码命中率时，不从M/m_q直接推出加速比。

至少比较：整文件获取（弱参考）、优化原生索引/range reader、同预算原生共享缓存、可驻留内存/连续memmap。两边都能相同粒度读取并获得相同缓存命中时，读量/解码差额可能为0，FS反而可能多出服务开销；统一查询/版本/文件语义仍可有功能价值，但不能当成性能优势。

训练端每batch的实际供给时间T_supply和GPU计算T_gpu在理想流水下：

\[
T_{step}\approx\max(T_{supply},T_{gpu})+T_{nonoverlap}.
\]

所以数据供给不是瓶颈时，减少解码未必减少step时间。若基线wall time中可被优化且暴露的等待占比为f_wait，其他工作不变，该部分至多加速s倍的Amdahl上界为：

\[
Speedup_{train}\le\frac1{(1-f_{wait})+f_{wait}/s}.
\]

其适用前提是固定的成本划分；若CPU/GPU资源争用改变，重新测量而不套此上界。模型配置、训练batch顺序和质量指标固定，不能用改变采样分布换来的效果归给FS。

## 7. 一个可手算的GRIB例子

**以下纯属模型示例，不是实测文件或性能。** 文件有V=64个变量、T=64个时刻，每变量/时刻一条等大message，无其他轴：M=4096，每条编码1 MiB，文件4 GiB。一次最多采集m_a=64条，即α=1/64。窗口取v=4变量、W=8步：m_q=32，ρ_m=1/128。

| 物理/接口条件 | 冷payload读取 | 含义 |
| --- | --- | --- |
| 必须整文件获取 | 4096 MiB | 弱参考，64个采集容量 |
| 精确message选择，支持一次离散gather | 32 MiB、理想1次 | 与弱参考的字节比为128；不是相对优化reader的加速比 |
| 按时刻排列，每时刻64变量为一个不可拆采集块 | 8×64=512 MiB | 需要8块，采集放大16倍 |
| 按变量排列，每变量64时刻为一个不可拆采集块 | 4×64=256 MiB | 需要4块，采集放大8倍 |
| 时刻优先、所选4变量相邻且支持精确连续range | 32 MiB、8个extent | 与gather相同字节，但接口不聚合时为8次调用 |

若当前查询又按S=1依次取8个窗口、变量相同且时间范围未越文件，独立需求为 `8×32=256` 条message引用，唯一需求为 `4×(8+7)=60` 条，理想解码工作比为256/60≈4.27。实际缓存必须保留所需片段，且压缩/采集粒度可以降低收益；随机打乱大量窗口后不能沿用4.27。

若同一序列每P=64步分文件、W=8且起点余数均匀，则跨文件窗口比例7/64≈10.94%，平均触达1.109375个文件。这个比例由逻辑时间跨度和分片定义，不由shuffle决定。

## 8. 用于选择数据与设计实验的参数空间

建议报告适用性向量，不压缩成一个未经验证的“适合分数”：

\[
\Theta=(B_f,M,V,\rho_m,\alpha,\pi,\rho_{io}^{cold},A_{dec},
D_{active}/C_d,P_s,h_{bytes},\mathbb E[K_f],J,\chi,f_{wait}).
\]

| 场景 | 模型条件 | 可检验结论 |
| --- | --- | --- |
| 大文件、小投影、源端可局部取 | ρ_m≪1，Γ_acq不强制全文件，规划/调用成本可摊销 | 即便随机且h≈0，仍可能比整文件获取省流量；必须再比原生range reader |
| 小输出但粗压缩/粗采集 | ρ_m≪1而ρ_io≈1或A_dec大 | 冷查询可能无优势，只有实际重复访问才可能摊销解码 |
| 顺序/局部训练或评估 | 物理RD+d≤C_d的比例高 | 共享基础片段可减少重复解码；相同输出batch仍需组装 |
| 大集合全局随机＋小缓存 | D_active/C_d大，测得h低 | 不能靠滑窗重叠声称缓存收益 |
| 跨文件连续序列 | K_f>1且有合法continuation | 可以按需建立完整样本；性能取决于边界耗时占比与原生对照 |
| 多实验复用 | 相同版本、驻留期间χ高、内存预算公平 | 可能减少重复解码/驻留；需同预算原生共享缓存对照 |
| 全数据已在内存/GPU计算占主导 | 原生reader无I/O等待或f_wait≈0 | FS训练加速空间很小，可能增加开销 |

控制变量实验先固定同一逻辑样本和采样顺序，再分别改变：(1) 文件总message数/背景变量数；(2) 查询变量比例v/V和窗口跨度；(3) α及可选择/固定块接口；(4) π的时间/变量排列和真实压缩单元；(5) 随机/顺序/局部采样与C_d；(6) 分片P；(7) 实验数J和通道重叠。改变分片/排列时保持值、mask和样本清单一致；分块随机是额外采样策略，质量另验。

记录每次查询的snapshot/file version、window ID、实体/变量/时间范围、U/A集合、采集层、字节/调用、decode hit/miss/single-flight、复用距离、结果复制、K_f、setup与Trainer等待。解码单元命中按字节和耗时分别加权，不能仅报告请求次数命中。

## 9. 依据与核验边界

- [ecCodes索引API](https://sites.ecmwf.int/docs/eccodes/group__codes__index.html)支持message索引和键选择；[message offset/size API](https://sites.ecmwf.int/docs/eccodes/namespaceec_codes.html)支持取得编码位置和长度。因此原生GRIB reader不能被默认建模为整文件解码。
- [Arrow格式规范](https://arrow.apache.org/docs/format/Columnar.html)给出List offsets、validity、IPC buffer与压缩结构。本文Γ_dec/Γ_acq必须按实际writer/reader验证，不承诺任意压缩buffer的细粒度独立解码。
- [Apache TsFile结构说明](https://github.com/apache/tsfile)给出page/chunk/chunk group/index层次；时间列与value依赖按实际版本reader测量。
- 本仓库 [训练窗口语义](../contracts/arrow-training-query-model.md)使用target anchor、L/G/H/S、显式coverage、输入目标变量与mask；本模型与之对齐，不把尚未实现的通用TsFile训练路径当作已有功能。
- 本轮用Python整数/有理数枚举核验：周期分片/采集块均值768组，有限序列跨界计数4,896组，随机message固定块期望264组，顺序窗口并集2,560组，共8,488组全部通过。没有运行存储或训练性能实验；数值例子只用于解释参数。
