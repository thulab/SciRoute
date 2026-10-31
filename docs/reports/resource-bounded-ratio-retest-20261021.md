# 两核有界资源、配对查询比值复测（2026-10-21）

已完成 144 次查询、12 个 run identity；全部 value/mask 与既有独立原生 oracle 一致。原数据库、配置、许可证／节点身份摘要、原 cgroup 和全部线程 CPU 亲和性已验证恢复，实验 cgroup 已删除。

**本轮没有出现 R≤1。** 现版相对保留基线的小 GRIB 优化已复现；HDF5 和大输出 GRIB 仍有显著差距，不能据此声称 IoTDB-FS 已追平 FS-index。

## 资源与计时协议

| 条件 | 实际配置 |
|---|---|
| CPU | Intel Xeon Gold 6130；CPU 2、3；各自可见 SMT sibling 仅自身；一个 cgroup v2 isolated partition，共享 `cpu.max=200000 100000` |
| 参与者 | 查询客户端、IoTDB-FS、FS-index、原 DataNode 共用上述两核；控制／恢复 supervisor 在组外 |
| query 内存 | 合计上限 2 GiB；计费峰值 1.298 GiB；无 max/oom/oom_kill 事件 |
| DataNode 内存 | 原 `-Xms/-Xmx15232M`、direct 2176M；单独计费峰值 13.163 GiB；受控 parent 峰值 14.458 GiB，不能称全栈 2 GiB |
| 样本 | 3 场景 × 2 实现 ×（2 对 warmup＋10 对测量）× 2 臂＝144 次；每对交替先后顺序；无追加或截停 |
| 主指标 | 同 block 的 `R=median(T_IoTDB-FS)/median(T_FS-index)`；保留逐对比值和两端全部绝对观测 |
| 缓存 | 保留 daemon／native context／已有索引；不清空 OS、DB 缓存；准备阶段全量字节哈希会暖 OS 页缓存；不缓存 ROI／查询答案 |
| 计时 | 沿用完整 discovery→选中值→owned 输出端点；准备、恢复、独立 oracle、profile 收集和资源观测在钟外 |
| 端点差异 | GRIB：FS 为 C++ owned Float32/mask，digest/JSON/pipe 不计入 native total；IoTDB-FS 为含 IPC 的 NumPy owned 输出。HDF5 两端均为 NumPy owned 输出 |
| 产品版本 | 保留基线 `bff1dd7a6fcc…`，现版 `36881bde0246…`；本轮未改产品源码、未构建新二进制 |
| 服务窗口 | 复用原实例／账号／许可证；窗口中 SYNC/partial=false，结束恢复 ASYNC/partial=true；owner 启动含 schema 检查和 staging 清理，无科学版本发布 |
| 环境局限 | 未隔离共享磁盘、内存带宽、全局 IRQ 或固定频率；没有人为填充 CPU 负载。单次连续挂载 block 是描述性重复，不构成独立统计复制 |

## 严格语料与查询身份

| 场景 | N/V/D/S 与查询 | 预期输出 |
|---|---|
| 独立 GRIB | N128、V1、D4、S=33,484 B；Latest，六属性等值 AND | K_files/K_file_versions/K_units=2/2/2；两个完整 64×64 字段；40,960 B |
| HDF5 四桶 | 独立发布 N512，B4/C128；V1、D4、S=241,559–241,983 B；Latest，64×64 ROI | 既有 b00/b01/b05/b12 四个完整桶，按已知命中选取；K=2/2/2；40,960 B；不能标为独立 N128 |
| 大 GRIB 多版本 | N8、V8、D4、S=8,306,636 B（约7.92 MiB）；identical；All 科学版本＋六属性谓词 | K=2/16/16；16个完整721×1440字段；83,059,200 B；不是无谓词 All 遍历 |

三组使用原始字节、独立发布绑定及原有 FS 索引。大 GRIB 只补齐身份记录，64条科学版本引用8个既有 base 文件；FS 侧使用既有64个独立文件副本。没有生成／重发语料或重建索引。

- `grib-small`：corpus `fcfcc777268da6cb86c6724d3ef114803f2b4e880e718b72381454cd2feb2ffc`；case `241e127c3583b454ba3149968b0f4e1a9643d153ed80d0f63e7123536082fcbc`。
- `hdf5-c128`：corpus `69afe940d4184232679e46266e4f56e6089b363b6b8d19fe19f00bc119c5cfae`；case `e3bbf388a538a86f11e6b66245172c9ac20df3fc24b9914eb832d1776f88bd98`。
- `grib-large`：corpus `ba437fe1e3182afe7d92956b39eb5929c8845e35f8e0b0388e0e490c553ac477`；case `153f1f25f7d5cfd38e3a567dc950014e727309ad104db7e66da10d0d3f771fca`。

## 完整暖查询结果

单位 ms；每臂10个测量样本的中位数。R 每行使用同一 block 的两端，不能用另一轮的 FS 最小值拼接。

| 场景 | 实现 | IoTDB-FS | FS-index | R |
|---|---|---:|---:|---:|
| GRIB 独立 N128/V1/Latest | 保留基线 | 33.578 | 1.339 | 25.072 |
| GRIB 独立 N128/V1/Latest | 现版 | 8.150 | 1.169 | 6.971 |
| HDF5 N512/B4/C128/V1/Latest | 保留基线 | 9.245 | 0.647 | 14.278 |
| HDF5 N512/B4/C128/V1/Latest | 现版 | 10.886 | 0.837 | 13.013 |
| GRIB N8/V8/identical/All（谓词选择） | 保留基线 | 919.588 | 242.238 | 3.796 |
| GRIB N8/V8/identical/All（谓词选择） | 现版 | 880.982 | 255.674 | 3.446 |

| 场景 | 保留基线 R→现版 R | 同科学case的历史参考最小R | 解释 |
|---|---:|---:|---|
| GRIB 独立 N128/V1/Latest | 25.072→6.971 | 7.454 | 现版完整查询下降；FS 也快约13%，R仍明显下降，优化复现。 |
| HDF5 N512/B4/C128/V1/Latest | 14.278→13.013 | 10.796 | IoTDB-FS绝对时间增加约17.8%，FS增加约29.2%；R下降不能直接解释为HDF5提速。 |
| GRIB N8/V8/identical/All（谓词选择） | 3.796→3.446 | 3.595 | 完整查询下降约4.2%，FS慢约5.5%；R改善仍不足以追平FS-index。 |

历史列为已有证据的参考：GRIB N128 原 block 各5个暖样本；HDF5 reverse/baseline-r3合计4个暖样本；大GRIB取完整 campaign 的最小R（各4个暖样本）。历史缺统一资源记录，且实现／缓存／重复协议不同；它们不是本轮受控基线，也不能据小幅差异声称新纪录或统计等价。`historical-minima.json` 仍按各自精确引擎／缓存／计时／交付协议分别分组；native-HDF5/VFD及prototype不与本表indexed路径合并。未把后续单侧IoTDB-FS结果与旧FS时间拼接。

## 剩余成本在哪里

将每次完整查询的总时间 T 分为发现 Td 和取值交付 Tq，保留原有计时校正及两端终点。使用原同组10对暖样本分别计算：

- `Rd = median(Td_IoTDB-FS) / median(Td_FS-index)`：发现阶段比值。
- `Rq = median(Tq_IoTDB-FS) / median(Tq_FS-index)`：命中确定后的取值／交付阶段比值。
- 原主指标 `R = median(T_IoTDB-FS) / median(T_FS-index)` 保持不变。

144条原始记录逐条满足 `T=Td+Tq`。**R不能写成Rd+Rq或Rd×Rq；不同阶段中位数也未必相加为完整查询中位数。** 对每一对样本才有 `R_pair=w×Rd_pair+(1−w)×Rq_pair`，`w=Td_FS/T_FS`。派生JSON同时保留每对比值、权重及阶段时间差，不用最高阶段倍数代替绝对瓶颈。

| 计时边界 | 发现 Td | 取值／交付 Tq |
|---|---|---|
| GRIB FS-index | SQLite谓词／版本选择、路径／stat／access检查、结果装配；实际为T减去累计取值子计时的残余量，读取与发现交错，不是单独连续的先发现区间 | 文件open/seek/read、ecCodes解码、Float32和mask构造；终点C++ owned buffer，不含后续digest/JSON/pipe |
| HDF5 FS-index | native.select及Python命中元数据映射；不打开／解码HDF数据 | 原生HDF5的ROI读取、必要文件／dataset handle获取、填充NumPy owned value/mask |
| IoTDB-FS | 发现RPC、元数据结果接收／映射，含该阶段实际发生的权限、准入和预算处理 | 精确命中取值RPC，含该阶段准入／预算、读取／格式处理、Arrow/FUSE IPC及NumPy owned拷贝 |

GRIB的Rq保留C++／NumPy终点差异，不能解释为同端点的纯IPC开销。HDF5两端终点一致。阶段内还含各自路径的其它工作，Rd不是纯索引查找倍数，Rq不是纯物理I/O倍数。

下表时间单位ms，均为各阶段中位数，阶段比值由未舍入的原ns数据计算。

| 场景 | 实现 | Td IoTDB-FS | Td FS-index | Rd | Tq IoTDB-FS | Tq FS-index | Rq |
|---|---|---:|---:|---:|---:|---:|---:|
| GRIB 独立 N128/V1/Latest | 保留基线 | 30.711 | 0.192 | 160.131 | 2.986 | 1.129 | 2.645 |
| GRIB 独立 N128/V1/Latest | 现版 | 5.726 | 0.167 | 34.355 | 2.333 | 0.998 | 2.337 |
| HDF5 N512/B4/C128/V1/Latest | 保留基线 | 4.142 | 0.183 | 22.695 | 5.166 | 0.460 | 11.231 |
| HDF5 N512/B4/C128/V1/Latest | 现版 | 5.267 | 0.240 | 21.905 | 5.473 | 0.596 | 9.189 |
| GRIB N8/V8/identical/All（谓词选择） | 保留基线 | 19.987 | 2.508 | 7.970 | 899.546 | 239.775 | 3.752 |
| GRIB N8/V8/identical/All（谓词选择） | 现版 | 7.807 | 2.497 | 3.126 | 873.117 | 253.170 | 3.449 |

- **小GRIB主要差在发现。** 现版Rd仍为34.36，但应结合绝对时间理解：5.726 ms对0.167 ms；取值为2.333 ms对0.998 ms。相对基线，发现从30.711 ms降至5.726 ms，现有全局复用的主要收益在此。
- **HDF5两段均有额外成本。** 现版发现约5.267 ms对0.240 ms，取值约5.473 ms对0.596 ms。虽然Rd高于Rq，两个阶段的绝对差额都约5 ms，不能只处理发现。相对基线Rd/Rq下降时，IoTDB-FS两个阶段的绝对时间均增加，保留前文“不能直接认定HDF5提速”的结论。
- **大GRIB主要差在取值交付。** 现版Rd=3.13、Rq=3.45看似接近，但FS发现只有2.497 ms，FS取值为253.170 ms；IoTDB-FS对应7.807 ms和873.117 ms。继续压缩约7.8 ms发现阶段，不能消除约620 ms的取值阶段差距。应先对取值路径内部的读取、解码、格式转换、IPC及owned交付做独占耗时分解；现有Rq不能证明全部差额都来自IPC或仍反复解码。

全部60次暖IoTDB-FS查询的profile均显示SDK `query_calls + version_store_query_calls = 0`。当前暖查询差距不能归因于每次重新执行SQL。

本次为既有144条记录的离线派生分析，未重跑查询、停启数据库或改写原始`queries.jsonl`、`analysis.json`、设计与run身份。历史参考最小R仍沿用原分组，不从不同历史block另拼最小Rd/Rq。源report/design/queries/analysis摘要及分析器SHA见[phase-analysis.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/phase-analysis.json)。

边界核查代码：[GRIB native](../../experiments/multi_file/native/multifile_native.cpp)、[GRIB版本选择](../../experiments/cross_version/version_matrix/native/version_matrix_native.cpp)、[HDF5计时切分](../../experiments/cross_version/hdf5_route3/route3_global_hot_ab.py)。运行时所用冻结helper仍由原prepared／manifest摘要绑定；这里引用当前对应源码说明结构，不替换冻结证据。

## 一次性成本与资源恢复

| 场景 | 保留基线挂载/恢复 s | 现版挂载/恢复 s | 既有FS-index一次准备 ms（本轮未重建） |
|---|---:|---:|---:|
| GRIB 独立 N128/V1/Latest | 212.459 | 154.283 | 400.136 |
| HDF5 N512/B4/C128/V1/Latest | 305.585 | 259.797 | 85.125 |
| GRIB N8/V8/identical/All（谓词选择） | 135.606 | 141.819 | 467.269 |

恢复和索引准备不是同一种操作，分别保留，不计入暖查询R。FS-index准备值沿用已核对的原收据（HDF5为四桶native context建索引总准备时间）；原IoTDB-FS发布成本未重测。所有原始数据／FS副本／索引／二进制在前后重新哈希，各挂载前后head及原／复制owner journal均不变。

原DataNode最终PID为`949517`；有效配置恢复为`ASYNC/true`；原配置SHA为`ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`。独立恢复检查确认全部线程回到原CPU亲和性0–31、原cgroup存在、三个实验cgroup已删除；释放前parent快照的累计`nr_throttled=0`，内存未触发max/OOM。许可证和节点身份仅校验摘要，未申请或复制新证书。

## 复现与证据

- 入口与资源／时钟定义：[resource_limited](../../experiments/cross_version/resource_limited/README.md)。
- 固定设计与原始行：[design.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/run/design.json)、[queries.jsonl](../../experiments/cross_version/work/resource-ratio-retest-20261021/run/queries.jsonl)。
- 完整分析及所有样本：[analysis.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/run/analysis.json)。
- 发现／取值阶段派生分析：[phase-analysis.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/phase-analysis.json)，沿用同一144行与12个run身份；原完整查询分析保持原字节。
- 历史原始block与精确协议分组：[historical-blocks.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/historical-blocks.json)、[historical-minima.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/historical-minima.json)。
- 原服务／资源恢复：[report.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/run/report.json)、[independent-restore-check.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/run/independent-restore-check.json)。
- 本地独立校验：[local-verification.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/local-verification.json)：41个公开文件SHA一致，144行分析重算完全一致，12个run及全部oracle／HDF选择范围验证通过，执行脚本摘要与本地一致。
- 早期零查询预检失败保留在[attempts.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/attempts.json)，没有混入正式样本。
- 公开证据包SHA：`1cbf5d1b70975e81952e3d2ef9f7b48318ba669bd666d6eb0fd9154ceeda9c88`；不含凭据、原配置备份或私有日志。服务器原结果目录为`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/resource-ratio-retest-20261021-run2`。
- 离线验证：复测模块62项、身份模块87项，共149项通过；其中worker失败/OOM恢复为纯mock控制流测试，不冒充真实OOM实验。

阶段拆分后补充验证：新增5项阶段分析测试，当前复测67项＋身份87项，共154项通过；[phase-verification.json](../../experiments/cross_version/work/resource-ratio-retest-20261021/phase-verification.json)确认原41个公开证据文件和全部原执行脚本摘要未变，原R保持不变，144行逐条闭合，并验证每对加权恒等式。未生成新run身份或查询样本。
