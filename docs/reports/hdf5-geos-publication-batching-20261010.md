# GEOS/HDF5 发布 manifest 合并与有界状态合批

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-10。同一16个GEOS原件（7,142,773,610 B），旧版、新版单块checkpoint、新版8 MiB checkpoint各完整发布一轮。默认合批的完整发布为 **102.616 s**，本轮旧版为 **142.165 s**，观察下降 **27.82%**。发布root内SQL接口合计从57.502降至11.790 s；payload SQL从46.327降至6.127 s。**数据传输仍为1 MiB，不将8 MiB合并成一个OBJECT Tablet。**

## 实现与持久性边界

- `StructuredBlobStorage::write_payload_range` 和新增 `write_payload_batch` 共用一个生命周期操作。IoTDB backend在同一`mutation_mu_`锁内fresh读取manifest一次，校验key、PayloadOpen／未发布、无gap、范围与revision，再依次写原始OBJECT块，最后一次inode INSERT推进frontier；不再通过`replace_manifest`二次SELECT。其他生命周期CAS保持原有校验。
- 摄取以移动buffer方式积累最多8 MiB、64块，Adapter仍按原chunk边界和顺序consume；尾批在finish／seal前提交。每个成功批次revision推进一次。配置`writer.manifest_batch_max_bytes`为整数字节，默认8388608；必须容纳一个transfer chunk且不超过8 MiB。设置1048576可复现单块消融。
- 写任何artifact前完成校验，包括IoTDB INT64和process范围。明确的中间块失败不提交frontier；可能已有不可见artifact字节，重试fresh校验并覆盖相同offset，或abort清理。提交回执丢失可能已推进frontier，不能声称所有失败都不改变状态；该情况仍不可见且可幂等重试。只有完整B/L/Sigma后才能Published。
- 不新增跨请求manifest缓存，不变更schema/layout协议或发布顺序。批次持有mutation锁期间其他manifest操作会等待；本次验收为单客户端发布，未衡量并发发布／查询干扰。

## 三组同条件测量

相同Release构建、SDK、HDF5／Arrow依赖、database `geostime20261007`、writer lock、source顺序、query/cache配置、零attribute/entry TTL、四种取值case。原`geos_pilot.py`不改；wrapper只向新组写入checkpoint配置。计时仍包围逐文件`shutil.copyfile`，含创建、暂存写入及close-time摄取发布，排除mount、父目录mkdir、源SHA、stat与取值／全量SHA验证。不清OS／IoTDB cache；每臂新逻辑路径、保留精确版本。

| 指标 | 旧版 | 合并重复读取：1 MiB | 再合批状态：8 MiB |
| --- | ---: | ---: | ---: |
| 完整发布，s | 142.165 | 115.910 | 102.616 |
| 全部发布root SQL接口，s | 57.502 | 32.173 | 11.790 |
| payload阶段总wall，s | 95.920 | 76.690 | 62.580 |
| payload SQL接口，s | 46.327 | 27.281 | 6.127 |
| payload内其余未归属工作，s | 18.290 | 15.932 | 23.792 |
| 内容commit暂存fsync，s | 14.551 | 14.515 | 14.641 |
| HDF5索引构建，s | 2.215 | 2.224 | 2.193 |
| 全部发布root Tablet接口，s | 33.678 | 34.769 | 33.811 |

阶段wall包含其SQL/Tablet，不可把父阶段与分项再相加。SQL接口合计包括查询提交、hasNext/next结果访问、nonquery、close；Tablets另列。计时是客户端接口elapsed，含连接池、SDK、网络及服务器等待，不能称纯SQL执行CPU。

## 调用次数严格验收

| payload操作 | 旧版 | 新版1 MiB | 新版8 MiB |
| --- | ---: | ---: | ---: |
| manifest SELECT | 13,640 | 6,820 | 863 |
| frontier INSERT | 6,820 | 6,820 | 863 |
| 原1 MiB OBJECT Tablet | 6,820 | 6,820 | 6,820 |
| 结果访问方法 | 27,280 | 13,640 | 1,726 |
| 查询关闭 | 13,640 | 6,820 | 863 |

8 MiB组逐文件ceil(size/8 MiB)共863批；不使用ceil(总字节/8 MiB)，也不跨文件合批。所有组OBJECT Tablet仍6820次。全部发布root的SQL SELECT+nonquery共21,164 → 14,344 → 2,430次；fetch/close的方法次数不是新增SQL或网络RPC。三组的payload外调用次数完全相同。

## 收益与尚未归属的成本

单块组完整发布比旧版下降18.47%，payload SQL下降41.11%；8 MiB组相对单块组又下降11.47%，payload SQL再下降77.54%。默认组相对旧版payload SQL观察下降86.77%，payload wall下降34.76%。这是各组单轮同条件观察，不是多轮统计的稳定加速估计。

全部SQL的观察下降79.50%包含未修改阶段的波动：旧版description SQL6.683 s，新版单／批为2.243／2.401 s；namespace_prepare为2.175 → 0.704／0.875 s。旧版尖峰出现在中途文件，不能称已确认的冷启动，也不能全归因于此次代码。具体服务器等待成因未测。fsync三组约14.5–14.6 s，较稳定。

批量组payload其余未归属工作为23.792 s，旧版18.290 s、单块15.932 s；增加5.502／7.859 s。该区间包括暂存读取、buffer／Tablet构造、字段复制、manifest处理与局部控制，现有profile不能确定是哪项；不声称内存复制／分配已得到优化。Tablet总耗时仍约33–35 s，未尝试合并OBJECT传输。

先前135.092 s基线作为历史参照，当前102.616 s观察下降24.04%；主要A/B使用本轮旧版142.165 s，并保留上述阶段口径，避免仅以历史总time归因。

## 数据与故障验收

- Linux unit标签 **1435/1435通过**；新增storage单测覆盖借用buffer、1 MiB块保持、8 MiB／64块边界、empty／overflow／sealed、部分块失败、checkpoint失败、ACK丢失与重试；新增摄取测试覆盖chunk顺序、64块界限、尾批与已checkpoint后source失败清理。
- 三臂各64/64原生HDF5与typed IPC的Float32行数／SHA一致；各16个dataset layout `/5` digest完全匹配历史，shapes／object身份也匹配。
- 所有计时发布完成后，48个精确版本完整payload SHA-256与原件一致，覆盖21,428,320,830 B。首个post-timing harness导入PyArrow路径失败后，仅恢复验证；未重跑或改写三组计时。验证后改用4进程并行只读，已有14项串行结果保留，其余续跑；不把这些验证读计入发布profile。
- 额外format smoke：真实12,000,400 B Arrow stream与9,439,332 B／12 messages GRIB跨8 MiB，4项Arrow切片（含1/8 MiB与尾部）、3项GRIB message bytes（含跨8 MiB message）均正确；4源（另有3 B text、empty）全SHA与精确版本EOF通过，身份／layout正确。首轮只因测试Table保留mmap buffer导致卸载busy，手工正常卸载后测试复制IPC到heap并重跑；最终 `format-smoke-02` complete=true并正常卸载，不以失败首轮作为完成证据。
- 普通挂载／原版本保留；只使用独立source/build和临时实验挂载，最终无残留实验挂载。15项改动hash与服务器匹配，428项source manifest记录不含AppleDouble；新daemon `fef0f916ddbb20fdaf7c4bff8bf81a98e31c6a82a4af65253bbf530a368efd95`，两个新臂使用同一binary；旧binary冻结为 `e2eb9508528fa788051f7602808ed1d3f7ad42977a1fff202e645ab1c2a13f8f`。

## 证据与复验入口

摘要JSON（服务器归档：`docs/hdf5-geos-publication-batching-20261010.json`）、[三组原始汇总](../results/hdf5-geos-publication-batching-20261010/summary.json)、[48项完整SHA](../results/hdf5-geos-publication-batching-20261010/payload-verification.json)、[最终多格式smoke](../results/hdf5-geos-publication-batching-20261010/format-smoke-02/report.json)、[单元日志](../results/hdf5-geos-publication-batching-20261010/ctest.log)。每组目录`old/`、`single/`、`batch/`保存provenance、完整JSONL、原始pilot；根目录对应严格validator输出、命令与source哈希。

脚本：[profile wrapper](../../tests/benchmarks/hdf5_query/geos_publication_profile.py)、[严格计数验证](../../tests/benchmarks/hdf5_query/geos_publication_profile_validate.py)、[完整payload验证器](../../tests/benchmarks/hdf5_query/publication_payload_verify.cpp)、[多格式smoke](../../tests/benchmarks/hdf5_query/publication_batch_format_smoke.py)。profile wrapper省略`--manifest-batch-bytes`可跑旧冻结binary；新臂显式1048576／8388608，validator使用同参数；全部发布计时结束后对每个精确source/version运行full verifier。

远端保留 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-publication-batching-20261010/`。源码实现见 `StructuredBlobStorage`、`IoTDBStructuredBlobBackend`、`IngestionModule`与挂载配置；没有修改旧实验source/build或既有普通挂载binary。
