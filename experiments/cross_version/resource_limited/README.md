# 有界资源、配对比值复测

方案与授权边界见[复测方案](../../../docs/deliverables/resource-bounded-ratio-retest-plan.md)。本组只重新打开已有发布、使用已有索引和冻结二进制；不生成语料、重建索引或构建产品版本。

2026-10-22 资源修正后，当前入口使用 journal `/2` 和 exclusive `root` 分区；旧已完成 run 使用的 `isolated` 配置与原始记录保留，不能以本页新配置重标旧数据或混合统计。修正及新 GRIB 复测边界见[新资源协议报告](../../../docs/reports/grib-render-balanced-resources-20261022.md)。

| 项目 | 固定协议 |
|---|---|
| CPU | 一个 cgroup v2 exclusive `root` partition，启用内部负载均衡；2 个逻辑 CPU 由客户端、IoTDB-FS、FS-index 和原 DataNode 共用；总 quota 为 `200000 100000` |
| 内存 | query 叶组共 2 GiB；DataNode 另组保留原 15232 MiB 堆和 2176 MiB direct-memory 设置；全栈内存另计 |
| 场景 | 独立 GRIB N128/V1；HDF5 N512/B4/C128/V1；既有 GRIB N8/大 S/identical/V8/选择性 All |
| 样本 | 每场景 baseline/candidate 各一个保留挂载；各 2 对 warmup、10 对测量；每对交换先后顺序，共 144 次查询 |
| 主指标 | 同 block 两端测量中位数之比 R；同时保留两端绝对时间、逐对比值和所有原始行 |
| 阶段比值 | `Rd=median(Td_IoTDB-FS)/median(Td_FS-index)`，`Rq=median(Tq_IoTDB-FS)/median(Tq_FS-index)`；沿用同组10对暖样本，保留两端绝对阶段时间 |
| 时钟 | 原完整查询时钟；独立 oracle、profile 收集、资源观测、挂载恢复和索引准备在时钟外 |
| 交付差异 | GRIB FS-index 为 native C++ owned 数组，IoTDB-FS 为含 IPC 的 NumPy owned 数组；HDF5 两端均为 NumPy owned 数组 |
| 恢复 | 外部 supervisor 持有原实例控制器；query worker 失败时仍恢复原配置、原实例 cgroup／亲和性，并释放独占 CPU |

从仓库根使用原 pinned Python。正式执行会临时停启原 DataNode、设置 SYNC/partial=false，并短时减少其它共享任务可用的 CPU；必须具有相应维护窗口授权。2026-10-21 本轮用户已明确授权所列范围。

```sh
PY=experiments/single_file/work/session-20261012/venv/bin/python
# 只校验身份、完整 raw/FS 字节、已有索引和二进制，不改服务或 cgroup。
$PY -m experiments.cross_version.resource_limited.run \
  --repo "$PWD" --registry "$PWD/experiments/cross_version/work/RUN-registry" \
  --work "$PWD/experiments/cross_version/work/RUN-preflight" --prepare-only

# 使用另一个全新 work 目录；CPU 必须通过隔离校验，否则拒绝计时。
$PY -m experiments.cross_version.resource_limited.run \
  --repo "$PWD" --registry "$PWD/experiments/cross_version/work/RUN-registry" \
  --work "$PWD/experiments/cross_version/work/RUN-measured" --cpus 2,3

python3 -m unittest discover -s experiments/cross_version/resource_limited/tests
```

完整 run 结束后，可仅对原始计时做离线拆分，不重新查询或停启服务：

```sh
python3 -m experiments.cross_version.resource_limited.phase_ratios \
  --run-dir experiments/cross_version/work/RUN-measured \
  --output experiments/cross_version/work/RUN-phase-analysis.json
```

该入口校验原 report、design、queries、analysis 的 SHA，复用完整144行的身份／协议／配对校验，逐条验证 `T=Td+Tq`，并输出独立摘要、12对逐阶段比值和来源摘要。不重写原证据，不用 `T−Td` 填补缺失的取值计时。GRIB FS-index 的 Td 原本就是 `T−累计取值子计时`，为发现及其它非取值工作的残余量，读取与发现交错；其 Tq 终点仍为 C++ owned buffer。HDF5 两端以连续时间点切分且均交付 NumPy owned buffer。阶段比值不能直接归因于单独的索引、物理I/O或IPC。

R 不等于 Rd+Rq 或 Rd×Rq。每一对样本才有 `R_pair=w×Rd_pair+(1−w)×Rq_pair`，其中 `w=Td_FS/T_FS`；分别取中位数后不能套用该恒等式。

公开证据包括 `prepared.json`、`design.json`、`queries.jsonl`、`analysis.json`、资源配置／采样／恢复 JSON，以及原服务恢复标记。连接凭据只由既有 helper 在内存中继承；`.private.*`、原配置备份和服务启动日志不作为公开结果下载。

`historical.py` 从原始文件重算同 block 的历史比值，保留文件 SHA、样本行号和协议；缺资源记录只作历史参考。不能把单侧优化结果与旧 FS 时间拼接，也不能把 N512 中四桶的 C128 写成独立 N128。

CPU 分区隔离用户任务调度，未修改全局 IRQ 亲和性或共享磁盘、内存带宽配置。资源观测覆盖查询外的 oracle/profile 工作，不作为独占查询 CPU 时间，也不用于归一化墙钟。

## 原生 HDF5 两点复测

`native_hdf_run.py` 单独复测既有独立 N128 与 N512/B4/C128，固定当前产品二进制，仅显式启用 `native-hdf5`；不会运行 `iotdb-indexed`、生成语料或重建 FS 索引。每点 2 对 warmup＋10 对测量，共48条正式查询。资源与恢复流程复用上文，查询模式还须由实际进程配置和逐条原生 setup/read/slab/source 计数证明；若出现自建 chunk decoder 则失败。

```sh
$PY -m experiments.cross_version.resource_limited.native_hdf_run \
  --repo "$PWD" --registry "$PWD/experiments/cross_version/work/NATIVE-registry" \
  --work "$PWD/experiments/cross_version/work/NATIVE-preflight" --prepare-only
$PY -m experiments.cross_version.resource_limited.native_hdf_run \
  --repo "$PWD" --registry "$PWD/experiments/cross_version/work/NATIVE-registry" \
  --work "$PWD/experiments/cross_version/work/NATIVE-measured"
```

`native_hdf_scene.py` 复用原有两类适配器。N128 的 FS 交付仍止于 C++ owned buffer 且每次重开 HDF 句柄；C128 则交付 NumPy owned 并保留 H5F/H5D/chunk cache。两组不是纯 N 消融。N128 旧 corpus 缺掩码 oracle，保持 corpus 不变，建立现有模型支持的 value-only case，另外由真实原生读取补充两命中掩码收据，SHA 纳入运行身份；两端实际均交付并校验值和掩码。不能把补充证据写成历史 value＋mask case 已绑定。

分析与执行状态统一记录在[查询主线文档](../../../docs/deliverables/query-mainline-analysis.md)。失败 run 和零样本启动失败同样保留，成功必须同时满足科学矩阵完成、原服务恢复和资源恢复；stdout 单一完成标记不能替代这些证据。
