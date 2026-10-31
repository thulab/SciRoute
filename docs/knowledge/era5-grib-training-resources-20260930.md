# ERA5 训练基线：服务器资源核查与准备清单

核查日期：2026-09-30，服务器 `root@192.168.99.49`，核查时间 15:16–15:20 +08:00。本次仅做只读核查，未安装依赖、下载数据、创建数据库、重启挂载或启动训练。实验语义与验收门见 [ERA5 基线计划](../deliverables/era5-grib-training-baseline-plan.md)。下面严格区分实测资源、建议配置和待实现代码。

## 1. 服务器现状

| 资源 | 本次只读核查结果 | 判断 |
| --- | --- | --- |
| `/data_01` | 约 3.3 TiB 可用，使用率 55% | 主数据、模型和运行产物放这里 |
| `/data_02` | 约 359 GiB 可用，使用率 96% | 不继续承载本基线的主要数据增长 |
| CPU | 32 个逻辑 CPU，Intel Xeon Gold 6130 | 可做多 reader 与解码，正式运行需控制各进程线程数 |
| RAM | 总计约 377 GiB，available 约 327 GiB；swap 已用约3.5/4 GiB | D1 可完全驻留，不能称为 out-of-core 实验；已有swap占用不等于当前正在换页，运行时监控swap in/out |
| GPU | 8×NVIDIA A100-SXM4-40GB；核查瞬间均 0 MiB、0% | 足够单 GPU和2／4实验；瞬时空闲不等于已预留，启动前再核对 |
| 隔离 IoTDB | 端口 33667 的实验服务运行 | 沿用服务，另建 ERA5 namespace；不改生产或 Arrow 数据 |
| 系统 ecCodes | `grib_ls`、`grib_get`、`codes_info` 可用；2.34.1 | 可做原生目录审计；Python binding 尚需准备 |
| 已有训练 venv | `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/venv` | 作为可工作的依赖版本参考，不原地升级 |
| CDS 凭据 | root 的 `.cdsapirc` 不存在；未读取任何凭据 | 尚未确认可下载；其他用户或环境变量授权状态未检查 |

已有训练环境实测：Python **3.12.3**、Torch **2.9.1+cu128**、NumPy **1.26.4**、PyArrow **21.0.0**、Transformers **4.57.1**。此 venv 未安装 `eccodes`、`cfgrib`、`xarray`、`zarr`、`cdsapi`、`netCDF4`、`dask` 的 Python distribution。系统 ecCodes 可用不等于 Python import 已通过。

此结果证明硬件与基础服务适宜准备 EG00/EG01，**不证明 ERA5 数据、训练 reader 或 GRIB 训练窗口服务已经具备**。可用内存、磁盘和 GPU 均是瞬时值，正式跑实验需再检查。

## 2. 已有气象数据是否能复用

- `/data_01/era5_12h` 已有目录，但限定深度2核查见到的是 `0`…`12`、`surface` 下的138个 `.tsfile`、14个 `.tsfidx` 和14个 `.done`，合计141,581,574,644 B（约131.9 GiB），未发现GRIB。不能由目录名推断为所需6h、20通道、0.25°原始GRIB，也不能把TsFile转回GRIB作为“官方原始GRIB”性能数据。
- `/data_01/zzy/iotdb-fs-exp/datasets/grib` 的已知文件是现有 GFS 验收用数据，约 2.4 MB 和 525 MB。它们可回归 GRIB 查询，但不替代 ERA5。
- 本次不扫描整个 `/data_02/TimeBench`，也不依赖未公开训练器。未发现符合本计划的原始 GRIB 不等于证明整台服务器没有；正式下载前可对用户提供的具体档案位置补查 manifest。

因此，当前资源清单将 ERA5 D0/D1 标为**需要取得并审计**，不把现有 TsFile 记为已完成下载。

## 3. 计划目录和隔离边界

建议在现有实验根目录下新增独立子目录；以下是待创建布局，不是本次已创建的资源：

```text
/data_01/zzy/iotdb-fs-exp/era5-grib-baseline/
  venv/                         # 独立 Python 环境
  sources/                      # 外部参考代码的固定 commit/许可
  raw/d0/                       # 两天 CDS 原始 GRIB及请求记录
  raw/d1/                       # 一个月 CDS 原始 GRIB及请求记录
  raw/d2/                       # 可选跨年扩展，初期不下载
  manifests/                    # 文件hash、消息目录、coverage、split、patch
  transforms/                   # 仅训练域统计及稳定身份
  derived/zarr/                 # 同源 Z1 基础数组，非预物化窗口
  fixtures/revisions/           # 明确标注的受控版本修订
  runtime/config/               # 独立 namespace、socket、预算、挂载配置
  runtime/mount/                # 独立挂载点
  checkpoints/                  # 各 view/model/backend/repetition 分离
  results/                      # trace/profile/环境锁/日志/正确性和汇总
```

IoTDB namespace 建议 `iotdb_fs_era5_baseline`，实际创建前核对不存在名称冲突。沿用现有 writer-lock 和 socket 安全策略，不共用 Arrow 挂载的运行目录、结果注册表或缓存实例进行性能测量。共用 IoTDB 进程时记录其他负载，避免同时跑 Arrow 和 ERA5 吞吐测试。

代码进入本仓库 `tests/benchmarks/grib_training/`，服务器只部署锁定代码版本。原始数据、checkpoint 和大 profile 不进 Git；复现包保存文件校验和及获取方法。不得把 CDS token 写入请求 manifest、配置模板、日志或仓库。

## 4. 需要准备的外部资源

| 项目 | 来源与准备方式 | 首轮是否必需 | 当前状态 |
| --- | --- | --- | --- |
| ERA5 D0/D1 | CDS single levels / pressure levels，20 通道、每天4时次、GRIB；见主计划 | 是 | 待下载、审计 |
| CDS 账户授权 | 配置运行下载用户的个人 token，账户接受两数据集条款；密钥不回传报告 | 是，若从 CDS 下载 | root 配置文件未发现，授权未验证 |
| ecCodes Python API | 独立 venv 安装并记录 Python包及实际加载的 native library/definitions版本 | 是 | 系统库2.34.1已有，Python API待安装 |
| PyTorch/CUDA基础 | 参考现有3.12.3／2.9.1+cu128工作组合，单独重建并锁定 | 是 | 可复用版本与包缓存；新环境未建 |
| NumPy、PyArrow | 参考现有1.26.4／21.0.0，验证依赖兼容 | 是 | 现有环境可用 |
| xarray、Zarr、numcodecs | 同源 Z1；首版明确冻结 Zarr format/version 和无损 codec | Z1必需 | 待安装与兼容性验收 |
| cfgrib | 交叉核查、标准索引/过滤参照；性能 reader可直接用ecCodes | 建议 | 待安装 |
| cdsapi | 官方当前说明要求 `>=0.7.7`；下载前冻结实际安装版本 | 下载必需 | 待安装 |
| psutil、pytest、监测工具 | 进程组内存、单元与数值验证、profile | 是 | 新环境统一安装并核查 |
| Dask、netCDF4 | 仅当转换/额外气象工具需要，不作为训练热路径必需依赖 | 否 | 未装，可后补 |
| WeatherBench参考代码 | `pangeo-data/WeatherBench` 固定 commit、许可和CNN/评分参考 | 参考 | 待获取；无需安装旧Keras全套环境 |
| 预训练权重／GraphCast | 本计划首模型随机初始化 | 否 | 不下载 |
| TimeBench／TimerLathe | 私有路径不进入本基线 | 否 | 不依赖 |

官方 CDS 流程要求用户在网页接受数据集条款，并使用个人 API token；新版示例使用 `data_format="grib"`。下载脚本以实际页面生成的请求参数核对为准，按真实日历枚举天数，保存请求/响应摘要，临时文件完成校验后原子发布，失败可重试且不覆盖已冻结文件。[CDS API 设置](https://cds.climate.copernicus.eu/how-to-api)

首次只请求 D0。成功后记录 GRIB 压缩字节/场与 native decode peak，用它估算 D1，再扩展整月。允许保存 server 端返回的归档并逐成员记录 hash，不能把 ZIP 响应直接命名为 GRIB后摄取。下载等待和排队是获取成本，不计作训练查询时延。

新 venv 不直接执行无版本的全量升级。冻结 Python包 lock、`pip check`、Torch CUDA/cuDNN/driver信息、ecCodes native/binding路径、Zarr格式/codec；验证同一消息在 C++ kernel 和 Python oracle 的值一致。若 Python wheel 自带另一份 ecCodes，不能只记录系统 `codes_info` 作为训练解码版本。

## 5. 需要编写或接线的代码

| 组件 | 当前可复用部分 | 待交付 |
| --- | --- | --- |
| 下载/审计 | 系统 ecCodes CLI | CDS请求生成与resume、消息manifest、覆盖/单位/网格校验 |
| 训练模型 | 现有Torch GPU环境、公开CNN参考 | 主计划中的RegionalCNN-v1、loss/metrics、统一train/eval/save/resume |
| F1本地reader | 原生ecCodes message访问 | 持久offset索引、byte有界缓存、按trace取样、计数 |
| Z1 | 公开Zarr API | 同源float32/mask基础数组转换、chunk/codec冻结、精确对照 |
| F2 | 现有共享缓存设计经验 | 本地文件source共享解码服务、single-flight/预算及等价输出 |
| Q2 | 已验收GRIB typed message/crop/provenance | 气象valid-time catalog、GRIB训练snapshot/coverage/assembler、缓存和前端接线 |
| profile/实验驱动 | Arrow实验中的分阶段计时、trace配对方法 | 气象张量/消息粒度计数、1/2/4任务配额、汇总 |
| 更新与恢复 | 精确source/version与稳定身份设计 | snapshot持久声明、重新注册、完整训练状态恢复测试 |

以上列为实现任务，不能把计划中的文件名当作现成运行命令。资源准备阶段完成 D0＋环境后，才进入 EG01 纵向切片；需要修改公共查询 API 的设计另行审阅。

## 6. 初始资源预留建议

| 阶段 | 磁盘工作配额建议 | 内存/缓存建议 | GPU |
| --- | --- | --- | --- |
| D0正确性 | 20 GiB | 总工作集先限32 GiB；decoded主配置4 GiB以内 | 1张做forward/backward |
| D1供给与1/2/4实验 | 200 GiB | 单次实验矩阵总进程组先预留96 GiB；decoded总预算1/4/16 GiB消融，主配置4 GiB | 单实验1张；多实验每job1张、最多4张 |
| D2跨年扩展 | 暂规划1 TiB，必须用D0/D1实测重新估算 | 依据实际工作集与总资源约束重审 | 最多4张并发，先核实占用 |

这些是建议预留额度，不是实测消耗或下载大小。磁盘必须包含本地原GRIB、IoTDB存储/WAL/历史版本、Zarr、临时转换、checkpoint和profile。估算采用 `G_local + G_iotdb_actual + Z_actual + versions + temporary + results`，不能只按浮点值体积。D2 Float32展开已约339 GiB，压缩比、数据库空间与保留策略未知，1 TiB并非保证足够。

统计拟合和Zarr转换必须流式有界，不能一次 `.load()` 全球多年数据。D1可驻留服务器RAM，因此冷热定义严格按实际缓存层记录；若要测超内存，用真实更大数据或双方相同资源限额，并明确后者是受控预算实验。

CPU/GPU启动前再次查进程，按同一总核数预算比较后端；约束OpenMP/BLAS/ecCodes工作线程，避免4个job各自占满32核。先跑loader-only避免GPU竞争，J=2/4端到端时分配独立GPU；没有空闲独立GPU则推迟该项，不把共享GPU争用解释成供给性能。

## 7. 准备顺序与完成标准

1. **P0：隔离目录与环境。** 创建独立目录/venv；固定代码与依赖；GPU小张量、ecCodes/Python解码、Zarr往返通过；保存锁文件。现有Arrow环境不变。
2. **P1：取得D0。** 账户/条款可用后下载两天；160场、20通道、8时次、网格/单位/edition/bitmap审计通过；冻结hash与请求。
3. **P2：本地纵向切片。** F1＋RegionalCNN单样本/50steps/save/resume；同源Zarr逐值验证；冻结patch/split/统计/trace。
4. **P3：IoTDB-FS接线。** 独立namespace与挂载；源字节往返、精确版本、GRIB训练组装与profile通过；旧GFS查询回归通过。
5. **P4：D1与EG01/02。** 按实测容量取得整月，先正确性，再供给与训练；F2到位后进行多实验结论验收。
6. **P5：EG03。** 用冻结真实时次做append、独立fixture做修订模拟；验证重新注册与checkpoint恢复，最后决定是否扩展D2。

当前可以开始P0；P1需要确认下载账户授权或提供符合协议的既有原始GRIB。没有要求现在获取D2、购买资源或下载大模型权重。

## 8. 核查复现入口

以下为只读命令；Python依赖通过现有venv的`importlib.metadata.version`核查，CDS只检查文件是否存在，不读取token。

```bash
ssh root@192.168.99.49 'df -h /data_01 /data_02'
ssh root@192.168.99.49 'free -h'
ssh root@192.168.99.49 'lscpu'
ssh root@192.168.99.49 'nvidia-smi --query-gpu=name,memory.total,memory.used,utilization.gpu --format=csv'
ssh root@192.168.99.49 'codes_info -v'
ssh root@192.168.99.49 '/data_01/zzy/iotdb-fs-exp/runtime/bin/status.sh'
ssh root@192.168.99.49 'findmnt -n -o TARGET,FSTYPE,SOURCE /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/runtime/mount'
```

现有服务器 `ENVIRONMENT.md` 中许可证 `UNACTIVATED` 描述已过时，本次状态核查为 `ACTIVATED`；本次未改该历史文件。服务可用状态仍应在正式执行前复验。
