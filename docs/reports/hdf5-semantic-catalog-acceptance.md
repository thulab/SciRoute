# HDF5 属性目录与 CF 自动绑定：验收记录

日期：2026-10-06。功能契约与操作入口见
[dataset 发现与 CF 查询](../contracts/hdf5-dataset-discovery.md)，设计追踪见
[实施计划](../deliverables/hdf5-semantic-catalog-implementation-plan.md)。

本轮完成 dataset 属性谓词剪枝、跨文件数据库候选筛选、CF/netCDF 规则坐标自动
绑定，以及 layout `/4` 的摄取、注册、IPC、FUSE 和旧版本重摄取链路。所有科学
结果均基于原始 B 和已有 chunk，未创建新的派生空间布局。

## 验证结果

| 验证 | 结果 | 证据 |
|---|---|---|
| Linux 完整 CTest | 1,316/1,316，通过；含 1,310 单元测试 | `results/hdf5-sc-20261006/ctest.log` |
| IoTDB 属性目录 | 9 项 live 检查通过：同对象合取、UInt64、NULL、OR、latest、未发布隔离、授权、结果上限、清理 | [catalog 验收](../results/hdf5-sc-20261006/catalog-acceptance.json) |
| 公共 OISST 数值 | 4 个 dataset，20 个查询，8,294,656 行逐行核对，1,941,876 个 mask 检查；8 个空／不匹配查询零 payload | [独立 native oracle](../results/hdf5-sc-20261006/oisst-oracle.json) |
| 两天 OISST / FUSE | 属性目录返回两精确版本；每份 `/sst` ROI 64 行，科学值、mask、时间和经纬度与 h5py 一致；IPC mmap 可读 | [FUSE 报告](../results/hdf5-sc-20261006/fuse-report.json) |
| 生命周期 | rename 后发现新路径、旧路径不再发现；daemon 重启后目录仍存在 | 同上 |
| 真实旧 layout 升级 | 原 H09 二进制生成 `/3`；新命令 inventory/run/resume 生成 `/4` 新版本，旧 B/L 不变、B hash 相同 | [迁移报告](../results/hdf5-sc-20261006/migration-report.json) |
| 独立构建 | h04_query_probe 与 helper 使用真实 HDF5 1.14.6 安装构建成功 | `results/hdf5-sc-20261006/standalone-build.log` |
| 模块边界 | `ci/check_query_module_dependencies.py` 通过 | 代码结构检查 |

Linux helper 测试覆盖正常 dimension-scale 引用、源短读／异常、读取和关联预算、
driver 环境拒绝及墙钟超时回收。CF 测试还覆盖降序、非单调、证据冲突、单点轴、
原始历法、资源限制、空结果描述和物理／科学视图的原始 chunk 复用。

## 数据与环境

公开原始数据来自 NOAA OISST v2.1：

- [2024-01-01](https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/202401/oisst-avhrr-v02r01.20240101.nc)，SHA-256
  `fd2f772cbe1633b20ab0c53ed76b874406f933bbd97062d611827c5d8a73bc4c`。
- [2024-01-02](https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/202401/oisst-avhrr-v02r01.20240102.nc)，SHA-256
  `150c1ac6b727bc9ec1245fbc0feef57be0e1765c504e506e80da3bef86bff6c0`。

`/sst`、`/anom`、`/err`、`/ice` 均为 `[1,1,720,1440]` packed Int16，gzip＋shuffle，
一个空间大 chunk。原始 time 单位为 `days since 1978-01-01 12:00:00`，本次保留
该单位并记录缺省 `standard` calendar，没有转成 UTC。

实验服务器：`root@192.168.99.49`，独立工作目录
`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-sc-20261006`。
沿用已有隔离数据库 `h09probe1790939889dbfc1f` 和 H09 writer lock，只使用新的
`/h09-sc-*` 测试路径。原 Arrow/GRIB 挂载没有被停止或替换。测试挂载已卸载，
真实数据版本和报告保留以便复核；合成 catalog probe 的临时路径已清理。

独立 C++ oracle 使用原生 H5Dread 读取原始 Int16，并独立读取 packing 属性计算
期望值；挂载 oracle 使用 h5py/NumPy，并经 PyArrow memory_map 读取 IPC。
科学测试不是时间基线，未将 HDF5 原生 oracle 的读取耗时与 IoTDB-FS 作比较。

源文件哈希和服务器一致性检查见
[source hashes](../results/hdf5-sc-20261006/source-hashes.json) 与
[verification](../results/hdf5-sc-20261006/source-verification.json)。

## 发现的问题与解释边界

验收期间修复了两个直接影响目录生命周期的问题：IoTDB 不接受 DELETE 中的
LIKE tag 条件，因此先枚举具体路径，再精确删除；同时释放嵌套 SQL cursor，避免
较小 session pool 在清理和目录读取时耗尽。测试中残留的合成路径已逐一核实并清理。

macOS 无法安装 helper 所需的 RLIMIT_AS；生产代码明确返回不支持，未放宽限制。
本地依赖 helper 的测试显式跳过，Linux 完整验收不跳过这些用例。

数据库确实执行属性条件，但当前需要先读取选中 source 的有界 catalog artifact
做完整性与类型验证。两天 OISST 的一次发现读取 9,436 B 目录，报告 13 次 SQL
调用；这些调用包含目录和身份读取，不能视为只有一次索引操作。
本轮证明功能与结果正确性，没有证明数据库内部物理索引剪枝或查询性能收益。
随后完成的[七天 OISST 同条件性能实验](hdf5-scientific-query-benchmark.md)
将原生目录遍历、FS 预建目录与 IoTDB 属性发现，以及 A–E 五条数组读取
路径分开计时；结果显示当前实现没有查询速度优势，并定位了重复 layout/manifest
读取和注册／IPC 的固定成本。

目前不支持曲线网格、投影转换、跨经线循环选择、跨历法拼接；变长字符串属性
仍明确标为不可有界读取。自动绑定只用于证据充分的一维规则坐标，其他轴保留
ordinal 访问。后续扩展不应把这些限制静默解释为数据缺失。
