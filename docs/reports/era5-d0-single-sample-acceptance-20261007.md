# ERA5 D0：冻结样本与精确版本单样本验收

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

2026-10-07，在 `root@192.168.99.49` 的
`/data_01/zzy/iotdb-fs-exp/era5-grib-baseline` 完成 D0 纵向切片。
本次目标是核实相同原始 GRIB、冻结坐标与精确版本的数值一致性；
不是吞吐实验或完整 EG01a 验收。机器可读结果见
单样本报告（服务器归档：`docs/era5-d0-single-sample-result-20261007.json`）。

## 冻结合同

`tests/benchmarks/grib_training/sample_contract.py` 固定 20 通道的
`paramId`、level、来源类别和顺序，ERA5 721×1440、0.25° 的网格，
每个区域的 16 个 128×128 patch，以及输入 `[t−6h,t]` 和目标
`t+lead`。V0/V1/V2 lead=6h，V3 lead=24h。D1 使用 UTC 半开区间：
train `[2018-01-01,01-22)`、validation `[01-22,01-27)`、test
`[01-27,02-01)`；一个样本的输入与目标不得跨 split。

服务器上的完整冻结表：

- `manifests/d0-sample-contract.json`，snapshot fingerprint
  `12e3b6d7ca804c594c1e18856de608bc9687d11d8b9c951d40ca1ee6afdf9f40`；
  V0/V1/V2 各 96 个 train 样本，V3 有 48 个；文件 SHA-256
  `976a0fff214c8e068b5b97c316c0b43ae0daff937d99d33e7da8f1e7d26c8eef`。
- `manifests/d1-sample-contract.json`，snapshot fingerprint
  `f8dd5e6a1ad2922f33bf590868c00ab1aee26227f66496b334b1ce059fa0c585`；
  V0/V1/V2 各 1312/288/288 个 train/validation/test 样本，
  V3 各 1264/240/240 个；文件 SHA-256
  `5c06bff409ceaa4f69c77c97caca165f020a4dc1ad9f2a6d25b8be983c2f3b36`。

冻结前对两份源文件重新计算 SHA-256，并校验消息目录覆盖全部时次与通道。
样本 ID 仅依赖内容身份、view、init time 和 patch，不依赖进程或存储路径。
D1 冻结只核实样本与 split；本次未在 D1 上执行查询或训练。

## D0 源与查询

| 来源 | 字节 | SHA-256 | IoTDB-FS 路径 | 精确版本 |
| --- | ---: | --- | --- | ---: |
| single-levels | 66,450,816 | `bc11f14559073b85b8da103a94aab0282adb906913fa6d3f9a6d16e7ca4166c6` | `/d0/single-levels-201801.grib` | 1791373195423 |
| pressure-levels | 265,803,264 | `10bf9818bc776fd6c7deafd63a7bd7e22c78b3503c4f5e127cc6402e529501bd` | `/d0/pressure-levels-201801.grib` | 1791373225886 |

两文件的普通 FUSE 路径及隐藏精确版本路径均完整读回，大小及 SHA-256 与
原始 D0 文件一致。新版 GRIB layout 索引保存 `paramId`、`validityDate`、
`validityTime`；typed 查询支持这三个字段的精确谓词和投影，并投影物理
message offset。每个样本按时次对两类文件各发一次精确版本查询，共六次；
经纬度 slice 裁剪 patch，客户端按 offset 核对 manifest 后组装通道。
旧 layout 遇到新增谓词会明确报错，不会误报为空结果。

## 单样本逐值结果

V0、East Asia `r156:c400`，init `2018-01-01T06:00:00Z`；
输入 00:00 和 06:00，目标 12:00。样本 ID：
`be4fa55dd8e30fca680774e0d2eac30ad319b233df4ee57f68517a98a5a12c14`。
本地 ecCodes reader 与 IoTDB-FS typed reader 均得到 Float32
`X=[2,20,128,128]`、`Y=[1,20,128,128]`，mask 全有效。
`X`、`Y`、mask 逐值相同，最大绝对误差 **0**，双方 SHA-256 tensor digest
均为 `47cac2cae81c74ff03d3586d8cee3699801b90653fcd26af4a5be42da459b877`。

GRIB 定向 C++ 测试 40/40 通过；Python 样本边界与稳定 ID 测试 3/3 通过。
单样本产生六次 typed 查询与 43,367,136 字节 IPC。这个计数供后续
性能剖析使用，不能由一次 warm/cold 状态未控制的读数推断吞吐优劣。
后续仍需 D0 全样本流式对照、Zarr 对照、训练窗口服务以及 D1 供给与训练验收。

## 运行环境变更

现有 DataNode 的 direct-buffer 配额阻止新 SchemaRegion 分配。
经授权将 `datanode-env.sh` 默认内存从 16G 调为 17G，
off-heap 从 2048 MB 增至 2176 MB，只重启 DataNode。
旧进程退出至 RPC 恢复约 123 秒；原 Arrow/GRIB 挂载在恢复后可读。
原配置备份和变更记录在服务器 ERA5 实验目录的
`runtime/config/backups/` 与 `results/setup/d0-iotdb-memory-change.json`。
ERA5 的 SchemaRegion 82、DataRegion 83/84 均为 Running，独立 FUSE
挂载和查询 socket 保持运行。
