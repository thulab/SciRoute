# HDF5 dataset 发现与 CF 数组查询

本功能将属性目录发现与数组执行分开。目录返回同一 daemon / database 中的精确
文件版本、dataset 对象 ID、规范路径、manifest revision 和 layout digest；随后
使用已有 `kind=typed` 请求读取单个 dataset。发现结果不代表跨数据库身份，
也不自动拼接不同 dataset。

## 摄取与持久化

HDF5 layout 当前写入 `/5`。摄取保留原始文件 B，生成对象目录、chunk 页、属性和
CF／经验证的 GEOS 压力坐标证据；另外写入版本绑定的 dataset catalog。目录、属性行和原子发布标记
先后写入；只有完成发布的精确版本参与发现。rename 复制目录至新身份，删除
清理目录和属性行，已发布旧版本不会原地升级。

旧 layout `/1`–`/4` 可经 `hdf5-migrate inventory/run/resume` 从原始 B 重摄取为
新版本；旧 `/4` 仍可执行其原有物理视图，但不提供新增压力坐标语义。沿用
namespace writer lock，并要求其他主机停写。重新 inventory 到新报告目录；
旧迁移报告绑定的目标协议不同，不能用于本次 `/5` 迁移。

## 控制协议

通过已有 Unix socket 的长度限定 JSON 控制协议发送：

```json
{
  "kind": "dataset_catalog",
  "request": {
    "sources": {"paths": [{"kind": "subtree", "path": "/ocean"}]},
    "dataset_path": "/sst",
    "predicate": {
      "kind": "compare", "field": "hdf5.attr:long_name", "operator": "eq",
      "value": {"type": "utf8", "value": "Daily sea surface temperature"}
    },
    "max_sources": 1024,
    "max_datasets": 1024,
    "max_catalog_bytes": 67108864
  }
}
```

响应 `kind=dataset_catalog` 的 `datasets` 包含 `path`、`version`、`format_id`、
`revision`、`object_id`、`dataset_path`、`layout_digest` 和 `layout_protocol`。
计数还包括所选 source 数、筛选前 dataset 数、过滤 SQL 数和目录字节数。
不在该操作中生成 FUSE 文件。C++ 客户端使用 `submit_query_control`。
请求和响应各不超过 1 MiB；超出上限明确失败，不静默截断。

属性名采用规范 UTF-8 百分号编码。支持 compare、IN、AND/OR/NOT 和 null 检查，
复用 typed literal 类型规则。标量以及原 shape 元素数为一的属性参与比较；
多元素、变长字符串或读取受限的属性明确报不可用。完整目录中的缺失与未知
不同，不能将未知当作缺失。属性只用于 predicate，不支持投影为数组列。

每个合取都关联同一个 source/version/object。先选择 latest，再筛选属性；
最新版本不匹配时不会回退到历史版本。执行授权先于属性读取。

数据库执行相关属性 JOIN 和谓词，adapter 在规划 chunk 前再次检查属性。
当前实现仍读取各候选 source 的有界目录 artifact 来确认类型和完整性。
这是后端候选筛选，不代表已经验证数据库物理索引剪枝，也不保证优于 FS
预建目录。发现不读取文件 payload 或 HDF5 layout。

## 数组字段与语义

- `hdf5.value:%2Fsst`：物理原值。Int16 输出为无损 Int32；Float32/64 保留类型。
- `hdf5.science.value:%2Fsst`：科学值，输出 nullable Float64；先在原始数值域
  识别 fill、missing 和有效范围，再计算 `raw * scale_factor + add_offset`。
- `hdf5.axis.0`–`.3`：UInt64 ordinal 轴，半开区间。
- `hdf5.coord.time`、`.latitude`、`.longitude`：科学值视图的 Float64 坐标。
  坐标谓词转换为 ordinal 子数组，保留原始轴顺序，支持升序和降序。

自动绑定要求 CF 声明、可验证的正向 dimension-scale 关系、轴长度、可接受单位、
有限且严格单调的一维坐标。没有足够证据时仍可使用受支持的物理查询，科学查询
明确报不支持；不凭 `time`、`lat`、`lon` 名称推断。
时间保留原始数值单位和 calendar；缺少 calendar 时记录 CF 默认 `standard`
及默认来源。没有 UTC 转换或跨历法合并。空间不支持曲线网格、投影转换或跨经线
循环区间；其他轴仍通过 ordinal 查询。

IPC schema 结果描述保存精确 source、dataset、完整/所选 shape、C 顺序、原始
类型、缩放和 mask 依据、轴单位、calendar 及绑定证据；空结果也保留描述。
原始 decoded chunk cache 不因科学视图而改变，变换仅作用于输出。

## dimension-scale helper

安装时需将 `iotdbfs_hdf5_scale_worker` 与挂载二进制放在同一目录；受信任的
部署配置也可通过绝对路径 `IOTDBFS_HDF5_SCALE_WORKER` 指定。
它只通过父进程有界范围 RPC 获取源数据，返回对象关联。父进程复核对象、轴、
scale、长度和坐标证据。默认上限：512 MiB 地址空间、5 s CPU、10 s 墙钟、
64 MiB 源读取、65,536 次读取、4,096 条关联以及约 1 MiB 响应。
helper 与挂载进程均永久关闭动态插件，拒绝 driver/VOL 环境覆盖。

生产验收平台为 Linux。当前 macOS 无法安装该 `RLIMIT_AS`，所以自动 CF 关联
明确返回不支持，不以取消内存限制作为回退。普通文件访问、物理 HDF5 查询和
不依赖 helper 的元数据操作仍可用。墙钟限制约束 helper 通道与计算；父进程
同步 storage read 的超时仍由底层 I/O 控制。
