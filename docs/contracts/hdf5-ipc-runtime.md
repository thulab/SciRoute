# H08 — HDF5 IPC 与挂载运行时

> 当前依赖（2026-10-12）：HDF5 C 库统一要求 **1.14.4 EXACT**，shared／serial／thread-safe／DEFLATE；官方维护归档为 `1.14.4-3`。安装与校验见 [CI 依赖说明](../../ci/README.md)。下文阶段验收中的 1.14.6 为历史实测版本，未改写其证据或声称在 1.14.4 下重跑。

H08 沿用现有 typed 查询、Unix socket 和不可变 FUSE 结果路径。HDF5 原文件仍按字节读取；子集结果通过 Arrow IPC stream 返回。HDF5 ingestion 与查询 adapter 在挂载进程中注册，物理查询和请求级科学绑定共享同一个 `QueryRuntime`。动态 HDF5 插件在进程生命周期内保持禁用。

## 注册协议

每个连接发送一个 4 字节大端长度及 JSON 请求，响应使用同样的 framing；单帧上限仍为 1 MiB。HDF5 的 dataset 入口是值字段 `hdf5.value:<规范百分号编码路径>`，ordinal 轴是 `hdf5.axis.N`。本阶段没有新增 dataset subject。

物理子数组请求示例（`exact` 替换为实际已发布版本）：

```json
{
  "kind": "typed",
  "request": {
    "sources": {
      "paths": [{"kind": "exact", "path": "/observation.hdf5"}],
      "versions": {"kind": "exact", "exact": 7}
    },
    "subset": {
      "projection": ["hdf5.value:%2Fstrain%2FStrain"],
      "slices": [{
        "dimension": "hdf5.axis.0",
        "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": 3}},
        "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": 67}}
      }]
    },
    "limits": {"target_batch_rows": 16}
  }
}
```

显式 GPS 查询在同一个 `kind=typed` 请求根对象增加：

```json
"hdf5_binding": {
  "profile": "gwosc-gps-seconds/1",
  "dataset_path": "/strain/Strain"
}
```

投影或切片必须实际使用 `hdf5.coord.gps_seconds`；GPS 界限使用 `float64`。`dataset_path` 为未编码的规范绝对路径，值字段中的路径仍按 H01 编码。source/version 从请求中的精确选择获得，storage namespace 来自 daemon，客户端不能覆盖。绑定经 H07 属性与对象身份验证，创建独立 `QueryModule`，仅此注册可使用该语义；不修改全局 adapter 配置。alias 可解析到同一对象，但不能以另一个 dataset 的绑定启用坐标。

HDF5 请求的根、source、limits、subset 和 binding 对象拒绝未知或重复字段。没有显式绑定的 GPS 查询失败。HDF5 注册不接受 latest、stride、block 或 union 等尚未支持的能力。

## 有界结果描述

外层 IPC 协议仍是 `iotdbfs-query-batch-ipc/1`。新增格式无关的 plan metadata 接口；HDF5 描述写入 schema metadata，不在每个 batch 中重复。`iotdbfs.result_descriptors=1`，其余键具有前缀 `iotdbfs.result_descriptor.0.`：

| 键 | 含义 |
| --- | --- |
| `protocol` | `iotdbfs-hdf5-subarray/1` |
| `source.path/version/revision/format` | 精确已发布 source 身份 |
| `source.payload_size/layout_size` | 原文件和索引大小 |
| `source.storage_namespace/storage_namespace_scope` | backend 实例身份，scope 为 `process_local`，不能作为跨进程数据指纹 |
| `layout.protocol/directory_sha256` | layout `/3` 及 directory 校验值 |
| `dataset.path/object_id/value_type/rank` | 请求路径、持久化对象 ID、Float32/Float64、秩 |
| `axis.N.field_id/type/full_extent` | ordinal 轴身份、UInt64、完整维度长度 |
| `axis.N.selection_origin/selection_extent` | 所选子数组的原点与长度 |
| `selection.row_count/order/view` | 选区元素总数、`C` 顺序、物理或科学视图 |
| `coordinate.*` | 仅 GPS 视图存在的坐标绑定证据 |

GPS 证据包括 field/type/axis、`time_system=GPS`、`unit=second`、start/spacing/count、units/label provenance、`mapping=fma(ordinal,spacing,start)` 和 `transform=none`。provenance 区分 `verified_attribute` 与 `application_declared`，因此变长单位属性不可有界读取时的显式声明不会被表述成已验证属性。整数使用精确十进制字符串，浮点数使用 classic locale 和 `max_digits10`。

通用 encoder 最多接受 128 个附加 metadata 项、每个键最多 256 字节、键值总计 64 KiB，同时计入 IPC 总字节上限。service 在执行 cursor 前验证描述限额，encoder 在创建 Arrow schema 前再次验证；重复、保留或空键被拒绝。IPC 上限继续与 result store 的单结果上限取最小值，错误不发布部分 stream。

每个 record batch 保留原有 `iotdbfs.domain.*` ordinal slab 坐标和精确 source。消费者按 schema 中的选区形状分配数组，以 batch 的 origin/shape 放入对应位置，并按 C 顺序重塑值列。即使没有投影 axis 列，也不能丢弃 domain。绑定成功但选区为空时返回带描述的零 batch stream。若文件级 predicate 将 source 整体排除，则没有 dataset plan，沿用通用查询的无 source 空结果，不承诺提供该 dataset 的描述。

## 生命周期、权限与并发

注册响应返回包含 `&result=<内容 ID>` 的只读 FUSE 路径。沿用既有 stat/open/read/seek/mmap/EOF 行为和不可变结果校验：过期旧路径不能重新物化成另一份字节，已打开 handle 仍可读完。注册 TTL 释放请求级 module；结果 store、bridge 和 handle 的预算语义保持一致。

用户确认的属性可见性契约：Linux/libfuse 2 对只读句柄的属性刷新可能调用不携带 handle 的 `getattr`，无法区分 fstat 与新的路径 stat。因此，只要结果仍有打开句柄，daemon 保留该不可变路径的 size/mode 和打开计数，并可返回固定属性；知道完整路径的其他用户此时也可能看到结果大小。该记录不持有额外结果 lease，最后一个句柄释放后移除。新 open 和 socket 注册仍检查 source 权限，属性可见不授予数据访问权。此机制同时避免源文件 unlink 或注册过期后，内核属性刷新阻断已授权的旧句柄。

`access(2)` 是需要明确的例外：daemon 的 `op_access` 仍执行授权，但挂载启用 `default_permissions` 后，Linux 可直接使用公开的 `0444` mode，不调用该回调。因此打开期间 `access(R_OK)` 可能为真，即使随后 open 返回 EACCES/ESTALE；调用者必须以实际 open/注册结果为准。权限 smoke 同时记录这个提示值和真正的数据访问结果，不把 `access()` 当作安全判定。

socket 使用真实 peer uid/gid/补充组授权；读取已缓存结果时仍验证 source 权限。默认私有 socket；多用户挂载需显式配置共享 socket 和 `allow_other`。权限缓存、权限变更失效沿用既有挂载实现。

`query.hdf5.read_parallelism=1` 为默认值，可选择 1、2 或 4。物理查询和 GPS 注册使用相同配置、常驻 chunk cache 和 IoTDB session pool。该参数是 producer 内读波次上限，不是整进程并发总上限。

`IOTDBFS_QUERY_PROFILE_JSONL` 记录 HDF5 执行配置、共享 runtime 的 decode/hit/read 等差值，以及 codec wall/CPU 计数。并行注册的统计区间可能重叠，不能简单相加；缓存复用验收使用串行提交的两个独立客户端，另行验证并发打开。

bridge 等待有超时；一个等待者超时不会取消另一个生产者。编码失败取消 cursor。客户端提前断开时，服务完成处理后若发送响应失败，会注销 route 并释放 bridge pin；不承诺断开立即中断不可取消的 IoTDB/HDF5 调用。

## 验证

验证环境为 `root@192.168.99.49` 的独立目录
`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h08-20261002`，使用独立
IoTDB namespace、writer lock、socket 和 mount。原 Arrow/GRIB 实验挂载不变。
原生依赖锁定 HDF5 1.14.6、Arrow C++ 21.0.0；PyArrow 消费者与 h5py oracle
分属独立 Python 环境。测试不是性能对照，不据此推断并行加速比。

- Linux 全量 CTest **1,219/1,219**，包含 13 个新增 HDF5 socket/service 测试和 5 个新增 IPC/descriptor 测试；既有 Arrow、GRIB、cache 回归通过。
- macOS socket 专项 **45/45**（HDF5 13＋原有 32），HDF5 socket 的 ASan/UBSan **13/13**；IPC encoder **8/8**、HDF5 semantic/descriptor **16/16**。
- 真实 FUSE/PyArrow 验证 7 组查询：压缩二维多 batch、重复查询、hard-link 值列投影、空选区、两个并发客户端、公开 GWOSC GPS 切片。值按字节与 h5py oracle 比较，axis/slab 描述完整，原文件 SHA-256 不变。
- 两个独立进程的相同查询：首次解码 6 个 chunk、读 226 字节、4 次 payload 调用；第二次 **0 解码、0 payload 字节、0 调用**，命中 20 次。
- 另两个并发进程查询新 source 的全部 9 个 chunk：服务端处理区间重叠，静止边界间累计解码数只增加 **9**；不重复累加共享统计区间。
- stat/read/seek/EOF/mmap、只读限制、并发打开均经过实际挂载验证。错误、编码/store 限额、发布失败、TTL、断开清理及等待超时用服务级单测覆盖。
- 真实 root daemon＋无特权客户端验证补充组授权、无组拒绝、chmod 即时撤权/恢复、缓存结果再次打开，以及 unlink 后旧句柄 fstat/read 与新 open 拒绝；最后关闭后属性记录移除。固定属性可见期间，实测 `access(R_OK)` 可以为真而新 open 仍被拒绝。

可复用脚本为 `tests/benchmarks/hdf5_query/h08_mount_smoke.py` 和
`h08_permission_smoke.py`，命令见同目录 README。本地证据保存在
`docs/results/hdf5-h08-20261002/`（遵循现有 ignore 规则）；服务器保留原始
构建、CTest、profile JSONL 和验收报告。
