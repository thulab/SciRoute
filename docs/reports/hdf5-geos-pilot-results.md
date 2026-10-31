# GEOS-5.7.2 HDF5 单文件试点（2026-10-06）

本试点在 `root@192.168.99.49:/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/geos-pilot-20261006` 运行，使用已有隔离数据库 `h09probe1790939889dbfc1f` 和 HDF5 SC 构建产物。原始数据为 [NASA GEOS-5.7.2 `inst3_3d_asm_Np` 2011-07-02 09:00 样本](https://gmao.gsfc.nasa.gov/media/gmaoftp/ops/GEOS572_Sample_Data/inst3_3d_asm_Np/DAS.ops.asm.inst3_3d_asm_Np.GEOS572.20110702_0900.V01.nc4)，446 MB，SHA-256 `fd763bf40ed34e0e4f91b9da1bd94e272e2145c95a0323b292f7b48b143a723e`。IoTDB-FS 发布路径为 `/geos-pilot-9856a092a812/file1.nc4`，版本 `1791271058856`。发布耗时 9.54 s，不计入在线查询。试点未改动已有 OISST、GRIB 或 Arrow 实验数据。

原文件有 18 个 dataset、29,764 个 chunk。`/T` 为 `[1,42,721,1152]` Float32，chunk 为 `[1,1,91,144]`，共有 2,688 个压缩 chunk。本试点固定查询 `/T` 的四组 ordinal 子集：单点、16×16、160×160、5 个层×16×16。对应结果为 1、256、25,600、1,280 个值，涉及 1、4、4、20 个物理 chunk。具体坐标及压缩大小见 [source-inspection.json](../results/hdf5-geos-pilot-20261006/source-inspection.json)。

`/T` 实际样值约 284 K，但该样本将所有四维变量的 `valid_range` 写成 `[1e15,1e15]`，与 `_FillValue` 相同。科学值视图若严格遵守该属性会把正常值判无效，所以**本次仅比较 `hdf5.value:%2FT` 物理 Float32 和四维 ordinal 切片**。属性目录谓词使用 `standard_name = air_temperature`。这项数据质量问题以及未使用压力层语义坐标是本试点的明确限制。

六轮单客户端顺序查询，各路径的稳态中位数取第 2–6 轮，单位 ms。A/C/E 使用 h5py/PyArrow，B/D 使用相同 C++ adapter 查询和 owned-copy 逻辑；计时均排除摘要计算。A 是原生文件，B 是本地 payload + HDF5 adapter，C 是 h5py 直接打开 IoTDB-FS 挂载内的原文件，D 是直连 IoTDB 的 typed adapter，E 是 Unix socket 注册 + FUSE IPC 读取。C/E 所用挂载关闭了整文件预取与 raw-file cache，B/D/E 的 decoded-fragment cache 为 256 MiB，查询读并行度为 1。

| 切片 | A 原生 | B 本地 adapter | C 挂载文件直读 | D IoTDB typed 直连 | E 注册 + IPC | C/D |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 单点 | 1.247 | 0.670 | 35.452 | 18.761 | 120.646 | 1.89× |
| 16×16 | 1.726 | 0.678 | 35.754 | 18.155 | 120.985 | 1.97× |
| 160×160 | 1.819 | 0.921 | 33.341 | 18.056 | 122.315 | 1.85× |
| 5 层×16×16 | 4.203 | 0.898 | 98.468 | 22.452 | 120.828 | 4.39× |

所有 A/C/E 查询的行数和 Float32 SHA-256 相同；B/D 的 FNV64 也与原生值逐项一致，详见 [summary.json](../results/hdf5-geos-pilot-20261006/summary.json)。B 比 A 快 1.86–4.68 倍，说明预建 chunk 布局和进程内解码缓存有潜力；C 与 D 在同一 IoTDB-FS 数据上对比，typed 直连快 1.85–4.39 倍，说明 adapter 绕过原文件 HDF5 元数据和 VFD 读取链路有实际收益。但 E 仍比 A 慢 29–97 倍，甚至慢于 C。A/C 每次重新打开 HDF5 文件，B/D 在单个进程内复用 QueryModule；加上语言实现差异，B/A 倍数只作机制线索，不能单独作为系统收益结论。

稳态 E 的约 121 ms 被分成约 60–62 ms 注册、约 60–61 ms IPC 文件读取。进一步计时显示 IPC 读取中约 60 ms 花在 FUSE 路径的 `memory_map` 打开，Arrow stream 解析仅 0.3–0.5 ms。服务器注册 profile 的稳态中位数是 60.0 ms；每次约 13 个 IoTDB 查询，其中 9 个元数据查询耗时约 40.2 ms。即使 20 次稳态查询的 decoded-fragment cache 全命中、payload 读取为零，每次仍读取 4–5 次 layout，约 68–82 KB，layout 后端耗时约 14.0 ms。`typed_prepare` 约 23.3 ms，`typed_ipc_encode` 约 0.15 ms。阶段计时有嵌套，不可相加；原始计数见 [steady-profile.jsonl](../results/hdf5-geos-pilot-20261006/steady-profile.jsonl)。

上述 E 使用 `attr_timeout=0`，它也禁用了现有 typed 路径权限缓存。相同数据和六轮查询改用 `attr_timeout=1,entry_timeout=1` 后，E 的稳态中位数降至 71–76 ms，`memory_map` 打开降至约 12 ms，注册仍约 59–64 ms。现有缓存仅覆盖同一内容寻址 route 的短时 FUSE `getattr/access/open` 授权，不能跨每次新注册的 route 复用，也不缓存注册阶段的 catalog、inode 或 HDF5 layout。[稳态复测](../results/hdf5-geos-pilot-20261006/steady-attr1.json)和[profile](../results/hdf5-geos-pilot-20261006/steady-attr1-profile.jsonl)保留了这项参数消融。因此前述 120 ms 是禁用属性缓存的隔离测量，不代表默认挂载的固定成本。

稳态单点的 **9 次元数据 SQL** 可以精确拆成：socket 注册时扫描一次完整已发布版本 catalog；注册授权读取一次精确 source inode 和两个父目录 inode；执行准备再次扫描完整 catalog、再按精确版本读取一次 manifest；结果物化再次读取 source 和两个父目录 inode 做授权。即 `1+3+1+1+3=9`。另 **4 次 layout SQL** 是目录 header/正文两次、目标 chunk 索引页一次、execute 的 70 字节身份 header 复核一次；跨两个索引页的层切片为 5 次 layout SQL。原版查询重复读取目录，decoded-fragment cache 不覆盖这些元数据。最直接的改进是同一次注册共享 catalog 与授权结果、对精确 source/version 做定向查询，并用受限、可失效的缓存保存已验证的 manifest 和 layout。权限元数据须维持短 TTL 和本进程 mutation 失效，跨进程写入还需要版本/权限状态复核，不能无期限缓存授权。

首轮四种切片总共只需 20 个互不相同的 `/T` chunk，payload 约 240 KB；查询 runtime 将缺失 chunk 合并成 3 次 payload 请求、11 个物理范围。第 2–6 轮没有再读取或解码 payload。这验证了 chunk 选择和共享缓存生效，也说明当前瓶颈不是数据字节量、DEFLATE 或 IPC 编码。相反，layout 每次重读、重复目录/权限元数据访问，以及新结果路径的打开成本，在小切片上压倒了局部读取收益。

后续已在同一实验数据库完成[GEOS 语义剪枝与 GRIB 同入口调用对照](hdf5-geos-semantic-grib-control.md)：经属性和 dimension-scale 验证的变量、时间、压力层、空间条件可映射到 1–4 个 `/T` chunk；两种格式稳态都支付 9 次通用元数据 SQL，HDF5 另有 4 次 layout SQL，而 GRIB 的解析索引缓存使其暖态 layout SQL 为零。该对照进一步区分了格式剪枝收益与通用注册成本。

属性目录试点只含 **1 个文件**。`standard_name` 谓词正确返回 1 个精确 source/version，IoTDB 侧耗时 110.0 ms、7 个 SQL 查询、读取 14.7 KB 目录数据；原生扫描该文件 18 个 dataset 属性的 warm 中位数为 2.624 ms。现阶段没有跨文件目录性能优势证据；只有在大量候选文件、少量命中时，持久目录索引才可能摊销该固定成本。后续应增加真实连续时次的多文件负载，再衡量目录选择性、索引更新和查询延迟。

优先处理的查询成本是：按精确 source/version 复用已验证的 layout directory/page；减少每次注册中的重复 source/parent 授权与目录查询；复用不可变查询声明。正常属性缓存下 FUSE 结果首次 `open` 仍约 12 ms，可继续核查权限检查与路由获取。这些改进都应保持版本与 namespace 隔离、访问权限和结果内容寻址。仅增大 chunk cache 或提高 payload 并行度，对这里的稳态延迟预计作用很小，因为 payload 已为零。

原始证据保留在 [pilot.json](../results/hdf5-geos-pilot-20261006/pilot.json)、[steady.json](../results/hdf5-geos-pilot-20261006/steady.json)、[passthrough-6.json](../results/hdf5-geos-pilot-20261006/passthrough-6.json)、[B-raw-6.jsonl](../results/hdf5-geos-pilot-20261006/B-raw-6.jsonl)、[D-raw-6.jsonl](../results/hdf5-geos-pilot-20261006/D-raw-6.jsonl)、[server-profile.jsonl](../results/hdf5-geos-pilot-20261006/server-profile.jsonl) 与 [native-catalog.json](../results/hdf5-geos-pilot-20261006/native-catalog.json)。试点脚本位于 `tests/benchmarks/hdf5_query/geos_pilot*.py` 和 `geos_pilot_direct.cpp`。服务器上的原文件、发布版本和报告仍在独立试点目录；所有试点挂载均已卸载。
