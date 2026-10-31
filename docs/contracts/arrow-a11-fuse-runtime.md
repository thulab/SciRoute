# A11：FUSE typed 查询运行时

日期：2026-09-29。挂载进程在 FUSE `init`（后台派生完成后）构造一个长生命周期的 `QueryRuntime`、Arrow/GRIB typed adapters、IoTDB published-version catalog、结果存储和路由注册表。摄取、catalog 和查询使用同一个 `IoTDBStructuredBlobBackend` 实例；该实例的 namespace 是共享 decoded fragment cache 的边界。

## 注册与只读路径

独立客户端通过本地 Unix socket 提交完整 JSON 声明，服务端物化查询后返回挂载下的只读结果路径。默认 socket 使用当前用户拥有、权限为 `0700` 且可写的运行时目录，依次检查 `$XDG_RUNTIME_DIR`、`/run/user/<uid>`、`/run/iotdbfs/<uid>`，最后使用私有的 `/tmp/iotdbfs-<uid>`；文件名由挂载点稳定生成，实际路径写入挂载日志。`IOTDBFS_QUERY_SOCKET` 可显式覆盖。默认 socket 权限为 `0600`，只允许同 UID peer。共享挂载可显式指定可供客户端搜索的 socket 目录并设置 `IOTDBFS_QUERY_SOCKET_MODE=0666`；客户端要读取返回的 FUSE 路径，挂载还需允许跨用户访问（例如 `-o allow_other`）。服务端从 Unix socket 获取 peer 身份，逐源按该身份授权，且共享模式必须配置授权回调。Linux 上注册检查使用 peer 的实际补充组，无法确认时保守地只使用主组。若控制 socket 无法启动，挂载记录警告并保留普通文件访问，只有外部查询注册不可用。每条连接使用 4 字节网络序长度加 JSON 的单次请求/响应，帧上限 1 MiB；工作线程和等待队列分别有 4/16 的上限。C++ 客户端可以调用 `submit_query_registration(socket_path, json_request)`。客户端发送超时为 3 秒，响应读取超时默认为 5 分钟，可通过第三个 `std::chrono::milliseconds` 参数调整（1 毫秒至 24 小时）；窗口注册需先读取各 shard 的 layout 并构建覆盖索引。若客户端提前断开且响应发送失败，服务端会撤销该次注册与缓存结果。

同一路径的启动由持久的 `<socket>.lock` 文件串行化；进程退出或被强杀后，内核自动释放文件锁。若 `bind` 遇到旧 socket，daemon 仅在确认其属于当前有效 UID、没有监听者且路径 inode 未变化时删除旧 socket 并重试；活跃监听者、普通文件和符号链接不会被覆盖。锁文件保留在磁盘上供后续挂载复用。

普通查询的最小请求如下。物理 Arrow 子集必须把 `versions` 改为 `exact`，并提供 `subset.physical_array`；可以先用普通文件选择查询读出当前 `source.version` 与 `source.format`。

```json
{"kind":"typed","request":{"sources":{"paths":[{"kind":"exact","path":"/data.arrow"}],"versions":{"kind":"latest"}}}}
```

窗口请求包含 `kind=window`、`snapshot`（dataset_id、selector、精确版本的 shards、逻辑 rows）、`view`（变量、`L/G/H/S`、split、缺失值策略、变换）和有界 `request`（coordinates 或 anchors、max_windows、分页 continuation）。服务端先固定 snapshot、校验覆盖索引和视图，再注册不可变请求；省略 `view.snapshot_fingerprint` 时填入准备好的 fingerprint。窗口 route 只代表一个有界 sample group，分页仍由 continuation 驱动。具体可运行声明见 [socket 测试](../../tests/unit/query/frontend/test_QueryRegistrationSocket.cpp) 和 [真实挂载 smoke 脚本](../../tests/benchmarks/arrow_training/a11_mount_smoke.py)。

FUSE 合成 `/.iotdbfs/query/v1` 目录树，并处理注册得到的 `typed.ipc?id=…&result=…` / `windows.ipc?id=…&result=…` 文件。`result` 是完整已编码 IPC 字节的 SHA-256；相同字节复用结果，哈希冲突会在存储层拒绝。同一路径因此不会返回另一份字节。该命名空间保留为只读；普通文件和旧 GRIB `?function=` 路径继续沿原回调处理。挂载前若数据库里已存在 `/.iotdbfs`，初始化失败，避免遮蔽用户文件。

## stat/open/read 的一致性与权限

socket 注册时将一个 route 物化为完整 Arrow IPC stream，并保存精确 source path/version 列表与结果句柄，随后把结果 ID 加到返回路径。`stat`、`access` 和 `open` 只接受该 ID 与已发布结果匹配的路径，不会在旧路径重新执行查询；结果被驱逐或过期后旧路径返回 `ENOENT`。bridge 默认最多保留 64 条已物化 route，空闲 5 分钟后回收；frontend 注册项也有 5 分钟空闲 TTL。同一注册 ID 的等价路径共用一个 bridge 缓存项；等待正在物化的结果最多 30 秒，超时返回 `ETIMEDOUT`，不取消生产者。访问仍先解析注册项，再按当前调用者的 UID、主组及补充组检查每个精确源版本的读权限和去重后的父目录搜索权限。源与目录首次检查直接读 IoTDB，不持有全局命名空间锁；成功检查按调用者身份与结果路径缓存至多 1 秒（若 `attr_timeout` 更短则跟随缩短），本挂载的 inode 修改立即失效。socket 注册时按实际 peer 身份授权，FUSE 访问时再按当前调用者身份授权。结果路径不设置 `direct_io`，允许内核页缓存与 `mmap`；每个结果 ID 对应的字节和长度不可变。

`read` 只使用 `open` 固定的结果句柄和偏移，不再重新解析 latest 或访问源。已打开句柄即使 route/result 条目被淘汰仍可读；`release` 才释放它所持的内存预算。bridge 为 stat/open 保留的结果也计入默认 256 MiB 驻留预算。发布新结果遇到该缓存的内存压力时，bridge 按最近使用顺序释放无外部持有者的缓存句柄，并用同一份已编码字节重试发布；查询和编码不会重做，频繁访问导致的 TTL 延长也不能阻止预算驱逐。真正被 FUSE 打开句柄持有的结果仍不可回收，这些句柄占满预算时新结果返回 `ENOSPC`。未物化请求若源版本已删除，返回 `ESTALE`；已物化 route 的新访问也要重新授权，已打开句柄遵循 POSIX 打开文件语义。错误按 QueryStatus 类别转换成 `EINVAL`、`EACCES`、`ENOENT`、`ESTALE`、`ENOSPC` 等 errno，并在挂载日志中保留原始诊断消息。

## 验证

上述修复后，完整单元测试通过 **916/916**；socket 与 bridge 定向测试在 ASan/UBSan 下通过 **18/18**。隔离 FUSE 挂载下，PyArrow 21 的 `pa.memory_map` 成功打开并解码普通 typed 查询、物理 Arrow 查询和训练窗口 IPC。共享 `0666` socket 的端到端测试使用非 daemon UID：只有携带源文件所属补充组的客户端可以注册并打开结果，移除该组后注册与打开均遭拒绝。只读挂载点父目录的路径测试以非 root 用户通过；隔离 FUSE 挂载还验证了默认 socket 在指定的运行时目录创建，以及显式 socket 路径不可用时普通目录仍可读且有警告日志。下面的 901 项记录是初版 A11 集成验证结果。

实验服务器 `192.168.99.49` 的生产目标构建成功，完整单元测试 **901/901** 通过；socket 与 bridge 的 5 个定向测试通过 ASan/UBSan。独立 Python 客户端通过 socket 在隔离挂载中注册普通文件选择、Arrow 精确物理查询与训练窗口请求；PyArrow 21 成功解码 IPC，核对多变量值、三行训练窗口、`stat` 大小、双句柄偏移/EOF、源删除后已打开结果可读及未物化窗口失效。另一个独立进程注册相同物理查询并读得相同 IPC 后，挂载的 runtime 计数从 `hits=1, misses=2, decodes=1` 到 `hits=4, misses=2, decodes=1`；随后窗口查询到 `hits=6, misses=2, decodes=1`。这证明同一 mount 的不同客户端重用了已解码片段，没有增加 decode 次数。该脚本只创建并删除自己的临时 Arrow 源；测试挂载与 socket 已卸载/关闭。
