# 参考资料

## 14. 参考资料

### 14.1 项目目录结构

```text
include/iotdbfs/     项目头文件
src/                 源文件与可执行程序入口
third_party/         第三方库与静态依赖
build/               CMake 构建目录
resources/           默认配置和资源文件
tests/unit/          单元测试
tests/integration/   集成测试和手工测试
tests/performance/   性能测试程序与脚本
packaging/           发布包构建、校验与运行时脚本
scripts/dev/         本地开发启动、停止、清理脚本
docs/                文档
```

### 14.2 常用命令速查

| 任务 | 命令 |
| --- | --- |
| 构建 | `cmake -S . -B build && cmake --build build` |
| 前台挂载 | `./build/bin/iotdb_fs -f -d /mnt/dfs` |
| 后台挂载 | `./build/bin/iotdb_fs /mnt/dfs` |
| 卸载 | `fusermount -u /mnt/dfs` |
| 自然语言检索 | `./build/bin/iotdb_fs grep "500百帕温度场" /mnt/dfs` |
| 跨文件查询 | `./build/bin/iotdb_fs cross-file-client --file-query=shortName=clwmr --inner-query=level=500` |
| Web 浏览 | `./build/bin/iotdb_fs_web --root /mnt/dfs --port 8080` |
| 单元测试 | `ctest --test-dir build --output-on-failure` |

### 14.3 配置项速查

| 配置项 | 说明 |
| --- | --- |
| `iotdb.host` | IoTDB 地址 |
| `iotdb.port` | IoTDB 端口 |
| `iotdb.user` | IoTDB 用户 |
| `iotdb.pass` | IoTDB 密码 |
| `iotdb.db` | IoTDB 数据库 |
| `iotdb.pool_size` | 连接池大小 |
| `mount.point` | 默认挂载点 |
| `log.dir` | 日志目录 |
| `log.level` | 日志级别 |
| `web.host` | Web 监听地址 |
| `web.port` | Web 监听端口 |
| `cache.decoded.max_bytes` | 解码缓存上限 |
| `cache.read.chunk_size` | 读取缓存块大小 |
| `cache.read.full_prefetch_max_bytes` | 全量预取上限 |
| `cache.raw_file.max_bytes` | 原始文件缓存上限 |
| `writer.db_request_max_bytes` | 写入请求上限 |
| `inode.cache.capacity` | inode 缓存容量 |
| `inode.cache.flush_interval_ms` | inode 缓存刷新间隔 |
| `grib.extension.enabled` | 当前高级结构化访问案例开关 |
| `grib.message_cache.max_entries` | 当前高级案例消息缓存数量 |

### 14.4 查询参数速查

| 参数 | 说明 |
| --- | --- |
| `--mode=auto|file|block` | 自动、文件或子数据块模式 |
| `--limit=N` | 最大返回结果数 |
| `--scan-limit=N` | 最大扫描候选文件数 |
| `--debug` | 输出调试信息 |
| `--debug-limit=N` | 调试样本上限 |
| `--all-versions` | 包含历史版本 |
| `--file-query=KV` | 跨文件客户端的文件级过滤 |
| `--inner-query=KV` | 跨文件客户端的子数据过滤 |
| `--out-dir=DIR` | 输出目录 |

### 14.5 扩展访问参数速查

| 参数 | 说明 |
| --- | --- |
| `function=read` | 读取子数据 |
| `function=crop` | 对读取结果裁剪 |
| `function=interp` | 对读取结果插值 |
| `type=binary` | 返回原始二进制块 |
| `dtype=f32` | 返回 32 位浮点数值 |
| `dtype=f64` | 返回 64 位浮点数值 |
| `offset=N` | 按偏移读取 |
| `level=500` | 按层次过滤 |
| `shortName=tmp` | 按变量短名过滤 |
| `lat0`、`lon0`、`lat1`、`lon1` | 空间裁剪范围 |
| `outNi`、`outNj` | 插值输出网格大小 |

### 14.6 相关脚本说明

| 脚本 | 用途 |
| --- | --- |
| `scripts/dev/start.sh` | 本地开发启动 |
| `scripts/dev/stop.sh` | 本地开发停止和卸载 |
| `scripts/dev/clean.sh` | 清理开发环境 |
| `packaging/current/build_release.sh` | 构建当前系统发布包 |
| `packaging/current/verify_release.sh` | 校验当前系统发布包 |
| `packaging/kylin/build_release.sh` | 构建 Kylin 静态发布包 |
| `packaging/kylin/verify_release.sh` | 校验 Kylin 静态发布包 |
| `packaging/runtime/self_check.sh` | 发布包运行时自检 |
| `tests/integration/web_api_smoke.sh` | Web API 烟测 |
| `tests/performance/scripts/build_perf_test.sh` | 构建性能测试程序 |
| `tests/performance/scripts/test.sh` | 运行性能测试序列 |

### 14.7 术语表

| 术语 | 说明 |
| --- | --- |
| FUSE | 用户态文件系统框架 |
| IoTDB | 用于存储元数据和索引的时序数据库 |
| 挂载点 | 用户访问文件系统的本地目录 |
| inode | 文件或目录的元数据记录 |
| 最新视图 | 默认展示每个路径最新版本的视图 |
| 历史视图 | 可回溯覆盖历史的视图 |
| 扩展路径 | 在文件路径后追加查询参数的访问形式 |
| 结构化子数据 | 大文件中可按条件读取的局部数据 |
| function 链 | 多个扩展访问操作按顺序串联 |
| 自然语言 grep | 将自然语言描述转换为检索计划的查询入口 |
