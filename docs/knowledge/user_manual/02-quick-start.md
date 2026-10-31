# 快速开始与安装运行

## 2. 快速开始

### 2.1 环境依赖

最小运行环境需要 FUSE、CMake、C++ 编译器和可访问的 IoTDB 服务。高级结构化访问案例可能需要 ecCodes 等库。

Ubuntu/Debian 类系统可先安装常用依赖：

```bash
sudo apt update
sudo apt install libfuse-dev libeccodes-dev cmake g++ pkg-config
```

如果只验证普通文件系统能力，`libeccodes-dev` 不是核心概念依赖；如果要运行当前仓库中的高级结构化数据示例，建议安装。

### 2.2 获取与构建项目

在项目根目录执行：

```bash
mkdir -p build
cmake -S . -B build
cmake --build build
```

常用运行产物位于 `build/bin/` 或 `build/` 下，具体取决于构建配置和目标。

### 2.3 准备配置文件

默认配置文件为：

```text
resources/config.properties
```

常用配置包括：

```properties
iotdb.host=127.0.0.1
iotdb.port=6667
iotdb.user=root
iotdb.pass=root
iotdb.db=dfs
mount.point=/mnt/dfs
log.dir=logs
log.level=INFO
web.host=127.0.0.1
web.port=8080
```

如果要使用其他配置文件，可以设置：

```bash
export IOTDB_CONFIG_FILE=/path/to/config.properties
```

如果只想改变配置目录，可以设置：

```bash
export IOTDB_CONF_DIR=/path/to/conf-dir
```

此时程序会读取 `$IOTDB_CONF_DIR/config.properties`。

### 2.4 启动基础服务

请先确保 IoTDB 服务可访问，并且配置中的 `iotdb.host`、`iotdb.port`、`iotdb.user`、`iotdb.pass`、`iotdb.db` 与实际环境一致。

开发环境也可以使用仓库脚本作为挂载入口：

```bash
bash scripts/dev/start.sh
```

这个脚本会读取配置、准备挂载点，然后直接启动 `iotdb_fs`。如果使用它，就不要再重复执行第 2.5 节中的手工挂载命令。

停止已挂载的开发环境：

```bash
bash scripts/dev/stop.sh
```

清理开发环境：

```bash
bash scripts/dev/clean.sh
```

这些脚本会读取 `resources/config.properties` 中的挂载点等配置。

### 2.5 挂载文件系统

先创建挂载目录：

```bash
mkdir -p /mnt/dfs
```

以前台调试模式启动：

```bash
./build/bin/iotdb_fs -f -d /mnt/dfs
```

也可以不传挂载点，让程序读取配置中的 `mount.point`：

```bash
./build/bin/iotdb_fs
```

后台运行时可省略 `-f -d`：

```bash
./build/bin/iotdb_fs /mnt/dfs
```

### 2.6 写入并读取第一个文件

挂载成功后，在另一个终端执行：

```bash
echo "hello iotdb-fs" > /mnt/dfs/hello.txt
cat /mnt/dfs/hello.txt
ls -l /mnt/dfs
```

如果能看到写入内容，说明基本文件读写链路已经跑通。

### 2.7 卸载文件系统

使用 FUSE 卸载命令：

```bash
fusermount -u /mnt/dfs
```

如果提示目录忙，先关闭正在访问 `/mnt/dfs` 的 shell、编辑器、Web 服务或测试程序，再重试。

## 4. 安装、配置与运行

### 4.1 软件依赖

基础依赖：

- CMake 和 C++17 编译器。
- FUSE 开发库。
- 可访问的 IoTDB 服务。
- 项目所需第三方静态依赖。

高级结构化数据示例依赖：

- ecCodes：用于当前 GRIB 高级案例。
- CDO：用于生成或处理某些示例数据时可能有用。

安装示例：

```bash
sudo apt install libfuse-dev libeccodes-dev cdo
```

### 4.2 配置文件说明

程序按以下优先级定位配置文件：

1. `IOTDB_CONFIG_FILE` 指向的完整文件路径。
2. `$IOTDB_CONF_DIR/config.properties`。
3. `resources/config.properties`。

IoTDB 连接：

| 配置项 | 含义 | 默认示例 |
| --- | --- | --- |
| `iotdb.host` | IoTDB 地址 | `127.0.0.1` |
| `iotdb.port` | IoTDB 端口 | `6667` |
| `iotdb.user` | 用户名 | `root` |
| `iotdb.pass` | 密码 | `root` |
| `iotdb.db` | 数据库 | `dfs` |
| `iotdb.use_prepared` | 是否使用 prepared 模式 | `0` |
| `iotdb.pool_size` | 连接池大小提示 | `16` |

运行与挂载：

| 配置项 | 含义 | 默认示例 |
| --- | --- | --- |
| `mount.point` | 默认挂载目录 | `/mnt/dfs` |
| `log.dir` | 日志目录 | `logs` |
| `log.level` | 日志级别 | `INFO` |
| `web.host` | Web 默认监听地址 | `127.0.0.1` |
| `web.port` | Web 默认端口 | `8080` |

缓存与写入：

| 配置项 | 含义 |
| --- | --- |
| `cache.decoded.max_bytes` | 解码数据缓存上限 |
| `cache.read.chunk_size` | 读取缓存块大小 |
| `cache.read.full_prefetch_max_bytes` | 全量预取上限 |
| `cache.raw_file.max_bytes` | 原始文件缓存上限 |
| `writer.db_request_max_bytes` | 单次写入数据库请求上限 |
| `inode.cache.capacity` | inode 缓存容量 |
| `inode.cache.flush_interval_ms` | inode 缓存刷新间隔 |
| `grib.extension.enabled` | 是否启用当前高级结构化访问案例 |
| `grib.message_cache.max_entries` | 当前高级案例的消息缓存数量 |

### 4.3 启动脚本

开发脚本位于 `scripts/dev/`：

```bash
bash scripts/dev/start.sh
bash scripts/dev/stop.sh
bash scripts/dev/clean.sh
```

`start.sh` 会读取配置并准备挂载点，然后通过 `exec` 启动 `iotdb_fs`。它本身就是一种启动挂载方式，不是单纯的后台基础服务启动器；使用它之后不要再对同一挂载点重复运行 `iotdb_fs`。`stop.sh` 用于停止相关进程和卸载；`clean.sh` 用于清理开发环境。脚本适合本地调试，生产部署应结合发布包和系统服务管理。

### 4.4 命令行参数

`iotdb_fs` 的第一类用法是启动 FUSE 文件系统：

```bash
./build/bin/iotdb_fs /mnt/dfs
./build/bin/iotdb_fs -f -d /mnt/dfs
./build/bin/iotdb_fs -o default_permissions /mnt/dfs
```

第二类用法是子命令：

```bash
./build/bin/iotdb_fs grep "500百帕温度场" /mnt/dfs
./build/bin/iotdb_fs cross-file-client --file-query=shortName=clwmr --inner-query=level=500
```

如果未传挂载点，程序使用配置中的 `mount.point`，并默认追加 FUSE `kernel_cache` 选项。

### 4.5 日志级别与调试开关

配置文件可设置：

```properties
log.dir=logs
log.level=INFO
```

也可以使用环境变量覆盖：

```bash
export IOTDB_LOG_DIR=logs
export IOTDB_LOG_LEVEL=DEBUG
```

如果设置 `IOTDB_DEBUG`，且未显式设置日志级别，程序会使用 `DEBUG`。排查挂载、连接、查询和扩展访问问题时，建议先切到 `DEBUG`。

### 4.6 常见部署目录

仓库约定目录：

| 路径 | 用途 |
| --- | --- |
| `include/iotdbfs/` | 项目头文件 |
| `src/` | 源文件和可执行程序入口 |
| `resources/` | 默认配置 |
| `tests/unit/` | 单元测试 |
| `tests/integration/` | 依赖环境的集成测试 |
| `tests/performance/` | 性能测试 |
| `packaging/` | 发布包构建、校验和运行时脚本 |
| `scripts/dev/` | 本地开发脚本 |
| `docs/` | 文档 |
