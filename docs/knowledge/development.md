# 本地开发与 Web 启停

开发环境直接复用 `packaging/runtime/start_web.sh` 和 `stop_web.sh`。它们可以通过环境变量使用构建目录中的程序，无需发布包，也无需维护另一套启停脚本。

## 构建

在仓库根目录执行：

```bash
IOTDB_DEV_REPO="$(pwd -P)"
cmake -S "$IOTDB_DEV_REPO" -B "$IOTDB_DEV_REPO/build" \
  -DBUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build "$IOTDB_DEV_REPO/build" --target iotdb_fs_web -j4
```

程序生成在 `build/bin/iotdb_fs_web`。虽然这里只构建 Web 程序，项目配置阶段仍会检查 FUSE、ecCodes 和 IoTDB C++ 客户端依赖；依赖准备见[快速开始](user_manual/02-quick-start.md)。运行 Web 程序浏览普通目录不需要启动 IoTDB。

## 启动一个开发实例

以下示例仍在仓库根目录执行，使用独立临时目录保存配置、日志和进程记录。先选择空闲端口；示例使用 `18080`。

```bash
IOTDB_DEV_REPO="$(pwd -P)"
IOTDB_DEV_STATE="$(mktemp -d /tmp/iotdbfs-web-dev.XXXXXX)"
cp "$IOTDB_DEV_REPO/resources/config.properties" "$IOTDB_DEV_STATE/config.properties"
mkdir -p "$IOTDB_DEV_STATE/browse"
printf 'Hello from the development Web service\n' > "$IOTDB_DEV_STATE/browse/hello.txt"

export IOTDB_CONFIG_FILE="$IOTDB_DEV_STATE/config.properties"
export IOTDB_FS_WEB_BIN="$IOTDB_DEV_REPO/build/bin/iotdb_fs_web"
export IOTDB_FS_RUN_DIR="$IOTDB_DEV_STATE/run-18080"
export IOTDB_LOG_DIR="$IOTDB_DEV_STATE/logs-18080"
export IOTDB_WEB_HOST=127.0.0.1
export IOTDB_WEB_PORT=18080
export IOTDB_MOUNT_POINT="$IOTDB_DEV_STATE/browse"

bash "$IOTDB_DEV_REPO/packaging/runtime/start_web.sh" "$IOTDB_MOUNT_POINT"
```

`IOTDB_CONFIG_FILE`、`IOTDB_FS_WEB_BIN`、`IOTDB_FS_RUN_DIR` 和 `IOTDB_LOG_DIR` 应使用绝对路径。脚本默认按发布包布局查找文件，直接在源码目录中使用默认值会指向 `packaging/` 下的位置。

若使用已有的其他构建目录，例如 `build-merge-baseline`，将 `IOTDB_FS_WEB_BIN` 改为该目录中 `bin/iotdb_fs_web` 的绝对路径即可。

启动成功后，在浏览器打开 `http://127.0.0.1:18080/`，或检查首页、目录列表和文件内容：

```bash
curl -fsS -o /dev/null "http://$IOTDB_WEB_HOST:$IOTDB_WEB_PORT/"
curl -fsS "http://$IOTDB_WEB_HOST:$IOTDB_WEB_PORT/api/list?path=/"
curl -fsS "http://$IOTDB_WEB_HOST:$IOTDB_WEB_PORT/api/download?path=/hello.txt"
```

这些操作检查 Web 服务和普通目录访问；IoTDB/FUSE 联合验证需使用实际挂载点，见[集成测试说明](../../tests/integration/README.md)。Web 脚本本身不会启动 IoTDB 或挂载文件系统。

## 停止、重复启动和重启

在保留上述环境变量的终端中执行：

```bash
# 已运行时重复启动会报告原有 PID，不会创建第二个进程。
bash "$IOTDB_DEV_REPO/packaging/runtime/start_web.sh" "$IOTDB_MOUNT_POINT"

# 停止该运行目录记录的实例。
bash "$IOTDB_DEV_REPO/packaging/runtime/stop_web.sh"

# 重新启动；省略位置参数时使用 IOTDB_MOUNT_POINT。
bash "$IOTDB_DEV_REPO/packaging/runtime/start_web.sh"

# 开发结束后停止。
bash "$IOTDB_DEV_REPO/packaging/runtime/stop_web.sh"
```

进程记录位于 `$IOTDB_FS_RUN_DIR/iotdb_fs_web.pid`，输出日志位于 `$IOTDB_LOG_DIR/iotdb_fs_web.out`。再次停止已停止的实例会正常返回。若换用新终端，需恢复对应的环境变量，尤其是 `IOTDB_FS_RUN_DIR`，才能操作原实例。改变端口、根目录或程序路径后，应先停止原实例再启动。

## 多个端口

实例通过运行目录中的 PID 文件区分，**每个实例必须使用不同的 `IOTDB_FS_RUN_DIR`**；仅改变端口不足以启动第二个实例。建议同时使用独立日志目录。保留上文环境变量时，可以在另一个空闲端口启动：

```bash
IOTDB_WEB_PORT=18081 \
IOTDB_FS_RUN_DIR="$IOTDB_DEV_STATE/run-18081" \
IOTDB_LOG_DIR="$IOTDB_DEV_STATE/logs-18081" \
  bash "$IOTDB_DEV_REPO/packaging/runtime/start_web.sh" "$IOTDB_MOUNT_POINT"

curl -fsS -o /dev/null "http://127.0.0.1:18081/"

IOTDB_FS_RUN_DIR="$IOTDB_DEV_STATE/run-18081" \
  bash "$IOTDB_DEV_REPO/packaging/runtime/stop_web.sh"
```

上述临时赋值不会改变终端中 `18080` 实例的环境变量。

## 配置行为与限制

以下优先级针对 `start_web.sh`，脚本会把最终值作为命令行参数传给程序：

| 设置 | 取值顺序 |
| --- | --- |
| 浏览根目录 | 第一个位置参数 → `IOTDB_MOUNT_POINT` → 配置中的 `mount.point` → `/mnt/dfs` |
| 监听地址 | `IOTDB_WEB_HOST` → 配置中的 `web.host` → `127.0.0.1` |
| 监听端口 | `IOTDB_WEB_PORT` → 配置中的 `web.port` → `8080` |
| 日志目录 | `IOTDB_LOG_DIR` → 配置中的 `log.dir` → 默认日志目录 |

脚本支持向程序继续传递参数，例如 `start_web.sh "$IOTDB_MOUNT_POINT" --workers 2`。显式追加的 `--root`、`--host` 或 `--port` 由程序最后处理，但脚本的启动探测仍使用脚本解析出的地址和端口；地址和端口应通过上表的环境变量设置。

`IOTDB_WEB_ROOT` 不受支持；应使用位置参数或 `IOTDB_MOUNT_POINT`。当前服务监听 IPv4 地址。示例绑定本机回环地址，页面上传、创建和删除操作直接作用于浏览根目录。

Web 服务默认仅监听 `127.0.0.1`。绑定非回环地址（例如 `0.0.0.0`）必须配置
`web.auth_token`、`IOTDB_WEB_AUTH_TOKEN` 或 `--auth-token`，并会要求所有请求携带
`Authorization: Bearer <token>`。回环监听下，写操作在配置 token 时需要认证；带有
`Origin` 的跨源写请求会被拒绝。也可以设置 `web.require_auth_all=true`（或
`IOTDB_WEB_REQUIRE_AUTH_ALL=true`、`--require-auth-all true`）让回环地址的读取请求也需要认证。

请求资源上限可通过配置、环境变量或 CLI 覆盖：`web.max_header_bytes` /
`IOTDB_WEB_MAX_HEADER_BYTES` / `--max-header-bytes`、`web.max_body_bytes` /
`IOTDB_WEB_MAX_BODY_BYTES` / `--max-body-bytes`、`web.max_upload_bytes` /
`IOTDB_WEB_MAX_UPLOAD_BYTES` / `--max-upload-bytes`、`web.socket_timeout_ms` /
`IOTDB_WEB_SOCKET_TIMEOUT_MS` / `--socket-timeout-ms` 和 `web.max_queue` /
`IOTDB_WEB_MAX_QUEUE` / `--max-queue`。上传必须带 `Content-Length`，服务端先写入目标目录
内的临时文件并在同步后原子发布；符号链接目标不会被覆盖。

启停脚本需要 Bash 和常用 Unix 工具；安装 `curl` 后，启动脚本会探测 HTTP 响应，缺少 `curl` 时会跳过该探测。停止脚本发送 SIGTERM 并等待最多约 10 秒，不会强制结束进程。容器中应由 init 或父进程回收退出的子进程；未回收的僵尸进程可能使脚本仍认为 PID 存活。

## 已执行验证

2026-09-11 使用 `build-merge-baseline/bin/iotdb_fs_web`、独立临时目录和两个空闲端口完成验证：位置参数与 `IOTDB_MOUNT_POINT` 均可指定根目录；首页、目录列表和下载均返回 HTTP 200，下载内容正确；重复启动保留同一个 PID；停止后可重新启动；停止其中一个实例不影响另一个；重复停止正常返回。验证只启动 Web 进程，结束后已停止并回收这些进程，未启动 IoTDB。这是启停与普通目录冒烟验证，不替代 IoTDB/FUSE 集成测试。
### Configuration-driven metadata

Extended GRIB metadata is declared in `resources/metadata.properties`, referenced
by `metadata.config` in the main properties file. Each field has a stable id,
ecCodes source key, scalar type, and optional xattr/query flags. The writer
extracts configured and canonical values from every message, de-duplicates
them into typed sets, and stores the file summary as a canonical JSON document
in the inode `metadata` field. Integer `level` and `step` summaries also expose
scalar minimum and maximum fields. Set members used for cross-file selection
are written to `blob_metadata_set` for exact equality pruning; GRIB fields are
not fixed inode columns. Configured fields are exposed through their configured
xattrs and the Web metadata API. Configuration errors fail startup, and
malformed persisted documents are ignored rather than fabricated.

The shipped [`resources/metadata.properties`](../../resources/metadata.properties)
file is an executable example. Field identifiers must start with an ASCII
letter and may contain letters, digits, and underscores (up to 64 characters).
Each field declares an ecCodes `source` key and one scalar `type`: `string`,
`int64`, `double`, or `bool`. `aliases` is an optional comma-separated list of
up to eight alternate source keys. A `default` is used only when all source
keys are absent or invalid while extracting a new GRIB file. `scale` applies
only to `double` fields. `required=true` rejects the write when no valid value
or default exists.

An `xattr=user.*` mapping exposes a value through the filesystem extended
attribute API. Xattr names are unique, and names under `user.grib.index.*` are
reserved for the built-in GRIB index. `queryable=true` requires an xattr and
allows equality searches by field id or alias; comparisons are case
insensitive for strings and type-aware for numeric and boolean values. Unknown
fields in persisted JSON are retained by the decoder but are intentionally
omitted from xattr, Web, and query projections until the field is configured.

The main properties file may point at another schema with `metadata.config`;
relative paths are resolved next to that file. `IOTDB_METADATA_CONFIG_FILE`
overrides both the property and the sibling `metadata.properties` fallback.
Configuration is loaded at process startup. Components that need a live reload
must use `MetadataConfigStore::reload`: a successful reload publishes a new
immutable generation, while a failed reload leaves the previous snapshot and
generation untouched. Persisted metadata is always read as bounded typed JSON;
empty metadata is an empty document, and malformed or oversized documents are
rejected by row parsing and ignored by presentation layers. This clean schema
cutover requires existing files to be re-ingested.
