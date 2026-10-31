# Web 文件管理器

## 9. Web 文件管理器

### 9.1 启动 Web 服务

Web 文件管理器可浏览挂载目录：

```bash
./build/bin/iotdb_fs_web
```

默认读取配置中的：

```properties
mount.point=/mnt/dfs
web.host=127.0.0.1
web.port=8080
```

访问：

```text
http://127.0.0.1:8080/
```

服务默认只监听回环地址。若需要让局域网访问，配置 `web.auth_token`（或设置
`IOTDB_WEB_AUTH_TOKEN`、传入 `--auth-token`）；非回环监听会拒绝无 token 启动，所有请求
都必须带 `Authorization: Bearer <token>`。回环监听下，配置 token 后上传、新建目录和删除
需要认证；带 `Origin` 的跨源写请求会返回 403。`web.require_auth_all=true` 可让读取请求
也需要认证。

上传接口要求 `Content-Length`，受 `web.max_upload_bytes` 限制，服务端使用同目录临时文件、
`fsync` 和原子发布；目标为符号链接时返回 409，避免覆盖链接指向的文件。请求头、请求体、
socket 超时和待处理连接队列分别由 `web.max_header_bytes`、`web.max_body_bytes`、
`web.socket_timeout_ms` 和 `web.max_queue` 控制。

### 9.2 浏览目录与文件

页面提供类似文件资源管理器的目录浏览能力。用户可以进入目录、返回上级路径，并通过面包屑查看当前位置。

### 9.3 页面使用操作说明

打开 Web 首页后，页面默认展示 `--root` 或 `mount.point` 指向的根目录。页面上方显示当前浏览根目录、文件和目录数量、当前路径、已选择项以及排序状态；中间区域是文件列表；选中文件或目录后会在该条目下方展开操作面板。

#### 9.3.1 目录导航

- 单击面包屑中的 `Root` 或任一级目录，可直接跳转到对应路径。
- 双击目录条目，或选中目录后点击 `Open Folder`，进入该目录。
- 非根目录下会显示返回上级的目录项，可通过双击或 `Go Up` 返回上级目录。
- 点击 `Refresh` 会重新读取当前目录或当前元数据搜索结果。

#### 9.3.2 查找、隐藏文件与排序

- `Filter files in this folder` 只在当前列表内按文件名过滤，不会递归扫描子目录。
- `Show Hidden Files` 用于显示以 `.` 开头的隐藏文件和历史版本文件。
- 排序下拉框支持 `Name A-Z`、`Name Z-A`、`Recently Modified` 和 `Largest First`。目录会优先排在文件之前。
- `Search Metadata` 会在当前目录下递归搜索 GRIB 扩展元数据。输入格式为逗号分隔的 `key=value` 条件，例如：

```text
shortName=clwmr,typeOfLevel=isobaricInhPa
```

元数据搜索要求启用 GRIB 扩展，并且只匹配可读取扩展属性的 GRIB/GRIB2 文件。条件值按扩展属性的完整值精确匹配；消息级 `level=500` 这类条件更适合在 partial read 或跨文件查询入口中使用。搜索结果展示后，按钮会变为 `Clear Metadata Search`，点击可回到普通目录浏览。

#### 9.3.3 选择与详情面板

- 单击条目会选中该文件或目录，并展开详情操作面板；再次单击同一条目会取消选中。
- 每个条目前方的复选框用于多选。多选后，页面会按选择内容显示 `Query Selected`、`Download Selected` 或 `Delete Selected`。
- GRIB/GRIB2 文件会显示 `Extended Metadata` 面板，用于查看 `user.grib.*` 等扩展属性。较长字段可点击 `View All` 展开。

#### 9.3.4 上传文件与新建目录

- 点击 `Upload File` 可选择一个或多个本地文件上传到当前目录。
- 也可以把文件拖放到 `Contents` 区域上传。
- 上传同名文件会写入当前目录中的同名目标，具体覆盖或版本行为取决于底层挂载目录的文件系统语义。
- 点击 `New Folder`，输入目录名后点击 `Create`，可在当前目录创建子目录。
- 上传和新建目录完成后，页面会自动刷新当前目录。

#### 9.3.5 打开、预览与下载

- 双击文件，或选中文件后点击 `Open in New Window`，会打开文件查看器。
- 文件查看器展示基础信息、可读取的扩展元数据和内容预览。文本内容按文本展示；二进制内容以十六进制预览展示。
- 点击 `Download File` 或查看器中的 `Download Raw Result` 可下载原始文件。
- 选中多个文件后点击 `Download Selected`，会下载一个 `.tar` 归档；目录不能参与批量下载。

#### 9.3.6 GRIB Partial Read 与批量查询

GRIB/GRIB2 文件启用扩展后，会出现 `Partial Read` 操作。输入读取参数后，可以直接打开查询结果或下载查询结果。默认示例参数为：

```text
function=read&level=500&dtype=f64
```

多选 GRIB/GRIB2 文件后点击 `Query Selected`，可为每个文件设置 partial read 参数，并在一个批量结果页面中打开所有查询结果。批量结果页支持逐项下载，也支持 `Download All Results` 下载所有结果。

#### 9.3.7 删除文件和目录

- 选中单个文件或目录后，点击 `Delete File` 或 `Delete Folder` 删除。
- 多选后点击 `Delete Selected` 可批量删除。
- 删除目录时会删除目录及其所有内容。
- 根目录不能删除。删除操作不可撤销，执行前应确认目标路径。

### 9.4 页面能力汇总

Web 页面支持：

- 文件和目录列表浏览。
- 按名称、修改时间、大小排序。
- 文本文件预览。
- 文件下载。
- 在支持的页面区域上传文件到当前目录。
- 新建目录、删除文件或目录。
- GRIB/GRIB2 文件扩展元数据查看、Partial Read 和批量查询。

### 9.5 自定义监听地址、端口与根目录

可以通过命令行覆盖配置：

```bash
./build/bin/iotdb_fs_web --host 0.0.0.0 --port 8080 --root /mnt/dfs
```

可用选项：

| 选项 | 说明 |
| --- | --- |
| `--root PATH` | 浏览指定目录，而不是 `mount.point` |
| `--host HOST` | 监听地址，默认 `127.0.0.1` |
| `--port PORT` | 监听端口，默认 `8080` |
| `--workers N` | 请求处理线程数 |
| `--help` | 查看帮助 |

### 9.6 Web API 简要说明

Web 服务提供页面和若干 API。常用检查命令：

```bash
curl -sG 'http://127.0.0.1:8080/api/list' --data-urlencode 'path=/'
curl -sG 'http://127.0.0.1:8080/api/preview' --data-urlencode 'path=/hello.txt'
curl -sG 'http://127.0.0.1:8080/api/viewer-info' --data-urlencode 'path=/hello.txt'
```

API 主要用于 Web 页面自身，也可用于轻量自检。`preview` 和 `viewer-info` 需要传入已存在的文件路径；上例假设 `/mnt/dfs/hello.txt` 已存在。

常用 API：

| API | 方法 | 说明 |
| --- | --- | --- |
| `/api/list?path=/dir` | `GET` | 列出目录内容 |
| `/api/search?path=/dir&query=k=v` | `GET` | 递归搜索 GRIB 扩展元数据 |
| `/api/metadata?path=/file.grib2` | `GET` | 读取可展示的扩展属性 |
| `/api/viewer-info?path=/file` | `GET` | 读取查看器基础信息 |
| `/api/preview?path=/file` | `GET` | 读取预览内容 |
| `/api/download?path=/file` | `GET` | 下载单个文件或 partial read 结果 |
| `/api/download-batch?path0=/a&path1=/b` | `GET` | 下载多个文件为 `.tar` |
| `/api/upload?path=/dir&name=file` | `POST` | 上传文件到目录 |
| `/api/mkdir?path=/dir&name=subdir` | `POST` | 创建子目录 |
| `/api/delete?path=/file` | `POST` | 删除单个文件或目录 |
| `/api/delete-batch?path0=/a&path1=/b` | `POST` | 批量删除文件或目录 |

如果需要访问 partial read 结果，在 `preview`、`viewer-info`、`download` 或 `download-batch` 请求中追加 `read_query` 参数。

### 9.7 Web 使用限制

- Web 根目录必须是进程可访问的本地路径，通常是挂载点。
- 远程访问时应确认防火墙、安全组和监听地址。
- 大文件预览可能只适合下载，不适合直接展示。
- Web 服务不替代 FUSE 挂载；它是挂载目录上的浏览入口。
- 上传、新建目录和删除操作会直接作用于 `--root` 对应的目录，应避免把 Web 服务暴露到不可信网络。
- GRIB 扩展元数据搜索和 partial read 依赖 GRIB 扩展开关和底层扩展属性支持；普通文件不会显示这些 GRIB 专用操作。
### Extending metadata fields

Add fields to `resources/metadata.properties` and restart the service. For
example:

```properties
metadata.schema_version=1
metadata.field.experimentId.source=experimentId
metadata.field.experimentId.type=string
metadata.field.experimentId.xattr=user.weather.experimentId
metadata.field.experimentId.queryable=true
```

Configured fields are shown by the metadata API and can be searched only when
`queryable=true`. The values are retained in the inode metadata JSON alongside
the existing GRIB columns.

Field ids use letters, digits, and underscores and must begin with a letter.
The `source` key is read from ecCodes; `aliases` can list alternate keys, and
`default` supplies a value only when those keys are unavailable. Supported
types are `string`, `int64`, `double`, and `bool`; `scale` is valid for doubles
only. Mark a field `required=true` to reject a GRIB write when no value can be
extracted. An `xattr=user.*` mapping controls the extended attribute name;
queryable fields must define one. Searches accept `field=value` terms joined by
commas and may use the field id or an alias. String matching ignores case.

The schema is loaded when the service starts. Set `metadata.config` in the
main properties file to select a schema path relative to that file, or set
`IOTDB_METADATA_CONFIG_FILE` to override it. A bad configuration prevents the
metadata schema from being enabled and is reported in the startup log; it does
not replace a previously published snapshot when a `MetadataConfigStore`
reload fails. The cross-format schema requires existing files to be
re-ingested; there is no old-row fallback. Invalid or oversized metadata JSON
is ignored by the Web and xattr views and is never synthesized into attributes.
