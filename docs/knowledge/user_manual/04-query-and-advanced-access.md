# 查询检索与高级扩展访问

## 7. 查询与检索

### 7.1 查询能力概览

IoTDB-FS-C 提供两类用户侧查询入口：

- `grep`：用自然语言描述目标文件或结构化子数据。
- `cross-file-client`：对多个文件执行条件过滤和子数据读取。

查询能力依赖 IoTDB 中已有的 inode 表、文件元数据和可用索引。

### 7.2 文件级检索

文件级检索返回匹配文件：

```bash
./build/bin/iotdb_fs grep --mode=file "全球降水" /mnt/dfs
```

也可以对多个目录检索：

```bash
./build/bin/iotdb_fs grep "10米风" gfs ecmwf
```

相对目录会按配置中的挂载点补全。

### 7.3 自然语言 grep

自然语言 grep 的用法是：

```bash
./build/bin/iotdb_fs grep [options] "自然语言描述" [folder ...]
```

示例：

```bash
./build/bin/iotdb_fs grep "500百帕温度场" /mnt/dfs
```

当前本地规划器优先识别变量、层次、时效和区域等语义，例如温度、湿度、风、位势高度、降水、海平面气压、`500hPa`、`10米`、`24小时预报`、`global` 等。

### 7.4 检索模式与常用选项

常用选项：

| 选项 | 说明 |
| --- | --- |
| `--mode=auto` | 自动判断返回文件还是子数据块 |
| `--mode=file` | 只返回文件 |
| `--mode=block` | 返回结构化子数据块 |
| `--limit=N` | 最大返回结果数，默认 `100` |
| `--scan-limit=N` | 最大扫描候选文件数，默认 `500` |
| `--debug` | 输出候选过滤和匹配原因 |
| `--debug-limit=N` | 调试样本上限，默认 `10` |
| `--all-versions` | 不做最新版本去重 |

### 7.5 调试检索计划

当查询没有结果时，先打开调试模式：

```bash
./build/bin/iotdb_fs grep --debug "500百帕温度场" /mnt/dfs
```

调试输出会展示候选过滤和拒绝原因，例如变量不匹配、层次不匹配或路径缺少关键词。常见调整方式包括扩大目录范围、增加 `--scan-limit`、改用 `--mode=file` 先确认文件是否能被检索到。

### 7.6 跨文件查询

跨文件客户端适合对多个文件执行结构化条件过滤：

```bash
./build/bin/iotdb_fs cross-file-client \
  --file-query=shortName=clwmr \
  --inner-query=level=500
```

常用选项：

| 选项 | 说明 |
| --- | --- |
| `--file-query=KV` | 文件级过滤，例如 `shortName=clwmr` |
| `--inner-query=KV` | 子数据级过滤，例如 `level=500` |
| `--short-name=NAME` | 等价于 `--file-query=shortName=NAME` |
| `--out-dir=DIR` | 输出目录；默认只打印大小 |
| `--max-files=N` | 限制处理文件数 |
| `--all-versions` | 不做最新版本去重 |

注意：当前跨文件候选加载主要依赖最新版本、可识别扩展名和可从 `--file-query` 提取的 `shortName` 等条件。源码中虽然保留了若干路径类参数解析，但当前用户手册不建议依赖这些参数过滤跨文件结果。

### 7.7 查询结果解释

grep 输出分为两类：

- `[FILE]`：返回匹配文件，通常包含挂载路径。
- `[BLOCK]`：返回结构化子数据块，通常包含可继续读取的 `query_path`。

`[BLOCK]` 的查询路径可能形如：

```text
/mnt/dfs/file.data?function=read&type=binary&offset=...
```

用户可以把它交给 `cat`、脚本或上层分析程序继续读取。

## 8. 高级扩展访问

### 8.1 扩展访问机制概览

高级扩展访问允许用户在普通文件路径后追加查询参数，从一个文件中读取结构化子数据。它的目标是让用户不必先导出完整文件，再用外部工具拆解，而是在文件系统路径上直接表达读取需求。

普通读取：

```bash
cat /mnt/dfs/show.data
```

扩展读取：

```bash
cat '/mnt/dfs/show.data?function=read&type=binary&offset=0'
```

扩展访问必须由对应数据格式的解析、索引和处理逻辑支持。

当前实现要求扩展路径中必须出现 `function=`；裸 `level=500&shortName=tmp` 这类查询会被拒绝。支持的函数包括 `read`、`crop`、`interp`。

### 8.2 function 链式调用模型

扩展路径使用 `function` 表达操作，并允许多个操作按顺序串联：

```text
file?function=read&... ?function=crop&... ?function=interp&...
```

实际路径中可以直接连续书写：

```bash
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64'
```

链式调用的含义是先读取目标子数据，再对读取结果执行后续处理。

### 8.3 二进制片段读取

二进制片段读取适合保留原始子块：

```bash
cat 'show.grib2?function=read&type=binary&level=500' | wc -c
```

如果查询结果已经给出 `offset`，可以按偏移读取：

```bash
cat '/mnt/dfs/file.grib2?function=read&type=binary&offset=123456' | wc -c
```

### 8.4 结构化子数据访问

结构化子数据访问使用条件参数定位目标数据，例如层次、变量名或其他索引字段：

```bash
cat 'show.grib2?function=read&level=500&dtype=f64' | wc -c
```

常见参数包括：

| 参数 | 含义 |
| --- | --- |
| `function=read` | 读取目标子数据 |
| `type=binary` | 返回原始二进制块 |
| `dtype=f32` | 返回 32 位浮点数值场 |
| `dtype=f64` | 返回 64 位浮点数值场 |
| `level=500` | 按层次过滤 |
| `shortName=tmp` | 按变量短名过滤 |
| `offset=N` | 按索引偏移定位 |

### 8.5 空间裁剪与插值类操作

空间裁剪示例：

```bash
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' | wc -c
```

裁剪后插值示例：

```bash
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' | wc -c
```

常见参数：

| 参数 | 含义 |
| --- | --- |
| `lat0`、`lon0` | 裁剪区域起点 |
| `lat1`、`lon1` | 裁剪区域终点 |
| `outNi`、`outNj` | 插值输出网格大小 |
| `interp=bilinear` | 插值方法；当前示例默认双线性 |

### 8.6 高级案例：GRIB 气象数据访问

GRIB 在本手册中是高级结构化访问的具体案例。系统主体仍是文件系统；GRIB 示例用于说明如何把一个复杂科学数据文件中的 message、层次、变量和网格区域映射成可查询、可读取的路径。

写入示例：

```bash
cp /root/demo-data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
ls -l /mnt/dfs/show.grib2
```

读取完整文件：

```bash
cat /mnt/dfs/show.grib2 | wc -c
```

读取指定层次的数值场：

```bash
cat '/mnt/dfs/show.grib2?function=read&level=500&dtype=f64' | wc -c
```

读取原始 message：

```bash
cat '/mnt/dfs/show.grib2?function=read&type=binary&level=500' | wc -c
```

### 8.7 高级案例：GRIB 子块检索

自然语言 grep 可以返回可直接读取的子块路径：

```bash
./build/bin/iotdb_fs grep "500百帕温度场" /mnt/dfs
```

返回的 `[BLOCK]` 结果中包含 `query_path`，用户可以直接读取：

```bash
cat '/mnt/dfs/show.grib2?function=read&type=binary&offset=123456' | wc -c
```

如果无结果，使用：

```bash
./build/bin/iotdb_fs grep --debug "500百帕温度场" /mnt/dfs
```

### 8.8 高级案例：跨 GRIB 文件分析

跨文件分析示例：

```bash
./build/bin/iotdb_fs cross-file-client \
  --file-query=shortName=clwmr \
  --inner-query=level=500
```

这个命令从文件级索引中筛选候选文件，再用子数据条件读取匹配内容。它展示的是“跨文件结构化访问”能力，GRIB 只是当前可运行的数据案例。

### 8.9 扩展访问的限制与注意事项

- 扩展访问依赖索引。未生成索引的数据只能按普通文件读取。
- 扩展访问依赖配置。当前高级案例可通过 `grib.extension.enabled` 控制。
- 不支持的 `function` 或 `type` 会失败。
- 查询条件过窄会导致无匹配结果。
- 大数据读取、裁剪和插值可能受缓存、内存和 IoTDB 连接性能影响。
