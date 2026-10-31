# 基础文件操作与版本管理

## 5. 基础文件操作

### 5.1 创建文件和目录

```bash
mkdir -p /mnt/dfs/demo
echo "normal file" > /mnt/dfs/demo/a.txt
ls -l /mnt/dfs/demo
```

### 5.2 读取文件

```bash
cat /mnt/dfs/demo/a.txt
head -c 4 /mnt/dfs/demo/a.txt
```

### 5.3 覆盖写入

```bash
echo "first version" > /mnt/dfs/demo/versioned.txt
echo "second version" > /mnt/dfs/demo/versioned.txt
cat /mnt/dfs/demo/versioned.txt
```

覆盖后，默认读取最新内容；历史版本信息由元数据和版本管理能力维护。

### 5.4 删除文件和目录

```bash
rm /mnt/dfs/demo/a.txt
rm /mnt/dfs/demo/versioned.txt
rmdir /mnt/dfs/demo
```

删除目录前请确保目录为空。

### 5.5 重命名

```bash
echo "rename me" > /mnt/dfs/old-name.txt
mv /mnt/dfs/old-name.txt /mnt/dfs/new-name.txt
ls -l /mnt/dfs/new-name.txt
```

### 5.6 查看文件属性

```bash
ls -l /mnt/dfs/new-name.txt
getfattr -d -m - /mnt/dfs/new-name.txt
```

如果系统未安装 `getfattr`，可安装 `attr` 包。

### 5.7 常见 POSIX 操作示例

```bash
cd /mnt/dfs
echo "hello world" > a.txt
cat a.txt
echo "random data" > b.bin
head -c 4 b.bin
mv b.bin c.bin
rm c.bin
ls
```

这些命令可作为挂载成功后的最小手工验证清单。

## 6. 元数据、索引与版本管理

### 6.1 元数据存储方式

IoTDB-FS-C 使用 IoTDB 管理文件系统元数据和索引信息。元数据支持目录遍历、属性读取、查询过滤和版本可见性判断。

部署时请确保 IoTDB 连接配置正确。如果 IoTDB 不可访问，挂载、查询或写入可能失败。

### 6.2 文件索引机制

普通文件会记录基础路径和属性信息。支持结构化索引的数据可以额外写入子数据索引，用于后续自然语言检索、跨文件查询或扩展路径访问。

索引不是用户手工维护的独立文件；它由写入流程和扩展模块生成。查询无结果时，常见原因是目标文件未写入索引、检索条件过窄或扫描上限过低。

### 6.3 文件覆盖与版本生成

当同一路径被覆盖写入时，系统可以记录新的版本。默认文件系统视图优先展示最新版本，这让挂载目录看起来像普通文件系统。

示例：

```bash
echo "v1" > /mnt/dfs/history.txt
echo "v2" > /mnt/dfs/history.txt
cat /mnt/dfs/history.txt
```

输出应为最新内容 `v2`。

### 6.4 最新版本视图

最新版本视图适合日常操作：

```bash
ls -l /mnt/dfs
cat /mnt/dfs/history.txt
```

查询工具默认也会尽量面向最新版本，避免同一路径的历史数据重复干扰结果。

### 6.5 全历史视图

需要查看或处理历史数据时，可以先查看包含隐藏项的目录列表：

```bash
ls -la /mnt/dfs
cat /mnt/dfs/.history_1710000000000.txt
```

隐藏历史文件名中的时间戳由系统生成，实际名称以 `ls -la` 输出为准。历史版本可以读取，但写入和截断会被拒绝。

查询工具也提供不做最新版本去重的选项：

```bash
./build/bin/iotdb_fs grep --all-versions "全球降水" /mnt/dfs
```

跨文件客户端也支持：

```bash
./build/bin/iotdb_fs cross-file-client --all-versions --file-query=shortName=clwmr --inner-query=level=500
```

### 6.6 跨版本访问示例

跨版本访问通常用于对比同一路径在不同时刻的内容或索引。当前用户侧最稳定的入口是带 `--all-versions` 的查询命令。对于需要读取具体历史版本内容的流程，应先通过查询或调试输出确认路径、版本和索引信息，再构造后续访问。
