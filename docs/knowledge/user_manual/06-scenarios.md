# 典型使用场景

## 10. 典型使用场景

### 10.1 挂载后作为普通文件系统使用

```bash
mkdir -p /mnt/dfs/work
echo "hello" > /mnt/dfs/work/a.txt
cat /mnt/dfs/work/a.txt
mv /mnt/dfs/work/a.txt /mnt/dfs/work/b.txt
rm /mnt/dfs/work/b.txt
```

这是最基础的使用方式，适合验证挂载和普通读写语义。

### 10.2 写入文件并通过元数据查询定位

```bash
echo "rain sample" > /mnt/dfs/rain.txt
ls -l /mnt/dfs/rain.txt
getfattr -d -m - /mnt/dfs/rain.txt
./build/bin/iotdb_fs grep --mode=file "rain" /mnt/dfs
```

实际查询效果取决于元数据和索引内容。对于普通文本关键词，不应把自然语言 grep 理解为全文扫描工具；它主要复用 inode 和索引字段做候选过滤。

### 10.3 管理文件覆盖产生的历史版本

```bash
echo "2024-03-18 run" > /mnt/dfs/run.txt
echo "2024-03-19 run" > /mnt/dfs/run.txt
cat /mnt/dfs/run.txt
./build/bin/iotdb_fs grep --all-versions --mode=file "run" /mnt/dfs
```

默认读取最新内容，历史相关查询使用支持版本的入口。

### 10.4 使用自然语言定位目标文件

```bash
./build/bin/iotdb_fs grep --mode=file "全球降水" /mnt/dfs
./build/bin/iotdb_fs grep --debug "全球降水" /mnt/dfs
```

先使用文件模式确认候选文件，再使用自动模式或块模式定位子数据。

### 10.5 使用扩展路径读取结构化子数据

```bash
cat '/mnt/dfs/show.grib2?function=read&level=500&dtype=f64' | wc -c
cat '/mnt/dfs/show.grib2?function=read&type=binary&level=500' | wc -c
```

这个场景展示高级扩展访问。使用其他数据格式时，需要对应格式实现类似的索引和处理逻辑。

### 10.6 使用 Web 浏览挂载目录

```bash
./build/bin/iotdb_fs_web --host 127.0.0.1 --port 8080 --root /mnt/dfs
```

打开：

```text
http://127.0.0.1:8080/
```

### 10.7 综合演示流程

下面流程适合演示系统能力：

```bash
./build/bin/iotdb_fs /mnt/dfs
cp /root/demo-data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
getfattr -d -m - /mnt/dfs/show.grib2
ls -l /mnt/dfs
cat /mnt/dfs/show.grib2 | wc -c
cat '/mnt/dfs/show.grib2?function=read&level=500' | wc -c
./build/bin/iotdb_fs cross-file-client --file-query=shortName=clwmr --inner-query=level=500
fusermount -u /mnt/dfs
```

演示时可追加 Web 浏览器和自然语言 grep，以展示不同入口如何复用同一挂载目录和索引信息。
