# 打包发布、测试验证与故障排查

## 11. 打包、发布与自检

### 11.1 发布包结构

发布包包含可执行程序、默认配置和运行时脚本。常见内容包括：

```text
bin/iotdb_fs
bin/iotdb_fs_web
bin/grib_client
resources/config.properties
scripts/start.sh
scripts/stop.sh
scripts/self_check.sh
```

部分构建还会包含高级案例客户端或第三方静态依赖。

### 11.2 当前系统发布包构建

为当前主机系统构建发布包：

```bash
packaging/current/build_release.sh
```

产物位于：

```text
build/dist/
```

### 11.3 Kylin 静态发布包构建

构建 Kylin 静态发布包：

```bash
packaging/kylin/build_release.sh
```

该包用于更受控的运行环境，脚本会检查静态链接和包内布局。

### 11.4 发布包校验

当前系统包校验：

```bash
packaging/current/verify_release.sh
```

Kylin 包校验：

```bash
packaging/kylin/verify_release.sh
```

校验内容包括必要文件是否存在、二进制是否可加载、帮助命令是否能执行等。

### 11.5 运行时自检

源码中的运行时自检脚本位于：

```bash
packaging/runtime/self_check.sh
```

打包后通常复制为包内脚本：

```bash
./scripts/self_check.sh
```

该脚本主要验证 `bin/iotdb_fs`、`bin/iotdb_fs_web`、`resources/config.properties` 和 IoTDB 端口连通性，并提示是否存在 `/dev/fuse`。它不会替用户挂载文件系统。

### 11.6 包命名规则

发布包命名格式：

```text
iotdb-fs-c-<version>-<platform>-<arch>-<linkage>.tar.gz
```

示例：

```text
iotdb-fs-c-1.0.0-kylin-v10-x86_64-static.tar.gz
iotdb-fs-c-1.0.0-ubuntu-22.04-x86_64-native.tar.gz
```

## 12. 测试与验证

### 12.1 单元测试

构建并单独运行 UT：

```bash
cmake -S . -B build-ut -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
cmake --build build-ut --target unit_tests -j 4
ctest --test-dir build-ut -L unit --output-on-failure -j 4
```

单元测试集中在 `tests/unit/`，目标名为 `unit_tests`，默认不要求真实 IoTDB 服务。
覆盖率、随机重复运行和内存检查命令见 [单元测试说明](../../../tests/unit/README.md)。

### 12.2 集成测试

集成测试位于 `tests/integration/`。这些测试依赖 FUSE、IoTDB、真实数据路径或外部服务，不一定适合默认构建时自动运行。

Web API 烟测示例：

```bash
bash tests/integration/web_api_smoke.sh ./build/bin/iotdb_fs_web
```

### 12.3 性能测试

性能测试位于 `tests/performance/`，用于论文或演示中的读写性能检查。常用脚本包括：

```bash
bash tests/performance/scripts/build_perf_test.sh
bash tests/performance/scripts/test.sh
```

多数脚本假设存在 `/mnt/dfs`、`/mnt/local` 等本地挂载点。换环境运行前，请先阅读脚本中的变量和路径。

### 12.4 手工验证清单

最小验证清单：

```bash
mkdir -p /mnt/dfs
./build/bin/iotdb_fs -f -d /mnt/dfs
echo "hello" > /mnt/dfs/hello.txt
cat /mnt/dfs/hello.txt
ls -l /mnt/dfs
fusermount -u /mnt/dfs
```

查询验证：

```bash
./build/bin/iotdb_fs grep --mode=file "全球降水" /mnt/dfs
```

Web 验证：

```bash
echo "hello web" > /mnt/dfs/hello.txt
./build/bin/iotdb_fs_web --root /mnt/dfs --port 8080
curl -sG 'http://127.0.0.1:8080/api/viewer-info' --data-urlencode 'path=/hello.txt'
```

### 12.5 测试数据准备

普通文件测试可直接使用 `echo`、`cp` 或 `dd` 生成。高级结构化访问案例需要可解析并可建立索引的数据文件，例如当前示例中的 `.grib2` 文件。

示例：

```bash
cp /root/demo-data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
```

请根据本地环境调整数据路径。

## 13. 故障排查

### 13.1 构建失败

检查：

```bash
cmake --version
g++ --version
pkg-config --cflags fuse
pkg-config --libs fuse
```

常见原因是缺少 `libfuse-dev`、CMake 版本不满足、编译器不可用或第三方依赖未初始化。

### 13.2 挂载失败

检查挂载目录是否存在：

```bash
mkdir -p /mnt/dfs
```

以前台调试方式启动：

```bash
export IOTDB_LOG_LEVEL=DEBUG
./build/bin/iotdb_fs -f -d /mnt/dfs
```

如果目录已被旧进程占用，先卸载：

```bash
fusermount -u /mnt/dfs
```

### 13.3 IoTDB 连接失败

检查配置：

```bash
rg -n "iotdb.host|iotdb.port|iotdb.user|iotdb.pass|iotdb.db" resources/config.properties
```

确认 IoTDB 服务已启动，地址和端口可访问，用户名、密码和数据库名称正确。

### 13.4 文件读写异常

排查顺序：

1. 确认路径在挂载目录下。
2. 确认挂载进程仍在运行。
3. 检查日志目录 `log.dir`。
4. 使用普通小文件验证最小读写。
5. 再验证大文件或高级扩展访问。

最小验证：

```bash
echo "probe" > /mnt/dfs/probe.txt
cat /mnt/dfs/probe.txt
```

### 13.5 查询无结果

先开启调试：

```bash
./build/bin/iotdb_fs grep --debug "500百帕温度场" /mnt/dfs
```

再尝试：

```bash
./build/bin/iotdb_fs grep --mode=file "500百帕温度场" /mnt/dfs
./build/bin/iotdb_fs grep --scan-limit=2000 "500百帕温度场" /mnt/dfs
./build/bin/iotdb_fs grep --all-versions "500百帕温度场" /mnt/dfs
```

如果文件级也没有结果，检查数据是否已经通过挂载目录写入并建立索引。

### 13.6 扩展访问失败

检查：

- `grib.extension.enabled` 是否为 `true`。
- 查询路径是否加引号，避免 shell 解释 `?` 和 `&`。
- `function`、`type`、`dtype`、`level`、`shortName` 是否支持。
- 目标文件是否已经建立结构化索引。

示例：

```bash
cat '/mnt/dfs/show.grib2?function=read&type=binary&level=500' | wc -c
```

### 13.7 Web 服务不可访问

检查服务是否启动：

```bash
./build/bin/iotdb_fs_web --host 127.0.0.1 --port 8080 --root /mnt/dfs
```

检查 API：

```bash
curl -sG 'http://127.0.0.1:8080/api/viewer-info' --data-urlencode 'path=/hello.txt'
```

远程访问时，把监听地址改为：

```bash
./build/bin/iotdb_fs_web --host 0.0.0.0 --port 8080 --root /mnt/dfs
```

同时确认防火墙和网络策略允许访问。

### 13.8 卸载失败

先确认哪些进程正在使用挂载点：

```bash
lsof +f -- /mnt/dfs
```

关闭相关进程后卸载：

```bash
fusermount -u /mnt/dfs
```

如果仍失败，检查是否有 shell 当前目录位于挂载点内。

### 13.9 日志定位方法

优先查看配置中的 `log.dir`。需要更多信息时设置：

```bash
export IOTDB_LOG_LEVEL=DEBUG
```

或在配置文件中设置：

```properties
log.level=DEBUG
```

然后以前台方式复现问题：

```bash
./build/bin/iotdb_fs -f -d /mnt/dfs
```
