# IoTDB-FS-C

## 项目结构

```text
include/iotdbfs/     项目头文件
src/                 源文件与可执行程序入口
fs_index/            可独立构建的 GRIB/HDF5 科学文件索引与原生读取组件
cmake/QueryModules.cmake  查询/训练/格式模块的构建清单
third_party/         第三方库与静态依赖
build/               CMake 构建目录，运行产物在 build/bin/
resources/           默认配置和资源文件
tests/unit/          GTest 单元测试
tests/integration/   依赖 FUSE、IoTDB、真实 GRIB 数据的集成/手工测试
tests/performance/   读写性能测试程序与脚本
packaging/           发布包构建、校验与运行时脚本
scripts/dev/         本地开发启动、停止、清理脚本
ci/                  GitHub Actions 依赖、测试运行器和报告校验
docs/                文档
```

Typed query、训练查询与格式 Adapter 的目录及依赖方向见
[查询模块布局](docs/knowledge/query-module-layout.md)。
文档按契约、知识资产、阶段报告和手工交付物分类，入口见 [文档索引](docs/README.md)。
旧数据的离线重摄取与恢复流程见 [Arrow 迁移](docs/knowledge/arrow-data-migration.md) 和
[HDF5 迁移](docs/knowledge/hdf5-data-migration.md)；两者共用带 namespace 绑定的报告协议。

FS-index 的独立构建、Python API 和当前能力范围见 [组件说明](fs_index/README.md)。

## Pull Request CI

PR 合并前 CI、固定依赖安装、覆盖率门槛和 GitHub 分支保护配置见
[PR CI 说明](docs/knowledge/pr-ci-design.md) 和 [CI 工具说明](ci/README.md)。工作流要求仓库变量
`IOTDB_SDK_URL` 指向带 `OBJECT` 扩展的自定义 IoTDB C++ SDK 压缩包；缺少该变量或校验
失败会阻止合并。

## 智能体协作

智能体工作约定、模块路由、验证入口及授权边界统一见 [AGENTS.md](AGENTS.md)。

## FileSystem

### 环境配置
```bash
sudo apt update
sudo apt install libfuse-dev
```

### Owner 不可变对象读取并行

`storage.version_directory.mode=owner` 时，以下配置限制该 owner 的 IoTDB
不可变对象读取连接数；默认 4，允许 1–256 的正整数：

```properties
storage.version_directory.reader_pool_size=4
```

每个读取独占一个 session，直到结果集关闭后归还。池满时等待可用 reader，
获取等待超时为 5 秒；写入、head 发布和恢复继续使用串行路径。
对象 reader 按需建立；两个原有控制连接另计。读取或校验失败会丢弃该 reader，
当前操作不自动重试；等待期间继续检查查询取消和截止时间。
该配置仅在 owner 模式有效，空值、0、负数、非整数或超过 256 会使启动失败。
`iotdb.pool_size` / `IOTDB_POOL_SIZE` 控制原有连接池，不覆盖 owner reader pool。
比较 1/2/4 路读取时分别设置此项，保持查询、数据、缓存状态和其他参数一致；
连接数是并发上限，实际并发还取决于查询的读取任务数。

### 公共查询任务调度

公共查询运行时通过有界任务池调度独立查询任务，可配置：

```properties
query.tasks.worker_threads=4
query.tasks.max_queued_tasks=64
query.tasks.max_batch_size=4
```

三项均要求正整数。`worker_threads` 允许 1–256，`max_queued_tasks` 允许
1–65536，`max_batch_size` 不超过 `worker_threads`。未设置 `max_batch_size`
时取 `min(4, worker_threads)`，因此只配置一个 worker 即可使用串行基线。
非法配置在创建查询运行时前拒绝启动。该任务池用于查询间并行；
`query.cache.worker_threads` 控制片段缓存工作线程，
`storage.version_directory.reader_pool_size` 控制 owner 对象读取连接上限。
socket 收包后异步入池；就绪请求按空闲槽位攒批派发，不等待凑满。
调度前预约任务资源；Native HDF5 争用任务留队，显式 Arrow 请求可继续执行。
Indexed HDF5 不受 native 资源预约限制。
`typed_batch` 内部继续顺序执行。详细边界见
[公共查询任务调度契约](docs/contracts/query-task-scheduler.md)。

已发布的不可变结果可通过同一控制 socket 的 `result_fetch` v1 异步读取。
服务端重新授权后交由有界 I/O reactor 发送，默认最多 16 个在途 fetch，
每个传输暂存不超过 64KiB；等待发送不占用公共查询执行槽。
发送线程可在守护进程的 properties 文件中配置，默认 1，允许 1 或 2：

```properties
query.result_fetch.sender_threads=2
```

该项独立于 `query.tasks.worker_threads`，客户端请求不能覆盖。
两路发送仍共享全局 16 个在途 fetch 上限。空值、0、负数、非整数或大于 2
的值在创建查询运行时前拒绝启动。
请求、错误和资源释放边界见 [result-fetch 契约](docs/contracts/result-fetch.md)。

### 构建
```bash
mkdir build && cd build
cmake ..
make
```

### Web 文件管理器
项目内置了一个轻量的 Web 文件浏览器 `iotdb_fs_web`，默认读取 `resources/config.properties` 中的 `mount.point` 作为浏览根目录，并提供类似文件资源管理器的页面。

```bash
cd build/bin
./iotdb_fs_web
```

默认访问地址：

```bash
http://127.0.0.1:8080/
```

也可以在启动时覆盖监听地址、端口和浏览根目录：

```bash
./iotdb_fs_web --host 0.0.0.0 --port 8080 --root /mnt/dfs
```

页面能力：
- 目录树式面包屑导航
- 文件/目录列表浏览
- 按名称、修改时间、大小排序
- 文本文件预览
- 文件下载

### 自然语言 grep 检索
`iotdb_fs` 现在提供 `grep` 子命令，用自然语言描述检索文件或 GRIB 子数据块。实现方式不是全量扫文件内容，而是先将自然语言翻译成检索计划，再复用 typed file metadata 与 GRIB adapter-private layout 做两阶段过滤。

```bash
cd build

# 返回匹配文件
./iotdb_fs grep --mode=file "全球降水" /mnt/dfs

# 自动识别为子数据块检索，返回可直接读取的 query_path
./iotdb_fs grep "500百帕温度场" /mnt/dfs

# 多目录检索
./iotdb_fs grep "10米风" gfs ecmwf
```

输出分两类：
- `[FILE] ... path=/mnt/dfs/...`：返回匹配文件
- `[BLOCK] ... query_path=/mnt/dfs/file.grib2?function=read&type=binary&offset=...`：返回匹配 message 子块

常用选项：
- `--mode=auto|file|block`
- `--limit=N`
- `--scan-limit=N`
- `--debug`
- `--debug-limit=N`
- `--all-versions`

排查“为什么没匹配”时可以直接打开调试模式：

```bash
./iotdb_fs grep --debug "500百帕温度场" /mnt/dfs
```

如果候选文件数为 0，程序会额外输出目录内若干样本文件的拒绝原因，例如：
- `shortName expected tmp but got clwmr`
- `typeOfLevel expected isobaricInhPa but got surface`
- `path term missing xxx`

当前自然语言规划器是本地启发式实现，优先识别这几类语义：
- 变量语义：温度 / 湿度 / U 风 / V 风 / 位势高度 / 降水 / 海平面气压
- 层次语义：`500hPa` / `500百帕` / `surface` / `海平面` / `10米`
- 时效语义：`24小时预报`
- 域语义：`global` / `全球`

如果要接入外部智能体或 LLM，直接替换 `src/SemanticGrepClient.cpp` 里的“自然语言 -> 检索计划”步骤即可，后面的 IoTDB 候选过滤和子块枚举逻辑不需要改。

### 运行挂载
```bash
mkdir -p /mnt/gribfs
mkdir -p /data/fuse-backing

export IOTDB_FS_LOG_LEVEL=DEBUG
./iotdb_fs -f -d -o default_permissions /mnt/gribfs

./iotdb_fs -o default_permissions /mnt/gribfs

cat gribfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/level=500,shortName=tmp.values?bbox=lonW,latS,lonE,latN
cat gribfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/level=500,shortName=tmp.values?bbox=...&interp=bilinear&nx=...,ny=...
```

### GRIB 读取（function 链式调用）

以 `function` 区分操作，并支持多个操作嵌套（按 `?` 串联）。

```bash
# 整场读取
cat 'show.grib2?function=read&level=500&dtype=f64' | wc -c

# 读取原始 GRIB message（二进制）
cat 'show.grib2?function=read&type=binary&level=500' | wc -c

# 整场读取 + 空间裁剪
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' | wc -c

# 空间裁剪 + 空间插值（interp 默认 bilinear）
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' | wc -c

cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./
ls -ld /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2
ls -l /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/raw
ls -l /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/binary?level=500
ls -l /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/data?level=500
cat /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/data?level=500 | wc -c 
ls -l /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/binary?level=550,shortName=clwmr
ls -l /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/binary?level=600,shortName=clwmr2
cat /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/raw | wc -c
cat /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2/binary?level=500 | wc -c

cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 ./
ll /mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2/level=500
```

### 卸载挂载目录
```bash
fusermount -u /mnt/gribfs
```

### 常规文件测试
```bash
cd /mnt/gribfs
echo "hello world" > a.txt
cat a.txt

echo "random data" > b.grib2
cat b.grib2

echo "xxx" > b.grib2
cat b.grib2

head -c 4 b.grib2

rm b.grib2
ls

echo "123" > c.grb
echo "normal file" > d.txt
cat c.grb
cat d.txt

echo "from backing" > /data/fuse-backing/e.txt
ls /mnt/gribfs
cat /mnt/gribfs/e.txt
```


## Grib Read And Write

### 环境配置
```bash
# Grib 读取工具
sudo apt install libeccodes-dev

# 标准操作工具
sudo apt install cdo
```

### 示例文件生成
```bash
cdo -f grb2 \
  -setgrid,global_1 \
  -settaxis,2020-01-01,00:00,1day \
  -setname,TMP \
  -mulc,0 \
  -addc,2.5 \
  -topo \
  template.grib2


g++ -o generate_template generate_grib2_template.cpp -leccodes
./generate_template
```

## Grib Write Example

### eccodes 安装
```bash
sudo apt update
sudo apt install build-essential cmake libcurl4-openssl-dev
sudo apt install libeccodes0 libeccodes-dev

git clone https://github.com/ecmwf/eccodes.git
cd eccodes
mkdir build && cd build
cmake -DENABLE_FORTRAN=OFF ..
make -j4
sudo make install
sudo ldconfig

grib_ls --version
grib_count Z_NAFP_C_BABJ_20230101000000_P_NWPC-GRAPES-3KM-CN-00000.grb2
```

### 测试程序
- 编译：

```bash
g++ -O2 object_example.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -liotdb_session \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -std=c++11 \
    -o object_example

g++ -std=c++11 -o grib_test grib_test.cpp -I/usr/include -L/usr/lib -leccodes
./grib_test /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2

g++ -O2 grib_write.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -std=c++11 \
    -o grib_write

g++ -O2 grib_read.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -std=c++11 \
    -o grib_read

g++ -O2 grib_crop.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -std=c++11 \
    -o grib_crop

g++ -O2 grib_interpolation.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -std=c++11 \
    -o grib_interpolation

g++ -O2 grib_utils.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -std=c++11 \
    -o grib_utils

g++ -O2 iotdb_fs.cpp -D_FILE_OFFSET_BITS=64 \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    $(pkg-config --cflags --libs fuse) \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -o iotdb_fs

./iotdb_fs /mnt/dfs
./iotdb_fs -o default_permissions -f -d /mnt/dfs
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./base.grib2
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 ./

ls test.grib2/data?level=500,lat0=25.2,lon0=102.9
ls test.grib2/data?level=500,lat0=30,lon0=100,lat1=20,lon1=120,dtype=f64
ls test.grib2/data?level=500,lat0=35,lon0=105,nlat=200,nlon=300

cat test.grib2/data?level=500,lat0=25.2,lon0=102.9 > crop.bin
cat test.grib2/data?level=500,lat0=30,lon0=100,lat1=20,lon1=120,dtype=f64 > crop.bin
cat test.grib2/data?level=500,lat0=35,lon0=105,nlat=200,nlon=300 > crop.bin

echo "1234" > test.txt
ls
cat test.txt
fusermount -u /mnt/dfs


g++ -O2 test_grib.cpp \
    -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
    -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -I/usr/include -L/usr/lib -leccodes \
    -liotdb_session \
    $(pkg-config --cflags --libs fuse) \
    -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
    -o test_grib
./test_grib /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 
./test_grib /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 > result.jsonl
```

- 查看

```bash
bash /data/iotdb-deploy/1c1d-open/datanode1/iotdb/sbin/start-cli.sh -sql_dialect table
```

- 空间裁剪

```bash
# 32位浮点（默认）
ls -l /mnt/dfs/test.grib2/data?level=500
# 64位浮点
ls -l /mnt/dfs/test.grib2/data?level=500,dtype=f64

ls -l '/mnt/dfs/test.grib2/data?level=500,lat0=25.2,lon0=102.9'
# 预览前 8 个值（64位浮点）
cat '/mnt/dfs/test.grib2/data?level=500,lat0=25.2,lon0=102.9,dtype=f64' | head -c 64 | xxd

ls -l '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=20,lon1=120,dtype=f64'
# 输出整个裁剪窗口的数值（双精度，小端）
cat '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=20,lon1=120,dtype=f64' > 500_30_20_100_120.bin

ls -l /mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=20,lon1=120

cat '/mnt/dfs/test.grib2/data?level=500' > /root/fs/IoTDB-FS-C/full.bin
cat '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=-10,lon1=150' > /root/fs/IoTDB-FS-C/cropped.bin
ls -l '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=-10,lon1=150'
```

## 空间裁剪
```bash
# 编译
cd /root/fs/IoTDB-FS-C/build
/usr/local/cmake/bin/cmake --build . --config Debug --target all -j 20 --

# 1) 无裁剪（基线）
ls -l '/mnt/dfs/test.grib2/data?level=500'
ls -l '/mnt/dfs/test.grib2/data?level=500,dtype=f64'

# 2) 起点裁到末端
ls -l '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100'

# 3) 矩形裁剪 + 双精度
ls -l '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=-10,lon1=150'
ls -l '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=-10,lon1=150,dtype=f64'

# 4) 导出二进制并比对大小（应与 ls 一致）
cat '/mnt/dfs/test.grib2/data?level=500' > full.bin
cat '/mnt/dfs/test.grib2/data?level=500,lat0=30,lon0=100,lat1=-10,lon1=150' > cropped.bin
ls -l full.bin cropped.bin
```

# Ubuntu 环境安装

```bash
sudo apt update
sudo apt install libfuse-dev
sudo apt install libeccodes-dev
sudo apt install -y build-essential cmake libboost-all-dev
```

# 性能测试
```bash
time cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /root/qxj-data/GRIB-Test
time cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 /root/qxj-data/GRIB-Test

time cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /data
time cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 /data

time cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
time cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 /mnt/dfs

time cp /root/qxj-data/GRIB-Test/KWBCGFS_VVP_100_2024031800_GLB_1_2_0P25.grib2 /data
time cp /root/qxj-data/GRIB-Test/KWBCGFS_VVP_100_2024031800_GLB_1_2_0P25.grib2 /mnt/dfs/big.grib2
```
