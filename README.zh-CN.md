# SciRoute

**从科学查询到原生文件访问。**

[English](README.md) | **简体中文**

SciRoute 通过可复用的**语义访问路径（semantic access paths）**统一科学选择与发布版本选择。**Structured Blob** 将科学与结构元数据、原生布局和原始编码内容关联到精确的发布文件版本；复用元数据与定位信息，在保持科学解释和来源身份的同时减少重复发现。

仓库包含两条执行路径：**FS-index** 提供独立索引和原生读取；**IoTDB-FS** 提供基于 IoTDB 的受管文件系统与查询运行时。本项目对应研究论文 *SciRoute: From Scientific Queries to Native File Access*。构建目标与运行时仍沿用 IoTDB-FS 名称。

原生库验证从[独立 FS-index](#独立-fs-index)开始；服务部署见[受管 IoTDB-FS](#受管-iotdb-fs)；论文负载与计时要求见[复现论文实验](#复现论文实验)。

## 为什么为科学文件建立索引？

科学查询即使只返回少量结果，也可能需要重复发现文件，并解码远大于结果的原生单元。复用索引还需要保证科学属性与访问位置始终对应正确的文件版本。

![科学文件访问的三个方面：原生解码粒度、可复用的发现过程与精确来源版本绑定。](resources/readme/iotdb-fs-motivation.png)

*最新论文图 1：粗粒度解码、重复发现和多个发布版本。仅凭科学条件不能确定应读取哪个发布版本。*

## 设计

Structured Blob 将发布身份、类型化科学元数据、原生布局和原始字节关联起来。声明的**语义映射（semantic mapping）**确定物理量、单位、坐标、时间约定和缺失值规则；语义访问路径将这些含义关联到原生单元及特定来源的定位信息，读取器能力决定物理执行计划。

![Structured Blob 与分层查询流程：版本绑定、目录发现、格式规划、执行和类型化结果。](resources/readme/iotdb-fs-framework.png)

*最新论文图 2：来源绑定、可复用发现、原生规划、执行与结果，全程保留选定来源。[插图来源记录](resources/readme/README.md)包含论文版本与图片哈希。*

查询路径分为四步：

1. **绑定来源和版本。** 先解析发布版本策略，再应用科学谓词。科学时间与发布顺序属于不同维度。
2. **发现相关单元。** 复用科学元数据，选择候选文件、消息、数据集或受支持的 Arrow 结构。
3. **按格式能力规划。** 将投影和选择放到目录层或原生访问层，保留需要解码值才能判断的条件。
4. **执行并保留来源。** 读取、解码选中单元，计算剩余条件，返回带有效性与来源身份的类型化结果。

索引可以减少发现工作，但不一定降低所有解码成本。收益取决于文件布局、可用定位结构、选择条件和读取器。例如，原生即可直接定位的 Arrow 窗口，可能留给额外索引的访问优化空间很小。

## 选择执行路径

| | FS-index | IoTDB-FS |
| --- | --- | --- |
| 适用场景 | 静态或低频更新的原生文件 | 已受管数据，需要协调发布与查询执行 |
| 发现方式 | 持久化 SQLite 元数据索引 | 版本化目录与格式适配器 |
| 读取方式 | 原生 GRIB/HDF5 读取器 | 受管负载访问及受支持的原生文件模式 |
| 版本与更新 | 已注册版本；限定范围内的增量发布与读端刷新 | 集成发布、版本选择与来源验证 |
| 访问控制 | 由外围系统提供 | 在受管执行与结果访问时验证 |
| 运行依赖 | 无需 IoTDB 服务或 FUSE 挂载 | 兼容的 IoTDB 服务；文件系统访问使用 FUSE |

FS-index 支持增量维护，并非只能使用一次性静态索引。当前维护接口在文档限定范围内提供单写者目录协议，分布式管理由外围系统负责。

### 格式范围

- **GRIB2：** 科学消息元数据、消息位置和原生解码；受管查询还支持已实现的空间选择与变换。
- **HDF5：** 数据集元数据与原生选择；受管适配器提供 `iotdb-indexed` 和显式绑定的 `native-hdf5` 模式，见 [HDF5 查询模式契约](docs/contracts/hdf5-query-modes.md)。
- **Arrow IPC：** 受管适配器支持其声明的表格和嵌套数组配置。科学角色必须由核验后的格式或产品元数据确定，不能仅凭任意列名推定单位或时间语义。

独立的 [`fs_index/`](fs_index/README.md) 库当前实现限定的 GRIB/HDF5 配置：六个属性等值条件，以及完整的 4,096 元素选中单元。通用 HDF5 科学角色映射与 Arrow 适配不属于该库当前接口。论文另外使用 Arrow sidecar 和面向大文件的 Python catalog 配置，与该 C++ 固定单元接口不同。Agent 提议的语义映射须核对原始记录；已评估的外部映射版本固定，动态映射版本传播尚未纳入验证。

## 快速开始

```bash
git clone https://github.com/thulab/SciRoute.git
cd SciRoute
```

### 独立 FS-index

使用 Linux 或 WSL2、C++17、线程安全的 HDF5 **1.14.4**、ecCodes、SQLite、Boost 头文件及 OpenSSL Crypto。此构建无需 IoTDB、FUSE 或 Arrow。

```bash
cmake -S fs_index -B build/fs-index \
  -DCMAKE_BUILD_TYPE=Release \
  -DHDF5_DIR=/path/to/hdf5-1.14.4/lib/cmake/hdf5 \
  -DFS_INDEX_BUILD_TESTS=OFF
cmake --build build/fs-index --target fs_index_native --parallel 2
```

生成的库为 `build/fs-index/libfs_index_native.so`。Python API、输入要求、增量发布与功能／恢复测试见 [FS-index 指南](fs_index/README.md)。测试构建包含故障注入钩子；正式测量应使用关闭测试的独立 Release 构建。

### 测量前先验证功能

以下两组 Python 测试使用 fixture reader，无需 IoTDB 服务或原生库：

```bash
python3 -m unittest fs_index.tests.test_lifecycle fs_index.tests.test_hdf5_handles -v
```

验证真实 GRIB/HDF5 读取、增量维护与恢复时，需要 Python **3.10+**、NumPy、h5py 和可用的 ecCodes sample definitions，使用独立测试构建。原生依赖按 [FS-index 指南](fs_index/README.md)匹配；部分实验生成器还要求 h5py 实际加载 HDF5 1.14.4。

```bash
cmake -S fs_index -B build/fs-index-test \
  -DCMAKE_BUILD_TYPE=Release \
  -DHDF5_DIR=/path/to/hdf5-1.14.4/lib/cmake/hdf5 \
  -DPython3_EXECUTABLE=/path/to/venv/bin/python \
  -DFS_INDEX_BUILD_TESTS=ON
cmake --build build/fs-index-test --target fs_index_native --parallel 2
ctest --test-dir build/fs-index-test --output-on-failure
```

CTest 注册 `fs_index_native_functional` 与 `fs_index_maintenance` 并设置原生库路径。测试生成小型 fixture，验证功能及失败语义；通过测试不等于复现论文性能。正式测量使用前文关闭测试的独立构建。

### 受管 IoTDB-FS

锁定的 Linux CI 环境为 Ubuntu 22.04 和 GCC 11。根目录构建需要 C++17、FUSE、ecCodes、OpenSSL、zlib、启用 IPC/LZ4/ZSTD 的 **Arrow 21.0.0**，以及启用 DEFLATE 的线程安全串行 **HDF5 1.14.4**。还需要支持 **`OBJECT` 扩展的定制 IoTDB C++ SDK** 与兼容服务端；仅标准 SDK 不足以构建本项目。

根据 [CI 依赖指南](ci/README.md) 和 [依赖锁定文件](ci/dependencies.lock.json) 准备依赖前缀，再填写实际位置：

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTS=OFF \
  -DIOTDB_ROOT=/path/to/object-enabled-iotdb-client \
  -DArrow_DIR=/path/to/arrow-21/lib/cmake/Arrow \
  -DHDF5_DIR=/path/to/hdf5-1.14.4/lib/cmake/hdf5
cmake --build build --target iotdb_fs iotdb_fs_web --parallel 2
```

定制 SDK 和服务端属于外部前置依赖，仓库不附带其二进制文件。CI 通过 `IOTDB_SDK_URL` 指定核验后的 SDK 压缩包，配置说明见 [PR CI 设计](docs/knowledge/pr-ci-design.md)。

以 [`resources/config.properties`](resources/config.properties) 为模板创建配置，按实际环境设置数据库地址、凭据、命名空间与挂载路径，并在挂载前启动兼容的 IoTDB 服务：

```bash
cp resources/config.properties /path/to/iotdb-fs.properties
# 启动前编辑 /path/to/iotdb-fs.properties。
export IOTDB_CONFIG_FILE=/path/to/iotdb-fs.properties
mkdir -p /path/to/mount
./build/bin/iotdb_fs -f -o default_permissions /path/to/mount
```

在另一个终端通过挂载路径执行普通文件操作。可选 Web 文件浏览入口为 `http://127.0.0.1:8080`：

```bash
./build/bin/iotdb_fs_web --host 127.0.0.1 --port 8080 --root /path/to/mount
# 使用后卸载（Linux）。
fusermount -u /path/to/mount
```

查询语法、部署和详细配置见 [用户手册](docs/knowledge/user_manual/01-overview.md) 及以下契约文档。

## 复现论文实验

**当前材料范围：**仓库提供实现源码、依赖锁、功能测试和保留的实验驱动，**尚不是复现全部论文数值的自包含归档**。定制 SDK／服务端、完整数据与转换输入、已接受的科学映射，以及部分冻结控制器和原始结果包仍是外部前置材料。旧协议中的本地路径与历史报告名称不能当作公开下载入口。

### 1. 固定实现与环境

记录 `git rev-parse HEAD`、`git status --short`、编译器／系统／CPU、[依赖锁](ci/dependencies.lock.json)摘要、实际加载的原生库版本、二进制哈希和 Python 环境（`python3 -m pip freeze`）。该锁覆盖原生 CI 配置，不覆盖全部实验 Python 包。

论文测量环境为 Ubuntu 24.04.4 LTS、Intel Xeon Gold 6130 2.10 GHz、32 个逻辑 CPU，不同批次记录 189–378 GiB RAM；CI 构建环境为 Ubuntu 22.04／GCC 11。局部查询实验由客户端、原生 helper、服务和 DataNode 共享两核，查询内存为 2 GiB，数据库内存单独受限且随批次不同。这不代表全系统仅使用 2 GiB。远程访问实验在同一主机上分别限制查询端与存储端资源，具体数值以各协议为准。

### 2. 准备数据、语义映射与原生参考

| 输入 | 必需材料 |
| --- | --- |
| 受控 GRIB/HDF5 集合及历史版本 | 按 [FM1](experiments/fixed_matrix/README.md) 和[多文件准备](experiments/multi_file/README.md)保留 corpus／case 身份及生成器。固定配置每文件四个 64×64 单元，以六个属性等值条件返回两个单元。新生成语料须记录新的运行身份，不能冒充原归档数据。 |
| 真实 GFS GRIB、OISST、GEOS HDF5 | 获取已接受批次中的精确产品文件和摘要，仅产品名或新下载的每日文件不足以复现。原生辅助代码见[单文件实验](experiments/single_file/README.md)，完整来源清单未随本仓库打包。 |
| Beijing、Jena 与 Arrow 扩展 | 来源包括 [Beijing](https://archive.ics.uci.edu/dataset/501/beijing+multi+site+air+quality+data)、[Jena 原始记录](https://www.bgc-jena.mpg.de/wetter/weather_data.html)、[Appliances](https://archive.ics.uci.edu/dataset/374/appliances+energy+prediction) 和论文指定的 [GIFT-Eval revision](https://huggingface.co/datasets/Salesforce/GiftEval/tree/30841734ac5cfddbd0c3bad6d09d2b6b32becbb0)。还需精确转换、sidecar、查询 manifest 和原始记录核验；完整已接受 Arrow 实验包尚未在本仓库分发。 |

Beijing 使用平面列及经验证的日历到行映射；Jena 使用嵌套列表和 ordinal window。不能仅凭 Arrow 类型推断时区、单位或通道含义。接受运行结果前，须对照原生读取器验证来源／版本、shape、顺序、数值、validity mask 与适用坐标，并记录三路径共享的源摘要与语义映射版本。

### 3. 选择对应驱动和证据包

| 论文实验 | 保留入口 | 额外要求与边界 |
| --- | --- | --- |
| 语义映射与索引构建 | [FS-index 测试/API](fs_index/README.md)、格式[契约](docs/README.md) | 离线重建时间不含 Agent 开发；完整 Beijing/Jena 适配包属于外部材料。 |
| 跨文件与历史查询 | [FM1 协议](experiments/fixed_matrix/README.md)、`experiments.fixed_matrix.run`、`hdf5_filesystem`、`multiversion_scan` | 需要封印语料、native/build binding、注册收据；当前源码不能单独还原所有已接受批次，严格绑定历史身份的控制器需配套快照。 |
| 单文件选择与独立会话 | [单文件协议](experiments/single_file/README.md)、`section42_20261026/` | 独立的受管模式实验，不能改称三路径 FS-index 实验。 |
| 已存储数据上的暖查询 | [Resident 协议](experiments/resident_table5/README.md)、`run.py`、`cache_run.py` | 需要已持久化来源、目录、owner journal 和先前运行 manifest。HDF5 句柄组属于后续原生补测，受管组沿用原批次。 |
| 增量发布与维护 | [M1 协议](experiments/maintenance_lifecycle/README.md)、`run.py`、`supervise.py` | 八个初始文件、十六次更新、每更新 1/10/100 次查询、三路径三轮。完整轨迹包含复制／发布，不是已有数据库负载上的边际索引成本。 |
| 远程访问模拟与 Arrow 三路径扩展 | 当前格式／运行时源码及外部批次包 | 已接受的网络控制器、Arrow sidecar、manifest 和原始测量尚未全部分发。远程结果为单机延迟模拟，不是真实多主机执行。 |

保留的辅助模块见[实验索引](experiments/README.md)。`resident_table5` 等目录名是历史标识，不对应当前论文表号。`tests/benchmarks/arrow_training/` 中部分代码提供原生 helper，其早期训练实验不能替代论文 Arrow 扩展实验。

服务实验应使用独立环境：supervisor 会管理 cgroup、进程亲和性、私有挂载及 namespace。使用新工作目录和必需的先前 manifest，保留服务恢复收据。冻结的源码／二进制检查属于协议，不应绕过，也不能将新构建重新标记为历史已接受二进制。

### 4. 保持测量契约

- **相同任务：**三路径使用相同科学映射、发布版本、单元、输出和掩码；先选择发布版本，再过滤科学属性。
- **分开计时：**按各实验定义区分获取／发布、索引构建、更新／提交、恢复、查询服务和收尾。比较已存储数据的查询服务时，不能再次向 IoTDB-FS 收取完整导入成本。IoTDB-FS 的索引构建与发布耦合，当前测量没有独立估计其边际索引维护成本。
- **缓存状态：**保留协议中的 OS／数据库缓存，并注明读取器每次重开还是保留句柄。验证后的首查不是冷缓存测量；区分发现复用、句柄复用和已解码状态复用。
- **统计量：**Arrow 窗口使用 30 次暖请求中位数；集合实验每 filesystem 单元为 10 次、managed 为 40 次，报告中位数和请求 min–max。历史表为三轮中位数的中位数及其样本标准差。独立 session 表使用中位数；已存储数据 session wall/Q 和增量轨迹总成本使用三轮算术均值。吞吐量用完成工作量除以实测批次时间，保留各批次定义。
- **验收：**保留所有测量样本、失败尝试、源码／二进制哈希、原生 oracle 结果和资源恢复状态。逐请求的阶段时间可相加，独立聚合后的阶段中位数不一定等于总时延中位数。

论文受控 512 文件 GRIB 配置的中位数为 **FS-scan 1984.135 ms、FS-index 8.083 ms、IoTDB-FS 18.177 ms**，相对扫描分别约 **245×、109×**。这是特定暖查询负载的结果，不是默认性能保证。已知位置的 Arrow 控制组接近原生访问成本，受管执行也可能比原生路径更慢。

## 文档

[FS-index API](fs_index/README.md) · [维护契约](docs/contracts/fs-index-maintenance-v2.md) · [查询模块](docs/knowledge/query-module-layout.md) · [用户手册](docs/knowledge/user_manual/01-overview.md) · [单元测试](tests/unit/README.md) · [集成测试](tests/integration/README.md) · [打包](packaging/README.md) · [文档索引](docs/README.md)

## 仓库结构

```text
fs_index/            独立 GRIB/HDF5 索引与 Python 绑定
include/iotdbfs/      C++ 接口
src/                 文件系统、存储、查询运行时和格式适配器
resources/           配置模板与 README 插图
tests/               单元、集成与性能测试
experiments/         负载定义与实验驱动
ci/                  依赖安装与测试工具
packaging/           发布打包与运行工具
docs/                当前契约、手册与冻结实验协议
```

## 贡献与许可

修改时遵循已有查询／存储契约，并运行与改动相关的检查。贡献者与编程智能体的工作约定见 [AGENTS.md](AGENTS.md)。项目采用 [Apache License 2.0](LICENSE)。
