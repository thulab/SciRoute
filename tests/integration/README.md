# 集成测试

## 环境预检

`iotdb_fs_preflight` 在运行真实 IoTDB/FUSE 测试前检查 TCP 端口、IoTDB
凭据和 SchemaRegion、`/dev/fuse`、`fusermount`、测试二进制及其动态依赖。
每项检查向标准输出写入一行 JSON；密码不会出现在输出中。退出码为 0
表示全部检查通过，1 表示环境检查失败，2 表示参数错误。

```bash
./build/bin/iotdb_fs_preflight \
  --host "${IOTDB_HOST:-127.0.0.1}" \
  --port "${IOTDB_PORT:-6667}" \
  --user "${IOTDB_USER:-root}" \
  --db "${IOTDB_FS_TEST_DB}" \
  --fs-bin "$PWD/build/bin/iotdb_fs" \
  --web-bin "$PWD/build/bin/iotdb_fs_web"
```

失败记录包含稳定的 `code` 字段，例如 `IOTDB_PORT_UNREACHABLE`、
`IOTDB_AUTH_FAILED`、`IOTDB_SCHEMA_REGION_UNAVAILABLE`、
`FUSE_DEVICE_MISSING` 和 `DEPENDENCY_MISSING`，便于 CI 按环境问题分类。

默认 CTest 包含单元测试、Web API smoke、平台配置、预检 CLI 以及各集成 harness 的帮助入口。
这些检查使用本地临时目录或参数校验，不需要 IoTDB 服务或 FUSE 挂载。

## 通用文件系统回归

`generic_fs_integration` 从 `general` 分支的通用测试迁入，面向 Linux 的真实 IoTDB/FUSE 环境。它自行启动文件系统及 Web 服务，检查普通文件读写、追加、随机覆盖、稀疏写、截断、历史版本、权限、时间、重命名、删除和 Web 读取，并卸载重挂后校验持久化内容。GRIB 扩展开启和关闭分别运行，测试普通文件与 GRIB 功能开关共存。

准备条件：

- 本机允许访问 `/dev/fuse`，安装了 `fusermount`。
- 测试 IoTDB 支持本项目的表模型与 `OBJECT` 类型，所选数据库的分区已就绪。
- 使用独立测试数据库；通过 `--db` 或 `IOTDB_FS_TEST_DB` 显式指定。测试没有默认业务数据库。

在仓库根目录构建并手动运行：

```bash
cmake -S . -B build -DBUILD_TESTS=ON
cmake --build build --target iotdb_fs iotdb_fs_web generic_fs_integration -j4

export IOTDB_HOST=127.0.0.1
export IOTDB_PORT=6667
export IOTDB_USER=root
# 按测试环境设置 IOTDB_PASS；不要将凭据提交进仓库。
export IOTDB_FS_TEST_DB=iotdbfs_test

./build/bin/generic_fs_integration \
  --fs-bin "$PWD/build/bin/iotdb_fs" \
  --web-bin "$PWD/build/bin/iotdb_fs_web" \
  --pool-size 1 --grib-enabled true

./build/bin/generic_fs_integration \
  --fs-bin "$PWD/build/bin/iotdb_fs" \
  --web-bin "$PWD/build/bin/iotdb_fs_web" \
  --pool-size 1 --grib-enabled false
```

`--work-dir` 指定临时工作目录的父目录，每次运行都会创建独立子目录并保留日志。`--skip-web` 可显式跳过 Web 检查；默认缺少 Web 程序视为失败。其余选项见 `--help`。

真实挂载测试默认不注册到 CTest。需要在 CI 或本机完整运行时，先设置上述连接环境变量，再启用：

```bash
cmake -S . -B build -DBUILD_TESTS=ON -DENABLE_FUSE_INTEGRATION_TESTS=ON
cmake --build build -j4
ctest --test-dir build -R '^GenericFsGrib(Enabled|Disabled)$' --output-on-failure
```

这两项测试串行运行。缺少数据库配置、连接失败、挂载失败或内容不一致均报告失败；不会把环境故障计为通过。它们不启动或停止 IoTDB 服务，也不删除整个数据库。

迁移后的断言使用当前行为：非零截断应成功，扩展区域补零，历史版本保持只读。GRIB 关闭模式通过配置验证，不再要求删除 `grep` 子命令。

## IoTDB 客户端与其他测试

`iotdb_client_integration` 验证连接池、查询结果生命周期、并发读写及初始化失败后的重试，需要手动提供独立 IoTDB 环境。查看参数：

```bash
./build/bin/iotdb_client_integration --help
```

默认执行完整生命周期检查，并以连接池大小 1 和 4 分别运行 3 个写者、2 个读者、每个线程 24 次操作的并发检查。可配置入口来自 `concurrent-new` 分支，使用当前客户端及内容校验实现：

```bash
# 连接已准备好的独立测试实例；端口按实际环境设置。
# 密码可通过 IOTDB_PASS 提供，显式 --pass 的优先级更高。
./build/bin/iotdb_client_integration \
  --host 127.0.0.1 --port 27667 \
  --db client_check --table concurrent \
  --writers 4 --readers 3 --ops 40 --pool 1

# 仅运行并发场景，保留逐行内容和最终总数校验。
./build/bin/iotdb_client_integration \
  --host 127.0.0.1 --port 27667 \
  --db client_check --table concurrent \
  --writers 4 --readers 3 --ops 40 --pool 4 --concurrency-only
```

`--db` 是前缀，每次运行生成唯一数据库，成功或普通异常退出时尝试删除该次创建的数据库；不会复用同名业务数据库。`--table` 也是前缀，表名附加连接池大小。测试账户需要建库、建表、读写和删库权限；强制终止进程可能留下输出中 `Test database:` 指定的临时库。

| 参数 | 默认值 | 范围与含义 |
| --- | --- | --- |
| `--writers` | 3 | 1–256 个写线程 |
| `--readers` | 2 | 0–256 个读线程；读写线程总数不超过 256 |
| `--ops` | 24 | 每线程 1–1,000,000 次操作；写者数乘操作数不超过 1,000,000 |
| `--pool` | 依次测试 1、4 | 指定后仅测试该并发池，范围 1–256；不改变生命周期场景的池大小 |
| `--db` / `--table` | `iotdb_client_test` / `concurrent` | 1–40 个 ASCII 字母、数字或下划线，以字母开头 |
| `--concurrency-only` | 关闭 | 跳过初始化与结果集生命周期场景 |

写入交替使用 SQL 和 Tablet；读取扫描当前表，逐行核对时间、写者、序号和内容，并在所有线程结束后检查数量和重复数据。`elapsed_ms` 包含并发操作及其中的内容检查，不包含建库建表、最终完整性查询和清理。它与来源分支只读取少量行的耗时不能直接比较，也不代表文件系统的读写吞吐。

目录中的其他 GRIB、FTP、MySQL 对比程序需要各自的外部服务和数据集，不在默认 CTest 中自动运行。开发环境复用 Web 启停脚本的方法见 [开发说明](../../docs/knowledge/development.md)。
