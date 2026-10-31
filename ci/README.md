# CI 工具与依赖

仓库中的 `.github/workflows/pr-ci.yml` 会在 PR、`merge_group` 和 `master` 更新时运行。
合并门禁名称是 `pr-ci-required`；只有工作流检查、依赖验证、UT/覆盖率和 Sanitizer 全部
成功时该检查才成功。真实 IoTDB/FUSE 集成测试不在首期 PR 门禁内，按
`tests/integration/README.md` 单独运行。

## 配置自定义 IoTDB SDK

当前项目使用带 `TSDataType::OBJECT` 的自定义 C++ SDK。它没有纳入 Git 仓库，且不能用
官方最新版直接替代。请把经过核验的 SDK zip 放在可由 GitHub Actions 读取的 HTTPS 地址，
然后在仓库 **Settings → Secrets and variables → Actions → Variables** 中设置：

```text
IOTDB_SDK_URL=https://<受信任存储>/client-cpp-2.0.5-SNAPSHOT-object.zip
```

`ci/dependencies.lock.json` 固定了 SDK 内 177 个文件的 SHA-256 和整个 zip 的 SHA-256。
下载地址只允许 HTTPS 且不允许用户信息；下载、解包和每个文件校验均在安装完成前执行。
地址未配置、下载失败、校验失败或 SDK ABI 不匹配都会让 `pr-dependencies` 失败，随后
`pr-ci-required` 阻止合并。不要把 SDK URL 写入 PR 代码或提交凭据。

依赖安装工作流还会从固定 commit 下载 ecCodes、ecbuild 和 GoogleTest，构建并核验完整
安装前缀。缓存只复用带依赖锁文件哈希的已验证前缀；恢复后仍会逐文件重新校验。
Arrow C++ 21.0.0 使用 Apache 官方归档及锁定的 SHA-256；安装器从其 `cpp/` 构建
共享库，启用 IPC、LZ4 和 ZSTD，关闭与本项目无关的模块。LZ4/ZSTD 的 bundled
源码版本与 SHA-256 由该 Arrow 归档内的 `cpp/thirdparty/versions.txt` 固定。
验证时同时检查 Arrow 版本、构建选项、共享库及安装的 LICENSE/NOTICE。
Arrow 21.0.0 的 CMake 构建要求至少 3.25；CI 工具环境固定为 3.26.5。

## HDF5 当前依赖

HDF5 C 库锁定为 **1.14.4**，使用 [HDF Group 官方 release archive](https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.4.3/hdf5-1.14.4-3.tar.gz)。
SHA-256 为 `019ac451d9e1cf89c0482ba2a06f07a46166caf23f60fea5ef3c37724a318e03`，
已与该 release 的 [sha256sums](https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.4.3/hdf5-1.14.4-3.sha256sums.txt)
及下载字节核对。完整 CI 安装默认包含 HDF5；激活器输出 `HDF5_DIR=<prefix>/lib/cmake/hdf5`。
本次版本变更改变锁摘要，旧 CI 依赖前缀应重新构建，不向旧前缀覆盖安装。
官方 1.14.4 维护归档带 `-3` 后缀（release tag `hdf5_1.14.4.3`）；
CMake package 与 `H5get_libversion` 均报告 `1.14.4`，项目继续要求 EXACT 匹配。
历史报告中的 1.14.6 保留其实际测量版本，不作为当前依赖要求。

构建采用 shared-only、serial（无 MPI）、thread-safe **C API**，供 C++17 调用；
启用系统 zlib 的 DEFLATE，保留 h5dump 等工具。关闭 C++ wrapper、Fortran、Java、HL、
SZIP、插件包、ROS3/HDFS/subfiling 和示例/上游测试。线程安全库内部会串行化 API，
不等于并行执行 HDF5 操作。HDF5 1.14.4 提供 H5Dget_num_chunks、H5Dget_chunk_info、
H5Dread_chunk、H5FDregister、H5is_library_threadsafe 与 H5PLset_loading_state，
覆盖后续布局索引、VFD 和运行时安全探测需求。

`HDF5_ENABLE_PLUGIN_SUPPORT=OFF` 只禁止构建插件包，**不等于禁止动态加载**。
native probe/adapter 必须在打开用户文件前调用 `H5PLset_loading_state(0)`，
并限制 VOL/VFD、外部链接及外部 raw storage；生产 HDF5 adapter 已注册；查询 decoder 不调用动态插件。
系统 zlib 由操作系统提供（Linux 需要 `zlib1g-dev` 或同等开发包），未作为新源码依赖锁定；
应在实验报告记录其版本。安装校验检查实际共享库、CMake profile、thread-safe/DEFLATE
头文件宏及 COPYING/COPYING_LBNL_HDF5，不把完整 SDK 验证与单独 HDF5 安装混为一谈。

可以先在实验服务器的**新目录**仅安装 HDF5，不触碰现有挂载或 SDK：

```bash
python3 ci/install_dependencies.py --only hdf5 \
  --prefix /path/to/new-hdf5-1.14.4-prefix \
  --source-archive /path/to/hdf5-1.14.4-3.tar.gz \
  --cc gcc --cxx g++ --jobs 4
python3 ci/install_dependencies.py --only hdf5 \
  --prefix /path/to/new-hdf5-1.14.4-prefix --verify-only
```

省略 `--source-archive` 时从锁定 URL 下载。单独安装允许明确选择服务器已有编译器，
并将选择写入安装 marker；完整 CI 仍固定 gcc-11/g++-11。
单独前缀不能传给完整 SDK 激活器，构建实验 probe 时直接传
`-DHDF5_DIR=/path/to/new-hdf5-1.14.4-prefix/lib/cmake/hdf5`。

## 本地运行

```bash
python3 ci/install_dependencies.py \
  --prefix /tmp/iotdbfs-ci-deps \
  --sdk-archive /path/to/client-cpp-2.0.5-SNAPSHOT-object.zip \
  --jobs 4
python3 ci/activate_dependencies.py --prefix /tmp/iotdbfs-ci-deps
IOTDB_SDK_ROOT=/tmp/iotdbfs-ci-deps/iotdb \
  python3 ci/run_tests.py coverage --build-dir build-ci
IOTDB_SDK_ROOT=/tmp/iotdbfs-ci-deps/iotdb \
  python3 ci/run_tests.py sanitizer --build-dir build-ci-sanitize
IOTDB_SDK_ROOT=/tmp/iotdbfs-ci-deps/iotdb \
  python3 ci/run_tests.py release --build-dir build-ci-release
```

运行器每次使用 `cmake --fresh` 重新生成配置，避免复用构建目录并切换编译器时，
CMake 丢失覆盖率或 ASan/UBSan 编译参数。覆盖率与 sanitizer 使用各自的构建目录。
`release` 模式还会构建所有运行程序，执行无外部服务的集成 harness，安装到临时前缀并
运行 Web API smoke 检查。

也可以只验证已有前缀：

```bash
python3 ci/install_dependencies.py --prefix /tmp/iotdbfs-ci-deps --verify-only
```

报告校验器可单独使用：

```bash
python3 ci/check_reports.py coverage --build-dir build-ci
python3 ci/check_reports.py sanitizer --build-dir build-ci-sanitize
```

## 仓库设置

首次成功运行 `pr-ci.yml` 后，在 `master` 的 Ruleset/分支保护中：

1. 要求通过 PR 合并并禁止直接推送。
2. 将 Actions 检查 `pr-ci-required` 设置为 required status check，并绑定预期来源。
3. 要求分支在合并前与 `master` 最新提交同步。
4. 可选启用 merge queue；工作流已经声明 `merge_group` 触发。

GitHub 不会仅因为仓库中存在 workflow 就阻止合并；required status check 必须在远端设置。
若仓库接收 fork PR，SDK URL 必须是无需 PR secret 的只读地址。工作流使用固定的 Actions
提交 SHA，PR 代码不会通过 `pull_request_target` 获取高权限凭据。

## 本地校验

```bash
python3 -m unittest discover -s ci/tests -p 'test_*.py' -v
bash ci/lint.sh
```

当前门槛是 UT 业务代码行覆盖率至少 95%，不是 100%。覆盖率统计范围及剩余未覆盖路径见
`docs/reports/ut-report-2026-09-15.md`。
