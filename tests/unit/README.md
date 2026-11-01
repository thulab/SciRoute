# 单元测试

单元测试使用 GTest/GMock，目标名为 `unit_tests`，CTest 标签为 `unit`。
测试不连接真实 IoTDB，不挂载 FUSE，也不需要外部气象数据文件；GRIB 用例通过
ecCodes 自带 sample 在内存中生成小网格。构建仍需项目的 FUSE、ecCodes、IoTDB
C++ 客户端头文件及 GTest 依赖。最低 CMake 版本为 3.13，测试专用链接选项使用
该版本引入的 `target_link_options`。

## 构建与运行

在仓库根目录执行：

```bash
cmake -S . -B build-ut -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON \
  -DENABLE_FUSE_INTEGRATION_TESTS=OFF
cmake --build build-ut --target unit_tests -j 4
ctest --test-dir build-ut -L unit --output-on-failure -j 4 \
  --output-junit ut-results.xml
```

每个单元测试的超时为 30 秒。仅构建 `unit_tests` 时应使用 `-L unit`，避免调用尚未
构建的 Web/预检集成测试程序。JUnit 报告写入 `build-ut/ut-results.xml`。

检查同一进程中多轮执行的状态隔离和顺序依赖：

```bash
build-ut/bin/unit_tests --gtest_shuffle --gtest_random_seed=915 --gtest_repeat=10
```

运行单个模块：

```bash
build-ut/bin/unit_tests --gtest_filter='MetadataCache.*:InodeCacheTest.*'
```

## GCC 覆盖率

```bash
cmake -S . -B build-ut -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON \
  -DENABLE_FUSE_INTEGRATION_TESTS=OFF \
  '-DCMAKE_CXX_FLAGS=--coverage -fprofile-update=atomic' \
  -DCMAKE_EXE_LINKER_FLAGS=--coverage
cmake --build build-ut --target unit_tests -j 4
ctest --test-dir build-ut -L unit --output-on-failure -j 4
python3 -m venv build-ut/tools
build-ut/tools/bin/pip install gcovr
build-ut/tools/bin/gcovr --root . --filter 'src/' --filter 'include/iotdbfs/' \
  --html-details build-ut/coverage.html \
  --json-summary build-ut/coverage-summary.json \
  --txt build-ut/coverage.txt --fail-under-line 95 build-ut/tests/unit
```

`-fprofile-update=atomic` 用于并发测试的计数器，避免数据竞争损坏覆盖率统计。
修改编译选项或源文件后，重新统计前应清除该构建目录中的旧 `*.gcda` 文件。
报告仅统计 `unit_tests` 实际编译的业务实现和头文件，不能代表整个仓库的覆盖率。
`--fail-under-line 95` 在该统计范围的行覆盖率不足 95% 时返回失败。

## Linux GCC 内存和未定义行为检查

```bash
cmake -S . -B build-ut-sanitize -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON \
  -DENABLE_FUSE_INTEGRATION_TESTS=OFF \
  '-DCMAKE_CXX_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie' \
  '-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined -no-pie'
cmake --build build-ut-sanitize --target unit_tests -j 4
ASAN_OPTIONS=detect_leaks=1 UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
  build-ut-sanitize/bin/unit_tests \
  --gtest_shuffle --gtest_random_seed=916 --gtest_repeat=10 \
  --gtest_output=xml:build-ut-sanitize/results.xml
```

Sanitizer 的检查范围为参与插桩的代码，系统 ecCodes 动态库未重新插桩。

Linux UT 通过测试专用链接包装为位置读写、截断、创建、删除、同步和可执行路径查询注入
受控故障。脚本只在当前测试线程的作用域内生效，退出作用域后恢复真实系统调用；
同时覆盖 GCC 优化构建生成的 FORTIFY 调用。应用程序目标不使用这些包装。
文件写出用例临时设置当前测试进程的文件大小上限，验证写入失败后保留原文件和清理
临时文件；每次调用后恢复资源限制和信号处理器，不需要填满宿主磁盘。

## 测试维护

- 临时文件使用独立目录，环境变量和进程级配置在退出测试时恢复。
- 全局缓存的键必须对每个 fixture 实例唯一，支持 `--gtest_repeat`。
- 并发用例通过完整结果、版本和持久化回调验证行为，不使用允许随机失败的阈值。
- 参数化 GRIB 用例覆盖 GRIB1 和 GRIB2；数值预期来自明确的小网格。
- 实际业务缺陷保留失败断言，不通过禁用、跳过或放宽断言掩盖。

本次执行结果和已修复缺陷见 2026-09-15 UT 报告 (local archive: `docs/reports/ut-report-2026-09-15.md`)。
