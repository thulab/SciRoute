# IoTDB-FS C/C++

在 `IoTDB-FS-C/` 仓根执行命令。按任务查 [README.md](README.md)、[CONTEXT.md](CONTEXT.md)及 [CMakeLists.txt](CMakeLists.txt)，无需通读。源码不自动证明论文或性能结论。

## 工作约定

- 默认中文，先说结果；进度只报新发现、决定和阻塞，交付说明改动、实际检查及未验证项。
- 修改／修复请求应推进至实现、必要验证和交付。任务内可逆操作直接执行，普通选择自行判断；仅在缺失信息实质影响结果或授权不足时提问，并继续独立工作。沿用既有授权，需确认时先准备可审查结果。
- 在系统／开发者约束内，用户明确指令优先于本文件和技能。技能导致暂停时，链接文件、引用条款并说明原因，不把建议扩展为审批。
- 修改前检查 Git 状态，保留无关改动。用 `rg` 定位后读取相关文件；独立检查可并行。简单任务无需计划、委派或多轮审查。除非用户要求，不写兼容性代码；避免任务外重构、文档和实验。
- 清理优先移到 `.trash_bin/`；环境重置不属于构建。Git 写操作和共享环境变更须有用户明确授权；暂存显式列出的任务文件，提交前核对暂存差异，只包含本次改动。

## 模块与协作

独立调查、实现或验证可在有收益时委派；明确目标、文件归属和验收条件，避免重复调查及重叠写入，主智能体负责整合验证。以下名称仅为专长提示，不要求预置同名智能体。

- FUSE／POSIX：`src/main.cpp`、`src/InodeCache.cpp`，`fuse_specialist`。
- IoTDB／Structured Blob／版本与跨文件：`src/IoTDBConnector.cpp`、`src/StructuredBlobStorage.cpp`、`src/IoTDBStructuredBlobBackend.cpp`、`src/storage/versioning/`、`src/CrossFileClient.cpp`，`iotdb_specialist`。
- 查询核心：`src/query/core/`；Arrow 训练：`src/query/training/arrow/`；格式适配器：`src/formats/{arrow,grib,hdf5}/`，头文件位于对应 `include/iotdbfs/` 目录。GRIB 空间操作可用 `grib_specialist`。
- Web：`src/iotdb_fs_web.cpp`；自然语言检索：`src/SemanticGrepClient.cpp`，后者可用 `semantic_grep_expert`。测试、发布与文档使用通用角色。

公共查询核心保持格式无关；边界变更查 [模块布局](docs/knowledge/query-module-layout.md)及相关 `docs/contracts/`。新增文档和原始输出按 [文档保留规则](docs/README.md)分类。

## 构建与验证

- 按改动选择最小有效检查，缺陷先复现；测试核对内容、边界与失败语义。低影响可逆改动无需新增复刻实现的测试。通过后只因新改动、失败或未解疑点扩大／重复验证；CI 必需门禁仍须完成。
- 纯文档只查差异、路径和事实。代码验证按需读取 [验证技能](.agents/skills/iotdb-fs-verify/SKILL.md)和所选测试说明，无需遍历所有阶段。
- C++17；FUSE、ecCodes、定制 IoTDB SDK、Arrow、HDF5 的版本和配置见 CMake 及 [CI 依赖说明](ci/README.md)。先核对平台／依赖；缺失时报告阻塞并继续静态检查，不替换锁定依赖。
- 依赖就绪后：`cmake -S . -B build -DIOTDB_ROOT=/实际/client/目录`，再 `cmake --build build --target iotdb_fs iotdb_fs_web`。复用匹配的构建目录，避免无故清理／重装。
- 单元测试已注册 CTest：配置 `-DBUILD_TESTS=ON -DENABLE_FUSE_INTEGRATION_TESTS=OFF`，构建 `unit_tests`；运行相关 GTest filter 或 `ctest --test-dir <构建目录> -L unit --output-on-failure`。详见 [单元测试](tests/unit/README.md)；只构建单元目标时不要跑未筛选 CTest。
- 真实 IoTDB／FUSE 按 [集成测试](tests/integration/README.md)使用独立数据库和目录，先核对实例、挂载点及副作用。`Test.md` 仅为历史人工记录。
- 构建、功能与性能结论分别表述；未执行的检查明确标注，提示词调整不等于已测得的效率收益。

## 固定实验组

正式多文件／跨版本性能实验使用 `experiments/fixed_matrix/catalog.json` 的已确认组和封印 corpus/case，按 `docs/deliverables/fixed-experiment-matrix-plan.md` 记录实际环境、资源、完整时钟和恢复证据。优化只新增 run，不重新发布语料制造效果；新组须先确认并写入实验表。历史环境／资源证据不完整的数据不进入 FM1 的基线、统计或主张。

## 维护依据

2026-10-26 核对 OpenAI [GPT-6 提示最佳实践](https://developers.openai.com/api/docs/guides/latest-model#prompting-best-practices)和 [AGENTS.md 指南](https://learn.chatgpt.com/docs/agent-configuration/agents-md)。家族建议主要基于 Astra 观察，GPT-6.1 Sol 效果需在本仓任务验证。出现额外开销时再按 [维护实践](https://developers.openai.com/blog/maintaining-software-with-the-gpt-6-family)对照修正规则，无需每次任务重跑评估。
