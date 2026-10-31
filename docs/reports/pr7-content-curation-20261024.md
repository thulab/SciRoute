# PR #7 内容占比与归档整理（2026-10-24）

## 统计口径

PR：[降低单次查询成本](https://github.com/SpriCoder/IoTDB-FS-C/pull/7)。
基线 `master=74f37159104d98e6db6b0f495bc5805c13b61097`，整理前 head
`6e637945e13e107709aba733eb00ded350629750`。按三点 diff 的新增行统计，
不是净增行，也不是提交间 diff 的总和；Git 的文件重命名检测保持开启；跨目录重命名按目标路径归类。

“产品源码”指 `src/` 与 `include/`；“全部代码”另含测试、实验程序和构建脚本，
按 `.c/.cc/.cpp/.cxx/.h/.hh/.hpp/.hxx/.py/.sh/.bash/.cmake/.js/.ts/.mjs`
及 `CMakeLists.txt` 统计。JSON 规格、结果和手工 Markdown 不算代码。

## 整理前

| 类别 | 变更文件 | 新增行 | 占全部新增行 |
| --- | ---: | ---: | ---: |
| 产品源码 | 81 | 13,089 | 7.82% |
| `tests/` 测试与探针 | 52 | 15,875 | 9.49% |
| 实验工具、规格及结果 | 336 | 130,229 | 77.82% |
| `docs/` 文档 | 68 | 7,918 | 4.73% |
| 构建、CI、根目录说明等 | 12 | 233 | 0.14% |
| 合计 | 549 | 167,344 | 100.00% |

全部代码新增 68,809 行，占 41.12%；产品源码占 7.82%。两者分母相同，不能
将“实验目录”整体称为非代码，因为其中有可复现的实验程序和测试。
旧 `experiments/single_file/results/` 单独新增 81,230 行，占 48.54%。

## 整理范围

共 74 个原已跟踪文件（8,695,965 bytes）取消跟踪，原路径本地文件保留：

- 66 个单文件实验生成结果：JSON/JSONL、manifest、日志摘要和压缩原始行。
  同目录 10 篇手工总结继续跟踪，历史分析脚本保持不变。
- `experiments/multi_file/feasibility-20261013/metadata.json`：生成的旧可行性快照。
- 两个已退出正式构建的源码副本：
  `experiments/legacy/frozen-typed/test_QueryRegistrationSocket.cpp.txt` 与
  `experiments/legacy/hdf5-catalog-strict/dataset_catalog_probe.cpp`。
  历史协议说明、冻结源码定位和现行语义测试继续跟踪。
- 五篇已被后续契约或最终报告覆盖的文档：旧服务器环境快照、HDF5 子结构索引
  初步实现验收、V04 根校验排查、根校验试验及 V03/V04 中间性能排查。
  具体相对路径见归档清单。

所有现行 `tests/unit/`、versioning probes、实验 runner 的测试、实验规格和固定组
catalog 保留。6 份跨版本 acceptance 收据作为小型证据索引保留，不按 JSON
后缀一概忽略。GRIB render 资源复测等尚未被统一总结的负结果报告保留。
V04 的根校验与 head 热路径负结果及回退决定继续见
[最终 head 热路径报告](version-directory-v04-head-hotpath-optimization-20261017.md)。

`.gitignore` 同时屏蔽未来生成的实验 `work/`、原始结果和指定旧记录。
新增规则不覆盖测试源码、规格或人工实验总结。指向取消跟踪结果的 Markdown
链接改为整理前不可变 Git 提交链接，干净 clone 仍可定位原证据。

## 恢复与校验

本地完整归档：

```text
experiments/cross_version/work/pr7-content-curation-20261024-6e63794/
├── manifest.json       # 原始 HEAD、相对路径、字节数、逐文件 SHA-256
├── files.tar.gz        # 74 个原件，保留原相对路径
├── receipt.json
└── before-numstat.txt
```

服务器归档目录：

```text
/data_01/zzy/iotdb-fs-exp/document-archives/20261024-pr7-content-curation-6e63794/
```

- `files.tar.gz` SHA-256：`dd68c871e564266979b7e366dc3b718bca52469cbce340a02114d140ea19d4d2`
- `manifest.json` SHA-256：`1c17f29e1f5e2b79cca0eeb4337f62c6d981f8c33e73cb5b004cfcd9e8cef78a`

本地与服务器归档均已逐项核对 74 个成员的 SHA-256；服务器校验收据为
`server-verification.json`。本地原件字节不变。整理前 Git 提交
仍保留完整内容，历史数据也可通过该提交恢复。归档包仅保存本轮取消跟踪的
原件，不声称包括所有既有本地/服务器 `work/` 数据。

## 整理后

| 类别 | 变更文件 | 新增行 | 占全部新增行 |
| --- | ---: | ---: | ---: |
| 产品源码 | 81 | 13,089 | 15.62% |
| `tests/` 测试与探针 | 53 | 15,875 | 18.94% |
| 实验工具、规格及结果 | 267 | 46,935 | 56.00% |
| `docs/` 文档 | 64 | 7,651 | 9.13% |
| 构建、CI、根目录说明等 | 12 | 263 | 0.31% |
| 合计 | 477 | 83,813 | 100.00% |

新增行减少 83,531 行（49.92%），删除行由 915 变为 1,594。
全部代码新增 68,393 行，占 81.60%；产品源码仍为 13,089 行。
减少的代码新增行来自已退出正式构建的 strict catalog 探针副本。清理规模不是
运行时优化，不改变性能实验结果，也不从现行测试中删减测试用例。

- 74 个取消跟踪文件的本地原件与本地、服务器归档 SHA-256 一致。
- 74 个原路径和未来 work/raw 输出均命中忽略规则；当前测试、手工总结与 acceptance 收据不被忽略。
- 取消跟踪文件没有残留的本地 Markdown 入链；原始证据链接已改为不可变历史提交。
- 528 个冻结候选构建输入仍全部受跟踪，内容哈希与整理前一致。
- 查询模块依赖方向检查和本轮 `git diff --check` 通过。
- 本轮只改变跟踪范围与说明，没有重新构建或重跑性能实验。
