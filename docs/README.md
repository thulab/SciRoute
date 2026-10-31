# 文档索引与保留规则

`docs/` 在 Git 中只保留查询／存储契约、架构与操作知识、实施计划、阶段性验收／实验报告，以及用户明确要求落盘的手工产物。实验原始输出和已被总结覆盖的调查中间记录在服务器归档。本地副本保留，不影响继续查验或复现实验。

## 四类文档

| 目录 | 保留内容 | 文档数 |
| --- | --- | ---: |
| [contracts/](contracts/) | 查询、存储格式、身份、预算、缓存、语义绑定与运行时协议 | 27 |
| [knowledge/](knowledge/) | 架构、操作／迁移手册、研究分析、优化原理与方法 | 37 |
| [reports/](reports/) | 有明确数据、计时口径和验证边界的阶段性验收／实验报告 | 103 |
| [deliverables/](deliverables/) | 用户要求落盘的实施／实验计划与手工总结 | 22 |

每篇文档只保留一个分类副本。优先按用途分类：协议进 contracts；操作和可复用分析进 knowledge；实测阶段结论进 reports；明确要求交付的计划／总结进 deliverables。用户手册的分章节结构保留在 knowledge/user_manual/。

## 主要入口

| 主题 | 文档 |
| --- | --- |
| FS-index 独立组件 | [构建与 API](../fs_index/README.md)、[迁移验收](reports/fs-index-migration-20261031.md)、[增量维护计划](deliverables/fs-index-incremental-maintenance-plan.md)、[维护 v2 契约](contracts/fs-index-maintenance-v2.md)、[维护验收](reports/fs-index-maintenance-20261031.md)、[维护生命周期](reports/fs-index-maintenance-lifecycle-20261031.md)、[HDF5 句柄缓存补测](reports/fs-index-hdf5-handles-20261031.md)、[旧查询矩阵复测范围](deliverables/fs-index-query-refresh-scope-20261031.md) |
| 使用、开发与部署 | [用户手册](knowledge/user_manual.md)、[开发说明](knowledge/development.md)、[平台支持](knowledge/support-matrix.md)、[CI 设计](knowledge/pr-ci-design.md) |
| 格式与实验方法 | [格式设计](contracts/format.md)、[实验设计](knowledge/experiment.md)、[查询工作负载模型](knowledge/fs-query-workload-mathematical-model.md)、[查询模块边界](knowledge/query-module-layout.md) |
| Arrow 设计与协议 | [实施计划](deliverables/arrow-query-implementation-plan.md)、[typed 查询](contracts/arrow-typed-query.md)、[layout](contracts/arrow-layout-format.md)、[缓存](contracts/arrow-decoded-fragment-cache.md)、[训练窗口](contracts/arrow-window-assembly.md)、[迁移](knowledge/arrow-data-migration.md) |
| Arrow 查询阶段报告 | [局部查询 A–E](reports/arrow-query-paths-results-20260930.md)、[压缩／过滤／并发](reports/arrow-query-extended-results-20260930.md)、[布局与系统对照](reports/arrow-layout-system-control-results-20260930.md)、[多实验共享对照](reports/arrow-shared-training-views-results-20260930.md) |
| 训练与派生布局 | [公开训练计划](deliverables/public-training-baseline-plan.md)、[环境设置](knowledge/public-training-environment-setup.md)、[50-step 基线](reports/public-training-ab-long50-20260929.md)、[prepared 注册](reports/public-training-prepared-registration-20260930.md)、[执行优化](reports/public-training-execution-cost-20260930.md)、[64/128 点训练块](reports/electricity-compiled-training-blocks-20260930.md) |
| HDF5 设计与协议 | [当前两种查询模式](contracts/hdf5-query-modes.md)、[模式整理验收](reports/hdf5-route3.md#modes)、[实施计划](deliverables/hdf5-query-implementation-plan.md)、[typed 查询](contracts/hdf5-typed-query.md)、[摄取 layout](contracts/hdf5-layout-ingestion.md)、[共享缓存](contracts/hdf5-shared-cache.md)、[科学绑定](contracts/hdf5-scientific-binding.md)、[dataset 发现](contracts/hdf5-dataset-discovery.md)、[迁移](knowledge/hdf5-data-migration.md) |
| HDF5 阶段报告 | [强基线与消融](reports/hdf5-query-benchmark.md)、[OISST 科学查询](reports/hdf5-scientific-query-benchmark.md)、[GEOS 单文件](reports/hdf5-geos-pilot-results.md)、[生产多时次](reports/hdf5-geos-production-multitime.md)、[16 时次筛选](reports/hdf5-geos-time-scale-results.md)、[Native 进程池验收](reports/hdf5-native-process-probe-20261026.md)、[规模与 Indexed 并行探针](reports/hdf5-native-indexed-scale-probe-20261026.md)、[Native 回退与读取优化](reports/hdf5-native-read-probe-20261026.md) |
| HDF5 子结构索引优化 | [契约](contracts/hdf5-dataset-substructure-index.md)、[N=128 验收](reports/hdf5-substructure-index-n128-20261015.md)、[谓词预编译](reports/hdf5-predicate-precompile-ablation-20261015.md)、[manifest＋marker 合并消融](reports/hdf5-manifest-marker-ablation-20261015.md)、[方向 3 实验与优化总报告](reports/hdf5-route3.md) |
| 版本目录与内容共享 | [实施计划](deliverables/version-directory-content-sharing-plan.md)、[冻结与实现契约](contracts/version-directory-content-sharing-v1.md)、[写入口审计](reports/version-directory-v00-write-audit-20261015.md)、[V00 原语验证](reports/version-directory-v00-validation-20261015.md)、[V03 准入验收](reports/version-directory-v03-validation-20261016.md)、[V04 payload 共享验收](reports/version-directory-v04-validation-20261017.md)、[head 热路径试验及最终回退](reports/version-directory-v04-head-hotpath-optimization-20261017.md)、[N=8 单／跨版本完整矩阵](reports/version-directory-v04-n8-version-matrix-pilot-20261017.md)、[大文件有利场景](reports/version-directory-v04-large-file-upper-20261017.md)、[GRIB 尺寸与缓存诊断](reports/version-directory-v04-grib-size-diagnosis-20261018.md)、[GRIB 内容共享解码缓存修复](reports/version-directory-v04-grib-native-cache-fix-20261018.md) |
| 跨请求 payload 块缓存 | [共享与回收契约](contracts/payload-read-coalescing.md)、[实现验收与大 S 短探针](reports/payload-block-cache-large-s-probe-20261027.md)、[ON／OFF 慢化诊断](reports/payload-cache-regression-diagnosis-20261027.md)、[不同 message 跨请求复用探针](reports/payload-cache-distinct-message-probe-20261028.md) |
| HDF5 indexed 内容共享尝试（已回退） | [现行精确源缓存契约](contracts/hdf5-shared-cache.md)、[历史实现与正确性验收](reports/hdf5-indexed-content-cache-20261028.md)、[原 S2 ON/OFF 消融](reports/hdf5-content-cache-s2-ablation-20261028.md)、[回退后 S 增长及 FS 对照](reports/sgrowth-after-rollback-20261028.md) |
| 查询与发布优化汇总 | [用户要求的优化总览](deliverables/uncommitted-optimization-summary-20261011.md)、[累计查询复验](reports/hdf5-geos-cumulative-20261010.md)、[发布 checkpoint](reports/hdf5-geos-publication-batching-20261010.md)、[transfer 负结果](reports/hdf5-geos-publication-transfer-20261010.md)、[最后一轮 JOIN 验证](reports/dataset-unused-join-ab-20261011.md) |
| 论文 4.2 单文件实验 | [查询／会话／发布验收与生命周期补跑](reports/section42-single-file-20261026.md) |
| GRIB 训练与跨格式边界 | [ERA5 计划](deliverables/era5-grib-training-baseline-plan.md)、[D0 正确性](reports/era5-d0-single-sample-acceptance-20261007.md)、[D1 50-step 验收](reports/era5-d1-50step-acceptance-20261007.md)、[Q2 瓶颈](knowledge/era5-d1-q2-bottleneck-analysis-20261007.md)、[GRIB/HDF5 计时边界](reports/grib-hdf5-query-boundary-results-20261007.md) |
| 有界资源与配对比值复测 | [冻结方案](deliverables/resource-bounded-ratio-retest-plan.md)、[两核复测结果](reports/resource-bounded-ratio-retest-20261021.md)、[查询主线问题与分析](deliverables/query-mainline-analysis.md)、[固定实验组与重采集](deliverables/fixed-experiment-matrix-plan.md)、[FM1完整首轮验收](reports/fixed-matrix-fm1-20261021.md)、[语料／case／run 身份](contracts/experiment-identity.md) |

## 服务器归档

2026-10-24 的 PR #7 整理将 74 个生成结果、已废弃测试副本和前期记录归档并取消跟踪。保留现行测试、实验脚本／规格、手工结果总结、6 份版本验收收据与最终负结果报告。归档路径、逐文件 SHA-256 和清理前后规模见 [PR 内容整理记录](reports/pr7-content-curation-20261024.md)。被后续总结覆盖的旧环境快照、子结构索引初步实现记录与三篇 V04 中间排查保留本地及服务器原件；V04 的回退决定与负结果统一从[最终 head 热路径报告](reports/version-directory-v04-head-hotpath-optimization-20261017.md)查验。

2026-10-11 整理前的 **184 个 Git 文档文件**已完整复制到 `root@192.168.99.49`：

```text
/data_01/zzy/iotdb-fs-exp/document-archives/20261011-docs-curation-79e8cbb
├── manifest.json
├── documents.tar.gz
└── docs/...
```

归档保留原相对路径。`manifest.json` 记录精确 HEAD、保留／取消跟踪清单、每个文件的字节数与 SHA-256；184 个解包文件已逐项核验。该归档记录整理前的原件，不随本地文档编辑更新。

归档后不再纳入 Git 的清单包含 **83 个文件**：33 篇被后续总结覆盖的 Markdown 中间记录，另有 50 个 JSON/JSONL/XML 或原始结果包。SQL／授权／目录读取的逐项消融与发布构造诊断，统一从 [优化总览](deliverables/uncommitted-optimization-summary-20261011.md)查验；其独立计时口径和负结果仍保留，不能将各轮比例连乘。旧环境状态不能代替当前环境验收。

标为“服务器归档”的引用可按 `docs/...` 相对路径在上述目录找到。其余已长期被忽略的 `docs/results/` 原始实验树继续保留于既有服务器实验目录；本次 184 文件归档不声称包含所有本地 results。具体服务器 run 路径与证据 hash 见对应阶段报告，常用根目录为 `/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/` 和 `/data_01/zzy/iotdb-fs-exp/era5-grib-baseline/`。

2026-10-19 的六篇 HDF5 方向 3／两模式阶段报告已合并为[单篇总报告](reports/hdf5-route3.md)。原文完整归档于服务器 `/data_01/zzy/iotdb-fs-exp/document-archives/20261019-hdf5-route3/`，六成员 SHA-256 已核验；原路径、清单和归档哈希见总报告的[证据章节](reports/hdf5-route3.md#evidence)。本地副本位于被忽略的 `experiments/cross_version/work/hdf5-route3-docs-curation-20261019/`。

## 后续落盘规则

当前 HDF5 数据通路变更见[数据库目录＋原生FS方案](deliverables/hdf5-native-filesystem-plan.md)和[两模式契约](contracts/hdf5-query-modes.md)。同名历史数据库VFD与当前FS读取必须按run区分。

- **保留**：现行协议、身份与预算契约、架构边界、操作／迁移流程、可复用的研究分析、明确阶段结论，以及用户指定的手工交付物。历史报告保留其日期和实施快照，不冒充当前状态。
- **归档并忽略**：自动生成的 JSON/XML/日志/profile、数据样本、二进制结果、临时源码／构建快照，以及已有阶段总结覆盖的中间排查和单项运行记录。
- **先归档后取消跟踪**：不要删除唯一证据副本。保留文件与服务器副本按 SHA-256 核验；Git 已跟踪文件需要取消跟踪，`.gitignore` 本身不能完成此操作。
- **新增手工资产按目录归类**：放入四个分类目录之一，并更新本页入口；不再逐文件修改 `.gitignore`。自动输出放在被忽略的 `docs/results/` 或服务器实验目录，不要写入四个手工文档目录。
- **用户指定优先**：`uncommitted-optimization-summary-20261011.md` 等明确要求落盘的手工文件持续保留；配套机器清单在服务器归档，不因文件名包含日期而被整体移除。

本规则不影响测试 fixtures、benchmark 脚本、`tests/` 下的 README、配置或源码，也不删除 Git 历史中的旧证据；它只约束当前文档树与未来提交。文件移动使用普通文件操作；旧路径需取消 Git 跟踪，新路径由用户暂存／提交。
