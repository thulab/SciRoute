# 第一阶段大 N 优化：固定 FM1 A/B

本轮只比较第一阶段公共发现链路改动。A 为最新已接受 GRIB discovery 构建 `f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087`；B 由本轮实际 binary 和标准 `build-binding.json` 固定身份，无占位 SHA。产品差异必须恰好等于 `nscale_phase1.py` 的 `PRODUCT_DIFF`，两份候选 XML 均须包含全部唯一、已完成的新回归测试。测试总数从真实 XML 核验，不固定历史数量。A/B 编译器内容、Release flags、Arrow/HDF5 和 SDK 配置必须一致。

| tier | 已封印独立 case | 四 campaign 配置顺序 | 每配置／臂 | 查询行 |
| --- | --- | --- | --- | --- |
| n8 | GRIB、HDF5 N8/V1/C8/D4/K2/S0/Latest | AB、BA、BA、AB | 5 暖身＋10 正式 | 480 |
| n128 | GRIB、HDF5 N128/V1/C128/D4/K2/S0/Latest | AB、BA、BA、AB | 5 暖身＋10 正式 | 480 |
| n512 | GRIB、HDF5 N512/V1/C512/D4/K2/S0/Latest | AB、BA、BA、AB | 5 暖身＋10 正式 | 480 |

共六个现有 case、1,440 行（480 暖身、960 正式）。每格式每 tier 为 240 行。顺序固定，不按效果追加轮次、重试或剔除慢值。每配置独立 mount、独立 FS-index context；两臂按 repeat＋campaign＋格式＋configuration 奇偶交替。先完成 N8 全部采集及服务／资源恢复，再启动 N128；N512 必须以本轮 N128 完整恢复报告为门禁。各门禁绑定同一 A/B、科学输入集合、位置绑定、资源策略与执行源码闭包，并核验原 queries/bindings/analysis SHA。

复用原 `collection-r2` 的 corpus/case/oracle、完整 raw/FS 文件、通用 SQLite 索引、发布 head，以及已接受 HDF5 原生位置绑定。三个 N 是三份独立完整语料，不从 N512 选子集冒充 N8/N128。查询只复制 owner journal 到本轮私有执行目录；不生成、不发布、不重建索引、不新增位置绑定。HDF5 固定受管原生文件、`native_cache=true`、客户端 `batch=True`；GRIB 固定 `format_catalog` 发现和原有 typed 精确取值。缓存容量、线程数和两臂 helper 参数保持原值。

主时钟为客户端拥有 Float32 值及 uint8 mask 时结束的完整查询，逐行验证 `T=d+q`。profile 和 oracle 在计时外，A/B 使用相同 profile 设置；不新增只在 B 执行的计数或探针。新所有权／selector 机制使用单元回归和源码审计验收。每 campaign 先取同配置两臂中位 T/d/q 的 R/Rd/Rq，再计算配对 B/A；同时列原始 IoTDB-FS 与 FS-index T 的 B/A、各轮方向和全部慢值。指标下降不自动等于完整 T 加速。

沿用原 balanced root v2：guest vCPU 2/3 有效及独占 cpuset、共享两核 quota、query 叶组 2 GiB、原 DataNode heap 15,232 MiB/direct 2,176 MiB，以及原账号／许可证。保留原服务、全部线程归组、配置字节和临时 cgroup 清理门禁。磁盘预检沿用最低 1% 可用空间；实际空间写入环境证据。资源包络包含计时外审计，不称查询独占资源；不作冷盘、并发或物理核无竞争主张。原始大文件留服务器，本地只提取派生摘要和 SHA，不输出凭据。

所有阶段位于新根目录 `experiments/cross_version/work/nscale-phase1-20261023/`。参数为 `--repo --work --binary --baseline-binary --build-binding --baseline-build-binding --library --inspector --source-collection --current-collection --tier n8|n128|n512`。先在独立 preflight work 执行 `--prepare-only`，再以相同参数及 work 执行 `--verify-prepared`；二者只读零科学查询，不改服务或资源。正式采集使用新的 work。N128 另传 `--gate-report <n8/report.json>`；N512 传 `<n128/report.json>`。prepare、verify、worker 均复核同一源／构建证明和执行闭包。正式启动及服务操作由主任务统一执行；本目录登记本身不启动任何实验。
