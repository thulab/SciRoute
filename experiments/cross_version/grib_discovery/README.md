# GRIB定位复用／JSON发现消融

仅使用已封印FM1语料，不生成、发布、重建SQLite索引或改变固定case。

| 阶段 | 固定case | campaign | 配置 | 每配置查询数 |
| --- | --- | ---: | --- | ---: |
| N8小门槛 | `FM1-grib-N8-V1-C8-S0-base-latest`，独立N8 | 1 | A／B／C | 两臂各2暖身＋10正式，共24 |
| N128正式 | `FM1-grib-N128-V1-C128-S0-base-latest`，独立N128 | 3 | A／B／C | 同上 |

N8共72条、N128共216条；N8不汇入正式结果。D=4、K=2、S0、Latest、owned输出40,960 B保持既有case。N128须先提供同一候选二进制、服务及资源恢复完备的N8报告；两个阶段分别显式启动，不自动放大。

- A：已验收E4二进制 `b7e7d995…`，原typed发现。
- B：本轮候选二进制，typed发现；只体现定位提示复用。
- C：同一候选，`format_catalog`直接JSON交付精确path/version/revision/unit_id/offset/length。
- 每配置均配对同一预建SQLite FS-index；q保留原FM1逐命中typed取值、Arrow/FUSE、客户端owned Float32/mask，未改批量或并发。A/B直接调用既有客户端。
- 每campaign配置顺序预登记ABC／BCA／CAB。每配置新mount与FS-index上下文，查询臂逐次交替；2条暖身均保留，10条正式无速度删选。
- 完整T计时包含发现控制、元数据解读、所有取值与owned数组；profile／oracle校验在T外。每campaign先算同块R／Rd／Rq，再报告三轮中位数及范围。完整T和同块R共同决定是否保留；不以局部计数代替收益。
- 数据库与客户端／两查询臂共享隔离CPU2、3及2核父配额；query组2GiB，DataNode原15232MiB堆／2176MiB direct单列。沿用原数据库、账号和许可证。控制器记录前后及逐秒实际资源、mount启动成本、全量环境、head／journal／SQLite摘要及恢复收据；失败不自动重试。

从仓库根运行 `experiments/cross_version/grib_discovery/run.sh --help` 查看参数。候选需独立build receipt和无失败的focused／all-unit XML；A路径取E4既有receipt。`--source-collection`仅接受原`fixed-matrix-20261021/collection-r2`。各次`--work`使用全新目录；科学源journal保持归档，每个配置使用私有副本。`--prepare-only`仅只读核验并写proof，不修改服务／资源，不执行查询；再次执行使用新work目录。

默认是N128正式入口，须显式指定`--smoke-report`。N8指定`--smoke`。候选通过前不要运行非`--prepare-only`入口。没有产品安装、默认配置修改或HDF5实验。

实际采集须显式使用已验收的 `experiments/single_file/work/session-20261012/venv/bin/python`，或将该venv的 `bin` 放到PATH首位再运行入口；系统python仅用于离线mock／help。venv沿用E4环境，bootstrap导入门槛必须在任何服务／资源操作前通过。
