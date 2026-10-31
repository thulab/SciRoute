# GRIB解码后渲染复用消融

只新增run，不新建语料或实验case；固定封印源仍为原FM1 `collection-r2`。A精确绑定已验收 `format_catalog` 产品 `f1b89624…` 及前轮288条独立审计；B/C同一候选产品，所有配置均使用原 `grib_catalog_client` 的相同JSON发现及逐命中typed取值，交付40,960 B owned Float32/mask。

| 阶段 | 独立发布case | 配置／campaign | 两臂查询数 |
| --- | --- | --- | ---: |
| 小门槛 | G-H1-N8，N8/V1/C8/D4/K2/S0/Latest | A/B/C，1 campaign | 72 |
| 正式 | G-H1-N128，独立N128/V1/C128/D4/K2/S0/Latest | A/B/C，3 campaign | 216 |

每臂每配置2暖身＋10正式；预登记顺序ABC／BCA／CAB，每块新mount和FS-index上下文，查询臂交替。FS-index复用原SQLite及相同封印oracle，禁止重发／重建。R/Rd/Rq在每个campaign同块两臂求比，再取三轮中位数；所有慢值与暖身保留。保留以完整T与R共同判断，不以局部渲染计数择优。

A不发送新配置；B为 `query.grib.geometry_cache.enabled=false`，C为 `true`。实际启动收据和每个取值profile的 `grib.geometry_cache.enabled` 必须一致。candidate每命中64×64单位检查：value4096、double中间数组0、经纬度轴0、完整网格快路1、moved4096／copied0；geometry hits/misses/builds/replacements为0。完整非插值固定case的B/C均不走几何映射查找；不能据此声称ROI缓存性能收益。A不存在新计数时标未插桩。

沿用共享两逻辑CPU2、3／父2核，query组2 GiB，原DataNode15232 MiB堆及2176 MiB direct单列；原数据库／账号／许可证不变。实际环境、每查询及周期资源、mount启动时间、所有源／索引／journal摘要和恢复证据分别记录。无自动重试、无自动放大。

`run.sh`显式选择已有 `experiments/single_file/work/session-20261012/venv/bin/python` 所在PATH，并清除客户端LD库覆盖。系统python只用于离线mock/help。候选必须先通过独立build receipt与focused/all-unit XML；建议顺序：

1. 新work目录执行 `--smoke --prepare-only`：只读核验源／构建，导入依赖，保存proof与源码闭包；不改服务／资源或查询。
2. 相同参数及同一个preflight work执行 `--smoke --verify-prepared`：另一进程严格核对proof、闭包JSON往返和依赖收据；不做normalize掩盖类型差异。
3. 由主代理显式在另一个新work启动 `--smoke`。N8完整72条、独立审计及恢复通过后，才在新work启动正式入口，指定 `--smoke-report`。

具体binary/library/inspector/source路径均显式传参。正式N128不接受N512中的C128替代。旧GRIB发现runner、client、catalog和native helper均不修改；本入口复用原有规划、资源和恢复原语。
