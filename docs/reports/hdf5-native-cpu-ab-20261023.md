# HDF5原生热路径三项CPU优化：固定组A/B验收

结论：候选B未通过完整查询性能验收，未纳入主线。已按字节摘要核验，将两个产品文件及对应测试恢复到本轮开始前的已接受A；候选源码、patch、构建、测试和全部实验记录保留。原数据库的磁盘余量阈值按用户确认设为1%，服务及资源已恢复。本轮没有重新发布语料、补绑定或重建FS-index。

## 1. 比较范围与固定条件

| 项目 | 本轮条件 |
| --- | --- |
| A | 已接受构建`f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087` |
| B | `1e18bb26604840083288cb509ff0ee7c5e5005534d415858e4af565d625af2c1`；仅改Hdf5SourceDriver.cpp、Hdf5FormatQueryAdapter.cpp |
| 三项修改 | 无新增费用的归还快路；仅miss分配State；native不构造indexed decoded-cache身份 |
| 固定组 | 独立`FM1-hdf5-N8-V1-C8-S0-base-latest`及独立`FM1-hdf5-N128-V1-C128-S0-base-latest`；D4、K2、Latest；后者不是N512中的C128 |
| 通路 | 两配置均为native-hdf5、受管FS精确版本、会话缓存开启、typed批量开启；相同原生FS-index及缓存参数 |
| 交付与时钟 | 客户端拥有Float32 values/masks；完整T=d+q；profile、oracle及资源采样在查询时钟外 |
| 次序及数量 | 每组4轮AB／BA／BA／AB；每配置每臂5暖身＋10正式。每组240条，总480条，其中320正式、160暖身；全部保留，无补采或慢值剔除 |
| 资源 | guest vCPU2/3、balanced root分区及共享两核配额；查询组2 GiB；原DataNode heap15,232 MiB/direct2,176 MiB另计；原数据库、账号、许可证 |

统计先取同轮、同配置、同臂10条正式记录的中位数，再计算R、Rd、Rq及对应B/A。下表为四个配对B/A变化的中位数；不是先合并全部时钟，也不是各中位数再次相除。负值表示减小。

## 2. 完整查询与相对成本

| 固定组 | IoTDB-FS完整T变化 | R变化 | Rd变化 | Rq变化 |
| --- | ---: | ---: | ---: | ---: |
| 独立N8 | −3.93% | +11.12% | +1.61% | +10.29% |
| 独立N128 | +39.45% | +6.73% | −6.79% | +11.75% |

各轮方向全部列出，避免用总体中位掩盖差异：

| 固定组 | 轮次／配置顺序 | IoTDB-FS T变化 | FS-index T变化 | R变化 |
| --- | --- | ---: | ---: | ---: |
| N8 | 1／AB | +31.79% | +0.28% | +31.42% |
| N8 | 2／BA | −3.12% | −17.04% | +16.77% |
| N8 | 3／BA | −4.73% | −9.67% | +5.47% |
| N8 | 4／AB | −11.95% | −6.18% | −6.15% |
| N128 | 1／AB | +47.48% | +27.71% | +15.48% |
| N128 | 2／BA | +31.41% | +34.11% | −2.02% |
| N128 | 3／BA | +7.43% | +28.74% | −16.55% |
| N128 | 4／AB | +226.94% | +145.24% | +33.32% |

N8的完整T有三轮下降，但R有三轮上升，FS-index也同步变快。N128的完整T四轮均增加，R方向各两轮，未形成稳定相对收益。按用户“完整查询明显减慢则回退”的验收规则，本轮保守恢复A；不将局部减少分配／复制直接等同于端到端加速。

## 3. 资源证据与解释边界

两组分别核验538／1,036个资源快照、1,613／3,107次owned进程观测，未见cgroup或CPU亲和性越界。正式查询包络中，parent／query／database的限流、OOM及OOM kill增量均为0；服务、配置和资源恢复门槛全部通过。这里的隔离成立于guest内，不代表宿主物理CPU、共享缓存、频率或内存带宽独占。

N8各块其他30个guest vCPU忙度约4.02–4.87%。N128末轮从A的12.92%升至B的70.59%，相交周期持续约71–72%；此时IoTDB-FS和FS-index的完整T与CPU包络都同步增长。选定vCPU2/3仍有较多idle，未见配额耗尽；CPU／I/O PSI量级及数据库相交周期CPU记录不足以解释整段增长。

频率信息在虚拟机中不可用。现有证据支持末轮存在环境混杂，不能量化宿主竞争、频率、缓存／带宽、数据库后台工作与候选代码的各自贡献，不能把表中+39.45%写成已证实的代码固有退化。全部登记轮次仍参与统计，未剔除末轮或追加查询。

本轮没有单独计量release排序、State分配和身份构造的独占耗时。嵌套profile时钟不能相加为独占成本；资源包络还包含计时外oracle／profile／采样，不能当作d或q的独占CPU时间。

## 4. 磁盘阈值及恢复

首次采集在原DataNode重启时因默认5%空闲限制退出，科学查询为0；保留服务器stage下原`collection-n8/`及失败收据。启动前data_01已有约290 GiB空闲、约3.92%，低于默认约370 GiB要求；本轮源码及构建不足0.2 GiB，没有新增科学语料。

盘点其他盘后未迁移数据。用户明确选择1%阈值，已备份原配置，只追加`disk_space_warning_threshold=0.01`，重启同一原ConfigNode及DataNode。有效配置返回0.01，约保留74 GiB保护。A/B两侧共享该环境，恢复时保留1%。

最后只读核验：DataNode为Running、原ASYNC／partial-insert=true；许可证和节点身份摘要未变；配置SHA为`f69f408de5c09edabae3a8293cf662327f794b908cbf98d5a02b3878fea154f4`；原cgroup及全部线程0–31亲和性恢复。临时资源组已清理，默认实验挂载未替换成B。

## 5. 正确性与证据位置

候选完整Release构建、模块边界检查通过；最终101项专项及1,828项全量单元测试无失败／跳过。本地runner的12项测试及两份离线审计自检通过。回退按本轮保存的A/B文件SHA逐项校验，不执行git写操作。

服务器stage：`root@192.168.99.49:/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/hdf5-native-cpu-20261023/`。

| 证据 | stage内位置 |
| --- | --- |
| 本轮科学及环境／资源原始记录 | `retest-1pct/collection-n8/`、`retest-1pct/collection-n128/` |
| 完整配对派生统计／资源审计 | `retest-1pct/derived-audit.json`、`retest-1pct/resource-audit.json` |
| 最终服务和1%阈值核验 | `retest-1pct/final-verification.json` |
| 原0条失败记录及恢复 | `collection-n8/`、`recovery-1pct/`、`resource-release-result.json` |
| 构建与测试 | `build-binding.json`、`focused-tests-r2.xml`、`all-unit-tests.xml` |

本地仅保存派生JSON、测试XML、脚本与摘要；科学及资源原始记录留服务器。候选与回退证据在本地stage的`candidate/`、`candidate.patch`、`baseline-original/`和`rollback-receipt.json`。对应派生统计SHA为`212757db157d64dfdc4c813e6277d8a280cefcfd5d3e60e52671bdca4a0bc80f`，资源审计SHA为`1ce4aede9b1d8b6989fba226b92017d7dc732b4e1c08d3f9f9951d9528a996f5`。
