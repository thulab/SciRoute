# V04 head 热路径优化与验证

日期：2026-10-17。**实验已完成，性能改动已撤回**：局部模型变快，但真实完整 HDF5 查询两轮均更慢。用户明确要求以完整查询加速为依据，故本轮 head 热路径改动及前一轮根校验复用均已撤回，恢复到 V04 r3。以下实现／测试／计时描述的是被撤回的候选版；最终状态见[回退记录](#性能门禁与最终回退)。

## 实现边界

相对已修复重复根校验的 V04，仅修改六个生产文件。Owner::view/check_dependencies 保持现有锁和 ready 检查，逐请求观察相同 head fee identity；编码长度改用现有 state_->committed 字节，不新增缓存。initialize 占位 head 始终 Frozen；发布确认／journal 持久化及完整恢复之后，committed/current 同 gate 安装并最后解冻。

ReadContext::manifest 保留首个 owner view 健康检查并同步借给 payload 根校验。新内部 View 重载不授予权限、不持有 View、不自行计 head 费用；standalone 入口仍自行观察。根读取／解码／大小绑定、源限定费用、取消、namespace 与前后 ready 检查，以及最终依赖重验／准入保持原样。必要的每源根检查不省略，减少的是其额外 head 调用及编码／摘要工作。

## 同源红绿回归

红测试仅在原 rootfix 逻辑加 encode_head profile 计数，使用相同最终 Owner／context 测试：Owner 35 项中原有 33 项通过，新增两项因重复编码失败。绿完整 unit_tests 1644 项、138 suites，0 失败／错误／禁用／跳过，40.514s；包含新增 Owner2、Payload2。

真实 backend 的同源 probe 已完成：先以直接 encode/decode roundtrip 校准实际编码计数，随后两个独立请求各保持2源、根2、费用10602B；旧实现 head control=15、head encode=17，优化后分别为13、0。原13项语义用例全部通过，覆盖实际 Store 计数、未命中候选保留、温热SQL0、独立请求费用、取消、源数/catalog/累计预算、无关提交、Latest固定exact、257次提交后的全源和祖先重验、源和祖先权限撤销、exact删除。

## 组件级性能

同一 MemoryStore fixture，旧 arm 链接未经 encode 计数插桩的原 rootfix 源码；新 arm 链接本补丁源码，只有新 arm 使用本次已有 View 的重载。21 对样本交错顺序、子进程固定同 CPU；每个样本／component 40 个独立请求，热 Store IO=0，费用相同。模型不包含完整 ContentArtifacts、权限、查询、IPC 或数据库开销，不能作为整次查询延迟。

| 模型组件 | 原版中位 ms | 优化后中位 ms | 配对差中位 ms | 配对差范围 ms |
| --- | ---: | ---: | ---: | --- |
| 769 次 head 观察 | 3.198450 | 0.351338 | −2.845614 | [−3.238228, −2.286131] |
| 128 次根校验及其已有 caller view | 1.548410 | 0.485845 | −1.063882 | [−1.140185, −0.902018] |
| 897→769 次 head 的发现切片模型 | 4.210580 | 0.779783 | −3.432668 | [−6.462818, −2.967325] |

三个模型重叠，差值不可相加。第三个只模拟头部和根检查切片，不执行完整 discovery；路径更短、payload 为4KiB。全部21对均下降不能替代独立多轮端到端门禁。

## N=128、恢复与封存

复用原 V04 r5 的 namespace/journal、raw/FS/SQLite 原件与冻结 runner，未重新摄取。N=128、K=2、D=4、depth=2、seed=20261013；固定历史 A（HDF5 106.811351 ms、GRIB 57.404444 ms）不变。两轮分别原版→优化版、优化版→原版，四次挂载严格依次打开同一 namespace，未同时启动两个 owner。每挂载每格式5次，共40次查询全部通过精确版本与独立数值 oracle。保留冷首项，按各挂载／格式排除首项后每 arm 每格式有8个温热样本，单轮为4个。恢复耗时单列，不计查询时钟。

| 发现阶段每请求 | 原 rootfix | 本优化 |
| --- | ---: | ---: |
| HDF5 payload-map 根校验 | 128 | 128 |
| HDF5 head control | 897 | 769 |
| GRIB payload-map 根校验 | 128 | 128 |
| GRIB head control（温热） | 1155 | 1027 |
| 查询中的 head 编码 | 旧 daemon 未新增该计数，不伪造为0 | 0 |
| 温热 discovery＋选中值的旧 SDK／Store 查询 | 0／0 | 0／0 |

head control 和 head encoding 都是进程内计数，不是数据库 head 查询。GRIB 冷首项原版1165、优化后1037；该10次额外索引准备开销与温热精确计数分开记录。HDF5 冷首项与温热头部计数一致。全部4个挂载实际 daemon 摘要通过核验，退出后 head/journal 与原 prepare 一致，raw/FS/SQLite 摘要前后不变。

真实计时如下，单位 ms，为各 arm 请求中位数，不能当作逐请求配对效应或把阶段中位数相加。

| 格式／campaign | 原版总耗时 | 优化后总耗时 | 原版 discovery client | 优化后 discovery client |
| --- | ---: | ---: | ---: | ---: |
| HDF5，0（原→新） | 29.120 | 35.845 | 19.620 | 22.737 |
| HDF5，1（新→原） | 46.666 | 77.097 | 28.450 | 37.507 |
| GRIB，0（原→新） | 42.054 | 36.972 | 34.974 | 31.661 |
| GRIB，1（新→原） | 55.970 | 83.760 | 45.747 | 56.656 |
| HDF5，合并8个温热样本 | 33.821 | 51.542 | 20.907 | 32.273 |
| GRIB，合并8个温热样本 | 51.778 | 47.316 | 41.795 | 39.126 |

**这轮没有证明完整查询加速。** HDF5 两轮均更慢；GRIB 的合并中位数下降掩盖了 campaign 1 的反向结果，不能只引用合并值宣称收益。campaign 1 的选中值 client 中位数在两种格式也同时增大（优化版 HDF5 35.204／GRIB 27.154 ms，对照18.216／10.434 ms）。目前没有同步 CPU/调度证据隔离这些差异，不能把全部新增 wall time 归因于补丁，也不能断言已经证明是环境波动。功能与目标工作量减少已证实，整链路性能原因仍待进一步分离；本轮不替代原3×5全系统消融或固定 A 门禁。

首个控制窗口只因实验前缀误写为vd_v04而被native probe的vd_v03_参数guard拒绝，在arguments阶段结束，未执行backend用例或N=128；记录保持complete=false，原服务恢复及ASYNC/partial=true核验通过。修正仅涉及控制器前缀与独立输出目录，源码／构建不变。

两个维护窗口合计4次正常 DataNode SIGTERM；参数失败窗口及完整复验窗口均已恢复原服务。最终只读 SHOW ALL CONFIGURATION 确认 ASYNC／partial insert=true，配置 SHA-256 为 `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，`.env`、license、system_info 摘要不变，最终 DataNode PID **3666010**。四个私有挂载全部释放。

## 来源与最终证据

服务器目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/v04-head-hotpath-opt-20261017/`，本地同相对 work 路径保留凭据无关的封存副本。旧 rootfix 源包／二进制、原验收和前轮排查结果保持不变，没有 Git 写操作。

- `optimization.diff`、`green-binding.json`：候选版六个生产文件的精确补丁与全部191个生产文件摘要，封存时逐项与当时本地一致；回退后这些摘要仅绑定被撤回的实验版本。没有改变持久格式、默认strict、客户端协议或增加新的验证／授权缓存。
- `build_opt.py`、`red-unit-tests.log`、`red-binding.json`、`unit-tests.xml`：旧逻辑只加 codec 计数的同源失败回归、相同最终Owner／context测试源码摘要、完整Linux绿测试。Payload新View重载的2项测试在绿实现编译运行。
- `hotpath_bench.cpp`、`run_micro.py`、`micro.json`：21对模型样本、源／头文件／命令／二进制摘要和费用／计数检查。用于真实计时和微基准的旧 arm 都使用未经计数插桩的原rootfix，避免人为增大旧版成本。
- `live-report-r2.json`、`live-r2/startup-receipts.jsonl`、`live-analysis.json`：真实红／绿backend、13项语义结果、40次请求及阶段／计数、四次实际启动与分campaign计时。启动wrapper只把90s上限扩为900s并补收据，冻结查询时钟不变。
- `live-report.json`、`live/red/stdout`、`attempt1-run-live.py`、`post-restore-attempt1.json`：首轮参数拒绝及恢复记录，不拼入完整复验。
- `live-r2/control/operations.jsonl`、`post-restore.json`、`seal_evidence.py`、`evidence.json`：控制动作、原服务身份与保护文件摘要、恢复后的实际有效配置、最终封存一致性核验。

优化后 daemon SHA-256 为 `2802abef6e580692f5fe08c03ea82fca175aee7f07772bc5ce8542770298190e`，probe 为 `0a4d15be0ec70a732e0ec16d3ef6d284aba1b637f807fd07da2446a9b215ba8a`；未经插桩的旧 daemon 为 `6c10f764b7f6541a3222f21f9e80fece1a5a9517c4203edcb52235e9ad7d5336`。最终 `evidence.json` SHA-256 为 `919458bbc2ab8c96d44c36fbc97513ac58296d6e6634254ec605273807553f7c`；67个凭据无关文件逐一核对 SHA 和大小通过。封存包 SHA-256 为 `2dbe0593bfea7be010ec6b132babb4a9df94fb3712ef0d806c68f92f163e9511`。

## 性能门禁与最终回退

用户随后明确：**所有优化以完整查询计时加速为依据；完整查询明显减慢时适当回退。** 本轮 HDF5 完整查询两轮均更慢，合并温热中位数为33.821→51.542 ms，因此撤回整个 head 热路径补丁，不用组件收益作为保留理由。此前根校验复用试验的完整 HDF5 为30.489→32.208 ms、GRIB 为43.821→45.116 ms，同样没有证明完整收益，也一并撤回。该决策采用保守的性能准入标准，不把两轮有限计时解释为已隔离所有环境因素后的因果结论。

最终恢复 **V04 r3**：六个生产文件及三份测试从 SHA-256 为 `6271c8633637bcdee83eec1d6966ab38f609e1c19cbca502b3e074dcca96e642` 的原冻结源包精确恢复。全部191个生产文件、289个测试文件逐项匹配 r3 receipt。两轮专属的四个新增单测和 native probe 断言随补丁撤回，保留 r3 原有功能与预算测试，native probe 恢复为原11项。契约、实施计划及索引已同步标记回退；V04 payload 共享功能、持久格式与原始验收保留。

服务器原 r3 source/build 及全部八个二进制摘要复核通过，并重新执行其完整 `unit_tests`：**1640项、138个 suite，0失败／错误／禁用，38.391s**。使用已冻结且源码／测试完全对应的原构建，没有为回退重写历史构建。最终选定 daemon 为 `v04-verification-r3-20261017/build/bin/iotdb_fs`，SHA-256 为 `83b90b15181f3311e5b70bf8dfbd334295b3e5422bf1ded388a9948ce2144305`；两轮候选二进制仅保留为实验档案。服务器仓库根目录不是本轮冻结构建源，未向该不同步的历史工作目录覆盖文件。

本次回退没有再次运行完整查询，不声称取得一组新的回退后耗时或解决了全部 V04/V03 差距。保留前述所有完整计时作为撤回依据，不改写原67文件证据包。原数据库 PID仍为3666010、正常存活，配置摘要不变，无两轮遗留挂载；回退没有执行数据库命令、停启或许可证操作，也没有 Git 写操作。

独立回退记录位于本地／服务器 `experiments/cross_version/work/v04-performance-rollback-20261017/`：`rollback.py` 与 `production-rollback.json` 记录六文件前后身份；`local-binding.json` 绑定全部生产／测试；`validate_remote.py`、`remote-validation.json`、单测命令／XML／日志绑定恢复基线及本次复验。旧实验证据和本次回退收据分别保留，避免将负结果误标为已接受优化。
