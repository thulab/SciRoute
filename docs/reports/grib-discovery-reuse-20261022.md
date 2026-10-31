# GRIB 属性定位复用与发现交付消融

状态：2026-10-22完成实现、固定语料消融及独立验收，保留C作为GRIB发现通路。独立N128三轮的完整R相对A为0.712，原始完整T相对A为0.717，三轮均改善；仅提示复用B尚无稳定的独立加速结论。候选二进制SHA为`f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087`。

## 实现范围

全局属性索引的结果现在保留精确源与message ordinal，通过公共不透明提示交给GRIB prepare。提示绑定namespace、path/version、revision、payload/layout大小、验证后的layout身份、完整六属性键和全部请求限额。prepare仍重新观察layout身份和费用；只有全部一致时才跳过解析缓存查找与逐message谓词扫描。未命中候选的授权、错误及费用证明继续覆盖完整候选域。

新增显式`format_catalog`控制操作，用Adapter visitor直接返回精确版本的unit身份、offset及length。选择、授权、租约、预算、残余语义和交付前准入由公共层执行，GRIB字段与定位含义留在Adapter。发现不产生Arrow流、结果路由或FUSE定位读取；后续取值仍使用既有逐命中typed请求和解码缓存。未新增全局缓存或扩大额度。

契约见[GRIB缓存与定位接口](../contracts/grib-query-cache.md)，确认边界见[共用模块方案](../deliverables/shared-cross-format-hot-query-plan.md)。

## 固定实验条件

| 项目 | 冻结条件 |
| --- | --- |
| 小探针 | `G-H1-N8`，独立发布N8/C8/V1/D4/K2/S0/Latest，1个campaign |
| 正式组 | `G-H1-N128`，独立发布N128/C128/V1/D4/K2/S0/Latest，3个campaign |
| 语料与索引 | 原FM1 `collection-r2`封印语料、oracle和SQLite；不重新发布、生成或建索引 |
| A | 原E4产品，typed offset发现，二进制SHA `b7e7d9955580c5f9401912dc5948685d4208c7fde9d6d34e6160e2364d236589` |
| B | 当前候选产品，typed offset发现，启用定位提示 |
| C | 同一候选产品，显式format_catalog发现，启用定位提示 |
| 取值与输出 | 三路径均调用原客户端value_request，交付2个64×64 Float32值和uint8 mask，共40,960字节；oracle验证在计时外 |
| 资源 | CPU2/3由查询客户端、IoTDB-FS、FS-index与原DataNode共享；查询组2GiB；数据库原15,232MiB固定堆／2,176MiB direct另计 |
| 顺序 | campaign轮换ABC/BCA/CAB；每块新mount与新FS-index context，交替两臂，2次预热＋10次正式 |
| 完整时钟 | d从查询入口至精确定位已交付；q至owned值/mask到达客户端；T=d+q；不以server局部时长代替完整T |
| 统计 | 每块先取正式查询中位数，与同块FS-index形成R/Rd/Rq；再取campaign中位数；保留全部慢记录与失败 |

N128是独立N128语料，不是N512内的C128候选桶。两个case的K_files=K_versions=K_units=2；实际S及每文件摘要以原封印`input/manifest.json`为准。本轮只有既有两个case的新run，不扩展V、S或并发主张。N8仅作机制和安全门槛，不与N128混合统计。

## 完整查询消融结果

以下为同campaign两个配置的比值，再取campaign中位数；小于1表示改善。完整R来自同块IoTDB-FS与FS-index完整查询中位数之比，不使用历史最佳绝对耗时。原始两臂毫秒值及全部A组结果保留在服务器，不在此重复。

| 独立语料／用途 | 对照 | 完整R相对值 | 原始完整T相对值 | Rd相对值 | Rq相对值 |
| --- | --- | ---: | ---: | ---: | ---: |
| N8，单轮门槛 | C/A，两项合用 | 0.895 | 0.913 | 0.574 | 1.058 |
| N128，三轮正式 | B/A，仅定位提示 | 0.933 | 1.110 | 0.943 | 1.010 |
| N128，三轮正式 | C/B，再使用轻量交付 | 0.746 | 0.648 | 0.609 | 0.978 |
| N128，三轮正式 | C/A，两项合用 | **0.712** | **0.717** | **0.542** | 0.964 |

正式C/A的完整R三轮比值为0.695／0.712／0.749，原始完整T三轮为0.703／0.717／0.719。完整R改善28.8%、原始完整T改善28.3%，支持保留C。C的完整R中位数为3.712，即当前IoTDB-FS仍约为FS-index的3.7倍；本轮改善不外推为大N、大V或并发优势。

**B的限制必须单独保留。** B/A完整R三轮为0.933／0.931／1.042；原始完整T三轮均更慢（1.145／1.045／1.110）。同块FS-index原始T也三轮变慢（1.228／1.122／1.065），q两臂亦同时变慢，表明存在块间时钟漂移；这些记录不能确定漂移原因，也不能证明hint没有成本。因此不把B当作稳定加速结果，主线验收依据是C的完整R与完整T均一致改善。typed继续承担通用投影查询，发现客户端显式使用`format_catalog`；未增加隐式协议回退或新的调参版本。

## 机制、语义与资源验收

| 每次暖发现／取值 | A | B | C |
| --- | ---: | ---: | ---: |
| 发现阶段定位hint命中 | 无此机制 | 2 | 2 |
| 发现阶段parsed-index缓存再次查找 | 2 | 0 | 0 |
| 发现阶段逐message谓词评估 | 旧binary未计数 | 0 | 0 |
| 定位Arrow／FUSE结果 | 保留 | 保留 | 无 |
| 直接JSON定位unit数 | 无 | 无 | 2 |
| q的typed请求数 | 2 | 2 | 2 |
| q的native-field命中／新解码 | 2／0 | 2／0 | 2／0 |

暖测量中上述机制均逐条验收。q未改变算法或交付方式，Rq的变化不作为本轮q优化收益。费用、全候选授权、精确版本、执行租约及交付前准入继续生效；C的权限／空结果／预算失败／取消等语义由公共端点回归覆盖。两份产品的全部src/include/cmake摘要对比，差异仅为本轮8个获准产品文件。

资源限制与实际用量分别记录。N8／N128查询组kernel memory peak分别为74.54／99.29 MiB，数据库分别为3.61／11.82 GiB；数据库原15,232 MiB固定堆与2,176 MiB direct配置保持原值，不把堆配置当RSS。全部逐查询资源帧、周期样本、CPU2/3隔离及2核父配额检查通过，没有OOM。N8三次mount启动合计30.01秒，N128九次合计722.32秒，单列于查询计时之外；本轮结果是暖查询结果，不包含数据库维护与服务启动均摊。

96项聚焦测试、全部1,825项单元测试、15项运行器回归及独立审计工具10项回归通过。有效采集为288条：N8为72条，N128为216条，其中240条正式、48条预热；没有按速度删选或重跑任何科学记录。独立审计复核case/corpus/oracle、原始语料与索引、费用／权限机制、顺序、完整时钟及资源恢复；q请求同一性依据冻结源码、精确oracle身份、server request_bytes和typed输出验证，没有宣称捕获了未保存的wire body。

## 验收与证据

验收同时检查完整查询R、配对原始T，以及hint命中、零重复扫描、零定位IPC／路由等机制计数。q保持原路径，其波动不能直接解释为本轮机制收益；完整查询明显减慢的改动不纳入主线。

服务器证据根：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/grib-discovery-20261022/`。构建、运行、资源及恢复记录分别放在根目录、`smoke-r3/`与`formal-r1/`；原始查询为各自`queries.jsonl`，资源为`resource-samples.jsonl`，统计为`analysis.json`。`independent-audit.json` SHA为`2187f0254e4fb45e9cc590e4394691dcfb4c1e657fec445e90ee0f6933154d6b`；本地副本见`work/grib-discovery-20261022/audit-evidence/`。

最终只读实机检查`live-postflight.json`通过：原DataNode PID2654499、227个线程均恢复CPU0–31及原cgroup，配置SHA恢复为`ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`，所有本轮cgroup、挂载与活动实验进程已清理。复用原数据库、账号和许可证，没有独立实例或新激活。

`smoke-r1/`在客户端bootstrap阶段因系统Python缺少h5py退出，科学查询为0，未修改数据库或资源。`smoke-r2/`使用E4相同的`session-20261012/venv`后，因运行器顺序tuple与JSON复读list不等，在worker开始查询前退出，查询为0，原数据库与资源完整恢复。两次错误原样保留；修正为显式list、增加完整proof往返回归及服务变更前预检后，真实只读`preflight-r3/`证明跨进程proof相等、closure稳定、完整依赖导入成功，再手动启动`smoke-r3/`。候选产品、case、语料和索引均未更换。服务器连接曾有两次有界SSH连接超时；SSH只负责启动和读取证据，不进入服务器内的科学查询计时。
