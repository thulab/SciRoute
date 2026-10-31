# HDF5 indexed 内容解码共享：实现与正确性验收

状态：已依用户 2026-10-28 的要求回退本次 HDF5 indexed 内容共享实现、范围证明接口及其消融开关；保留此前 payload 块缓存。以下保留当时实现与实验记录，不代表当前运行路径。

日期：2026-10-28。实现范围为 HDF5 indexed；不调整 native HDF5、查询并行度或客户端行为。

## 结果与边界

已将 indexed 解码缓存扩展为“精确源别名 → 内容对象”。不同精确版本的查询在验证自己的 payload 范围并计费后，可以复用已完成的相同内容解码结果。版本 provenance、权限准入、请求预算、取消和错误仍归属于各自查询。新增对象接入已有公共 QueryRuntime 的字节、entry、pin 和 LRU 回收，不增加独立无界热池。

本轮验证实现及内容正确性，未执行真实 IoTDB/FUSE 大文件耗时探针，未测得端到端加速比。既有服务未启停、语料未重新发布，论文及飞书实验表未改写。

## 实现

| 层次 | 身份及内容 | 生命周期与费用 |
|---|---|---|
| 精确源别名 | 原 source namespace/path/version/revision、payload/layout 大小、layout checksum、dataset ID、chunk/tile 位置及解码安全配置；值只保留可信范围摘要 | 公共 runtime producer 收集本源证据费用及 payload/extent 需求；请求重放本源费用。请求保留小 alias lease，避免跨 slab 重访生成新费用身份；不 pin 内容对象 |
| 内容对象 | Owner 验证的范围身份＋完整原生解码条件；保存原生端序的解码字节 | 仅复用已完成成功对象；原位提升精确源成功项，不复制数据、不重复计入分配。内容项不保存首个源费用 |

可信范围身份由 published reader 的能力接口签发：验证 extent map，以 namespace/incarnation、请求长度、有序 block 内容 ID、块大小、块内偏移和长度生成 SHA-256 摘要。该操作不读取 payload block；未提供能力的 source 保持原精确源路径。内容键另外包含 storage layout、逻辑类型、native type class、元素宽度、字节序、chunk shape/options、effective filter mask、完整 filter ID/flags/parameters 顺序、解码长度和 decoder 安全上限。布局其他位置或其他块变化不会无条件使全部内容失效。

范围证明采用与冷传输共用的排序、合并及拆分计划，保持整块取数与 extent 预算。源别名和内容使用相同 runtime 管理；pin 项被摘除映射后仍持续计费，最后引用释放才减账。预算不足仍按原等待／超时语义失败。同组相同内容只取数／解码一次，原始逻辑字节与 range 需求照常计费。科学转换、选区和来源信息由当前 cursor 单独处理；fill 保持精确源身份。

跨来源不共享未完成 producer：某源读取失败不会成为另一版本的错误。两个来源同时冷读可能各解码一次；成功提升发生竞争时，额外真实分配仍受公共字节上界约束并可回收。内容身份保守依赖完整 payload block ID，因此同一物理块内的其他字节变化仍可能阻止其未变子范围命中。

## 验证

依照仓库验证技能，在既有服务器的独立目录使用锁定的 thread-safe serial HDF5 **1.14.4** 构建。开启 BUILD_TESTS、关闭 FUSE integration；构建 iotdb_fs、iotdb_fs_web 和 unit_tests。测试不连接真实 IoTDB、不挂载 FUSE。

| 检查 | 结果 |
|---|---:|
| Linux 应用与单元目标构建 | 通过 |
| HDF5、PayloadArtifacts、StructuredBlobStorage、公共 decoded cache、QueryModule 针对性测试 | 508 / 508 |
| Linux 全量单元测试 | 2,133 / 2,133 |
| 启动参数配置回归，无连接／挂载 | 22 / 22 |
| 本地 10 个源码／测试文件语法检查 | 10 / 10 |
| 本地独立公共缓存及 HDF5 范围读取测试 | 70 / 70 |

本地安装的 HDF5 为 1.14.5，故本地未用它替代锁定库做 native 链接；完整构建及 HDF5 测试采用上述 Linux 环境。本地检查前后核对源码未变。

本轮新增 18 项回归：HDF5 内容共享 10 项，公共缓存 ready lookup／原位提升 3 项，存储范围身份 5 项，覆盖：

- 不同路径／版本、不同 layout 和无关 payload 字节变化下复用相同已证范围；第二个源 **零新增 payload read、零新增 decode**，输出 provenance 仍为第二个源。
- 相同字节但不同字节序、chunk shape、filter 参数或 flags 不错误复用；不同 namespace、不同 payload 内容以及缺失范围证明的 source 保持隔离。
- 同组四个相同原生块只读取／解码一次，仍返回全部 16 个正确值；严格逻辑需求预算拒绝不被内容去重绕过。
- 新源只重放自身费用，不重放首个源费用；warm hit 低预算仍失败且账本保持 sticky，后续独立请求可成功。
- 范围证明失败、条数异常、映射损坏、Frozen Owner 和取消不能借用 warm 内容绕过；跨来源在途读取失败隔离。
- 跨 slab 重访保留稳定需求身份；max_entries=1 下可流式推进，释放 cursor 后 evict_all 的 live/pinned 字节均归零。
- 原位提升保持同一 allocation；已有目标、错误 lease、带源费用的项不提升；evict 后 pin 仍计费，最后引用释放才回收。

既有 HDF5 registration 权限先于 layout、warm result 仍鉴权等回归同时通过；未改变公共授权入口。测试层面的机制与回归通过不替代真实多版本大文件性能测量。

## 证据

服务器根目录：`/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/cross_version/work/hdf5-content-cache-20261028/`。本地同名 work 目录保留脚本、输入及取回收据，遵循现有 ignore 规则。

- 首次冻结源码：536 文件，`source-r1.tar.gz` SHA-256 `de33aa09c25c4d24764c996cda9965c4cf10d873ae75627b20f6ce03f398ccda`；逐文件清单 `source-manifest-r1.json`。
- 应用二进制 SHA-256：`612d5d944882edbfde5324fc947825af5bff570e129f8d3193063acb5107f021`。
- 首次构建及完整门禁：`product-r1/build-report.json`、`focused.xml`、`unit-tests.xml`、`startup-configuration.log`；`build-binding.json` 绑定源码与二进制哈希。
- 取消回归测试随后改为显式 gate 同步，避免测试修改仍可能被后台读取的回调。生产源码未变；最终测试覆盖文件、完整源码清单、编译和链接命令及回归结果独立保存在 `test_Hdf5SharedCache-final.cpp`、`source-manifest-final.json` 和 `final-validation-r2/`，不覆盖首次冻结证据。
- 独立链接最终测试的首次复验漏放可执行文件同目录的 HDF5 dimension-scale helper，6 项既有 native/CF 辅助读取测试失败（502/508），10 项新增内容缓存测试均通过。失败日志保存在 `final-validation/`；修正只补齐原构建 helper 的路径，记录其哈希，不修改生产代码或测试断言。
- 修正环境后的最终针对性／全量复验为 **508/508、2,133/2,133**，无失败或跳过。`source-manifest-final.json` SHA-256 为 `f98265e7a8b732ba6699ba42863f4231a133ea7de811054f8392cbc39edfd7f7`；最终 unit_tests SHA-256 为 `2478c9c60f3df2da59336469aa0537ada6e5def957987f255bb4bb5d49830bb5`。除取消测试同步修订外，与首次冻结输入逐文件一致；所有生产源码与首次通过构建的应用一致。

当前行为契约见 [HDF5 共享缓存](../contracts/hdf5-shared-cache.md)。
