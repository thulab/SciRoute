# 版本目录与内容共享：V01–V04 实现契约

日期：2026-10-15；V02/V03 更新：2026-10-16；V04 实现与验收更新：2026-10-17。适用计划：版本目录与内容共享 (local archive: `docs/deliverables/version-directory-content-sharing-plan.md`)。本契约保留 V00 冻结的 V01–V06 设计边界，并记录 V01 已接线的 owner、持久目录和 codec；第 8 节记录 V02 内容对象、证明、缓存及新持久格式边界，第 9 节记录 V03 准入，第 10 节记录 V04 实现；V04 验收 (local archive: `docs/reports/version-directory-v04-validation-20261017.md`)已通过最终构建及真实数据库/FUSE 门禁。V01 验收 (local archive: `docs/reports/version-directory-v01-validation-20261015.md`)记录 251 项单测及真实 Store/FUSE/恢复证据。新模式显式启用，默认 strict 及已有持久数据的解释保持不变。D1–D5 均经用户确认；用户后续明确所有实验共用现有 owner 账号，不创建新账号或修改 ACL。V00 的实验是 head 候选原语与现行语义证据；V00 报告 (local archive: `docs/reports/version-directory-v00-validation-20261015.md`)包含实际 JAR 字节码、10 组 head 用例及独立生效配置短验。

## 1. 已确认决策与启用门槛

| 决策 | 冻结内容 |
|---|---|
| D1 | 一个 namespace 只有一个受控 owner；受支持的业务写入、目录修改和新模式 reader 经 owner。V01 限定同一主机的合作式入口，由固定 canonical namespace flock 与 journal flock 排他；共享现有账号，不声称数据库级隔离或跨主机 fencing。 |
| D2 | 选择时冻结 exact path/version；每次新 execute、结果发布及新 open 重验当前存活和权限。已打开 IPC 保持现行读取/fstat 语义。 |
| D3 | 默认 strict。新模式显式启用且验证新 namespace 协议、持久身份、owner 排他和数据库能力。无隐式旧格式回退，不原地解释已有 HDF5 `/5` 等对象为新协议。 |
| D4 | GC 首版只在停写且无活动读取租约时显式执行 mark-and-sweep。完整标记后先 dry-run；失败不使用不完整标记删除对象；tombstone 不随内容回收。 |
| D5 | 先 metadata/layout，再试点 1 MiB 原始编码字节定长块。目录、映射、读取预算见第 6 节；不引入 CDC、差分或历史重建链。 |

V01 依据写入口清单 (local archive: `docs/reports/version-directory-v00-write-audit-20261015.md`)接线受支持的 mount/backend：inode 权限、普通 FUSE、typed 目录以 committed root 为可见真相；rename 的目标发布与源删除共用一笔事务；删除撤销目录绑定并保留 tombstone，不执行旧模式物理回收。旧独立 reader、迁移和直连客户端不能作为新模式 owner 入口，显式 owner 模式下未接线命令拒绝运行。strict 客户端访问原表，不自动识别新前缀；知晓前缀并持有共享账号的 raw SQL 不受本协议阻止，属于支持边界之外。只读 head 诊断不打开 journal、不占写租约，也不返回 ReadContext。

配置为 `storage.version_directory.mode=owner`，同时指定 `table_prefix`、绝对路径 `owner_directory`；首次空 namespace 显式 `initialize=true`，正常重启为 `false`。prefix 为 4–51 字节、以 `vd_` 开头的小写字母/数字/下划线。backend 接受 `127.0.0.1` 或 `localhost` 并归一为前者，数据库名称必须小写。canonical scope 为 `127.0.0.1:port/database/prefix`，用 ContentId framing 的 type=`owner-scope`、protocol=`iotdbfs-owner-scope/1` 求摘要，固定锁路径为 `/tmp/iotdbfs-v1-<摘要>.lock`，不读取 `IOTDBFS_NAMESPACE_LOCK`。更换 owner_directory 不能避开同一 scope 锁，空 journal 不能接管已有 head。此边界依赖同一主机及稳定锁文件；SSH 隧道、跨主机进程、同账号 raw SQL 或管理员替换本地状态均无隔离保证。

## 2. 协议身份、规范内容与不可变对象

新协议族固定为 `iotdbfs-version-directory/1`；head、commit、catalog-root、catalog-page 及首版内容对象使用 `iotdbfs-<对象名>/1`；对象协议由各阶段明确冻结，V04 的 file-descriptor 内外协议均改为 `/2`，payload-map/extent-page/payload-block 使用 `/1`（第 10 节）。VersionBinding/PermissionState 是下表的语义角色，并非 V01 单独存储的对象类型。不可变内容对象（Commit、目录页、描述与块等）的存储 key 至少包含持久 namespace UUID、incarnation UUID、对象类型、协议和 ContentId。NamespaceHead 是例外：其权威定位 key 固定且不随内容摘要改变，完整值中绑定 root/commit/hash。不同 namespace 默认不共享。UUID 是持久随机身份，不能由 endpoint、进程地址或 generation 代替。

ContentId = SHA-256(domain || type || protocol || length || bytes)。domain 固定为 UTF-8 `iotdbfs-content-id/1` 加一个 NUL；type、protocol 分别以前置大端 u32 字节长度编码；length 为实际 bytes 长度的大端 u64。禁止混用 hex 字符串与原始摘要。V01 codec 固定 head、commit、catalog-root、catalog-page、catalog-record、owner-journal；metadata 使用现有规范序列化器。下表的 VersionBinding、PermissionState、FileDescriptor、ExtentPage、FormatProof、VersionSeal 是完整计划的语义要求：V01 用 NamespaceRecord 联合承载现有 binding/权限/manifest，V02 的 FileDescriptor/FormatProof/VersionSeal 具体编码见第 8 节；V04 的 extent/payload 共享编码及实施边界见第 10 节。

| 对象 | 必须绑定的字段与约束 |
|---|---|
| NamespaceHead | namespace、incarnation、非负单调 commit sequence、唯一 commit ID、CatalogRoot ID、协议和完整性摘要；完整编码于固定 table/tag/time 的**一个 BLOB FIELD**。不能以多个 FIELD 或 OBJECT 分片拼成 head。 |
| Commit | 本次唯一提交 ID、namespace/incarnation、sequence、前一个 head 身份、目标 root、变更依赖集合及操作类别；本体不可变，在切换 head 前持久可读。前驱用于诊断/对账，不作为读取重建链。 |
| CatalogRoot | 同一次提交的 current-path、exact-version、liveness/tombstone、permission 四个根及协议。四视图不能分别发布。 |
| CatalogPage | 有序键、页类型/level、键界限、子页或叶条目；无重复键、循环、越界和混合协议。current 选择须保留格式过滤语义，可按 `(path,format)` 或等价有界结构索引；不能只保存一个跨格式 latest。 |
| VersionBinding | 原 path/version、format、size、FileDescriptor ID、外层版本封印；版本身份永久独立，删除后不得复用 exact key。 |
| PermissionState | exact 源 inode 的 mode/uid/gid/is_dir；当前祖先目录搜索权限；每个依赖的变更身份。不得放进 payload ContentId。 |
| FileDescriptor | 逻辑长度、完整 extent 根、layout ID、格式目录/内层证明 ID、格式及构建规则身份。排除权限、源路径和此次版本号。 |
| ExtentPage | 按 logical_offset 排序的 `(logical_offset,length,content_id,content_offset)` 或有界子页；完整覆盖文件，不存在空洞、重叠或上一版本补丁指针。 |
| FormatProof | 格式目录字节、layout ID、记录数、验证协议/构建规则、完整性状态；无 path/version。adapter 负责解释格式字段。 |
| VersionSeal | namespace/incarnation、exact path/version、format、size、FileDescriptor/FormatProof ID；rename 产生新封印。现行 path/version 相关 seal 不能直接作为内容 ID。 |

### 2.1 V01 已实现的 wire formats

除 NamespaceRecord 和 journal 的特别说明外，整数均为无符号大端，字符串为 `u32 字节数 + 原字节`。UUID 外部为 36 字节小写带连字符文本，head/commit 内为 16 原始字节；ContentId 外部为 64 字节小写 hex，结构引用为 32 原始字节。optional ID 为一个 `0/1` 字节，`1` 后接 32 字节 ID。decoder 拒绝截断、非规范 optional 值和尾随字节。

| V01 对象及协议 | 顺序编码 |
|---|---|
| head / `iotdbfs-head/1` | `VDH1`、namespace UUID、incarnation UUID、sequence u64、commit ID、root ID、32 字节完整性摘要；摘要是 ContentId framing 对 type=`head`、protocol=`iotdbfs-head/1`、此前 body 字节的结果。编码恰为 140 字节，硬上限 4 KiB。 |
| commit / `iotdbfs-commit/1` | `VDC1`、两 UUID、sequence u64、optional previous commit ID、root ID、nonce UUID、operation 字符串（最多 128 字节）、dependency count u32、各 dependency 字符串（最多 8,193 字节）。dependency 为 `table ordinal 单字节 + key`，最多 65,536 个，排序且去重。commit ID 是整个编码的 ContentId，nonce 区分不同提交。genesis sequence=0，之后递增。 |
| catalog-root / `iotdbfs-catalog-root/1` | `VDR1` 后依次为 Current、Exact、Liveness、Permission 的四个 optional page ID；空树用缺省 ID 表示。 |
| catalog-page / `iotdbfs-catalog-page/1` | `VDP1`、level u8、entry count u32、各 entry 的 key 字符串（最多 8,192 字节）与 ID。leaf level=0；内部 key 是子树 inclusive maximum，子层恰少 1。页非空、键严格递增，至多 256 条/64 KiB，level<8。遍历校验循环、层级和子边界。 |
| catalog-record / `iotdbfs-catalog-record/1` | 四树叶值指向的 opaque 记录，摘要包含该 type/protocol。backend 的 Exact、Permission、Current 值使用 NamespaceRecord；Liveness 值为单字节 `1`（活）或 `0`（永久撤销）。 |

NamespaceRecord 的编码协议字符串为 `iotdbfs-namespace-record/1`；**字符串使用 u64 长度前缀**，每串至多 65,536 字节，数值统一 u64。字段依次为：协议字符串、path、parent、name、version、is_dir（0/1）、size、mode、uid、gid、nlink、ctime、mtime、atime、format_id、规范 metadata 字符串、layout_size、manifest revision。published 固定为 true、phase 固定为 Published，不重复编码。检查有符号目标类型上限及非负值；path 复用 exact_key 语法，拒绝 NUL、双斜杠、`.`/`..`、非根尾斜杠、相对路径及超过 4,096 字节。parent=parent_of(path)，name=base(path)，根目录三者均为 `/`。inode/manifest 的 key、size、format 和规范 metadata 一致；重新编码必须逐字节等于输入。

Exact/Permission/Liveness key 为 `canonical path + NUL + 非负 version 的 u64 大端`，version≤INT64_MAX。Current 同时保存普通 `path` 和 `path + NUL + format_id`，保留普通最新及按格式最新。删除重算对应 Current，永久留下原 Exact key 的 Liveness=0。COW 更新只写变化路径的新页，读取当前根不遍历历史 commit 链。

### 2.2 IoTDB Store 与能力门禁

使用现有数据库的 `${prefix}_head(slot STRING TAG,value BLOB FIELD)` 和 `${prefix}_objects(namespace_id STRING TAG,incarnation_id STRING TAG,object_type STRING TAG,object_protocol STRING TAG,content_id STRING TAG,value BLOB FIELD)`，两表均含自动 time 列。head 固定 `time=0,slot='main'`；对象固定 time=0，五 TAG 构成完整定位。Store 不创建数据库、不改服务配置、不配置账号。DESCRIBE 校验完整列序、类型及类别；初始化可续建中断后仍为空的相同 schema，任一表存在数据即拒绝初始化。

SHOW DATANODES 必须只有一个 Running DataNode，其 RPC 端点与配置的 loopback/port 一致。`SHOW ALL CONFIGURATION ON <node_id>` 的 applied 值（空则取默认列）必须为 `wal_mode=SYNC`、`enable_partial_insert=false`。不符即拒绝，无静默降级；启动门禁不防护管理员运行中改配置。

inode、payload、layout、metadata set、version identity、dataset catalog、dataset entry、dataset attribute 八表全部映射至同一新 prefix。SQL inode 的 published 位不发布新 namespace，只有 root 引用的 NamespaceRecord 可见；未提交 artifact/manifest 可留为 staging 或孤儿。V01 不原地转换旧表。

独立 reader/writer TableSession 禁用 auto-fetch/redirection，显式 maxRetries=0。head 不使用旧 Connector 重试池，只发一次单 BLOB 写，异常返回 Indeterminate。读查询 SQL timeout=5,000 ms，连接超时=5,000 ms；实际 SDK 非查询请求固定 timeout=0，**没有全 SDK 或整个请求的有限 deadline 保证**。失败 reader 被丢弃，后续显式对账新建连接，head 不自动重发。对象写先查存在值、拒绝摘要/字节冲突，写后逐字节读回；不确定对象写仅在独立读回完全一致时继续。read_head 不解冻 writer，Owner 完整对账后才调用 accept_reconciled_head 清理不确定状态并重建 writer。

相同 ContentId 的重复写入必须核对类型、协议、长度和字节；注入摘要冲突时失败，禁止覆盖。locator 不能原地改指不同字节。后续格式解析缓存键须包含验证规则和解码上下文；payload 相同而 dtype/layout/filter/科学语义不同，不共享错误的解码结果。权限、来源与逻辑预算始终按各版本计算。

## 3. 提交、确认不明与恢复

状态顺序为：准备不可见对象 → 验证引用闭包及内外证明 → 确认对象/页/Commit 持久可读 → **写一个完整 head cell** → 发布内存当前根并失效依赖 → 返回成功。head 是唯一可见性线性化点。首次初始化也需排他；空 namespace 不是任意客户端创建新身份的许可。

候选持久原语限定受控单 owner、固定单元、WAL=SYNC、禁 partial insert；依赖同一 build 的源代码与恢复实验。它不提供数据库 CAS、多 writer 或跨对象事务。所有引用对象先持久化；head 原子不意味着先前任意多表写自动原子。默认 ASYNC 部署不能凭本实验获得 ACK 后持久保证。

RPC 超时、异常、ACK 丢失均属结果不明：owner 暂停准入，在同一 head key 对账 namespace/incarnation/sequence/commit ID。读回目标完整 head 可完成提交；V01 读回前一 head 时**仍 Frozen，禁止自动重试或丢弃 pending**，尚未实现排除在途 RPC 后的人工重试流程。其它身份/损坏/无法确认也拒绝准入。不能以 false 当成肯定未提交，也不能在旧 RPC 仍可能落地时推进下一次提交。

进程重启检查 head、Commit/root 和当前闭包，不扫描无限历史。正常 restart 保持 namespace/incarnation。V01 用独立本地 journal 的完整已提交 head 检测数据库回退；缺 head、缺 journal、身份不符或不同于 high-water head 均拒绝自动采用。复制/回滚后的新 incarnation 灾备流程属于 V06，V01 无自动修复或重建。数据库与 journal 同时回退、跨主机 fencing、主机断电持久性均不由单进程 SIGKILL 实验保证。

### 3.1 FileJournal 的 durable pending 与 high-water

owner_directory 是持久状态目录，不是可随意更换的租约路径。目录逐级 openat/O_NOFOLLOW，最终目录须属于 daemon 用户且无组/其它用户权限；`owner.lock`、`state` 为同用户、单硬链接、private 普通文件，flock 非阻塞排他。state 更新为新建 0600 临时文件 → 完整写入 → fsync 文件 → renameat 替换 state → fsync 目录。

state wire 为 `VDJ1`、两个 `u32长度+36字节UUID文本`、optional committed head、optional pending head，最后为 **64 字节小写 hex** 的 ContentId(type=`owner-journal`, protocol=`iotdbfs-owner-journal/1`, body)。optional head 为 `0/1 + 存在时u32长度和完整head字节`；文件上限 `2×4KiB+256`。两 head 的持久身份必须与 journal 一致。

提交先写对象和 root/commit，再 durable 保存完整 pending head，才发 head RPC。ACK 或独立读回目标后，保存 committed=target、pending=空，再发布内存 root。最后一次 journal 保存失败时 owner 仍 Frozen；fresh owner 可凭 durable pending 与数据库目标逐字节相等恢复。pending 存在但读回旧 head，即使测试知道 RPC 未发出，生产恢复也不能清 pending。无 pending 时只接受与 committed 完全相等的 head。恢复有界验证四树闭包，不追 previous commit 历史。

删除、chmod/chown、祖先权限变化和 rename 都提交一个新根。删除先写存活撤销和永久 tombstone；只在后续 GC 回收不可达内容。对共享对象任一引用的删除不得破坏其它版本。完成 head 写但尚未回 ACK 的崩溃按持久 commit 对账；未切 head 的 staging 是孤儿而非可见版本。

## 4. 现行语义与错误顺序

1. Latest 先过滤已发布状态、path/version 范围及 **format**，再按 path 取最大 version，之后应用文件/dataset 谓词。新版不命中谓词不回退。删除最新的指定格式版本后，下一次 Latest 可显露该格式仍存活的前一版；普通文件最新与 typed 指定格式最新不能混称。
2. 历史源 read 权限取精确版本 inode；祖先目录搜索权限取当前状态。补充组规范化后参与授权。version-path 的 chmod/chown 返回 EPERM。当前路径 chmod/chown 不是“同时修改所有历史版”。
3. exact 计划不因发布新版而升级。删除、rename 或内容/来源证据变化使旧计划失效；结果 ID 和 TTL 不授予永久访问能力。相同字节的版本拥有独立删除、权限和 provenance。
4. source 选择/目录错误先于授权；授权先于 manifest 精确重验；重验后才是封印、时间证据、marker/目录。当前两种正式 HDF5 引擎均要求发布封印（见 [查询模式](hdf5-query-modes.md)）；此前 strict 差异仅保留于历史二进制。只有时间候选进入 marker 检查。时间未命中不能绕过此前必须执行的校验。不得把错误变为空结果。
5. 已打开普通文件/写会话 unlink 返回 EBUSY。已打开只读 IPC 的 read/fstat 保持固定结果语义；新 open/access/注册仍需检查当前源。V01–V03 的普通底层存储句柄不等于物化 IPC 句柄；V04 新增已绑定不可变内容能力的底层读取，具体删除后行为及其边界见第 10.4 节。

以下完整 ReadContext/租约要求在 V01 时尚未完成，V03 已接线的范围见第 9 节；不因 V01 目录落地而提前视为完成。V01 backend namespace/权限/catalog 读取走 committed root，并在 metadata generation/缓存读取边界检查 owner 非 Frozen；typed 发现仍可做有界全 Exact 扫描，不声称消除发现 SQL 或完成依赖失效优化。

ReadContext 仅由 owner 构造，绑定 namespace/incarnation/root、exact 源、主体和依赖。用户不能提交“已验证”标记。每次 execute、结果发布、新 open 都在 owner 提交同步边界准入；内容验证成功缓存不缓存永久授权。当前依赖改变时重新授权/重验；exact 绑定不得偷偷变化。

## 5. 回收与租约

本节为 V05 待实现要求，V01 无 GC/sweep 命令。读取执行租约和已打开 IPC 结果句柄是两类生命周期。准备好的查询或未打开注册 ID 不永久 pin 内容。GC 停写、停止新准入并等活动读取租约归零后，冻结根集并完整标记；根集包括当前可见/可查历史版本、明确保留的恢复根及在途提交根。任意标记校验失败禁止 sweep。内部旧根不自动获得无限保留权或查询权。

GC 输出逻辑字节、唯一可达字节、共享引用、孤儿和可回收字节清单；dry-run 与执行使用同一冻结根集。tombstone 永久保留。失败重跑不能将已回收对象复用成不同内容，也不能因为恢复旧根复活删除版本。首版不提供在线并发 GC。

## 6. D5 硬预算及边界

以下冻结数值亦见 version-directory-v1.json (local archive: `experiments/cross_version/version_directory/specs/version-directory-v1.json`) 的 limits；该文件的历史阶段 status/v02 字段不作为 V04 实现或验收声明。以下是用户确认的新协议上限；head 候选实验只覆盖 256/4,096 字节，不将其它尚未实现的上限称为实测容量。

| 项目 | 上限 | 执行阶段 |
|---|---:|---|
| 完整 head | 4 KiB | V01 |
| 单目录/映射页 | 64 KiB，最多 256 条目 | V01/V04 |
| 目录/映射树高 | 8（根到叶含根和叶） | V01/V04 |
| 请求源 | 1,024 | V01–V04 |
| 每请求 extent | 65,536 | V04 |
| 请求元数据字节 | 64 MiB，包含 head/页/layout/目录等逻辑需求 | V01–V04 |
| 请求 payload 字节 | 1 GiB | V04 |
| 单批读取 | 64 范围且 8 MiB，二者都满足 | V04 |
| payload 首批定长块 | 1 MiB，尾块按实际长度 | V04 |

预算在缓存命中、内容共享和合并 I/O 后仍按各请求的逻辑需求计算；实际传输/缓存占用另计。新上限不能放宽已有更小的 adapter、单 catalog、输出、行数或公共缓存预算。批读可以拆分批次，但不能拆分以逃避请求累计预算。已有 QueryLimits 的 100,000,000 行上限继续有效。

所有 offset/length/加法/乘法需先检查无符号 64 位溢出，再检查文件逻辑边界；content_offset+length 也需落在块内。公开 scalar 零长度读取沿用现行空结果语义；内部 batch 空集合或零长度项拒绝，不发无意义 SQL。空文件允许无 extent 页，但 V04 仍写入有 ContentId 的规范空 payload-map；非空文件必须完整覆盖。短读、缺块、摘要/协议不符、非法树高/页边界和预算超限均明确失败，不退回整文件读取或追溯历史版本。具体错误码沿用现有接口类别并在 V01/V04 codec/reader 测试固定。

V01 core 已实施 head/page/height、单 scan 至多 65,536 条及目录遍历/恢复/事务对象操作的元数据计费；这些并不等于整个 typed 请求跨调用累计预算。源数、extent、payload、最终请求级预算与取消/deadline 接线仍由 V03/V04 完成。下述要求是后续 reader 验收边界，不表示 V01 全 SDK deadline 已实现。

每次遍历须同时受页/元数据/extent 上限、调用方 deadline 和取消信号约束；零 I/O 缓存命中也不能跳过取消/预算。无 deadline 的内部调用须由上层建立有限执行边界。V01/V04 对对应已实现模块覆盖 limit−1、limit、limit+1、uint64 溢出及损坏页；零长度、EOF、跨块由 V04 extent reader 单独覆盖，不能用 head 原语实验替代。

## 7. 固定基线与阶段责任

N=128/K=2/D=4/depth=2 主指标 A 保持 HDF5 **106.811351 ms**，同期 GRIB **57.404444 ms**。V00 不重跑或替换性能基线，不宣称发现 SQL 已消除。获批的跨版本环境为 N=8/D=4、每路径 8 个科学版本、identical/single_unit 两组；raw 复用比例不等于后端压缩率。

V01 实现提交身份/owner/codec、IoTDB Store、FileJournal 与 backend/FUSE 接线。V02 格式证明与验证缓存见第 8 节和 V02 验收报告；V03 完整准入/租约、共享请求预算和发现消融已由第 9 节及 V03 验收报告 (local archive: `docs/reports/version-directory-v03-validation-20261016.md`)交付。V04 extent 与 payload 共享已完成最终构建及真实验收（第 10 节和 V04 报告 (local archive: `docs/reports/version-directory-v04-validation-20261017.md`)）；V05 GC、V06 迁移/灾备恢复身份仍待实现。每阶段按计划 (local archive: `docs/deliverables/version-directory-content-sharing-plan.md`)验收后才能声明对应能力。V01 单测、实际 Store 故障包装及 mount 验证的最终结果与限制由主报告补充；进程内 wrapper 只证明对应故障控制流，不称为真实网络丢 ACK，后者另见 V00 实验。


## 8. V02：不可变内容证明与独立版本封印

2026-10-16 用户确认使用**全新 V02 表前缀**。owner genesis 的 Current 树原子写入保留键 `NUL + iotdbfs-namespace-profile`，值冻结 `namespace-record/2`、`iotdbfs-content-build/2`、原编码字节投影与 60 KiB 分页策略。reopen 要求相同 profile；V01 使用其固定旧二进制和原 journal，V02 拒绝隐式升级，无兼容分支。默认 strict 未变。

### 8.1 引用结构与内容边界

`namespace-record/2` 文件记录只保存 inode 身份/POSIX 字段、manifest 尺寸/revision、descriptor/seal 引用；目录 metadata 继续内联。`file-descriptor/1` 指向共享 metadata/layout image 与 `format-proof/1`。后者绑定构建规则、格式、布局协议、布局原始 SHA-256、payload/layout 长度、catalog image/字节数/记录数/属性数，以及目录/属性完整性。`version-seal/1` 独立绑定 namespace/incarnation、精确 path/version、format、payload 长度、descriptor/proof ID 和旧精确版本 envelope。

内容 hash 延用 V01 framing，类型和协议域仍参与摘要。新控制对象的字符串为 u32-BE 长度和原字节，整数为 u64-BE（布尔/level 为单字节）；ID 使用 64 个小写 hex 字节。具体 codec 由 `ContentArtifacts` 实现及反例测试固定，未知协议/规则、额外字节或非规范编码拒绝。

metadata 投影仅抽出五个 `__iotdbfs.dataset_index.{protocol,sha256,bytes,records,attributes}` 到外层版本封印，未知同前缀字段拒绝；其余 metadata 原样保留并规范编码。既有 HDF5 layout/5、GRIB layout/1、Arrow layout/1 与 dataset-catalog/1 原始字节不做语义归一化，布局中的内部 dataset 路径、物理偏移、对象 token、过滤器及 builder 观测值均保留。相同字节才能共享，不把“科学语义相同”当成内容相同。

`metadata/layout/catalog-object` 保存长度、chunk 数、索引根与原始 SHA-256。数据 chunk 至多 60 KiB；索引页至多 256 child、64 KiB，树高不超过 8。引用包含 child 的长度/chunk 数，范围读仅遍历命中页并核对完整 chunk 摘要，整图验证额外核对原始 SHA。没有整文件 layout 读取的错误回退。

### 8.2 发布、恢复与缓存

发布保留原 staging/marker/secondary 语义验证，并由 adapter 完整验证布局：HDF5 验证完整目录与所有 chunk 页，GRIB 验证全部记录语法/范围和现有 reader 解释，Arrow 完整解析 native layout。内容对象与版本封印全部预写、读回并核验后，才能随 namespace head 发布。没有 catalog 的 byte-pass 必须同时没有 layout、metadata 或格式证据。

恢复先完成 V01 head/journal/四树校验，再深验证当前 Exact 所有文件的内容闭包与格式布局；当前实现按各版本逻辑引用累计 64 MiB 预算，重复内容也计费，超限拒绝而不跳校验。成功后重试已提交文件的暂存清理：删除旧 layout/catalog OBJECT、清空 staging inode.metadata 并删除 metadata-set 行，保留每版本 payload 和 secondary dataset/attribute 行。该清理只针对已验证提交键，不是共享内容 GC；失败计数保留，不能把已提交 head 回报成未提交。

统一有界 LRU 缓存内容字节、解析 metadata/catalog、已验证文件与布局；条目包含 namespace/incarnation、对象类型/ID、构建规则及解码上下文。HDF5 目录/页与 GRIB parsed index 额外按经 backend 验证的布局身份共享；每次命中仍检查当前源的验证状态。失败不产生成功缓存。权限、版本/provenance、数值与解码缓存继续使用精确源身份，不能因为 layout 相同而复用另一版本 payload 数值。

缓存上限约束缓存自身持有量；调用者已借出的 immutable shared_ptr、临时解析内存和数据库内存另计。V02 的 `ContentArtifacts` 由 backend 的 mutation mutex 串行协调使用；V03 改为短缓存锁，并将已发布对象读取放到 mutation/admission 锁外，写入仍由 owner 协调。冷恢复目前逐版本强制深验证，不能把热解析复用称为冷启动解析消除。

### 8.3 身份/权限与内容分层

inode、children、source scan、permission 只读 committed namespace 身份；不预先触碰无关文件证明。文件 catalog skeleton 用仅进程内的 `metadata_materialized=false` 标记，授权后只加载所选 exact manifest，核对格式/尺寸/revision，再执行 metadata 谓词、seal/time/marker 与格式规划。该标记不进入持久格式，客户端不能据此提交已验证身份。

新 typed 注册/execute、授权缓存命中与新 open 重新校验内容；普通与 virtual 文件缓存读取核对 pinned exact 源。已打开物化 IPC 的 read/fstat 语义保留。此阶段仍没有 V03 的完整 ReadContext、最终发布原子准入或跨调用统一预算/deadline，不据 V02 热缓存提前宣称这些能力。


## 9. V03：请求上下文、准入和共享预算

V03 保持 `namespace-record/2` 与 V02 profile，不改已发布对象编码。ReadContext 是进程内对象，客户端无法序列化或上传授权证明。它保存同一 committed root 的全部冻结候选、精确版本内容绑定、Exact/Permission/Liveness 依赖和去重祖先的 Current 依赖。文件或 dataset 残余谓词未命中的候选、零命中结果仍保留这些依赖。Latest 只在初选解析一次；后续发布新版不替换上下文中的旧精确版本。

变更窗口同时受 256 次提交、65,536 个依赖键及 8 MiB 计费容量限制。只在提交完成后登记变更；覆盖缺口、单次变更过大、恢复后窗口丢失或 incarnation 不符均不得返回增量未变证明。Changed/Unknown 路径有界重验全部冻结源三表及当前祖先，重新判断真实请求主体权限，再验证内容绑定；不重新选择 Latest。对不同 uid/gid/规范化补充组不缓存永久授权。

准备、独立执行、发现结果返回、新结果注册和新 open 均重新准入。查询的远端读取、内容验证、解码、IPC 编码、结果哈希和空间回收在提交门外执行；最终 exact-head 门检查与结果注册/桥接条目 ready 安装之间保持提交串行。结果碰撞比较及大对象析构也在门外。失败不安装可访问结果。发现 socket 在发送前准入，以该检查作为返回结果的本地线性化边界，释放门后进行网络发送。

执行租约仅覆盖一次在途执行，不由 prepared ID 或未打开注册结果永久持有。共享解码 job 在入队时复制执行 token；即使等待请求取消或超时返回，也保留至实际 producer I/O 及源捕获释放之后，再在缓存锁外释放。完成的缓存、FragmentLease 不长期持有 token。当前尚无物理 GC，租约计数为 V05 停写回收提供生命周期依据。已有物化 IPC 句柄继续固定字节的 read/fstat；新的 open 重新准入全部候选。底层源读取与物化 IPC 的生命周期仍不同。

2026-10-16 用户另行确认请求级元数据预算。每个外部 owner 请求建立 64 MiB ledger，嵌套扫描、准备、执行、编码和最终发布共用；独立 execute/open 建立新 ledger，长期 ReadContext 不保存累计额度。普通读取 worker 显式传递预算；共享解码 producer 只收集费用 trace，各等待者在返回或热命中时按自己的 ledger 重放，单个请求超限不污染其它等待者或成功缓存。trace 的增量内存纳入运行时临时/常驻预算。超限状态不可清除；即使下层错误被捕获、后续为空结果，发布仍拒绝。默认 strict 沿用原流程。

计费按逻辑需求而非 SQL 传输量：head/目录页控制对象按不可变身份计，返回的 namespace 记录按 namespace/incarnation、表、键和编码内容计；祖先及依赖键也计。内容对象 trace 按精确 path/version 限定，相同物理对象被不同版本引用分别计；同一版本在同一请求内重复准入复用已计证据。布局身份的完整布局需求、每次显式 read_layout 的逻辑读取保守累计，缓存命中不能免计。原来更小的单 catalog、输出、源数及 adapter 限额继续执行；payload/range 限额沿用现有查询预算，V04 接续 extent 和 payload 共享读取边界。已有取消、缓存等待和调度超时错误禁止发布；本阶段不声称已提供整个 IoTDB SDK 调用链可抢占的统一 deadline。

目录控制对象 LRU 上限 64 MiB；ContentArtifacts LRU 另有 64 MiB 计费上限（包含对象 trace 的估算持有量）；current-root 身份快照另以 64 MiB 编码逻辑字节限制。活跃旧上下文、借用 shared_ptr、临时解析和结果内存分别存在，不把这些上限相加后的进程 RSS 宣称为 64 MiB。缓存锁仅保护短查找/安装。用户另行确认将写入串行锁与目录准入锁分离：事务准备不可变对象时不占准入锁，最终小型 head 写入、确认和 journal 对账窗口与准入串行；确认不明继续冻结。查询远端读取及内容解码不占提交门。恢复强制深验，不由热缓存隐藏持久损坏。


<a id="candidate-domain-lifecycle"></a>
### 9.1 候选域子视图与完成结果引用（2026-10-27）

Owner查询共享原HotQueryCache的64 MiB、16个完整域及64个主体证明额度，包含构建与退休引用。若同head的驻留父域覆盖全部请求精确源及其祖先记录，正向Exact选择可以作为该父域下的轻量子视图保留，每父域最多64个；子视图及派生状态另按实际保留估算增减父额度，不新增完整域名额。首次子视图沿用原选择和费用捕获，比较完整head及源／祖先的规范记录后才共享父snapshot。覆盖不足、未命中、Subtree或可选缓存拒绝继续原路径，不从部分父域推断空结果，也不重新选择已经冻结的Latest。

子视图保留自己的源集合、依赖、Facts、费用和主体证明；父域仍须真实驻留才可命中。父域淘汰后，正在使用的子视图继续固定版本并计费，最终引用释放后才退款。子视图不拥有父context或索引，避免形成循环引用。必要的完整候选权限和错误检查不因共享而省略。

完成结果可用`QueryReadContext::result_reference()`提供独立Auxiliary计费的轻量证据，保存原来全部冻结候选（包括谓词未命中项）、祖先、依赖与权限费用；不保存发现域、adapter派生状态或组合费用summary。新open使用新请求账本并按原依赖重新准入。可选构造拒绝时保留原context，不能新增查询错误。原执行context与请求账本由publication cleanup保活，最终发布准入释放后才析构；完成结果不长期保留执行租约。

profile的`hot_cache_before/after`是挂载共享额度快照，包括resident、inflight及retired域，不是单查询增量或RSS。`auxiliary_bytes`包括`handle_bytes`，不能重复相加；基础4096字节加domain/proof/auxiliary三类额度构成池总计费。取样在server_total时钟外，仍计入客户端完整请求耗时。

<a id="multiversion-reclamation"></a>
### 9.2 V≤32目标下的统一压力回收（2026-10-27）

热池的64 MiB、16完整域、64 Proof及每父域64子视图上限保持不变。派生索引、费用summary、精确子视图及Proof的可选缓存引用改用独立`RetainedSlot`；执行时取得自己的强pin，清槽只影响后续缓存命中。槽、容器key/node和弱注册表容量计入Auxiliary；最多4096个注册槽，已清空槽的管理额度保留至实际持有者退出。注册表只弱持槽，槽不拥有池或请求账本，不形成保活环。

`reserve`和`Reservation::grow`遇到字节压力使用同一路径：先尝试释放低优先级可选引用，再回收完整域LRU；正在增长的Domain不作整域淘汰。仅因Proof数量已满时直接旁路新增可选Proof，继续原准入路径，不以连续miss清除近期标记或轮换现有Proof；否则冷建或一轮多个溢出miss会使顺序重扫持续零命中。字节压力仍允许进一步释放近期槽及父域Proof，规划owner退休也继续清理可选槽。代价是同一长寿命域内的新热点可能需要等待字节回收或owner退休才能进入Proof缓存；这不是通用频次准入或任意工作集的命中保证。失效、额度拒绝和分配拒绝不能改变冻结来源、失败粘性、权限、预算、取消或科学查询错误。

派生`QueryDomainCache`通过格式无关的可选Retention接口接入；Claim alias持有不可变Entry及其原Allocation。清槽后可以重新构建，旧执行pin仍计费至最后一次释放，single-flight仍由构建Claim结束唤醒。Proof及其summary共用原费用额度；Proof退出时解除summary的可选保留，活跃费用账本或准入引用仍保活计费。完成结果只共享可退出Proof槽及原必要证据，不强持Proof对象；规划context退出时清理其可选证明，结果后续open仍按原依赖重新准入，不能提前使结果路由失效。

所有回收析构均在槽／池／候选context锁外进行。完整域`claim`在申请可能触发回收的额度前释放池锁，之后重新核验完整head和同key构建／驻留状态；竞争产生的临时额度锁外归还。新增`pressure_requests`、`slot_evictions`、`pressure_domain_evictions`、`admission_bypasses`及槽数量、注册表字节和累计`refunded_bytes`，均为池级计数／额度，不是逐查询独占CPU、RSS或磁盘GC。本轮不修改持久格式，也不实现物理对象回收或极多版本分页。

## 10. V04：完整块映射、真实批读与固定内容读取

本节依据 2026-10-17 r3 冻结生产代码记录已实现行为；最终真实数据库/FUSE 验收已通过，主代理报告 (local archive: `docs/reports/version-directory-v04-validation-20261017.md`)及机器可读验收收据 (local archive: `experiments/cross_version/results/v04-acceptance-20261017.json`)承载最终结果。用户已确认使用新 namespace/profile/descriptor，并在确认 head 后删除已提交版本的 payload staging；1 MiB、页大小、树高及请求上限沿用第 6 节，不因新协议调整。V04 不实现 V05 GC 或 V06 迁移，不重定义冻结性能基线。

### 10.1 新 namespace 与持久引用

V04 只在全新表前缀初始化。Current 保留键仍为 `NUL + iotdbfs-namespace-profile`，genesis 原子写入的 profile 完整字节为以下六行、每行含末尾 LF：

```text
iotdbfs-namespace-record/3
iotdbfs-content-build/3
identity-free-wire-bytes
60KiB-pages
1MiB-payload-blocks
complete-extent-map/1
```

reopen 要求完整 profile 字节相等；不识别为 V04 的 V01/V02/V03 namespace 拒绝启用，不替换旧 journal、不就地升级、不回退旧 payload 表。旧阶段数据应由其固定旧二进制读取。head、commit、四目录树及 Store 表 schema 延用原协议，变化由新 profile 和对象协议明确隔离。

`namespace-record/3` 延用 `/2` 的字段顺序：POSIX/精确版本与 manifest 尺寸/revision、descriptor/seal 引用；文件 metadata 仍不内联，目录 metadata 仍规范内联。文件 descriptor/seal 必须是有效 64 字节小写 hex，目录两引用为空。其字符串继续使用 u64 长度前缀；旧 `/1`、`/2` record 不被 `/3` decoder 接受。

FileDescriptor 的 wire 协议字符串与外层对象域均为 `iotdbfs-file-descriptor/2`。顺序为：协议字符串、format、payload_size u64、layout_size u64、metadata ID、layout ID、proof ID、payload_root ID。字符串为 u32-BE 长度及原字节，ID 为 64 个小写 hex 字节，四个引用均必需；descriptor 不含 path/version、POSIX 权限或历史前驱。`format-proof/1`、`version-seal/1` 对象域保持不变，但构建规则固定 `iotdbfs-content-build/3`，seal 通过 descriptor ID 绑定新 payload 根。相同内容可共享 descriptor/map/block，各精确版本的外层 seal 和权限/删除身份保持独立。

### 10.2 Payload map 与 extent wire

下列对象均使用第 2 节的 ContentId framing，存于同一 namespace/incarnation 的不可变 BLOB 对象表；不跨 namespace 去重。整数为大端；这里的引用使用 **64 字节 hex 文本**，不与 head/catalog-page 中的 32 字节原始 ID 混用。decoder 拒绝截断、尾随字节和非规范 ID。

| 对象域 | 编码及约束 |
|---|---|
| `iotdbfs-payload-block/1` | 原始编码文件字节，长度 1 至 1,048,576；无额外块头、填充或解码/重编码。尾块按实际长度。 |
| `iotdbfs-payload-map/1` | `VPM1`、payload_size u64、extent_count u64、height u8、根页 encoded_size u64、根页 ID 64 字节；恰为 **93 字节**。extent_count 必须等于 ceil(payload_size/1 MiB)。空文件的 count/height/page_bytes 为 0，wire 根页 ID 为 64 个 `0`，内存为空 ID；payload-map 自身仍是实际不可变对象。 |
| `iotdbfs-extent-page/1` | `VEP1`、level u8、begin/end/extent_count 各 u64、entry_count u32，随后是叶 extent 或内部 child。非空且至多 256 条、64 KiB；level 0 为叶，map height 为根到叶页数且至多 8。 |
| 叶 extent | logical_offset、length、content_offset、content_size 各 u64、block ID 64 字节。logical_offset 按 1 MiB 对齐，content_offset=0，content_size=length，length=min(1 MiB,payload_size−logical_offset)。 |
| 内部 child | begin、end、extent_count、encoded_size 各 u64、child page ID 64 字节。子页 level 恰少 1；返回页必须与父引用中的范围、条数及编码长度一致。 |

page 范围为 `[begin,end)`，叶 extent 或子页严格连续、无洞无重叠，条数累计一致，所有页均落在 descriptor payload_size 内。遍历拒绝重复/循环页及不一致树高。根始终描述完整文件，不含前版本指针、差分补丁或 fallback locator。每请求 65,536 extent 是读取需求上限，**不是整文件 extent 总数上限**；可对更大映射做受限范围 seek，仍受树高、元数据和调用方预算约束。

构建按顺序从 staging 读取最多 1 MiB，完整块经专用写通道持久化并逐字节读回，再构建 leaf/parent/map。相同块/页复用相同 ContentId，但当前发布仍读取此次 staging 并构造完整映射，不声称已实现按变化范围增量构建。块写、页/map 写或读回失败发生在 head 前：事务不发布，已有对象可成为不可达孤儿；中止后 owner 可接受新事务。head 确认不明仍遵守第 3 节 Frozen 规则，不能套用“准备失败可继续”的结论。

`verify(root,size,false)` 只验证 payload-map header 与 descriptor 长度，避免每次 manifest/range 读取扫描整文件；范围读取继续验证命中路径和完整命中块。`deep=true` 强制绕控制页缓存，遍历全部映射页并逐批读取/摘要校验全部块。恢复在开放 namespace 前对当前 Exact 所有文件执行内容和 payload 深验；控制元数据共享恢复累计 64 MiB 上限，不用请求的 extent 上限裁掉整个文件图。该恢复是全量读取成本，不能以热读或少量范围 seek 的结果称其为零 SQL/快速冷启动。

### 10.3 Store 批读与范围组装

Store 新接口 `get_objects(namespace_identity, requests)` 接收 `(ObjectRef, expected_size)`；实际 IoTDB 实现通过**一次 SELECT**读取该组精确 namespace/incarnation/type/protocol/ContentId 的 BLOB cells，按请求顺序返回，不循环调用单对象 SELECT。每批至多 64 个唯一引用且预期总字节至多 8 MiB；重复请求、重复/意外返回、缺项、非 BLOB、长度或摘要不符均失败。Owner 对整批再次校验后才安装可缓存的控制对象，失败不会将前半批当作完整成功缓存。部分控制页命中时只批读 misses；force 深验绕过缓存。payload-block 不走通用 metadata 单对象通道，不进入控制对象/内容 payload 缓存，也不计入 64 MiB metadata 域。

一次公开 batch 接受 1–64 个正长度范围，输出累计至多 8 MiB，逐项检查文件边界及溢出。返回顺序与调用者一致，允许乱序、重叠和重复范围。读取先按层批取命中映射页，再按块 ID 合并实际 I/O；每批最多 8 MiB 完整块。完整块验证成功后直接 scatter 到各输出切片，不先收集所有命中块再拼装。小跨块读取可以产生整块放大，重复/重叠只减少实际传输，不减逻辑请求费用。

公开 owner scalar 上限为 1 GiB，拆为至多 8 MiB 的多个 batch，保留同一固定根与累计请求 ledger；块放大后的累计费用仍可提前触限。scalar EOF 零长度保持空结果；内部 batch 空集合或零长度项拒绝。strict 原有 scalar 行为不因 owner 上限改变。短读、损坏或预算失败返回明确错误，禁止回退旧每版本 payload OBJECT、整文件读取或历史重建。

实际观测区分旧 SDK `query_calls` 与 daemon-wide `version_store_query_calls`、`version_store_head_reads`、`version_store_object_reads`、`version_store_blob_bytes`、`version_store_batch_query_calls`。总实际查询包含旧 SDK 与新 Store 两部分；缺 instrumentation 不当成 0。`payload_map.data_batches/block_reads/block_bytes` 等 profile 仅说明对应读取 scope；热四块读取能否只发一次真实 SQL，须由生产 ReadHandle 探针另行验收，不能以独立 Store inspector 替代，也不能由 discovery SQL=0 推导 payload SQL=0。

### 10.4 已发布内容能力与生命周期

`StructuredBlobManifest.payload_reader` 是仅进程内的不可变内容能力，不进入持久 wire、不能由客户端上传。owner 发出的能力核对 backend issuer、exact key、revision、Published 状态、format 与 payload/layout 长度，持有已验证 descriptor/seal、payload 根与内容服务；strict 默认返回空能力。新 open 仍从当前可见目录加载/重验 manifest 后绑定能力，删除后的新 open 失败。

ReadHandle 成功绑定后，payload/layout/layout identity 通过该能力读取原 exact 内容，不因另一个新版本成为 Latest 而重选，也不在每次读取时重新查目录映射。后续删除目录引用不改变已绑定内容；每次实际读取仍检查 owner 非 Frozen、范围及预算。FUSE 普通已打开文件的 EBUSY 规则、typed 新 execute/发布/open 的 ReadContext 再准入与物化 IPC read/fstat 规则保持原有边界。training snapshot/window 的已固定源沿用该能力，不绕回已清除 staging 的读取。

该能力不是新的永久授权凭证，不代替新请求授权，也不是已实现的 V05 GC 根集/在线回收机制。V04 没有物理共享内容回收；未来 sweep 必须明确考虑活动读取、固定内容根和第 5 节的停写租约边界。

### 10.5 三类预算、运行时预留与取消

metadata 64 MiB、payload 1 GiB、extent 65,536 分域累计，任一超限使当前请求失败且不可清除。metadata 仍包含 head/目录、内容证明、layout/catalog 及命中映射页；共享 map 页按精确 source-qualified 身份计逻辑费用，缓存热命中不能免计。payload 计每个逻辑 range 引用的**完整编码块长度**，尾块按实际长度；extent 计各 range 涉及的块条数。重复范围、不同 exact 源引用同物理块仍分别累计；同一物理 batch 的去重只影响实际 I/O。此处范围指提交给 payload reader 的逻辑范围：一个 batch 的重复项及独立直接调用分别收费。同一请求重复消费同一已解码 producer 节点时，重放同一份稳定 fee identity，已计需求可以复用；这属于同一 DAG 工作节点，不把每次缓冲复制另算一次存储读取。HDF5 distinct fragment 的 logical bytes/ranges、GRIB 原有 logical payload，以及输出/行数预算继续单独执行。不同 source/version 的 fragment key 不同，不因此共享 producer 费用。没有把 payload 块字节加到 metadata 账本中。

普通 owner Storage/backend 读取入口在没有活动 ledger 且没有 producer trace 时建立 standalone 请求 scope，覆盖首次 manifest/绑定/后续 map 读取，不能每个子调用各自重置 64 MiB。已有外层 ledger 复用；独立请求使用新 ledger；strict 不新增 owner scope。共享解码 producer 有中立 trace 时不借用首等待者 ledger，payload/extent/metadata 费用均捕获并由各 waiter/热命中重放，单个 waiter 失败不毒化其它 waiter 或缓存。

2026-10-17 按用户要求，以完整查询耗时决定是否保留性能改动。重复根校验复用与 head 热路径两轮试验均未通过该标准，已回退到 V04 r3 (local archive: `docs/reports/version-directory-v04-head-hotpath-optimization-20261017.md`)。当前 manifest 与 catalog 准备各自执行原根验证，head 观察沿用原编码计费；必要验证、请求预算、权限与最终准入保留。局部调用减少或微基准收益不能作为保留优化的依据。

`QueryStorageReadControl` 在映射/块处理检查取消与执行状态；DecodedFragmentCache producer 把存储临时预留计入其运行时 live/temporary/peak，实际存储 bytes/ranges 另加到原 adapter logical I/O 计数中。额外容量不足时先回收可回收缓存项，否则明确 ResourceExhausted，不等待借用者释放且不允许 loader 吞掉失败后发布成功。检查点或实际 I/O 回调的取消/原错误状态保留，不统一改写 Internal；预留 token 在锁外释放，失败也归还费用。

PayloadArtifacts 为 batch 输出申请临时容量，并为每批完整块及 SDK/result/hash 拷贝申请 `4 × 该批块字节`；输出至多 8 MiB、单数据批至多 8 MiB，因此该预留工作集上界为输出加 32 MiB。64 个跨块小范围即使引用 128 个唯一 1 MiB 块，也逐批 scatter，不将 128 MiB 完整块同时保留。这里是有控制回调时的保守预留范围，**不是整个进程 RSS 上界**：映射/解析、缓存、调用者最终 scalar 结果、数据库内存另有预算和生命周期。同步 SDK 请求仍不可由这些检查点抢占，不新增全链路强制 deadline 保证。

GRIB numeric grid 的独立 decoded-field 缓存同样在每次请求预留逻辑 message 字节，且不持有请求 ledger。2026-10-18 的内容共享修正将精确源身份与解码内容身份分开：源 alias 包含 storage namespace、path/version、revision、payload/layout 长度与 message 范围；共享字段按同 namespace、实际编码 message 的 SHA-256、长度及固定 `grib-native-f64/2` 规则定位。首次绑定新源仍必须成功读取其编码 message，不能用其它版本的内容命中绕过失败。底层 encoded-message key 与 decoder token 仍保留精确版本身份；layout identity 不能代替 payload 内容身份。

每个源 alias 仅保存自身中立 producer 的稳定费用轨迹，每次消费向当前 ledger 回放；共享 canonical Float64 字段不携带首来源费用。重新解码时产生的物理读取轨迹不会再向消费者加入另一套需求 ID。相同内容的并发解码合并，失败不负缓存，失败等待者用自身 producer 重试并归属本源；请求预算失败不污染共享结果。crop、输出 dtype、插值及 provenance 仍按当前源处理，不合并不同版本的输出。

native-field 保留预算仍为独立 64 MiB、最多 16 个字段，包含 pending 管理、精确源 alias/费用及淘汰历史；alias 与 ghost 各至多 256 项。两队列以 probation FIFO 接收首次内容、protected LRU 保留再次使用或 ghost 命中的内容，回收按 probation 的数量／字节四分之一阈值选择。插入 pending 前先计管理费用并检查上限，无法腾出空间时旁路不保留；过大字段不保留。该预算不保证超容量的不同字段永不重新解码，也不是进程 RSS 上限。

冷 message 与 BinaryView 复用受控 encoded-message loader，持 FragmentLease 的内存视图供 native 解码，实际存储块的临时预留/IO 因而经过同一 runtime。编码摘要在 fragment 加载时生成并计入其大小；暖 alias 命中不重新 hash 或获取编码 fragment。异步 grid 传递执行租约，超时后仍在读的内部 producer 保留租约直到结束。ecCodes/native-grid 临时分配、cursor 输出及 decoder message 副本仍沿用既有 GRIB 模型，不声称所有 native 分配都纳入共享 runtime 总量。native 观测账本仅在数值网格 cursor 中分配和汇总；局部计数减少不能代替完整查询性能验收。

### 10.6 Head 后清理与验收范围

发布完成的顺序是：原 staging/secondary 格式校验 → 写并读回 payload 块、完整 map、内容证明/descriptor/seal → namespace 四树/commit 与 durable pending → 确认 head/journal → 发布内存根 → 清理该已提交键。清理删除新 prefix 的 `blob_payload`、`blob_layout`、`blob_dataset_catalog`、`blob_metadata_set` 行，并把 SQL inode.metadata 清为 `{}`；dataset/attribute secondary 行保留。公开 owner 内容读取只走已发布图，不依赖被清理的每版本 payload。

清理是 head 后的尽力步骤，不具备跨 SQL 原子性；失败累计 `staging_cleanup_failures`，不能把已提交 head 反报为未提交。重启先完整认证当前所有 Exact 内容闭包，成功后重试已提交文件清理。head 前未提交 staging 或不可达块/页不是当前闭包，保留为孤儿；本步骤不执行共享对象 sweep、不声称回收旧 map/块或 tombstone。删除一个版本只撤销目录引用，不能删除其它版本仍使用的块。

V04 真实验收需分别记录：科学 HDF5/GRIB 版本与权限/重启语义；大于 3 MiB 的 byte-pass 相同/单块变化两组各 8 个真实版本；独立块 ID/整文件和范围哈希；独立 Store 图检查；生产 ReadHandle 的四块一 SQL 与乱序/重复/跨块输出；head 后 staging 清理及旧 V03 数据不变。逻辑共享引用、唯一引用 BLOB 字节、暂存/孤儿与物理数据库占用分开报告，raw 文件复用或唯一 block 比例不能直接称为端到端物理压缩率。本契约记录实现与要求，最终通过数量和真实门禁结果以独立验收收据为准。
