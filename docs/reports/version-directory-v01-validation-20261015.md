# V01：目录提交与持久身份验收

日期：2026-10-15。开发基线 `eb4911543fed0f3d0e68a0c97de3770c7eb8513d`。关联[实施计划](../deliverables/version-directory-content-sharing-plan.md)、[实现契约](../contracts/version-directory-content-sharing-v1.md)。

**V01 已完成。** 新增格式中立的持久目录、COW 页、单 owner 和恢复协议，并接入普通 FUSE 与 typed 目录。251 项相关单测通过；真实 IoTDB 的 8 项故障包装检查、新模式 12 项目录/恢复检查、9 项非 root 权限检查通过。新二进制默认 strict 模式复验原 256 个科学版本和 1,024 个 typed 单元通过。默认模式、原服务配置、许可证及原数据保留。

## 1. 交付与启用边界

新增 `include/iotdbfs/storage/versioning/`、`src/storage/versioning/`：NamespaceIdentity、固定单 BLOB Head、Commit、四树 CatalogRoot、确定编码、SHA-256 ContentId、有界 COW 页树、Owner、FileJournal 和 IoTDBVersionStore。head 为 140 字节；页至多 64 KiB/256 项，树高至多 8，恢复不遍历前驱 commit 链。具体字段、golden 编码与预算见契约。

backend 的 current、exact、liveness/tombstone、permission 由同一 head 发布。普通 inode/目录、typed 源目录和授权事实读取 committed root；旧 SQL 行仅作为 staging/不可变 artifact。发布、chmod/chown、utimens、目录修改、精确删除、整路径删除及 rename 均接入 owner；rename 在一笔事务内准备目标并撤销源。owner 模式跳过旧 inode dirty cache/后台 writeback，普通 reader 不读取另一个 writer 尚未提交的缓冲区。删除先撤销可见性，保留 tombstone 和 artifact，尚无物理 GC。

按用户要求，所有实验继续使用现有共享账号，**没有创建账号或修改 ACL**。首版排他范围是受控同一主机、固定 loopback 数据库与表前缀；canonical namespace flock 不受环境变量覆盖，私有 journal 再持有独立 flock。跨主机 fencing、知晓前缀的同账号 raw SQL、管理员删除/替换锁或同时回滚数据库与 journal 不在保证范围。旧 direct client/迁移在显式 owner 配置下拒绝运行；默认 strict 仍访问原表。

新模式须显式设置 `storage.version_directory.mode=owner`、新 `vd_` 表前缀、持久 `owner_directory`；仅首次空 namespace 允许 `initialize=true`。门禁读取实际 DataNode 配置，要求单个 Running loopback DataNode、有效 `WAL=SYNC`、`enable_partial_insert=false` 及精确 schema。正常 reopen 必须使用原 journal，禁止自动采用数据库 head、补造身份或清除结果不明的 pending。

对象/页/Commit 先写入并逐字节读回，再 durable 保存 pending head，最后单次写 head。ACK 或独立读回完整目标后才更新 committed journal 与内存根。读回旧 head 仍 Frozen，不能假定旧 RPC 不会稍后到达。SDK head 写不走旧 Connector 重试池；非查询 RPC 的实际 SDK timeout=0，因此本阶段不宣称全请求的有限 deadline。

## 2. 构建与单元测试

所有服务器路径以 `/data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/` 为根。独立构建位于 `experiments/cross_version/work/v01-verification-20261015/{source,build}`，未覆盖旧性能构建。使用现有 SDK、Arrow/HDF5 依赖，构建 `iotdb_fs`、`v01_store_probe`、`unit_tests`；完整新源码构建和修复后的增量构建均成功。

最终相关 filter 共 **251 tests / 35 suites，0 failed/disabled**，包括 34 项新目录核心测试、原 V00 的版本/权限/查询回归以及全部 QueryUtils SQL 相关测试。证据为该 work 下 `acceptance-tests-final.{xml,log}`。覆盖独立 SHA/golden、非规范编码、精确键和 record 自洽检查、页字节/条目/高度上限、COW 分裂、四树原子提交、旧 View 不变、写前/写后失败、ACK 不明、迟到目标、journal 缺失/损坏/锁/进程退出、数据库回退、缺对象/错引用闭包及无需历史链恢复。

两个最终生产二进制 SHA-256：

| 二进制（build/bin） | SHA-256 |
|---|---|
| `iotdb_fs` | `1244291ee196860e088a100973f8e5596e9fbafaba3959ebefb7e778adb97b50` |
| `v01_store_probe` | `3098f6f965729a48d1be18c5d0b20947d3b366ddd9b0c73ab7cab08e8f480b1c` |

最后同步仅注释变化的 header 后重新构建，二者 SHA 与真实验收使用的二进制逐字节相同；并非用旧 baseline 二进制替代新实现测试。源码清单、构建日志和最终状态保存在上述独立 work。

最终本地与远端冻结 source 的 src/include、CMake 描述、unit/support C++ 文件共 **263 项，0 差异**。`final-local-source-sha.json` SHA 为 `02bc9657c29d5f422bae7dcbe5b2809ac962b627db789a198b531f80a4f18415`；核对与测试绑定 `final-source-test-binding.json` SHA 为 `920faf4c4390b4e890e6d485436683de257416324dae3de89a839aa0c53538cd`。最终 XML SHA 为 `85be41a00f126df443a00d3055c286efd4a93a54067ec6d39ce0bb7d256fc5b0`。清单核对覆盖所列文件，不把未编入此 target 的其它程序也称为已运行。

## 3. 真实 Store 故障检查

`v01_store_probe faults` 使用原数据库 `h09probe1790939889dbfc1f` 的新表前缀 `vd_v01_fault_e2ff03bf1b0f4633`，不创建数据库或额外实例。八项检查全部完成：

1. 初始化、四树提交、fresh owner 恢复。
2. 真实 head 已持久成功后，由包装器丢弃 ACK；独立读回目标后完成提交。
3. 对象 RPC 未发送、Commit 对象阶段抛错：旧根保持可见。
4. head 成功后，包装器使清 pending 的 journal 保存失败；fresh owner 从 durable pending 恢复目标。
5. 包装器返回未发送 head 的 Indeterminate：当前 owner 与 fresh owner 均保持 Frozen，旧 head 不能清 pending。

该组实际发出 4 次 head 写，包装层记录 5 次尝试、53 次对象 put 调用、10 次 journal save、2 次对账。末尾故意保留 Frozen 与 pending 作为证据。它是**真实 IoTDB 上的进程内故障包装**，不冒充真实网络故障；V00 的真实响应丢失/ACK 原语证据另见 V00 报告。

## 4. 新模式 FUSE、科学文件与恢复

成功结果 `experiments/cross_version/results/v01-live-20261015-r2.json`，`ok=true`，SHA-256 为 `90be7ab5ad79b5a5f618c77462823d0bba24ee3d68136255c9f05bedde0e80bf`。私有配置、mount/probe 日志、原配置和操作轨迹在同名 work；未记录凭据。原始输入复用已有 corpus，不另造一份 GRIB/HDF5 环境。

主前缀为 `vd_v01_e2ff03bf1b0f4633`，持久身份：

```text
namespace   8da31d6e-41d3-4a5d-bcb2-ec06369c4e4d
incarnation 75943fbc-4848-40b0-b2d1-1c6b656ef055
final seq   63
final root  b3cd54617c8582f2a0e6772ebf13d75405c0061c1889d45504d7900f46b71659
```

实验复用 identical 组的一个 HDF5 和一个 GRIB 文件，各发布 8 个科学版本，共 16 版/64 单元；普通 create/O_TRUNC 产生的空版本另保留。所有科学版先检查完整原文件 SHA、目录 exact provenance，再按独立 native oracle 检查 Float32 数值与有效性掩码。rename 后再次检查全部 16 版，删除一个最新 GRIB 精确版本；崩溃恢复后检查其仍不存在，其余 15 版/60 单元保持一致。新模式此轮没有重发整个 256 版 corpus；原两组全矩阵由下节默认 strict 回归覆盖。

| 检查 | 实测 |
|---|---|
| mkdir/chmod/chown/utimens | 每次操作 sequence 恰增加 1。 |
| 同 prefix 第二 owner、不同 journal/旧 lock 环境变量 | 拒绝，明确报告 canonical namespace 锁被占用。 |
| 历史 readdir | 恰好显示所有旧版的隐藏名称，当前最大版本不重复显示为历史。 |
| writer staging | writer 自身可读新缓冲，另一个普通 reader 在 fsync 前读旧内容、之后读新内容。 |
| 整树 rename | 包括所有科学/空版本与目录，sequence 恰增加 1；旧路径撤销，精确字节与 typed provenance 正确。 |
| 精确删除 | sequence 恰增加 1；同字节的其它版本保持可读。 |
| DataNode SIGKILL/restart + fresh owner | seq=63 的 namespace/incarnation/commit/root 完全相同，无 tombstone 复活。 |
| 正常关闭后对已存在 namespace 再 initialize | 拒绝非空 namespace。 |
| 原 ASYNC 配置恢复后启用 owner | 有效 durability 门禁拒绝，无静默降级。 |

另外复用 V00 的 **9 项非 root 权限 oracle**，实际启动的是新 owner daemon：历史 exact inode 组权限、最新独立权限、当前祖先搜索权限、历史 chmod/chown 的 EPERM、普通已打开文件 unlink 的 EBUSY、已打开 IPC 的 read/fstat、撤销后新 open/注册拒绝，以及删除最新后 generic/HDF5 Latest 各自的存活范围，均通过。UID/GID=65534，补充组 `[40077]`，`allow_other,attr_timeout=0,entry_timeout=0`。权限报告显式区分实际 daemon SHA 与旧 runtime descriptor 的参考 SHA。

## 5. 原服务和默认 strict 回归

实验仅控制被识别的原 DataNode，ConfigNode 未停止；信号前核对 PID/start ticks/cwd/命令摘要，使用原启动脚本、原环境及许可证。首轮初次优雅停止超时后，按控制器流程记录 SIGKILL；成功第二轮的计划崩溃和恢复均在操作日志中。没有启动隔离实例、获取新证书、创建账号或扩大 region/内存配置。

结束时恢复配置 SHA `ddd678e310f4a48048593333bcf84b9896ab9d7d705d08c642f040723f423728`；额外只读查询实际 applied 配置为 **ASYNC / true**。许可证、`.env` 与 `activation/system_info` 的保护摘要核对通过。结束时 DataNode PID 为 `1306722`，仅为本轮观测值。本轮创建的 mount 均已卸载；owner journal 保留在 `work/v01-live-20261015-r2/owner`，目录 0700、state 0600/294 字节，state SHA `83ffdce8933db80fdc6a07fd96e8870b368de3430275f828b31eb42d5ec02070`。补充证据 `post-restore-observation.json` 不改写原成功结果。

`v01_strict_validation.py` 对原 publication report 建立副本，以**同一新 daemon、不设置任何 version_directory properties**重新审计原前缀 `/cross-version-1b723c3e5e464290`：256 历史科学版全文件 SHA、1,024 个 typed 单元数值/掩码/provenance、32 路径 latest/格式历史集合，以及 identical/single_unit 两组的 8 个 latest/属性谓词查询全部通过。旧 publication 绑定及原数据未重发。结果为 `results/v01-default-strict-20261015/publication.json`，含实际新 daemon SHA 和原报告来源摘要。

## 6. 写入与冷启动成本观测

以下为正确性 runner 的一次观测，缓存已受哈希/验收读取影响，**不是公平性能消融、物理压缩率或发现性能收益**。

| 指标 | 观测 |
|---|---:|
| 初始化新 owner mount（含建 schema/root） | 2.897205 s |
| 崩溃后 fresh owner mount | 5.068490 s |
| 8 个 HDF5 科学版发布中位数 | 4.142465 s |
| 8 个 GRIB 科学版发布中位数 | 3.106997 s |
| 包含所有历史的整树 rename | 47.521384 s |
| daemon 初始 VmRSS/VmHWM | 20,672 KiB |
| 发布/rename 后 VmRSS/VmHWM | 62,804 KiB |
| 恢复并读取后的 daemon VmRSS/VmHWM | 39,420 KiB |

初始目录对象 10 行/891 B，发布、rename、删除后 536 行/786,912 B，权限 oracle 完成后 664 行/956,621 B。最终按类型为 catalog-page 445 行/843,323 B、catalog-record 91 行/75,267 B、catalog-root 64 行/8,576 B、commit 64 行/29,455 B。从初始 seq=1 至最终 seq=63，62 次提交累计增加 654 个对象/955,730 B；包含旧根与事务中间孤儿页，展示首版 COW 写入开销，没有把它们扣掉后宣称已释放空间。

计量是 `_objects.value` 的逻辑 BLOB 字节，不包含旧结构的 payload/layout/catalog/index 表、WAL/TsFile 编码或文件系统分配；RSS 仅为 daemon，不含数据库、子 worker 或 OS page cache。没有 GC，因此本轮不给唯一可达/孤儿的完整账本或磁盘节省结论。V01 typed/children 部分路径仍有有界全 Exact 扫描；发现证明复用、查询级依赖准入优化和性能消融属于 V02/V03。

## 7. 失败记录与后续范围

首轮 `results/v01-live-20261015.json` 为失败结果，原样保留。head/目录提交与 8 个故障包装已经通过，第一次 HDF5 发现却返回空结果。只读 inspect 确认 committed Exact 中存在 format=hdf5、size=85,072 的版本；原因是 strict SQL filter 仍 JOIN staging inode 并要求 published=true。修复以显式 `DatasetCatalogVisibility::OwnerSelectedExactKeys` 使用已经选中、授权、精确重验且被提交锁保护的 exact keys；保留完整性计数、属性预检/谓词、排序/上限。默认 strict 的 PublishedInode 逻辑由原测试及新增 SQL 测试锁定，未用设置 staging published=true 绕过权威根。第二轮完整验收使用修复后的构建。

V01 完成的是目录提交与持久身份。V02 的格式中立内容证明/封印拆分、V03 ReadContext/最终发布准入与热发现重验消除、V04 payload 内容共享/直接范围映射、V05 租约与 GC、V06 迁移均未由本轮提前交付。不宣称跨主机 owner、真实断电持久性、无限 namespace 容量、payload 压缩收益或全请求 deadline。原服务恢复 ASYNC 后，重新使用 owner 前仍必须满足 SYNC/禁 partial insert 的显式门禁。

小型机器可读汇总见 [v01-acceptance-20261015.json](../../experiments/cross_version/results/v01-acceptance-20261015.json)，其远端路径和 SHA 可核查上述完整成功、失败与服务恢复记录。
