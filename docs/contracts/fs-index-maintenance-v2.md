# FS-index 增量维护 v2 契约

日期：2026-10-31。实现入口：`fs_index/maintenance.py`、`fs_index/src/maintenance.inc`。本协议与 `iotdbfs-fixed-native/1` 分开；不原地升级或解释旧目录为新 schema。

## 范围与身份

Linux、本地文件系统、独占受控 store 目录、单 writer、多独立串行 reader。沿用六属性等值科学谓词和每单元 4096 值 profile。GRIB 使用 ecCodes 原生 message；HDF5 为有限 Float32、文件内物理 dataset，不支持软/外部链接、VDS 或外部 raw storage。各句柄不得并发调用；不同句柄可分属独立进程。

句柄不得跨 fork 复用；新进程应使用 spawn/exec 后独立打开，避免继承 SQLite 连接与 writer 锁。

目录内容为 `writer.lock`、`index.sqlite`（及 SQLite WAL/SHM）、`objects/`。初始化要求父目录已存在且 root 不存在，生成随机 store_id；根目录必须是无符号链接的规范绝对路径。初始化失败保留现场，不自动覆盖/复用部分初始化目录。

目录库设置 `application_id=0x46534932`、`user_version=2`。sources 自包含 logical/native path、format、version、ordinal、size、mtime_ns、SHA-256 和发布 generation；source 是不删除、不复用的递增 ID。units 关联该 ID，保留科学属性和原生定位信息；latest 与全局 head 在同一事务更新。外部 JSON 不再作为可变权威。

store 的 `indexed` 模式创建后不可改变。`indexed=true` 登记并验证 units；`indexed=false` 只登记 sources/head，作为 native FS 发布目录。后者不提前提取或验证单元的 dtype/值，故不保证发布时即可执行任意查询；发现和读取仍拒绝不支持的子结构。正式比较只使用两路共同支持的输入。

## 发布与重试

`Writer.publish(input, logical_path, format, version, ordinal, expected_version, op_id)` 的稳定结果包含 generation、source_id、version/ordinal、native_path、SHA-256 和 store_id。每次返回的 metrics 是本次调用的计量，单独用 replayed 标明重试。

- `expected_version=0` 表示新的逻辑路径；否则必须等于该路径最新版本。version 和 ordinal 均严格递增。
- writer 持有 store 生命周期的非阻塞排他 flock；第二个 writer 立即失败。SQLite 写事务仍检查前置条件。
- 在 `objects/.stage-<op-id-hash>` 中复制并计算 SHA-256，检查输入在读取期间未发生普通 stat 身份变化，将文件置为只读后 fsync。
- 用 `renameat2(RENAME_NOREPLACE)` 将文件命名到唯一最终位置，再 fsync objects 目录。已有 orphan 只能在内容 SHA-256 相等时复用，不能覆盖。
- indexed 发布用共享原生 inventory/read 验证全部单元，包含值解码与掩码生成；这部分成本计入发布。写事务之外的提取使用独立短生命周期 Context，不能污染查询缓存和计数器。
- SQLite `BEGIN IMMEDIATE` 事务内写 sources、units、latest、head、operations，COMMIT 成功后才确认。每次只提取新增文件；无隐式全量 rebuild。
- operation ID 与规范化的 logical path、format、version、ordinal、expected version、profile 和内容摘要绑定。同 ID/同请求返回原稳定结果；异请求拒绝。请求源仍需可读取，以核验重试内容。

精确文件的物理可见性早于目录提交，所以协议只保证组件目录查询的原子可见性；外部 `readdir` 不属于事务读者。受控目录外部修改、删除或伪造目录库不属于该协议。发布时的内容摘要与读取时的 size/mtime 校验不能替代文件系统权限或抵御保留 size/mtime 的恶意修改。

## 读快照与缓存

`Reader.select(mode, selection, predicate, logical_scope)` 支持 index/scan，源目录只支持 scan。先更新连接私有 temp scope/ordinal 表，再在短主库读事务内绑定 head，并按稳定 source ID 加载新增映射。index 选择在同一快照完成；scan 在绑定不可变来源列表后释放目录读事务，再扫描原生文件。选出的 locator 列表物化后不再持有 SQLite 快照。

Latest 先选择最新发布，再应用科学谓词：旧版本匹配而新版本不匹配时返回空。`ordinal`/`between` 与 v1 相同，使用明确列出的发布 ordinal，不把 between 参数解释为端点区间。新逻辑路径在下一次 select 自动进入该 reader 的来源域。

locator 包含 store_id、source_id、绑定 generation 和原生位置。`read()` 检查目录身份、来源关联和版本身份，使用共享原生 reader 输出 owned Float32/uint8 数组。它不隐式切换 latest。旧 locator 在同一 retained reader 或加载了该来源的新 reader 上仍可读取；从其他 reader 传入一个尚未被当前 reader 刷新到的来源会失败，应先 select 或重新打开 reader。没有删除/GC，因此不需要长期目录读事务或文件回收 pin。

句柄缓存继续采用精确 native path/文件身份，每 context 最多 8 个文件、8 个 dataset；不同版本使用不同不可变文件名。新 reader/新进程重新加载 metadata，不继承其他 reader 缓存。异常路径 reset 查询 statement、回滚活跃事务并使 temp-scope 缓存失效；失败请求不得遗留阻塞 checkpoint 的快照。

## 恢复与持久化边界

writer 重开后在排他锁内清理本组件命名的 staging 文件并同步目录；已最终命名但未被目录引用的文件保留为 orphan，并在 status 中显式报告。重试在校验内容后可复用 orphan。COMMIT 前崩溃不暴露目录记录；COMMIT 后应答丢失通过 operations 返回原结果，不增加代次。

WAL、`synchronous=FULL`、`wal_autocheckpoint=1000` 在每个 writer 连接设置。新目录创建后同步父目录。原生文件同步失败不能确认成功；目录/数据库错误由调用方处理，不自动忽略或伪装成成功。初始化与 SQLite 目录持久化依赖其标准本地 VFS 和底层文件系统同步语义。SIGKILL 与系统调用顺序测试不证明硬件掉电安全或网络文件系统持久性。

`checkpoint()` 执行 TRUNCATE，返回 busy、log/checkpointed frames、耗时和剩余 WAL。busy 不是成功完成收尾，调用方应记录并在读事务释放后重试。close 也可能触发 SQLite 收尾，应包含在外层生命周期时钟。长期外部 SQLite 读事务可能阻塞 checkpoint，不代表组件普通 select 应长期持有事务。

## 计量

publication metrics 包含 copy_hash_ns、file_sync_ns、finalize_ns（命名/重复 orphan 核验）、directory_sync_ns、extract_validate_ns、commit_ns、total_ns，及复制字节、提取文件/单元数、验证读次数。total_ns 直接测本次调用，不是阶段中位数之和；调用方仍须计量 Python 边界、初始化、最后一次 owned output 和关闭。

status 提供 database/WAL/SHM、数据、staging/orphan 空间、刷新时间/来源数和原生计数器。它会扫描元数据与 objects 目录，不能放在 query clock 内。observed_wal_peak_bytes 只表示本句柄已观察到的最大 WAL 大小，不宣称精确进程/整轨迹峰值；正式实验另采空间和资源样本。

writer 串行调用 status 时，没有其他合法发布者，`space_snapshot_atomic=true`。reader 的目录快照与文件枚举不原子，返回 `space_snapshot_atomic=false`：会跳过刚被重命名的暂存项，可能把正在发布的最终文件暂计为 orphan。精确空间结论必须取 writer 的静止快照；不得将并发 reader 的 orphan 计数解释为失败发布次数。

测试构建才编译 `FS_INDEX_TEST_KILL_AT` 注入点，status 的 test_faults_enabled 明示该状态。正式性能构建必须关闭 FS_INDEX_BUILD_TESTS，并记录源码、二进制、依赖版本/链接路径和环境。测试开关不改变目录 schema。
