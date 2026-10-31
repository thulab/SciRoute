# FS-index 增量维护实施计划

日期：2026-10-31。状态：**P0–P3 已实现，M0 功能门禁已通过；M1三路径54条维护轨迹已完成，M2/M3未执行。** M1的共享两核资源、31,968次查询、持久化/恢复核验与结果见[生命周期报告](../reports/fs-index-maintenance-lifecycle-20261031.md)。下文保留设计基线，实际接口与范围以 [v2 契约](../contracts/fs-index-maintenance-v2.md) 和 [实施验收](../reports/fs-index-maintenance-20261031.md) 为准。

本轮实现映射：

| 计划能力 | 当前实现 |
| --- | --- |
| 单一目录权威、稳定 source ID | SQLite schema v2，独立 store_id，sources/units/latest/head/operations |
| 初始化与发布 | `Writer(..., create=True)`、`Writer.publish()`；单 writer、不可变文件、幂等与 expected_version |
| 快照与增量刷新 | `Reader.select()` 自动刷新；`Reader.read()` 读取精确来源；短目录事务 |
| 故障恢复与成本 | writer 重开清 staging，报告/重试 orphan；`status()`、`checkpoint()` 和发布计量 |
| Native 发布对照 | `indexed=False`，同样发布 sources/head，但不提取 units；使用 scan 查询 |
| M0 | 原有 7 项测试＋13 项维护测试通过；含两格式共 10 个 SIGKILL 子场景及独立进程重叠 |

新增协议通过同一翻译单元复用原生提取、选择和读取函数；v1 核心函数和 Python 边界保留。维护发布只支持单个新不可变版本文件。未加入 GC、删除、多 writer、Arrow 或任意尺寸输出；也未改动旧性能表或论文结论。

本计划承接论文仓库 `reviews/reject-response-20261031/EXPERIMENT_PLAN.md`，细化其中组件维护工作，并纠正与现有实现不一致的假设。当前代码入口见 [FS-index](../../fs_index/README.md)，迁移验收见[报告](../reports/fs-index-migration-20261031.md)。目标是补齐“发布文件＋增量维护索引＋后续查询”的完整 FS-index 路径，不预设 IoTDB-FS 的总成本更低。

## 1. 已迁移的能力与实际边界

正式 FM1 的 C++ 核心与 Python/ctypes 边界已经从 `experiments/fixed_matrix/` 迁至主仓库 `fs_index/`，两份源码保持逐字节一致；独立 CMake、CTest 和实验调用入口已接入。未将其他历史 helper 或 Arrow sidecar 混入同一实现，也未启动新性能实验。

现有组件支持 GRIB/HDF5 六属性等值选择、Latest/历史版本选择、SQLite 持久化目录、精确源绑定和原生 owned output。其约束包括每个选中单元恰好 4096 个值、HDF5 finite Float32/all-valid mask，以及固定的句柄/元数据缓存预算。这是保留的实验查询 profile，并非通用科学索引产品接口。

实施前缺少持续维护能力的原因：

| 现有行为 | 直接增加 INSERT 为什么不够 | 必须新增的能力 |
| --- | --- | --- |
| `build_index()` 要求新文件，整批建库 | 不能对已存在目录安全更新 | 独立初始化与增量发布接口 |
| 外部 catalog 全文绑定为 identity | 文件属性与 SQLite 分属两个权威；新 catalog 无法直接重开旧库 | 自包含目录库与 schema/protocol 版本 |
| source ID 是 catalog 数组下标 | 重排或插入后 locator 可能指向错误来源 | 稳定且不复用的 source ID |
| context 创建时固定 sources/latest/scope | 数据库更新后仍可能安全失败或使用过期状态 | 查询绑定代次、增量刷新与缓存身份 |
| 只有 size/mtime 检查 | 不能形成发布事务，也不能保证文件不可变 | 受控不可变发布与明确故障边界 |
| 读、提取共用状态/计数器 | 写入可能污染读取缓存和性能统计 | 分离 writer/reader 句柄与计量 |

## 2. 实施顺序与最小功能

每一阶段均在独立测试目录验证后再进入下一阶段。旧 `fixed_native_*` ABI 和 `iotdbfs-fixed-native/1` 继续表示冻结 profile；维护接口采用新协议，不把新能力追溯归给旧实验。

| 阶段 | 交付物 | 验收条件 |
| --- | --- | --- |
| P0：目录与接口 | schema v2、稳定 source ID、单一权威；初始化/打开/关闭接口 | 新库可重开；旧协议明确识别；拒绝不支持的 schema |
| P1：增量发布 | 单 writer、不可变文件、幂等 operation、原子更新 sources/units/latest/head | 只提取新增文件；失败无部分目录可见；重试恰好生效一次 |
| P2：持续查询 | 短读事务绑定 generation；内存镜像增量刷新；旧 locator 保持有效 | 发布与读取重叠时结果不混代；发布确认后新查询可见新代 |
| P3：恢复与成本 | 故障注入、事务释放、checkpoint/刷新/空间计量 | M0 全部通过；可以直接测完整生命周期而非拼接阶段中位数 |

### 2.1 目录权威与拟议接口

SQLite 持有完整 source 身份（native path、logical path、format、version、ordinal、size、mtime_ns、内容摘要、创建 generation）、units、latest，以及单行 head。source ID 单调分配且不复用；`(logical_path, version)`、`(logical_path, ordinal)`、native path、operation ID 设置唯一约束。用 schema 版本明确区分存储格式；外部 JSON 作为导入/审计材料，不再与数据库共同充当可变权威。

新增 `operations(op_id, request_hash, result)`。同一 op_id 与相同规范化请求返回原结果；同一 op_id、不同请求必须冲突。request_hash 应覆盖逻辑路径、版本、前置版本、文件内容身份及提取 profile，不能只绑定文件名。

拟议接口（名称尚未作为可用 API 发布）：

- `writer_open/close`：检查 schema、取得独占 writer 锁并恢复未完成发布；设置并读回 WAL/FULL 等连接参数。
- `publish`：输入稳定源、logical path、version/ordinal、expected previous version、op_id；返回已提交 generation、source ID、单元数、分阶段计量。
- `reader_open/close` 与 `select`：每个请求绑定一个目录 generation，并返回稳定 source/locator。
- `read`：仅按已绑定的精确源读取；新版本不能改变旧 locator 的含义。
- `status/checkpoint`：显式报告数据库/WAL/临时文件/待回收量和收尾工作。离线 rebuild 是恢复工具，不得由发布热路径隐式调用。

可在新增 `fs_index_*` C ABI 上提供同名 Python 包装；新旧协议共用提取和读取实现，不复制两份核心。Context 仍采用单线程调用约束；发布和查询使用独立句柄。

### 2.2 发布顺序与可见性

首阶段只支持单源文件、单 writer、同一本地文件系统上的受控目录。版本文件不可覆盖；不实现多文件跨源原子发布、删除或 GC。

1. 取得 writer 锁，检查 operation 幂等记录；确认输入稳定且符合当前 profile。
2. 将输入复制到本次拥有的 staging 路径，计算必要身份摘要；完成文件 fsync。输入生成不计时，但发布必需的 copy/hash/fsync 均计入成本。
3. 在同一文件系统内以**不覆盖**方式发布到最终唯一位置，并 fsync 父目录；新增目录时也同步必要的上层目录。目录之外的进程不得原地修改这些文件。
4. 在目录写事务之外提取新增文件 units；校验该 profile 的可读性，包括 4096 值限制，避免登记后才发现不能读取。任一不支持的单元使本次 source 发布失败，不静默漏记。
5. `BEGIN IMMEDIATE`，再次检查幂等记录和 expected previous version。要求每条逻辑路径 version/ordinal 合法递增；失败不能回退 latest。
6. 同一事务加入 source/units，更新 latest/head，写 operation 结果，COMMIT 成功后返回确认。只有该事务提交才让组件查询看到新文件。

文件系统目录枚举可能在 SQLite 提交前看到最终文件，因此本协议只保证**通过组件目录进行的查询可见性**。Native 对照也必须从已提交的 source/head 目录发现版本，不能直接扫描 staging。数据库事务不能自动保护原生文件字节；精确来源仍依赖受控不可变文件与保留策略，外部篡改应检测并报错。

任何 I/O 同步错误均不确认成功。提交失败时回滚并释放资源；最终文件可能成为 orphan，但不能被查询。重试需重新核验 operation 与内容身份，不能盲目覆盖。确认丢失而事务已提交时返回原结果，不重复增加 generation。

### 2.3 查询快照与增量刷新

查询首先准备自身临时 scope，在短 SQLite 读事务中读取 head、同步相应 sources/latest 增量并执行选择。物化有序 locator 列表后结束读事务；原生解码不长期占用 SQLite 快照。旧源不可变且不 GC，使结束目录事务后的原生读取仍有精确绑定。

一条查询的 source、version、unit、locator 必须来自同一个绑定快照。Latest 必须先选每条路径的最新版本，再应用科学谓词：v1 匹配而 v2 不匹配时，Latest 为空，Exact(v1) 仍返回旧数据。

刷新同时维护 logical scope 和 latest，不能只刷新 SQL 连接。缓存键绑定稳定 source ID/文件身份/profile；仅按逻辑路径的派生缓存需要随 head 失效。进程重启重新建立映射；发布者不借用 reader 的缓存或统计。

需要修复并测试异常路径上的 statement reset、读事务释放和写事务 rollback。RAII 清理或同等机制必须确保一次 select 失败不会长期阻塞 checkpoint，失败的 temp-scope 事务不会影响下一请求。

### 2.4 恢复、持久化与收尾成本

启动时在 writer 锁下识别本组件的 staging/orphan；只处理属于本 operation 的路径，不递归清理共享目录。重试可在校验身份后复用已完成且未登记的最终文件。提交后确认丢失的恢复以 operations 为准。

固定 WAL、synchronous=FULL、自动 checkpoint 设置并在每个 writer 连接验证。小规模的 16 次发布可能达不到默认自动 checkpoint 阈值：除前台完成时间外，必须测量轨迹末显式 checkpoint 的时间、返回状态及剩余 WAL。不能把未完成收尾工作当作免费成本，也不能把 checkpoint 等价于更强的断电保证。

记录 copy/hash、文件/目录 fsync、extract、transaction/commit、reader refresh、checkpoint 的耗时和计数。总生命周期直接采墙钟；阶段中位数不相加。索引空间分别列数据库、WAL/SHM、临时文件、峰值与收尾后大小，不包含原始数据。初始化 ANALYZE 等成本计入初始化；不在查询计时内运行 EXPLAIN 或验证全量 hash。

## 3. M0 正确性门禁

用两种格式小样例做下列检查，不扩展为故障性能矩阵：

1. 新版本改变谓词匹配：Latest、Exact 与历史结果正确；发布前取得的旧 locator 仍可读。
2. 重复 op_id 同请求恰好生效一次；异请求冲突；旧版本或错误 expected version 被拒绝。
3. 在文件同步、最终命名、目录同步、COMMIT 前、COMMIT 后应答前分别杀进程，重开并重试；无部分 source/units/head 可见，无 generation 重复增长。
4. 一个独立发布进程与一个查询进程重叠运行：不出现混代 locator、新路径不可识别或 stale latest 错误；查询开始前已确认发布的新版本必须可见。
5. 每次新增一个 source 时 extraction 文件数恰为 1，旧文件提取/打开数为 0；旧 unit 行不变，数据库不被删除重建。不得以完整 rebuild 充当增量维护。
6. 非支持 profile、源变化和访问失败明确报错；异常后后续查询/发布/checkpoint 仍可正常执行。
7. 留一份同步调用顺序的系统跟踪及恢复收据。进程故障与调用顺序只能验证相应范围，不等同于断电安全实测。

## 4. 对既有实验计划的必要修正

以下内容替代旧计划的对应假设；其他历史结果与统计口径不变。

| 原计划问题 | 本轮修正 |
| --- | --- |
| 迁移时拆 C++、采用 `fs_index/python/` | 本次保持 C++/Python 字节不变，使用 `fs_index/native.py`；维护实现另开阶段 |
| 将 ecCodes/SQLite 描述为已精确锁定 | 当前 CMake 只精确检查 HDF5；记录实际所有依赖路径、版本、hash，正式新 run 再冻结依赖与编译配置，不能声称全都已锁定 |
| M1 小规模可能无 checkpoint | 前台轨迹和显式收尾分别实测，报告总成本与待完成量；IoTDB-FS 公共 flush/后续维护边界也要说明，无法对齐时不声称持久性等价 |
| q=10 与 q=100 均仅一次 Exact | q≥10 使用固定 10% Exact；q=1 标为仅 Latest；分别报告查询类别 |
| Native 可能缺少版本发布目录 | Native 使用同一 source/head 发布协议但不登记 units；扫描当前选中版本的属性，保持同一 reader 与结果物化 |
| M3 拟扩大单元到 1–4 MiB | 当前每单元 4096×(4+1)=20,480 owned bytes；若需更大输出，增加每请求选中单元数，保持两档工作集请求形状相同，不在迁移中放宽 reader |
| 单一 B 代表三路缓存 | 分别核对三条路径的缓存对象、容量与命中；FS-index 的 8 文件/8 dataset 句柄缓存不等于一个字节型 payload cache，不能直接套用同一 B |
| 跨轮可能保留 managed 内容去重 | 三路同轮使用同一 payload；各独立轮使用可验证的不同 payload。重复内容另设对照，不混入主轨迹 |
| M2 完全依赖内部 generation 作 oracle | generation 用于诊断；外部以查询开始前已确认、完成前可能提交的版本及操作时间序列校验合法快照，不让某一路私有计数器定义公共正确性 |

优先完成 P0–P3/M0，再执行 M1 的 54 条三路完整轨迹。M2 与 M3 回答不同问题：M2 优先支持持续服务语义；M3 单独支持超缓存/远程访问边界。它们均不能被 M1 替代，未完成则明确收窄对应结论。M3 的 1 MiB 级输出需要约 52 个当前单元，不应把已有固定两单元结果直接外推到该负载。

正式计时继续使用独立环境：客户端/FS/FUSE与DataNode共享父级2 core，query子组限2GiB，数据库内存单列。不能把DataNode排除在CPU预算之外后声称三路总算力相同。迁移功能验证没有使用 2 GiB cgroup，不作为该预算下的性能证据。各路径拥有新目录/命名空间，保留原归档与服务配置。新版本代码、库、输入、资源和测试收据必须重新绑定，不能复用旧二进制的验收身份。

## 5. 投稿前暂不扩展的能力

不在本阶段实现多 writer、分布式一致性、复制、完整 ACL、删除/GC、块级 delta、通用 Arrow 合并、任意数组尺寸或完整科学角色推断。没有维护能力的旧 FS-index 结果继续按封印数据解释；新增维护结果不能推出数据库总运维成本更低。即使持续维护的 FS-index 最快，也应如实报告其适用条件。

## 6. 本机 Claude Opus 复审与处理

2026-10-31 使用本机 Claude CLI，实际返回模型 `claude-opus-5-5`、`is_error=false`。输入为冻结核心/包装/构建源码与上一轮实验计划；禁止工具调用。其评审范围不包含完整仓库和最终迁移 diff，不能冒充独立代码验收。

采纳：核心按字节迁移；保留 ABI/协议；单一目录权威与稳定 source ID；幂等/CAS/短读事务；checkpoint 成本；4096 约束；Native 发布公平性和混合查询比例。Boost 依赖已显式声明，实际构建依赖已记录。

限定采纳：精确锁定 ecCodes/SQLite 作为后续正式 run 的环境门禁，本次不更换机器依赖；异常路径修复在维护阶段进行，不改动迁移核心。评审建议中的 fsync/恢复方案仍需上述门禁验证，不能由文字审查推导正确性。本次无需再次询问用户已经授权的迁移选择。

原始输入和结构化输出保存在忽略目录 `build/fs-index-migration-20261031/review/`；验收报告记录对应摘要。
