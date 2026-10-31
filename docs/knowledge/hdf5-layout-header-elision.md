# HDF5 layout header 重读消除（2026-10-06）

## 变更与身份边界

原实现即使已缓存完整校验过的 layout directory 和 chunk 页，每次 `prepare` 仍读 70 B header，每次 `execute` 再读 70 B。暖态因此产生 2 次 layout SQL；在单文件 typed 直连消融中，`execute_setup` 约占 9.6 ms 查询的 3.0–3.2 ms。新增回归测试先复现了冷查询 2 次 layout 读、暖态 `prepare` 和 `execute` 各增 1 次的行为。

现改用进程内缓存键 `(storage namespace, path, version, manifest revision, payload size, layout size)`。**缓存未命中**时，读取并校验 header、directory 和所需页；命中时，按逻辑 directory/page 大小继续检查请求预算，不再为了缓存查找重读 header。执行计划持有经验证的不可变 directory、页和精确 manifest；`execute` 仍检查 source/version/revision、大小及 namespace，但不再读 layout。冷读取失败或损坏的 header 不会进入缓存。驱逐不使活跃计划失效。

这一选择依赖现有存储契约：`write_layout` 只在 `PayloadSealed` 阶段允许，已发布版本不可改写，删除后的 key 不可重新发布。同一精确身份下原地改变 layout 的自定义 `QuerySource` 不符合该契约；该情况不再通过每次查询重新读 header 检出。真正的新版本、revision 或大小变化仍会触发重新验证。此缓存只在本进程内存在，不跨进程持久化。

## 验证

在服务器同一 GEOS `/T` 单文件、相同版本及相同 6 轮请求下，新的[原始记录](../hdf5-layout-header-elision-results/D6-header-free.jsonl)与先前[缓存版记录](../results/hdf5-geos-pilot-20261006/direct-ablation/D6-direct-after-b.jsonl)的每项行数、mask 和值摘要完全一致。下表取第 2–6 轮中位数；两批实验**未交错运行**，耗时仅作观察，不能将差值直接作为严格因果收益。SQL 次数与读取字节的变化则由新执行路径和服务端计数共同确认。

| 请求 | 原暖态 layout SQL／字节 | 新暖态 layout SQL／字节 | 原 `execute_setup` | 新 `execute_setup` | 原总耗时 | 新总耗时 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 单点 | 2／140 B | **0／0 B** | 3.117 ms | **0.011 ms** | 9.550 ms | 3.417 ms |
| 16×16 | 2／140 B | **0／0 B** | 3.020 ms | **0.013 ms** | 9.395 ms | 3.781 ms |
| 160×160 | 2／140 B | **0／0 B** | 3.120 ms | **0.013 ms** | 10.143 ms | 3.428 ms |
| 5 层×16×16 | 2／140 B | **0／0 B** | 2.981 ms | **0.026 ms** | 9.208 ms | 3.289 ms |

首个冷查询的 layout 调用由 4 次降为 3 次（header、directory body、chunk 页）。公开 GEOS `/5` 的双文件语义验收又完成 4 个查询：两个时次各自的单点与 16×16 结果均和 h5py oracle 的 SHA-256 相同；每个文件第一次查询 3 次 layout SQL，第二次 0 次。[验收报告](../hdf5-layout-header-elision-results/geos-multitime-report.json)含各请求 SQL 和 profile。跨文件目录发现及通用元数据 SQL 此次未改变。

全套单元测试 **1329/1329** 通过。测试覆盖缓存冷／暖路径、namespace/version/revision/大小隔离、坏 header 不入缓存、页校验、预算、并发和源删除后的新请求行为。既有活跃计划可用其已持有的经验证缓存结果继续执行；新 `prepare` 对已删除版本仍失败。

## GWOSC 注册的缓存生命周期

随后检查发现，显式 `hdf5_binding` 每次注册都新建一个 Adapter，导致上述缓存仍只在单次注册内有效。回归测试使用同一已发布 HDF5 版本的两个不同 GPS 查询（原路径和 hard-link alias）：修复前 backend layout 读取由 2 次增至 4 次。现在注册服务持有一个有界 `Hdf5ValidatedLayoutCache` 和一组默认语义 profile，请求级 Adapter 共享它们，但仍分别保留其精确 GWOSC 绑定。挂载时普通 typed Adapter 与注册服务也接收同一个缓存，因此普通物理查询预热后，GPS 查询可直接使用已验证的 layout。缓存的 namespace、版本及大小隔离和逐次授权不变；缓存仍不包含请求级语义绑定。新增跨注册、跨 Adapter 测试后，全套单元测试 **1330/1330** 通过。
