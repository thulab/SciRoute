# GRIB BinaryView 编码 message 缓存验收 — 2026-10-12

已实现缓存命中后的零 payload SQL；首次读取及驱逐后重新读取保留真实 source I/O。

## 实现与契约

- 编码字节保留在共享 QueryRuntime 的有界 fragment cache，不新增无界全局缓存；生产挂载与 Arrow/HDF5 共用已有 byte／entry／inflight／worker 限额。
- key 包含 storage namespace、精确 path/version、revision、payload/layout size 和 message offset/length。GRIB parsed-index 身份同步补齐，避免同 Adapter 跨 namespace 选择错误范围。
- 请求 source 在每次获取缓存前预留逻辑 payload／range 额度；QueryModule 的聚合预算覆盖整个多源查询。命中也收费，shared producer 用独立 source 和服务 I/O 预算，避免首个请求限制绑定其他等待者。
- 公共服务处理 single-flight、取消、驱逐和失败重试；encoded fragment 不记录为解码。实际 source I/O 与复制通过 runtime 统计。
- 输出为独立拥有的 BinaryValue；仍每次执行 cursor、IPC 编码和结果发布，授权与精确版本准备保持原路径。

## 配对验收

复用原 GRIB 接受实验的三个精确已发布版本，未重发布源数据。两档真实 GFS文件（2,446,828 B／22 messages；549,606,683 B／4,598 messages），同一谓词选中一个 message。每种模式每轮使用独立旧／新 daemon，三轮顺序 before/after、after/before、before/after；每个会话 Q=100。共 1,200 次／版本、2,400 次总计。

旧版 daemon SHA-256：`3ac27a5bcd5bceee0b3b74fce9699b164ac954914bc689122707602352623699`。新版本：`f4c4786a949474fc4129348546983f9e869417991eb10e16fad5ff7e667ba75e`。相同原生库、配置与精确源版本；公共 cache 为 256 MiB／65,536 entries，该工作集不会因容量驱逐。

下表暖读 p50 为每组 297 条排除首读记录的 pooled 中位数，单位 ms。Q=100 总计为三轮会话的中位数，包括绑定与释放，未包含原来已完成的文件发布。

| 规模／模式 | 旧版暖读 | 新版暖读 | 暖读下降 | 旧版 Q100 总计 | 新版 Q100 总计 | 暖 payload SQL 旧→新 | 暖全部 SQL 旧→新 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small / unbound | 11.590 | 7.235 | 37.6% | 1291.077 | 752.994 | 1 → 0 | 2 → 1 |
| small / bound | 5.954 | 1.968 | 66.9% | 619.021 | 223.660 | 1 → 0 | 1 → 0 |
| medium / unbound | 12.237 | 8.115 | 33.7% | 1322.432 | 904.774 | 1 → 0 | 2 → 1 |
| medium / bound | 5.749 | 1.459 | 74.6% | 665.130 | 256.432 | 1 → 0 | 1 → 0 |

所有优化后首读均读取一个完整命中 message（small 66,606 B；medium 64,901 B），之后 99 次命中均没有 payload SQL；绑定组暖读全部 SQL 也为零，普通组保留一条 catalog SQL。独立拥有的输出 hash、来源 path/version、实际 100 次 cursor 与 IPC 执行和新 route 唯一性逐次检查通过。

## 验证

- Linux 完整单元测试：**1,459 项通过**。
- GRIB Adapter：**34 项通过，含 10 项新回归**；命中、namespace/version/revision、同 Adapter 不同 layout、服务额度、取消、single-flight、驱逐、重试和独立 producer source 覆盖。
- QueryModule 新增真实 GRIB 两源缓存命中回归：整个请求的 payload／range 限额不会被每源独立额度绕过，拒绝超限前不发出 payload I/O。
- Python runner／summary：**18 项通过**；BinaryView 首读／暖读断言改为新缓存契约。
- 本地独立重放全部 2,400 条原始观测：预算／SQL／runtime hits/misses、Q 总计与 message hash 全通过；本地源码指纹与实际服务器编译输入完全一致。
- 全部本轮私有 mount 已移除；预先存在的 PID 1001546、41286、3624304 均存活。

## 作用范围与限制

仅优化编码 message 保留／复用，不改变原始 payload、持久 layout 或数据版本，不把完整 IPC 结果作为查询缓存。数值视图的原生解码缓存不在此次改动范围内。暖读仍有新 IPC 和独立输出成本；此结果不代表冷盘或多客户端压力下的吞吐。缓存不足时遵循公共服务 admission／eviction／ResourceExhausted 契约，不做无界保留。

FS 本次没有重跑；原 FS 绑定组已复用编码字节。此次为同条件 IoTDB-FS 旧／新版本消融，不能将历史 FS 时长混入本次配对统计。查询前的发布成本也不能从此表推断降低。

源授权／删除等正常路径仍需先获得有效 source。实验是 private root、固定精确版本，未测试权限变更压力。runtime public 统计新增 encoded cache I/O／hits，不增加 decode_count；producer 的 I/O 在 worker 上执行，不能只用同步线程的嵌套 timer判断是否发生读取。

## 文件与复现

`ablation.json.gz` 保留全部实际查询；`summary.json` 保存聚合；`run-manifest.json` 保存源码、二进制、原始观测与输出指纹。原始日志和私有配置仅在服务器 `work/grib-binary-cache-20261012`。历史未缓存 BinaryView 实验和横向审计保留原值，不回写。

复现时使用独立输出目录：

```bash
python grib_binary_cache_ablation.py \
  --before-environment results/session-q100-20261012/environment.json \
  --after-daemon work/grib-binary-cache-20261012/build/bin/iotdb_fs \
  --baseline-results results/grib-paired-q100-20261012 \
  --writer-lock /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/grib-hdf5-boundary-final-20261007/namespace.lock \
  --output work/grib-binary-cache-repeat
```
