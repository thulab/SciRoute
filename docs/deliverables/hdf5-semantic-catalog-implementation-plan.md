# HDF5 dataset catalog、属性谓词与自动科学绑定

日期：2026-10-06。状态：**功能实现及正确性验收完成；性能对照尚未开展。**

本次工作由用户明确提出，扩展现有 H01/H07 的能力边界。依据
[format.md](../contracts/format.md)、[experiment.md](../knowledge/experiment.md) 的规则经纬度场景，
目标是从语义条件发现 dataset，再对原始数据执行有界局部读取。
本次不以修改 chunk 布局换取功能，不自动插值或重采样。
保留现有 H10/H11 未提交改动；不执行 Git 写操作。

## 实物与源码核查

已重新下载 experiment.md 中 NOAA OISST 2024-01-01 原始文件，SHA-256 与文档
一致：`fd2f772cbe1633b20ab0c53ed76b874406f933bbd97062d611827c5d8a73bc4c`。
[实物清单](../results/hdf5-semantic-catalog-20261006/oisst-inventory.json) 记录独立
h5py 读取结果：

- `/sst`、`/anom`、`/err`、`/ice`：Int16，shape/chunk 均为
  `[1,1,720,1440]`，带 scale_factor/add_offset、_FillValue 与 valid_min/max。
- 四个轴具有 dimension scales；`/time` 为 Float32，单位
  `days since 1978-01-01 12:00:00`；`/lat`、`/lon` 单位分别为
  degrees_north、degrees_east。不能只按路径名称推断。
- root 声明 `CF-1.6, ACDD-1.3`；没有 calendar 属性，默认历法应按选定 CF
  契约解释并记录默认来源，不能静默当成 Unix/UTC 纳秒。

实施前核查的缺口（现已补齐的范围见末尾验收链接）：

| 部位 | 现状 | 必须补齐 |
|---|---|---|
| Hdf5QueryModel | 拒绝全部 predicate；要求单一精确 source/version 与 value:path | dataset 属性字段与运算能力、发现结果到精确请求的绑定 |
| SourceDriver/decoder/kernel | rank 1–3，native Float32/64 | 四维及 packed 整数原值；独立科学缩放和 mask |
| Layout /3 | 有界 typed 属性、反向 dimension-scale 候选 | 可验证的轴关系、坐标内容和完整性证据 |
| HDF5 adapter | 读取对象目录后规划 chunk | 在 chunk 页和 payload 读取前精确排除不匹配 dataset |
| typed published catalog | 拉取 published manifests 后内存筛选 | 真正的数据库端 dataset 属性候选接口 |
| GRIB SQL membership | SemanticGrepClient 的 shortName 集合 JOIN | 不把其文件级集合直接复用成 dataset 属性集合 |
| H09 | 仅迁移最新 pass-through | 已注册旧 layout 的明确重摄取流程 |

GRIB typed adapter 的 message 属性筛选本身也会遍历 L；不能将“功能对齐”
理解为现有所有 GRIB 查询都已有数据库属性索引。

## 已确认的新契约

1. **发现与数组执行分开**：有界 dataset catalog 返回精确 source/version、
   object 身份、规范路径及属性／坐标证据，再产生单 dataset 数组请求。
   不自动拼接异构 shape、dtype、单位或历法。
2. **自动绑定范围**：基于 CF/netCDF 的一维时间、纬度、经度坐标与已验证
   维度关系，支持单调升序／降序。保留原时间单位和 calendar；不自动转换
   历法。补齐 OISST rank4、packed Int16 缩放和 mask。曲线网格、投影转换、
   冲突／不足证据明确不绑定，物理查询与语义查询分离。
3. **持久化及升级**：采用 layout /4＋版本绑定 dataset catalog，在源版本
   发布前写完；旧 /1–/3 经显式重摄取生成新版本，已发布版本不原地修改。
   相应扩展 inventory/run 的协议、策略与验证。

以上三项已获用户确认。dimension-scale 变长引用的受限 helper 方案也已确认：独立进程、有界只读范围 RPC、内存/CPU/墙钟/输出上限，父进程复核对象关联，永久关闭动态插件。
HDF5 动态插件加载继续在挂载进程整个生命周期关闭。

## 目录与谓词的正确性约束

- dataset 属性按对象隔离，不提升为文件级 Sigma。多个条件的交集必须落在
  同一 source/version/object；不能将一个变量名与另一个 dataset 的单位误配。
- 复用 typed expression；首先实现标量比较、IN、AND/OR/NOT 和缺失检查。
  属性原始 shape 保留；元素数为一的数组参与标量比较。
  数组属性不隐式转换成任意元素匹配。
- 完整属性目录中的不存在、值不可读取、目录截断和索引缺失分别表示。
  Unknown 不能作为 false 剪枝；精确结果不能以静默遗漏替代明确失败。
- SQL 比较与目录 residual 必须类型一致，包括 Int64/UInt64 边界、NaN、
  字符串编码和缺失规则；不通过转成 DOUBLE 或字符串近似数值比较。
- latest 必须先按现有 source selector 规则确定，再执行 dataset 属性筛选。
  禁止最新版本不匹配时返回旧版本。
- hard-link 别名按对象身份去重，路径解析不无限展开环；不跟随外部来源。
- 返回属性之前执行授权。索引命中不取代精确 manifest revision/layout digest
  校验；持久身份不含 PID 或随机进程 namespace。
- 报告 SQL 过滤与残余筛选的实际位置。没有执行计划或扫描计数时，不声称
  IoTDB 内部物理索引剪枝，仅声称后端谓词执行／返回候选减少。

## 语义与数值的正确性约束

- 自动绑定应有完整维度关系、长度、单位、有限性、单调性与歧义检查；现有
  REFERENCE_LIST 反向候选不能直接升级成已验证绑定。
- 坐标读取具有独立字节／元素预算并计入摄取 profile。支持范围内的坐标
  可持久保存／索引，超限明确返回不可绑定；不隐藏逐次全数据扫描。
- 不用名称 time/lat/lon 代替规范证据；不把“单位存在”当成完整坐标关系。
- 输出保留原坐标顺序和 source provenance。跨经线的循环选择必须有明确
  规则，不通过静默排序或改变 longitude 表示实现。
- packed 数值在原始域识别 fill/missing/有效范围，再执行确定的缩放；物理
  原值视图与科学值视图区分，IPC 声明缩放、单位和 mask 依据。
- 变长属性与 dimension-scale 关系读取需重新审查有界性；不能为支持自动
  绑定而在现有禁止无界 H5Aread 的路径中静默启用读取。

## 实施顺序与验收

| 阶段 | 交付 | 主要验证 |
|---|---|---|
| SC01 | 冻结上述契约、字段编码、目录身份和错误语义 | 模型验证、缺失／未知、属性类型边界 |
| SC02 | rank4/native integer 原值与科学值视图 | OISST 独立 oracle、fill/scale/NaN、原物理请求回归 |
| SC03 | 自动坐标证据提取、绑定与持久协议 | 正反向关系冲突、升降序、单点轴、calendar/单位、预算 |
| SC04 | dataset 属性表达式与 adapter 规划剪枝 | 不匹配对象零 chunk 页／payload 读取；OR/NOT 未知处理 |
| SC05 | 持久 dataset catalog、后端筛选、publication/rename/delete | 半写、latest、历史、跨namespace、同对象合取与别名 |
| SC06 | 注册／FUSE、IPC 描述与旧 layout 重摄取 | 授权、不可变版本、重启、迁移报告身份、旧句柄 |
| SC07 | 小型公开多文件验收及性能归因 | 原生目录遍历与 FS 预建目录双基线；不预设性能正收益 |

新增测试同时覆盖 GRIB、Arrow 既有契约，不能通过全局 schema 改动破坏其他
adapter。记录候选 source/dataset 数、SQL 返回行数、layout 读取、属性 residual、
未知状态、坐标读取、payload 与解码数量。样本 hash、构建指纹和运行参数需归档。

SC01–SC06 已实现；SC07 已完成公共数据、IoTDB、FUSE 与迁移正确性验证。原生遍历／FS 预建目录性能对照尚未执行，不声称系统性能正收益。结果与证据见 [验收记录](../reports/hdf5-semantic-catalog-acceptance.md)。
