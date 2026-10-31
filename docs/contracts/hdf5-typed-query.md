# HDF5 dataset 与多维查询契约（H01）

> 当前有两种正式文件内引擎，见 [查询模式](hdf5-query-modes.md)；两者共用本页 typed/scientific 输出语义。下文阶段状态保留其日期。

状态：2026-10-02，H01–H08 已实现。H02/H03 提供 source VFD 与摄取 layout，H04 将本契约接入真实 QuerySource 的非压缩 typed 查询与子数组组装；详见 [H04 执行说明](hdf5-query-execution.md)。H01 的 fake provider 测试仍作为模型单测保留，H04 另有真实摄取／查询 oracle。过滤器与确定性 fill 见 [H05](hdf5-filter-decoding.md)；[H06](hdf5-shared-cache.md) 已实现共享 cache、批量读取与并发隔离；[H07](hdf5-scientific-binding.md) 增加显式 GPS 秒绑定；[H08](hdf5-ipc-runtime.md) 已完成 IPC/socket/FUSE 接线及隔离挂载验证。

## 1. 唯一 dataset 入口

用户确认 B2 采用现有 `FieldRef`，不新增 dataset subject，也不借用 Arrow batch/row/list 地址。一次请求必须绑定一个精确文件路径和精确 published version，并投影且仅投影一个 dataset 值字段：

```text
source: /observations/event.hdf5, version 7
value field: hdf5.value:%2Fstrain%2FStrain
native dataset: /strain/Strain
optional coordinates: hdf5.axis.0, hdf5.axis.1, hdf5.axis.2
```

`make_hdf5_value_field(path)` 与 `parse_hdf5_value_field(FieldRef)` 是唯一编码／解码入口。不要自行拼接文件 URL、query string 或重复解码字段。`hdf5.` 为本契约保留前缀；字段属于该命名空间时，即使拼写有误或 source 最终未选中，也必须先验证请求。

编码规则：

- 原始 dataset path 必须为有效 UTF-8 绝对路径，最多 4,096 字节；拒绝 NUL、根 `/`、空 component、尾随 slash、`.` 和 `..` component。
- 不做 Unicode NFC/NFD、大小写、路径 alias 归一化。两个不同 UTF-8 byte 序列是不同路径；当前公开契约不接受任意非 UTF-8 HDF5 link 名称。
- URI unreserved ASCII `A–Z a–z 0–9 - _ . ~` 保持原样；其他字节使用大写 `%XX`，包括每个 `/` 和 `%`。拒绝小写 escape、坏／截断 escape、原始 slash、对 unreserved 字节的多余 escape。
- `/a%2Fb` 编为 `hdf5.value:%2Fa%252Fb`，解码一次后仍是带字面 `%2F` 的名称，不等同于 `/a/b`。
- dataset `/hdf5.axis.0` 的值字段仍以 `hdf5.value:` 开头，与轴字段无碰撞。轴仅接受 `hdf5.axis.0/1/2`，不接受 `.00`、`.3` 或其他别名。
- projection 中可以按任意顺序放置唯一值字段和当前 rank 的坐标列；最终结果保持请求顺序。仅投影坐标、混入第二个 dataset 或其他格式字段均拒绝。

hard-link alias 的对象去重和缓存身份属于 H03/H06。H01 只保持请求路径身份，不推测两个 path 是否为同一对象。

## 2. descriptor 与绑定顺序

公开接口位于 `include/iotdbfs/formats/hdf5/Hdf5QueryModel.h`，与 Arrow/GRIB QueryModel 并列。实现不包含 `hdf5.h`，不泄漏 native handle、对象地址、chunk layout 或解码库类型。

`Hdf5DatasetProvider::describe(path)` 提供一个 dataset 的路径、原生值类型、current extent 和 profile 支持状态／原因。H01 允许 rank 1–3 的 IEEE Float32/Float64；unsupported metadata 不自动变成缺失 dataset。provider 返回的 path 必须与请求一致；错误来源及 phase 原样传播。

流程为：

1. 核心结构校验调用 HDF5 请求模型，检查 exact source、字段、输出模式及切片语法。
2. catalog 选择 source；被 file predicate 或 format selector 筛除的现存源返回空选择，不查询 dataset。
3. adapter 的 `capabilities(context, fragment)` 通过 provider 绑定 dataset，构造不可变 `Hdf5QueryBinding`，随后生成 schema/capabilities。
4. 核心执行完整字段、类型与 capability 校验，再调用 prepare。fake adapter 在单次 planning context 中复用 binding，不重复 describe。
5. cursor 使用矩形 slab 契约输出；源路径／版本不会在执行时重新解析为 latest。

新增 `query_fragment_requires_exact_source` 是核心的有限校验入口：Arrow physical_array 和 HDF5 绑定需要 exact source；普通 GRIB 选择行为不变。只有 `QueryPlanning.cpp` 获准引用 HDF5 QueryModel 做请求校验，HDF5 的前缀解析留在格式模块；core 不能引用 HDF5 layout/decoder。目录依赖检查器同步维护这一精确例外与格式间隔离。

## 3. 索引与数值语义

- axis ordinal 是存储数组的维度顺序，不自动意味着时间、经纬度、变量或通道。
- 每个轴的 schema 类型是非 nullable UInt64；值列为非 nullable 原生 Float32 或 Float64。无隐式 widening、单位转换、标准化或其他变换。
- slice 只接受 `[lower, upper)`：lower 必须 inclusive，upper 必须 exclusive，类型均为 UInt64。拒绝 Int64/float/string/null 端点，不做数值强制转换，也不将 inclusive upper 自动加一。
- 未出现的轴为 `[0, extent)`。单边 slice 以 `0` 或 current extent 补全；显式 slice 若两端均缺失则拒绝，与通用核心一致。
- 检查重复轴、超 rank 轴、反向区间、超 extent 和乘法／字节溢出；不裁剪。`[extent, extent)` 合法，`[extent+1, extent+1)` 非法。
- descriptor 总元素数／原生字节数和选区输出字节数必须能由 UInt64 表示；选区行数受 `max_result_rows` 约束。包含零 extent 的 shape 先判空，不因其他轴中间乘积溢出而误拒绝。
- 当前没有点集合、stride、block、selection union、value predicate、type override、BinaryView 或插值能力。HDF5 JSON 的额外选择参数明确拒绝，不静默当作 unit-stride。
- fill sentinel、NaN、正负零均是普通非 null 数值；没有语义绑定时不根据 `_FillValue` 属性构造 mask。对未写存储是否可合成确定 fill 的判断留待 H03/H05，provider 不得把未知状态伪造成零。

`Exact` slice capability 仅表示结果索引语义精确，不表示压缩 chunk 内可按元素少读字节。

## 4. schema、domain 与 slabs

`binding.schema()` 包含唯一值字段及全部 ordinal axes；最终 projected schema 由请求顺序确定。dataset 路径保存在值 FieldRef 中，dtype 保存在 schema，精确 source 保存在 batch provenance，轴顺序／origin／shape 保存在 domain，不按元素重复长路径。

所有非空 batch 为 `DomainKind::DenseGrid`，rank 1 也保持同一 N-D 契约：

```text
order = C，末轴最快
origin = source-relative start，非本次 selection 内相对位置
shape = 当前矩形 slab 的 extent
row_count = product(shape)
dimensions = 所有 hdf5.axis.i，UInt64，按轴 ordinal 排序
```

即使只投影 value，domain 仍带全部 axes。`Hdf5SlabIterator` 延迟生成 slabs，不预分配与总元素数成正比的任务数组。每个 slab 同时满足 `target_batch_rows` 与 `target_batch_bytes / projected_row_bytes`，坐标列每个元素额外计 8 字节。非空结果若预算连一行都容不下则失败；空结果无需容纳一行。

切分优先保留完整内层维度；需要时沿最内轴分段，再按外层 C-order 推进。不能将跨行的任意平面 buffer 假报为矩形。因此实际 batch 可以小于目标行数，以保持矩形和整体顺序。整 cursor 拼接无遗漏、无重复。

空 slice／zero extent 产生 0 行且不生成 value slabs，cursor 的 schema 仍可查询；不因此调用 value reader。被 catalog 筛除的全部 source 则沿用核心空选择行为：prepared result_schema 未绑定，执行游标为无字段空 schema，不能虚构某个未访问 dataset 的类型。

## 5. 错误分类

| 情况 | QueryError |
| --- | --- |
| 路径编码非法、重复／多个值字段、非 exact selector、混合 Arrow physical_array | InvalidQuery（路径长度超限为 ResourceExhausted） |
| 缺少 dataset、未知／超 rank 轴或未知投影字段 | UnknownField |
| 错误端点类型／NULL 端点 | TypeMismatch |
| 重复轴、错误边界形式、逆向／越界 slice | InvalidDomainSlice |
| 不支持的 rank/dtype/profile、谓词／输出模式／变换 | UnsupportedCapability |
| shape／字节溢出、超过行数／slice 数／单行 batch 预算 | ResourceExhausted |
| provider 返回另一 dataset、执行 schema／domain 不一致 | SchemaMismatch |
| 真正缺失 exact source/version | SourceNotFound；未发布状态保留核心对应错误 |
| 现存 source 被筛除 | 成功空选择，不调用 dataset provider |
| provider 的 ReadFailure／CorruptLayout 等 | 原样传播，不转成 UnknownField |

格式本地验证 helper 的错误 phase 为 Validation；provider/native/storage 错误保留自己的 phase。经过 QueryModule 的通用校验可能更早拒绝同一无效请求，使用核心已有的错误分类，例如 source selector 自身非法时使用通用 InvalidQuery。错误 code 是层级明确的结果，不承诺所有非法输入一定到达 adapter。

## 6. C++ 与 JSON 示例

```cpp
QueryRequest request;
request.sources.paths = {{PathScopeKind::Exact, "/observations/event.hdf5"}};
request.sources.versions = VersionSelection::exact_version(7);
request.subset = QueryFragment{};
request.subset->projection = {
  make_hdf5_value_field("/strain/Strain").value()
};
request.subset->slices = {{
  hdf5_axis_field(0),
  DomainBound{LogicalValue::uint64(65536), BoundKind::Inclusive},
  DomainBound{LogicalValue::uint64(73728), BoundKind::Exclusive}
}};
```

应用代码应先检查 `make_hdf5_value_field` 返回状态；示例路径为固定合法字符串。等价 typed 注册声明如下；只有后续生产 adapter 注册并摄取源后，才可 materialize 真实结果：

```json
{
  "kind": "typed",
  "request": {
    "sources": {
      "paths": [{"kind": "exact", "path": "/observations/event.hdf5"}],
      "versions": {"kind": "exact", "exact": 7},
      "formats": {"adapters": "listed", "adapter_ids": ["hdf5"], "include_byte_passthrough": false}
    },
    "subset": {
      "projection": ["hdf5.value:%2Fstrain%2FStrain"],
      "slices": [{
        "dimension": "hdf5.axis.0",
        "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": 65536}},
        "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": 73728}}
      }]
    }
  }
}
```

Socket 对 FieldRef 不做 URL decode，UInt64 不经过 double；上界必须显式写 `exclusive`，因为通用 bound parser 默认 inclusive。

## 7. 验证范围

- `test_Hdf5QueryModel.cpp`：UTF-8/escaping/碰撞、rank/dtype、绑定、严格端点、空 extent、UInt64 精度／溢出、slab 顺序与预算。
- `test_Hdf5QueryBinding.cpp`：fake metadata/provider/adapter，穿过真实 QueryModule 的 capabilities → prepare → cursor，以及错误 schema/domain、源筛除、空查询和原生特殊值。
- 核心回归：HDF5 exact-source preflight，保留普通 GRIB 空选择语义，含零 extent 的 N-D shape 验证。
- socket 回归：HDF5 不支持的选择参数不能被 parser 静默丢弃。

H01 验证证据及命令保存于 `docs/results/hdf5-h01-20261001/`。没有使用真实 HDF5 decoder、生产注册或 IoTDB HDF5 摄取；这些必须分别完成后续阶段。

### 7.1 本轮实测（2026-10-01）

| 验证 | 结果 |
| --- | --- |
| Linux Release `iotdb_fs` + `unit_tests` | 构建通过；关闭 H00 probe，无 libhdf5 动态链接 |
| 本轮全部 unit CTest | **1,031/1,031 通过**，46.01 秒；较 H00 新增 35 项 |
| HDF5 名称定向筛选 | **33/33 通过**；另外两项通用核心回归包含在完整 CTest 中 |
| CI Python helper | 80 项，1 项跳过，其余通过 |
| macOS ASan/UBSan 模型与 slab harness | **1,350/1,350 通过**，无诊断；非完整服务 sanitizer 验收 |
| 独立 QueryModel 头及全部跨格式 include 顺序 | **9/9 通过** |
| 模块依赖检查、diff whitespace | 通过 |

Linux：GCC 13.3.0、CMake 3.28.3，独立目录
`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h01-20261001/`。
复用现有 Arrow/SDK 的只读依赖路径，未重启原挂载或连接 IoTDB 执行 HDF5 写入。
生产 binary 的 `ldd` 不含 libhdf5；H01 契约代码可独立于格式库编译。
macOS：Apple Clang 17.0.0，instrumented `QueryModel.cpp`、`Hdf5QueryModel.cpp` 与 slab harness；
未对完整 QueryModule/IPC/FUSE 插桩，leak detection 关闭。完整命令、临时 harness 与 header 检查保存在证据目录。

源码基点 `f32520c3ae8df7927f23698e182b4f7c1b4db9da`，叠加工作区中 H00/H01 修改；
以 `linux/provenance.json` 与 `source-hashes.json` 标识实际编译文件，未执行 Git 写操作。
`docs/results` 按现有规则被 Git 忽略，代码提交不会自动包含这些本地运行证据。

```bash
cd /data_01/zzy/iotdb-fs-exp/public-arrow-baseline/hdf5-h01-20261001
cmake --build build --target iotdb_fs unit_tests -j 8
ctest --test-dir build -L unit --output-on-failure
```


## H07 显式科学坐标扩展

默认物理契约不变。配置 GWOSC 精确 source/version/dataset 绑定后，支持额外的
`hdf5.coord.gps_seconds` Float64 字段及半开时间切片；切片转换为 ordinal 后
复用相同子数组组装和 cache。固定单位属性必须匹配；不可有界读取的变长单位
属性由显式配置声明。结果 domain 仍保留 UInt64 ordinal，避免 GPS 秒与整数
纳秒混淆。详情、精度范围及 unsupported 边界见
[H07 元数据与科学绑定](hdf5-scientific-binding.md)。
