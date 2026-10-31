# PB00 复验与 PB01 数据正确性验收（2026-09-29）

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

主机为 `root@192.168.99.49`，独立工作区为
`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline`。本次只使用公开 ETTm1、
UCI Electricity 与仓库内的转换、oracle 和 HF 冒烟脚本；没有使用
TimeBench 或 TimerLathe。原始 JSON 证据为
全量数据校验（服务器归档：`docs/public-training-pb00-pb01-data-20260929.json`）和
两模型训练冒烟（服务器归档：`docs/public-training-pb00-hf-smoke-20260929.json`）。

| 验收项 | 结果 | 证据与范围 |
| --- | --- | --- |
| PB00 环境和源文件 | 通过 | 独立环境的 `pip check` 通过；Python 3.12.3、Torch 2.9.1+cu128、Transformers 4.57.1、Accelerate 1.11.0、Datasets 4.4.1、PyArrow 21.0.0、NumPy 1.26.4；ETTm1、Electricity TXT 与 ZIP 均匹配固定 SHA-256。版本冻结文件 SHA-256 为 `d5584ebdcdc3ab615a73d472c5337708125e7832fe847d7fe047fea551e44c6d`。 |
| PB00 转换器回归与 fixture | 通过 | `python -m unittest test_prepare_public_data -v` 为 4/4；六个压缩、字典、null 和嵌套 List fixture 各检查 1,002 个窗口，共 6,012 个。 |
| PB00 HF Trainer 最小闭环 | 通过 | 在独立 `acceptance/pb00-20260929/` 输出目录，两模型均完成 forward/backward、一步训练、评估、checkpoint 保存与恢复到第 2 步。GPU 为 A100-SXM4-40GB。这个极小训练任务不产生准确率或吞吐结论。 |
| PB01 stream/NumPy 全量等价 | 通过 | 新增 [`accept_public_data.py`](../../tests/benchmarks/arrow_training/accept_public_data.py)；ETTm1 的 487,760 个、Electricity 的 51,894,720 个 Float32 叶值及对应 validity 全量逐项一致。共核对 27 个 segment、14 个 record batch。 |
| PB01 分片连续性与边界窗口 | 部分通过 | manifest 的 `series_id`、`segment_index`、`start/end_step`、`continuation` 与 Arrow 行一致；所有 25 个相邻分片边界选取三个通道，共 75 个跨分片窗口与 canonical 数组的值/mask 一致。这些分片目前位于同一 IPC stream，尚不是跨物理文件的正例。 |

执行命令（`BASE` 和 `PY` 如 [benchmark README](../../tests/benchmarks/arrow_training/README.md)）：

```bash
"$PY" "$BASE/accept_public_data.py" --work-dir "$BASE" \
  --output "$BASE/manifests/pb00-pb01-full-data-acceptance-20260929.json"
cd "$BASE" && "$PY" -m unittest test_prepare_public_data -v
"$PY" -m pip check
```

HF 冒烟使用相同脚本，在 `acceptance/pb00-20260929/raw/ETTm1.csv`
指向已校验的源文件，并将 `HF_HOME`、`PIP_CACHE_DIR`、`TMPDIR`
留在独立工作区、`CUDA_VISIBLE_DEVICES=0`。报告中的 checkpoint 路径属于
这次隔离运行，不覆盖先前冒烟结果。

**阶段结论：** PB00 的数据和最小训练闭环复验通过；PB01 已完成公开数据的
来源完整性、Arrow/NumPy 全量等价及同一 stream 内的连续分片窗口验收。
PB01 尚缺可审计的 split、仅以训练区间拟合的 normalization artifact、
确定性的 window manifest，以及独立行不续接和真实跨文件连续分片的
公开数据验收。冻结文件只锁版本，未锁每个 wheel 的 hash；原生训练采样
trace 也尚未收集，因此目前不能据此宣称解码缓存的训练收益。

后续 [首轮 A/B 范围修订](../deliverables/public-training-baseline-plan.md)将真实跨文件正例移出本轮文件系统/IoTDB-FS 标准采样对照；它仍是独立的 A08/N06 功能验收，不改变本页对已运行检查的描述。
