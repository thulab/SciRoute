# 公开 Arrow 训练基线：远端环境准备记录

> 文档整理：标为“服务器归档”的引用按本次清单不再作为 Git 文档保留，查验路径见 [文档归档说明](../README.md#服务器归档)。

日期：2026-09-27。主机：`root@192.168.99.49`。对应 公开训练基线计划 (local archive: `docs/deliverables/public-training-baseline-plan.md`) 的 PB00 环境部分；磁盘选址和准备前状态见 分区核查（服务器归档：`docs/public-training-environment-readiness.md`）。本记录只证明公开 ETTm1 上的最小训练闭环可运行，不表示 Arrow reader、IoTDB-FS 查询、完整训练或性能基线已完成。

## 位置与隔离

工作区：`/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/`。已建立 `raw/`、`canonical/`、`arrow/`、`memmap/`、`cache/pip/`、`cache/hf/`、`tmp/`、`runs/`、`manifests/` 和独立 `venv/`。`canonical/`、`arrow/`、`memmap/` 仍为空，供后续公开转换与读取对照使用。未改动已有 `timechodb/`、`runtime/`、GRIB FUSE 挂载或 `/data_02` TimeBench、`/root/zzy/TimerLathe`。

已将其他项目中校验过的 ETTm1 复制为 `raw/ETTm1.csv`，10,360,719 bytes，69,680 行、7 个数值通道；SHA-256：`6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e`。这是公开基线计划所记录的原文件内容；正式来源、许可和转换协议仍按该计划固定，不依赖原项目目录。

## 实装版本与复现

Ubuntu 24.04.4、Python 3.12.3、NVIDIA 驱动 575.57.08，GPU 检测为 A100-SXM4-40GB。独立环境已安装 PyTorch `2.9.1+cu128`、Transformers `4.57.1`、Accelerate `1.11.0`、Datasets `4.4.1`、PyArrow `21.0.0`、NumPy `1.26.4`、Pandas `2.2.3`。完整 65 个安装包的版本快照见 [requirements.ubuntu2404-py312-cu128.txt](../../tests/benchmarks/arrow_training/requirements.ubuntu2404-py312-cu128.txt)，远端同副本在 `manifests/requirements.freeze.txt`，SHA-256 `d5584ebdcdc3ab615a73d472c5337708125e7832fe847d7fe047fea551e44c6d`。`pip check` 输出 `No broken requirements found.`

安装时采用 [PyTorch 官方 CUDA 12.8 wheel 索引](https://pytorch.org/get-started/previous-versions/) 先安装 `torch==2.9.1`，再由 PyPI 安装其他直接依赖；重建时应先设置工作区环境变量、从 CUDA 索引安装 Torch，再用上面的完整版本文件补齐依赖。该文件锁定版本，**未锁定每个 wheel 的 hash**；正式 PB00 仍需验证 wheel 来源/checksum 和转换工具版本。

使用现有环境运行验证：

```bash
BASE=/data_01/zzy/iotdb-fs-exp/public-arrow-baseline
export HF_HOME="$BASE/cache/hf"
export PIP_CACHE_DIR="$BASE/cache/pip"
export TMPDIR="$BASE/tmp"
export CUDA_VISIBLE_DEVICES=0
"$BASE/venv/bin/python" "$BASE/smoke_hf_environment.py" --work-dir "$BASE"
```

可复现脚本是 [smoke_hf_environment.py](../../tests/benchmarks/arrow_training/smoke_hf_environment.py)，已复制到上述远端工作区；远端脚本 SHA-256 `c89d2a53a816d382f24aed31649a688d07860fb68363603f857540cd83ee0959`。它只使用公开 ETTm1、PyTorch 和 HF 依赖，使用训练区间均值/方差并检查输入 hash、通道顺序与形状。脚本中的极小模型、4 个训练窗口和 2 个评估窗口是环境冒烟参数，**不得用于论文性能或精度结论**。重跑会覆盖同名 `runs/smoke-*` 输出；需保留旧结果时先更改输出目录。

## 实测结果与剩余工作

GPU0 `torch.cuda.is_available()` 为 true。PatchTST 和 `mix_channel` PatchTSMixer 均完成单 batch forward/backward，预测张量为 `[2,24,7]`，loss 有限；两者都完成 HF Trainer 第 1 步训练、`evaluate()`、checkpoint 保存和从 checkpoint 恢复到第 2 步，最终模型已保存。原始结果在远端 `manifests/hf_environment_smoke.json`，本地审阅副本为 arrow-hf-environment-smoke-20260927.json（服务器归档：`docs/arrow-hf-environment-smoke-20260927.json`）。Transformers 发出文件系统 mtime 排序回退警告，训练和恢复成功；后续需要验证正式多 checkpoint 保留/选择逻辑。

安装后 `venv/` 约 7.2 GiB，`cache/` 约 3.9 GiB，`/data_01` 仍约 3.3 TiB 可用。缓存和临时文件位于工作区，根分区不承载训练输出。未运行公开数据到 Arrow IPC stream 的转换、R0–R4/Q0–Q2、完整 epoch、多实验并发或查询服务；这些继续按 PB01–PB04 验收。
