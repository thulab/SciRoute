#!/usr/bin/env python3
"""Audit accepted raw timings and compare HDF5/GRIB single-file experiments.

This replays evidence, not queries. Historical inputs are never modified.
All warm quantiles use the same pooled 297 post-first Q=100 observations.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import statistics

import grib_paired_summary
import session_summary
import summarize


RUNS = {
    "q100-20261012": ("HDF5", summarize.summarize, 2700),
    "session-q100-20261012": ("HDF5", session_summary.summarize, 5697),
    "grib-paired-q100-20261012": ("GRIB", grib_paired_summary.summarize, 3996),
}
ARMS = ("fs", "iotdb")
QS = (1, 10, 100)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def median(values):
    return statistics.median(list(values))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def groups(data, run, family):
    """Yield real sessions; unbound HDF5 has only one actual Q=100 session."""
    for campaign in data["campaigns"]:
        index = campaign.get("campaign", campaign.get("index"))
        sections = ([("unbound", campaign["cases"])] if run == "q100-20261012" else
                    [("unbound", campaign["unbound_cases"]),
                     ("bound", campaign["bound_cases"])] if family == "HDF5" else
                    [(None, campaign["cases"])])
        for mode, cases in sections:
            for case in cases:
                actual_mode = mode or case["mode"]
                source = campaign["sources"][case["tier"]]
                sessions = case["sessions"] if actual_mode == "bound" or family == "GRIB" else [None]
                for session in sessions:
                    q = session["q"] if session else 100
                    arms = {}
                    for arm in ARMS:
                        if session:
                            info = session["arms"][arm]
                            rows = info["observations"]
                            bind = info["binding"]["total_ns"] if family == "HDF5" else info["binding_ns"]
                            release = info["release"]["total_ns"] if family == "HDF5" else info["release_ns"]
                            wall = info.get("native_probe_process_wall_ns", info.get("native_process_wall_ns"))
                        else:
                            rows = case["observations"][arm]
                            bind = release = 0
                            wall = case["native_probe_process_wall_ns"] if arm == "fs" else None
                        arms[arm] = {"rows": rows, "bind_ns": bind, "release_ns": release,
                                     "native_external_wall_ns": wall,
                                     "write_ns": source["writes"][arm]["total_ns"],
                                     "locator_ns": (source.get("source_locator_ns",
                                                   source.get("exact_source_binding_ns", 0))
                                                    if arm == "iotdb" else 0)}
                    yield {"run": run, "format": family, "campaign": index,
                           "tier": case["tier"], "case": case["case"], "mode": actual_mode,
                           "q": q, "bytes": source["bytes"], "arms": arms,
                           "prefix_q": family == "HDF5" and actual_mode == "unbound"}


def audit(groups_for_run, family, expected):
    counts = dict.fromkeys(ARMS, 0)
    residuals, process_gaps, server_gaps = [], [], []
    sql_warm = defaultdict(list)
    for group in groups_for_run:
        for arm, info in group["arms"].items():
            rows = info["rows"]
            require(len(rows) == group["q"], "wrong actual Q")
            counts[arm] += len(rows)
            for index, row in enumerate(rows):
                require(type(row["total_ns"]) is int and row["total_ns"] > 0, "invalid duration")
                if arm == "fs":
                    expected_protocol = ("iotdbfs-single-file-native/1" if family == "HDF5" else
                                         "iotdbfs-single-file-grib-native/1")
                    require(row.get("protocol") == expected_protocol and "server_profile" not in row,
                            "native arm label differs")
                    phases = (("open_file_ns", "open_dataset_ns", "selection_setup_ns", "read_ns",
                               "owned_copy_ns", "close_ns") if family == "HDF5" else
                              ("open_file_ns", "discovery_ns", "payload_read_ns", "decode_ns",
                               "selection_setup_ns", "owned_copy_ns", "close_ns", "other_ns"))
                    require(all(type(row[k]) is int and row[k] >= 0 for k in phases), "bad native phase")
                    require(sum(row[k] for k in phases) == row["total_ns"], "native phase partition differs")
                    if group["mode"] == "bound":
                        expected_bind = info["bind_ns"] if family == "HDF5" or index == 0 else 0
                        require(row["session_bind_ns"] == expected_bind, "bind attribution differs")
                        require(row["session_close_ns"] == (info["release_ns"] if index + 1 == len(rows) else 0),
                                "release attribution differs")
                else:
                    operation = "typed" if group["mode"] == "unbound" else "experiment_typed_read"
                    profile = row["server_profile"]
                    require(profile["operation"] == operation and "protocol" not in row,
                            "typed arm label differs")
                    if group["run"] != "q100-20261012":
                        require("/mount/.iotdbfs/query/" in row["result_path"], "typed route differs")
                    copy_key = "owned_copy_and_close_ns" if family == "HDF5" else "owned_copy_close_ns"
                    residual = row["total_ns"] - sum(row[k] for k in
                        ("registration_ns", "mmap_open_ns", "ipc_decode_ns", copy_key))
                    require(residual >= 0, "client phases exceed total")
                    residuals.append(residual)
                    gap = row["registration_ns"] - profile["server_total_ns"]
                    require(gap >= 0, "server timer exceeds enclosing client registration")
                    server_gaps.append(gap)
                    if group["q"] == 100 and index > 0:
                        calls = profile["iotdb_delta"]["query_calls"]
                        sql_warm[group["mode"] + "/" + group["case"]].append(calls)
                        if group["mode"] == "bound" and group["case"] != "binary_one":
                            require(calls == 0, "warm bound value query repeats SQL")
            if arm == "fs":
                measured = info["bind_ns"] + info["release_ns"] + sum(r["total_ns"] for r in rows)
                gap = info["native_external_wall_ns"] - measured
                require(gap >= 0, "native timers exceed external process wall")
                process_gaps.append(gap)
    require(counts == dict.fromkeys(ARMS, expected), "wrong observation totals")
    return {"passed": True, "observations_per_arm": counts,
            "native_phase_partition_exact": True, "arm_labels_verified": True,
            "client_unassigned_residual_ns": {"min": min(residuals), "p50": median(residuals),
                                               "max": max(residuals)},
            "registration_minus_server_ns": {"min": min(server_gaps), "p50": median(server_gaps)},
            "native_external_minus_timed_ns": {"min": min(process_gaps), "p50": median(process_gaps)},
            "warm_sql_calls_median": {k: median(v) for k, v in sql_warm.items()}}


def aggregate(all_groups):
    buckets = defaultdict(list)
    for group in all_groups:
        buckets[(group["run"], group["format"], group["tier"], group["case"])].append(group)
    matrix = []
    for (run, family, tier, case), values in buckets.items():
        entry = {"run": run, "format": family, "tier": tier, "case": case,
                 "bytes": values[0]["bytes"], "modes": {}}
        for mode in ("unbound", "bound"):
            subset = [g for g in values if g["mode"] == mode]
            if not subset:
                continue
            result = {"q": {}, "warm": {}, "first": {}}
            for q in QS:
                chosen = [g for g in subset if g["q"] == q or g["prefix_q"]]
                require(len(chosen) == 3, "three campaigns per Q required")
                row = {}
                for arm in ARMS:
                    sessions = []
                    for g in chosen:
                        info = g["arms"][arm]; observations = info["rows"][:q]
                        reads = sum(r["total_ns"] for r in observations)
                        total = info["bind_ns"] + reads + info["release_ns"]
                        sessions.append({"bind_ns": info["bind_ns"], "release_ns": info["release_ns"],
                                         "query_sum_ns": reads, "session_total_ns": total,
                                         "lifecycle_total_ns": info["write_ns"] + info["locator_ns"] + total})
                    row[arm] = {k: median(s[k] for s in sessions) for k in sessions[0]}
                result["q"][str(q)] = row
            q100 = [g for g in subset if g["q"] == 100]
            for arm in ARMS:
                warm = [r for g in q100 for r in g["arms"][arm]["rows"][1:]]
                first = [g["arms"][arm]["rows"][0] for g in q100]
                result["first"][arm] = {"total_ns": median(r["total_ns"] for r in first)}
                metrics = {"observations": len(warm), "total_ns": median(r["total_ns"] for r in warm)}
                if arm == "fs":
                    for key in ("discovery_ns", "decode_ns", "read_ns", "open_file_ns", "open_dataset_ns"):
                        if key in warm[0]: metrics[key] = median(r[key] for r in warm)
                else:
                    for key in ("registration_ns", "mmap_open_ns", "ipc_decode_ns"):
                        metrics[key] = median(r[key] for r in warm)
                    copy_key = "owned_copy_and_close_ns" if family == "HDF5" else "owned_copy_close_ns"
                    metrics["owned_copy_close_ns"] = median(r[copy_key] for r in warm)
                    metrics["server_total_ns"] = median(r["server_profile"]["server_total_ns"] for r in warm)
                    metrics["client_minus_server_ns"] = median(
                        r["total_ns"] - r["server_profile"]["server_total_ns"] for r in warm)
                    metrics["catalog_ns"] = median(r["server_profile"]["stages_ns"].get("registration_catalog", 0)
                                                   for r in warm)
                    for key in ("query_calls", "payload_calls", "payload_returned_bytes"):
                        metrics[key] = median(r["server_profile"]["iotdb_delta"][key] for r in warm)
                    metrics["first_payload_returned_bytes"] = median(
                        r["server_profile"]["iotdb_delta"]["payload_returned_bytes"] for r in first)
                    metrics["output_rows"] = first[0]["rows"]
                result["warm"][arm] = metrics
            entry["modes"][mode] = result
        matrix.append(entry)
    return matrix


def markdown(report):
    """Keep timing definitions alongside every matrix, rather than imply parity."""
    primary = sorted((r for r in report["matrix"] if r["run"] != "q100-20261012"),
        key=lambda r: (r["format"], ("small", "medium", "large").index(r["tier"]), r["case"]))
    ms = lambda n: f"{n / 1e6:.3f}"
    label = lambda r: f"{r['format']} / {r['tier']} / {r['case']}"
    lines = [
        "# HDF5／GRIB 单文件横向矩阵与计时核验 — 2026-10-12", "",
        "重新读取已有原始记录，未重新运行完整查询。主矩阵使用 HDF5 `session-q100-20261012` 和 GRIB `grib-paired-q100-20261012`；两者 daemon 二进制 SHA-256 完全相同。历史 HDF5 `q100-20261012` 仅作复验参考。", "",
        "**结论：未发现组别交换、ns/ms/s 换算错误、绑定重复收费或正式查询总计错误。发现需要规范的统计口径与诊断字段，不能把跨格式差异直接归因于查询引擎。**", "",
        "## 1. 测量定义", "",
        "所有表格统一为 **毫秒**。暖读 p50 从每组 Q=100 的三轮、共 297 条排除首读的记录合并计算；累计／绑定／生命周期值是三轮独立总计的中位数。阶段及中位数不能直接相加：阶段可能嵌套，且不同列的中位数不必来自同一轮。", "",
        "- 普通模式：FS 每次重新打开、选择、读取、复制并关闭；IoTDB-FS 每次重新注册，并执行新 cursor、编码、发布 IPC 和挂载读取。daemon 内索引与解码缓存可复用。",
        "- 绑定模式：只付一次绑定成本，读取仍生成独立输出。FS 保留原生句柄／数据；IoTDB-FS 保留 PreparedQuery，仍执行 cursor 和 IPC，不复读旧结果。",
        "- 会话总计 = 一次绑定 + Q 次完整读取 + 一次关闭／释放；普通模式绑定项为零。",
        "- 生命周期 = 文件复制、file fsync 与关闭 + IoTDB-FS 一次精确版本定位 + 会话总计。源文件生成、正确性全文件 SHA、进程启动和父目录准备不计入。",
        "- 首读是 reader／daemon 的首次观测，不是冷盘；OS 和数据库缓存未清空。所有请求重复同一固定子集。", "",
        "## 2. 负载与缓存矩阵", "",
        "| 项目 | HDF5 | GRIB | 可比性限制 |",
        "| --- | --- | --- | --- |",
        "| 数据 | 合成有限 Float32，单 `/T` 四维 dataset | 真实 GFS `clwmr`，单文件多 message | 不是相同数据与压缩率的格式转换对照 |",
        "| 文件大小 | 6,728,832 / 537,684,072 / 3,441,125,376 B | 2,446,828 / 549,606,683 B | medium 大小相近；small 大小不同；GRIB 无同条件 large |",
        "| 原生发现 | 已知 `/T` 路径和 ordinal，原生 chunk 索引 | `level=500,step=0,typeOfLevel=isobaricInhPa,shortName=clwmr`，扫描 22／4,598 条 message | 一个是已知对象寻址，一个含语义谓词发现 |",
        "| 物理解码单元 | `[1,1,91,144]` chunk，shuffle＋gzip1 | 1440×721 完整 message，16-bit complex spatial differencing | 单元大小与解码工作不同 |",
        "| point | 1 Float32＋mask | 1 Float32＋两个 Float64 坐标＋mask | 原始值相同类型，返回列不同 |",
        "| ROI | small_roi 2,016 值；cross_chunk 192 值 | roi 16 值 | 不可按 ROI 名称视为等量工作 |",
        "| encoded message | 未测 | 命中 66,606／64,901 B | 没有对应 HDF5 二进制组 |",
        "| 普通 FS 缓存 | 每次关闭 HDF5 句柄，OS 页保留 | 每次销毁发现／解码状态，OS 页保留 | 原生缓存不会随普通请求保持 |",
        "| 绑定 FS 缓存 | H5F/H5D 和 native raw chunk cache 保留，每次仍 H5Dread | 命中 encoded bytes／完整 decoded field 保留，每次重裁剪复制 | GRIB 缓存是实验 reader 显式实现的 |",
        "| IoTDB-FS 缓存 | shared verified layout／decoded chunks，decoded 256 MiB | parsed index／private decoded field，decoded 64 MiB，index／message 独立预算 | 不宣称总 RSS 等额；本组工作集均可容纳 |",
        "| BinaryView 绑定缓存 | 不适用 | FS 缓存 encoded bytes，IoTDB-FS 每次实际 payload SQL | 此组不能代表两侧等价缓存命中 |",
        "| 授权／接口 | private root，TTL=1 s，socket＋FUSE IPC | 相同 | 不包含共享挂载多用户权限压力 |", "",
        "## 3. 完整暖读矩阵", "",
        "每格为一次完整读取的 pooled p50，单位 ms；绑定成本不在此表中。", "",
        "| 格式／规模／查询 | FS 普通 | IoTDB-FS 普通 | FS 绑定后 | IoTDB-FS 绑定后 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for r in primary:
        cells = [ms(r["modes"][mode]["warm"][arm]["total_ns"])
                 for mode in ("unbound", "bound") for arm in ARMS]
        lines.append(f"| {label(r)} | {' | '.join(cells)} |")
    lines += ["", "FS 绑定后的微秒级数字来自命中内存缓存的小输出，未访问磁盘；不可解释为磁盘随机读取只需几微秒。GRIB medium 普通 FS 的约 1.2 s 则主要是每次扫描 4,598 条 message。两种格式的 IoTDB-FS 数值暖读绑定后收敛到约 1–2 ms。", "",
              "## 4. Q=1／10／100 完整会话矩阵", "",
              "单位 ms，绑定组均包含本组的一次绑定与一次释放。HDF5 普通 Q=1／10 是同一 100 次运行的真实前缀；HDF5 绑定及 GRIB 两种模式均为各 Q 独立实际会话。两类 Q=1 并非同一抽样设计，不应把细小差值判作优化效果。", "",
              "| 格式／规模／查询 | Q | FS 普通 | IoTDB-FS 普通 | FS 绑定会话 | IoTDB-FS 绑定会话 |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for r in primary:
        for q in QS:
            cells = [ms(r["modes"][mode]["q"][str(q)][arm]["session_total_ns"])
                     for mode in ("unbound", "bound") for arm in ARMS]
            lines.append(f"| {label(r)} | {q} | {' | '.join(cells)} |")
    lines += ["", "GRIB 100 次约 117–121 s 的 FS 数字来自上表普通模式，是累计值；绑定会话约 1.24 s 已含一次扫描，不能再次添加这笔发现成本。不同 Q 重新测量的绑定时长有波动，因此 Q=10 总计略小于 Q=1 不构成累计计时错误。", "",
              "## 5. Q=100 一次性绑定与后续读取", "",
              "单位 ms，各列单独取三轮中位数；无需强求列值相加等于上一表的中位数总计。FS 建绑定仅保存固定查询需要的状态，不是任意谓词的磁盘完整索引构建。", "",
              "| 格式／规模／查询 | FS 绑定 | FS 100 次读取 | IoTDB-FS 绑定 | IoTDB-FS 100 次读取 |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for r in primary:
        q = r["modes"]["bound"]["q"]["100"]
        cells = [ms(q[arm][key]) for arm in ARMS for key in ("bind_ns", "query_sum_ns")]
        lines.append(f"| {label(r)} | {' | '.join(cells)} |")
    lines += ["", "## 6. Q=100 从文件发布到查询的矩阵", "",
              "单位 ms，包含复制／fsync／关闭、B 精确版本定位及对应会话。每行是独立场景，不能叠加所有行而重复计文件发布。", "",
              "| 格式／规模／查询 | FS 普通全流程 | IoTDB-FS 普通全流程 | FS 绑定全流程 | IoTDB-FS 绑定全流程 |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for r in primary:
        cells = [ms(r["modes"][mode]["q"]["100"][arm]["lifecycle_total_ns"])
                 for mode in ("unbound", "bound") for arm in ARMS]
        lines.append(f"| {label(r)} | {' | '.join(cells)} |")
    lines += ["", "该全流程从已生成的 raw 文件开始；没有把合成数据生成、环境安装或全文正确性校验当作文件系统操作。两侧 file fsync 的实现不同，未证明相同断电耐久性。所有已测绑定全流程均为 FS 更快。", "",
              "## 7. medium：共同成本与不同物理工作", "",
              "以下暖读诊断统一从逐请求记录 pooled 取中位数。client−server 是逐请求差值的中位数，不是两列中位数相减。server 阶段存在嵌套，不相加。", "",
              "| 格式／查询／模式 | B 完整暖读 ms | B server ms | B catalog ms | B client−server ms | SQL 次数 | 暖 payload 次数 | 首读 payload B | 输出行数 |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in primary:
        if r["tier"] != "medium": continue
        for mode in ("unbound", "bound"):
            b = r["modes"][mode]["warm"]["iotdb"]
            cells = [ms(b[k]) for k in ("total_ns", "server_total_ns", "catalog_ns", "client_minus_server_ns")]
            cells += [str(b[k]) for k in ("query_calls", "payload_calls", "first_payload_returned_bytes", "output_rows")]
            lines.append(f"| {r['format']} / {r['case']} / {mode} | {' | '.join(cells)} |")
    lines += ["", "HDF5 绑定数值组 server 约 0.29–0.39 ms，GRIB 约 0.25–0.33 ms；外部 socket／挂载打开／IPC 消费合计剩余约 0.8–1.3 ms。普通数值组均还有一条 catalog SQL，约 4.2–4.6 ms。绑定 binary 的例外由一条 payload SQL 解释，不是失效的绑定计时。", "",
              "## 8. 计时核验与潜在问题", "",
              "核验三套实验共 **12,393 次／侧、24,786 次合计**。所有原始文件／汇总／环境／数据 manifest SHA 与原记录一致；重新计算三套 summary 得到完全相同内容。原生分段逐条严格等于 total；B 子阶段不超过 total，server 不超过外部注册计时；原生外部进程观测均大于其计入的会话时间。", "",
              "| 项目 | 核验结果／处理 |",
              "| --- | --- |",
              "| A/B 交换、单位错误 | 未发现。FS native 协议与 B server operation 独立识别；主实验 B 路径也核验。早期 HDF5 没有保存 result_path，不能追加历史路径证据。 |",
              "| 绑定重复计时 | 未发现。HDF5 native 每行重复报告相同 session_bind_ns，汇总仅从第一行收费一次；GRIB 仅首行报告。两者按各自协议核验。 |",
              "| 单次／累计混用 | 矩阵分开。100 次累计不可与单次 p50 直接比较；首读和准备成本不在暖读 p50 中。 |",
              "| Q 采样不同 | HDF5 普通为真实前缀；其余为独立 Q。保留证据，未将前缀伪装为独立实验。 |",
              "| p50 聚合不同 | 原 HDF5 报告取三轮 p50 的中位数，GRIB 合并 297 条。当前矩阵两者统一 pooled p50，因此少量数字与原表不同；非原计时错误。 |",
              "| native_probe_process_wall_ns | HDF5 普通 runner 的该字段包含 subprocess 完成后的 Python JSON 解析与 oracle 校验；是原生波次 wall 上界，不是纯进程 wall。正式总计用 C++ total_ns，不受此问题影响。HDF5 绑定及 GRIB 在 subprocess 返回后立即截表。 |",
              "| 两侧计时边界 | FS 原生库选择／读取／值复制，B 还包含请求、授权、SQL、IPC 编码、发布、FUSE 和 Python 消费。适合测端到端额外成本，不能称为只比较物理读。 |",
              "| validity／描述处理 | FS 的值输出及 GRIB 坐标复制在计时内，all-valid mask 构造／摘要在计时外；B 消费 mask，并取部分 schema metadata 在计时内。小输出固定成本包含这一接口差异。 |",
              "| 输出／解码单元不等 | HDF5 ROI 为 192／2,016 行，GRIB ROI 为 16 行且有坐标；前者解码 chunk，后者整场。不能据此比较每字节或每行的格式效率。 |",
              "| 缓存及源发现不等 | 普通 FS 不保留外部索引／解码状态，B 保留摄取索引；绑定数据组消除了大部分重复工作。FS HDF5 已有文件内索引，不能称它没有任何索引。 |",
              "| Profile 计时开销 | B 均开启 profile，FS 没有同等阶段 profiler。未做 profile-off 消融，不能从该矩阵量化 instrumentation 的开销。 |",
              "| 跨运行环境 | 主 daemon SHA 相同；数据库 namespace 不同，数据、查询输出及容量不完全相同，没有对齐 DB occupancy／执行顺序的同负载消融。仅支持数量级和成本构成比较。 |", "",
              "GRIB medium 的秒级普通扫描还由独立 replay 支持：native 单次 1.224／1.180／1.188 s，ecCodes 自带 grib_ls 同一谓词约 1.607 s。见 [标签核验](../grib-paired-q100-20261012/LABEL_AUDIT.md)。这不改变两个格式工作负载不等的限制。", "",
              "## 9. 历史 HDF5 复验参考", "",
              "两套 HDF5 均重新按 pooled 暖读 p50 计算，单位 ms。旧／新 daemon 二进制不同，native reader 也增加了 bound 分支；此表不构成严格单变量消融。两轮普通模式数量级一致，不能将波动直接判为优化或回退。", "",
              "| 规模／查询 | 历史 FS 暖读 | 主实验 FS 暖读 | 历史 B 暖读 | 主实验 B 暖读 |",
              "| --- | ---: | ---: | ---: | ---: |"]
    historical = {(r["tier"], r["case"]): r for r in report["matrix"] if r["run"] == "q100-20261012"}
    for r in primary:
        if r["format"] != "HDF5": continue
        old = historical[(r["tier"], r["case"])]
        cells = [ms(x["modes"]["unbound"]["warm"][arm]["total_ns"])
                 for arm in ARMS for x in (old, r)]
        lines.append(f"| {r['tier']} / {r['case']} | {' | '.join(cells)} |")
    lines += ["", "## 10. 后续需要补齐的对照", "",
              "1. 普通 Q=1／10／100 全部独立会话，统一 pooled 与 campaign-paired 统计；现有历史结果保留原协议。",
              "2. 如果要比较两个格式的数组访问成本，应统一返回值／mask／坐标、选择规模、缓存状态与命中物理单元，不再把 message 谓词发现与已知 dataset 寻址混为一组。",
              "3. 将绑定／索引准备、首次解码、缓存命中读取、完整发布分别报告。若需任意谓词磁盘 FS 索引，应单独测完整构建与加载；当前固定-query binding 不是这项测量。",
              "4. 若目标是系统额外成本，补 profile-off、IPC 消费对照和统一诊断进程 wall 边界；本轮未修改生产代码或旧实验计时，也未外推优化收益。", "",
              "复算命令：", "", "```bash",
              "python3 experiments/single_file/cross_format_comparison.py \\",
              "  --output experiments/single_file/results/cross-format-audit-20261012/audit.json",
              "```", "", "完整归一化 Q 矩阵、逐套核验结果和输入指纹保存在 `audit.json`；`run-manifest.json` 保存本次脚本与输出指纹。", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = {"protocol": "iotdbfs-single-file-cross-format-audit/1", "date": "2026-10-12",
              "units": "nanoseconds; Markdown tables convert to milliseconds",
              "warm_aggregation": "Pooled median of all 297 post-first Q=100 observations per arm/scenario",
              "total_aggregation": "Median of three campaign totals; inclusive stages are never added",
              "inputs": {}, "checks": {}, "matrix": []}
    all_groups = []
    for run, (family, summarizer, expected) in RUNS.items():
        root = args.results / run
        raw = gzip.decompress((root / "campaign.json.gz").read_bytes())
        data = json.loads(raw)
        stored = json.loads((root / "summary.json").read_text())
        recomputed = summarizer(data)
        recomputed["campaign_sha256"] = sha(raw)
        require(recomputed == stored, "stored summary differs: " + run)
        manifest = json.loads((root / "run-manifest.json").read_text())
        for name in ("campaign.json.gz", "summary.json", "environment.json", "data-manifest.json"):
            require(sha((root / name).read_bytes()) == manifest["artifacts"][name], "artifact changed: " + run + "/" + name)
        env = json.loads((root / "environment.json").read_text())
        report["inputs"][run] = {"raw_sha256": sha(raw), "database": data["database"],
            "daemon_sha256": env["binaries"]["daemon"]["sha256"], "cache_semantics": data["cache_semantics"],
            "source": str(root / "campaign.json.gz")}
        normalized = list(groups(data, run, family))
        report["checks"][run] = audit(normalized, family, expected)
        report["checks"][run]["stored_summary_exact_match"] = True
        all_groups.extend(normalized)
    report["primary_daemon_identical"] = (
        report["inputs"]["session-q100-20261012"]["daemon_sha256"] ==
        report["inputs"]["grib-paired-q100-20261012"]["daemon_sha256"])
    require(report["primary_daemon_identical"], "primary comparison uses different daemons")
    report["matrix"] = aggregate(all_groups)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (args.output.parent / "RESULTS.md").write_text(markdown(report))
    manifest = {"protocol": "iotdbfs-cross-format-audit-artifacts/1", "complete": True,
                "script_sha256": sha(Path(__file__).read_bytes()),
                "inputs": report["inputs"],
                "artifacts": {name: sha((args.output.parent / name).read_bytes())
                              for name in (args.output.name, "RESULTS.md")}}
    (args.output.parent / "run-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"passed": True, "primary_daemon_identical": True,
                      "observations_per_arm": sum(v[2] for v in RUNS.values()), "output": str(args.output)}))


if __name__ == "__main__":
    main()
