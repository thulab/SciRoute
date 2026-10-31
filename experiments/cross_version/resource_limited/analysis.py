"""Strict ratio-first analysis of the predeclared, resource-bounded retest.

There are three scenes, two implementation blocks per scene, two warmup pairs
and ten measured pairs per block. Reject incomplete/failed/duplicate cells; never
drop an inconvenient observation. R is the quotient of the TWO medians from
ONE block, not the median of pair ratios and not a quotient of cross-run minima.
Pair ratios, absolute clocks, first observations and resource evidence remain
separate. CPU/scheduler/throttle observations never normalize query wall time.
"""
from copy import deepcopy
import hashlib
import json
import re
import statistics

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_PROTOCOL = ("engine", "clock", "cache", "delivery")


def _need(ok, message):
    if not ok:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def _text(value, name):
    _need(isinstance(value,str) and bool(value.strip()), name+" must be nonempty text")
    return value


def _sha(value, name):
    _need(isinstance(value,str) and _SHA.fullmatch(value), name+" must be an explicit SHA-256 identity")
    return value


def _time(value):
    _need(type(value) is int and value>0, "total_ns must be a positive integer wall time")
    return value


def _protocol(value, run_required=True):
    _need(isinstance(value,dict), "arm protocol must be an object")
    for name in _PROTOCOL:
        item=value.get(name)
        _need((isinstance(item,str) and item.strip()) or (isinstance(item,dict) and item),
              "explicit arm "+name+" protocol required")
        _canonical(item)
    if run_required:
        _sha(value.get("run_id"),"run_id")
    return {name:deepcopy(value[name]) for name in _PROTOCOL}


def _design(design):
    _need(isinstance(design,dict), "expected_design must be an object")
    _canonical(design)
    scenes,variants=design.get("scenes"),design.get("variants")
    for values,count,name in ((scenes,3,"scenes"),(variants,2,"variants")):
        _need(isinstance(values,list) and len(values)==count and len(set(values))==count,
              "exactly %d unique declared %s required"%(count,name))
        for value in values:_text(value,name)
    warm,measured=design.get("warmup_repeats"),design.get("measured_repeats")
    _need(isinstance(warm,list) and isinstance(measured,list) and len(warm)==2 and len(measured)==10,
          "design requires two warmup and ten measured repeat indices")
    indices=warm+measured
    _need(all(type(index) is int and index>=0 for index in indices) and
          len(set(indices))==12 and warm==sorted(warm) and measured==sorted(measured) and
          indices==list(range(indices[0],indices[0]+12)), "repeat ranges must be ordered, contiguous and disjoint")
    arms=design.get("arms")
    _need(isinstance(arms,dict) and set(arms)=={"fs","iotdb"} and arms["fs"]!=arms["iotdb"],
          "distinct explicit FS and IoTDB arm names required")
    for name in arms.values():_text(name,"arm")
    blocks=design.get("blocks")
    _need(isinstance(blocks,list) and len(blocks)==6,"exactly six declared scene/variant blocks required")
    by_key={};block_ids=set();scene_cases={}
    for block in blocks:
        _need(isinstance(block,dict),"block must be an object")
        key=(block.get("scene_id"),block.get("variant"))
        _need(key[0] in scenes and key[1] in variants and key not in by_key,"unknown or duplicate declared scene/variant block")
        identity=_text(block.get("block_id"),"block_id")
        _need(identity not in block_ids,"block IDs must uniquely identify implementation blocks")
        block_ids.add(identity)
        for name in ("case_id","corpus_id","resource_id"):_sha(block.get(name),name)
        _need(isinstance(block.get("resource_identity"),dict) and block["resource_identity"],"explicit resource identity required")
        _canonical(block["resource_identity"])
        pair=(block["corpus_id"],block["case_id"])
        _need(scene_cases.setdefault(key[0],pair)==pair,"variants of one scene must bind the same corpus/case")
        protocols=block.get("arm_protocols")
        _need(isinstance(protocols,dict) and set(protocols)==set(arms.values()),"both expected arm protocols required")
        for protocol in protocols.values():_protocol(protocol)
        by_key[key]=block
    _need(set(by_key)=={(scene,variant) for scene in scenes for variant in variants},"missing declared scene/variant cell")
    return scenes,variants,warm,measured,arms,by_key


def _absolute(values):
    return {"count":len(values),"median_ns":statistics.median(values),
            "mean_ns":statistics.mean(values),"min_ns":min(values),"max_ns":max(values),
            "samples_ns":list(values)}


def summarize(records, expected_design):
    """Validate all 144 rows and return descriptive within-block time ratios.

    Each row repeats its declared scene/variant/block, corpus/case/resource IDs,
    resource identity and per-arm run_id/engine/clock/cache/delivery. Extra
    payload/profile/resource observations are preserved verbatim, never pooled
    as wall costs. The two arms may have explicitly different delivery clocks.
    """
    scenes,variants,warm,measured,arms,blocks=_design(expected_design)
    _need(isinstance(records,list),"records must be a list; incomplete streams are not accepted")
    _need(len(records)==6*12*2,"complete matrix requires exactly 144 rows")
    indexed={}
    for row in records:
        _need(isinstance(row,dict),"query record must be an object")
        _canonical(row)
        block_key=(row.get("scene_id"),row.get("variant"))
        _need(block_key in blocks,"query belongs to an undeclared scene/variant")
        block=blocks[block_key]
        arm=row.get("arm");repeat=row.get("repeat")
        _need(arm in arms.values() and type(repeat) is int and repeat in warm+measured,"unknown arm or repeat")
        key=block_key+(repeat,arm)
        _need(key not in indexed,"duplicate arm in one declared pair")
        _need(row.get("phase")==("warmup" if repeat in warm else "measurement"),"warmup/measurement phase differs from frozen repeat ranges")
        _need(row.get("ok") is True and row.get("oracle_verified") is True,"failed or unverified rows cannot be silently excluded")
        _time(row.get("total_ns"))
        for name in ("block_id","case_id","corpus_id","resource_id","resource_identity"):
            _need(row.get(name)==block[name],"row mixes a different "+name+" within a block")
        protocol=block["arm_protocols"][arm]
        for name in ("run_id",)+_PROTOCOL:
            _need(row.get(name)==protocol[name],"row mixes a different arm "+name+" within a block")
        if "resource_observations" in row:
            _need(isinstance(row["resource_observations"],dict),"resource observations must be an object")
        indexed[key]=row
    expected={(scene,variant,repeat,arm) for scene in scenes for variant in variants for repeat in warm+measured for arm in arms.values()}
    _need(set(indexed)==expected,"missing declared arm/pair; no rows may be dropped")
    cells=[]
    for scene in scenes:
        for variant in variants:
            block=blocks[scene,variant]
            fs=[indexed[scene,variant,index,arms["fs"]]["total_ns"] for index in measured]
            io=[indexed[scene,variant,index,arms["iotdb"]]["total_ns"] for index in measured]
            ratio=statistics.median(io)/statistics.median(fs)
            pairs=[{"repeat":index,"phase":"warmup" if index in warm else "measurement",
                    "fs_ns":indexed[scene,variant,index,arms["fs"]]["total_ns"],
                    "iotdb_ns":indexed[scene,variant,index,arms["iotdb"]]["total_ns"],
                    "R_pair":indexed[scene,variant,index,arms["iotdb"]]["total_ns"]/indexed[scene,variant,index,arms["fs"]]["total_ns"]}
                   for index in warm+measured]
            per_arm={}
            for role,arm in arms.items():
                selected=[indexed[scene,variant,index,arm] for index in warm+measured]
                per_arm[role]={"arm":arm,"protocol":deepcopy(block["arm_protocols"][arm]),
                    "measured":_absolute([row["total_ns"] for row in selected if row["repeat"] in measured]),
                    "warmup":_absolute([row["total_ns"] for row in selected if row["repeat"] in warm]),
                    "first_observation_ns":selected[0]["total_ns"],
                    "all_rows":deepcopy(selected)}
            cells.append({"scene_id":scene,"variant":variant,"block_id":block["block_id"],
                "case_id":block["case_id"],"corpus_id":block["corpus_id"],"resource_id":block["resource_id"],
                "resource_identity":deepcopy(block["resource_identity"]),"warmup_pairs":2,"measured_pairs":10,
                "R":ratio,"R_definition":"median(T_IoTDB-FS) / median(T_FS-index), measured pairs in this block only",
                "pair_ratios":pairs,"arms":per_arm,"R_le_1_observed":ratio<=1,
                "statistical_equivalence_claimed":False})
    comparisons=[]
    for scene in scenes:
        left,right=[next(cell for cell in cells if cell["scene_id"]==scene and cell["variant"]==variant) for variant in variants]
        comparisons.append({"scene_id":scene,"left_variant":variants[0],"right_variant":variants[1],
            "left_R":left["R"],"right_R":right["R"],"right_to_left_R":right["R"]/left["R"],
            "same_resource_identity":left["resource_id"]==right["resource_id"] and left["resource_identity"]==right["resource_identity"],
            "same_arm_protocols":all(_protocol(left["arms"][role]["protocol"])==_protocol(right["arms"][role]["protocol"]) for role in arms),
            "interpretation":"descriptive block ratios; no statistical equivalence or CPU-normalized causal claim"})
    return {"complete":True,"protocol":"iotdbfs-resource-limited-ratio-analysis/1",
        "expected_design_sha256":hashlib.sha256(_canonical(expected_design).encode()).hexdigest(),
        "query_records":len(records),"warmup_records":24,"measured_records":120,
        "cells":cells,"scene_comparisons":comparisons,"all_rows":deepcopy(records),
        "observed_any_R_le_1":any(cell["R_le_1_observed"] for cell in cells),
        "notes":["Ratio of same-block measured medians is primary; pair-ratio medians are not substituted.",
            "Two excluded warmups include the first observation; neither establishes cold-disk behavior.",
            "Absolute clocks and CPU/wall/scheduler/throttle observations are retained without resource normalization.",
            "FS C++ owned-buffer and IoTDB NumPy owned-buffer delivery may differ for GRIB; declared arm protocols remain separate.",
            "HDF5 prototype/FUSE and official database-VFD engines must retain separate protocol identities.",
            "Small descriptive samples do not establish statistical equivalence or independent experimental replication."]}


def historical_minima(blocks, *, arms):
    """Descriptive historical minima, recomputed from each whole source block.

    Inputs contain case_id/corpus_id, block_id, arm_protocols, samples_ns keyed
    by the two arm names, source_evidence, and optional resource_controlled.
    Strict current-run IDs are not reconstructed. Group all per-arm engine,
    clock/cache/delivery facts; missing facts must not be guessed or combined.
    """
    _need(isinstance(arms,dict) and set(arms)=={"fs","iotdb"} and arms["fs"]!=arms["iotdb"],"explicit historical arm names required")
    _need(isinstance(blocks,list),"historical blocks must be a list")
    groups={};seen=set()
    for block in blocks:
        _need(isinstance(block,dict),"historical block must be an object")
        for name in ("case_id","corpus_id"):_sha(block.get(name),name)
        identity=_text(block.get("block_id"),"historical block_id")
        _need(identity not in seen,"duplicate historical source block")
        seen.add(identity)
        _need(isinstance(block.get("source_evidence"),dict) and block["source_evidence"],"historical source evidence required")
        _text(block["source_evidence"].get("artifact"),"historical artifact reference")
        _sha(block["source_evidence"].get("sha256"),"historical artifact SHA-256")
        _canonical(block["source_evidence"])
        protocols=block.get("arm_protocols")
        _need(isinstance(protocols,dict) and set(protocols)==set(arms.values()),"both historical arm protocols required")
        normalized={arm:_protocol(protocols[arm],False) for arm in arms.values()}
        samples=block.get("samples_ns")
        _need(isinstance(samples,dict) and set(samples)==set(arms.values()),"both same-block historical sample sets required")
        for values in samples.values():
            _need(isinstance(values,list) and values,"historical measured observations absent")
            for value in values:_time(value)
        _need(len(samples[arms["fs"]])==len(samples[arms["iotdb"]]),"historical source block has unequal arm sample counts")
        ratio=statistics.median(samples[arms["iotdb"]])/statistics.median(samples[arms["fs"]])
        key=_canonical({"case_id":block["case_id"],"corpus_id":block["corpus_id"],"arm_protocols":normalized})
        observed={"block_id":identity,"R":ratio,"source_evidence":deepcopy(block["source_evidence"]),
            "resource_controlled":block.get("resource_controlled") is True,
            "fs":_absolute(samples[arms["fs"]]),"iotdb":_absolute(samples[arms["iotdb"]])}
        groups.setdefault(key,[]).append(observed)
    result=[]
    for key,observations in sorted(groups.items()):
        best=min(observations,key=lambda block:block["R"])
        result.append({**json.loads(key),"minimum_R":best["R"],"minimum_block":deepcopy(best),
                       "source_blocks":observations,"strict_current_run_AB":False,
                       "limitation":"descriptive historical reference only; missing resource control is not inferred"})
    return result
