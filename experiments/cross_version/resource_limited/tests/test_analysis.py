"""Synthetic wall-clock documents only; no workloads or service operations."""
from copy import deepcopy
import unittest

from experiments.cross_version.resource_limited.analysis import historical_minima, summarize


def fixture():
    arms={"fs":"FS-index","iotdb":"IoTDB-FS"}
    design={"scenes":["grib-small","hdf-control","grib-large-history"],
            "variants":["baseline","candidate"],"warmup_repeats":[0,1],
            "measured_repeats":list(range(2,12)),"arms":arms,"blocks":[]}
    rows=[]
    for scene_index,scene in enumerate(design["scenes"]):
        for variant_index,variant in enumerate(design["variants"]):
            block={"scene_id":scene,"variant":variant,"block_id":scene+"-"+variant,
                "case_id":str(scene_index+1)*64,"corpus_id":str(scene_index+4)*64,
                "resource_id":"a"*64,"resource_identity":{"cpus":[2,3],"query_memory_bytes":2*1024**3},
                "arm_protocols":{}}
            for arm_index,arm in enumerate(arms.values()):
                block["arm_protocols"][arm]={"run_id":("%064x"%(100+scene_index*4+variant_index*2+arm_index)),
                    "engine":"native-eccodes" if arm_index==0 else "grib-indexed",
                    "clock":{"boundary":"native owned" if arm_index==0 else "client IPC owned"},
                    "cache":{"warmup_pairs":2},"delivery":"C++ owned" if arm_index==0 else "NumPy owned"}
            design["blocks"].append(block)
            for repeat in range(12):
                # Measurement FS [1,1,1,1,1,100,100,100,100,100],
                # Io [1,100,100,100,100,100,100,100,100,100].
                # Ratio of medians =100/50.5; pair-ratio median =1.
                fs=1000 if repeat<7 else 100000
                io=1000 if repeat==2 else 100000
                for arm in (list(arms.values()) if repeat%2 else list(reversed(arms.values()))):
                    row={name:deepcopy(block[name]) for name in
                        ("scene_id","variant","block_id","case_id","corpus_id","resource_id","resource_identity")}
                    row.update(deepcopy(block["arm_protocols"][arm]))
                    row.update(arm=arm,repeat=repeat,phase="warmup" if repeat<2 else "measurement",ok=True,
                        oracle_verified=True,total_ns=fs if arm==arms["fs"] else io,
                        resource_observations={"cpu_ns":500,"sched_wait_ns":100,"throttled_ns":25},
                        payload_metrics={"owned_bytes":40960})
                    rows.append(row)
    return rows,design


class Analysis(unittest.TestCase):
    def test_primary_is_same_block_ratio_of_medians_not_median_pair_ratio(self):
        rows,design=fixture();result=summarize(rows,design)
        self.assertTrue(result["complete"])
        self.assertEqual(result["query_records"],144)
        self.assertEqual(result["measured_records"],120)
        for cell in result["cells"]:
            self.assertAlmostEqual(cell["R"],100000/50500)
            self.assertEqual(len(cell["pair_ratios"]),12)
            self.assertEqual(cell["arms"]["fs"]["measured"]["count"],10)
            self.assertFalse(cell["statistical_equivalence_claimed"])

    def test_first_two_warmups_raw_resources_and_payload_metrics_are_retained(self):
        rows,design=fixture();original=deepcopy(rows);result=summarize(rows,design)
        self.assertEqual(rows,original)
        cell=result["cells"][0]
        self.assertEqual(cell["arms"]["fs"]["first_observation_ns"],1000)
        self.assertEqual(cell["arms"]["fs"]["warmup"]["count"],2)
        self.assertEqual(result["all_rows"],rows)
        result["all_rows"][0]["resource_observations"]["cpu_ns"]=999
        self.assertEqual(rows,original)
        self.assertIn("payload_metrics",cell["arms"]["iotdb"]["all_rows"][0])

    def test_explicit_different_native_and_ipc_arm_protocols_are_not_silently_aligned(self):
        rows,design=fixture();cell=summarize(rows,design)["cells"][0]
        self.assertNotEqual(cell["arms"]["fs"]["protocol"]["clock"],cell["arms"]["iotdb"]["protocol"]["clock"])
        self.assertNotEqual(cell["arms"]["fs"]["protocol"]["delivery"],cell["arms"]["iotdb"]["protocol"]["delivery"])

    def test_missing_duplicate_extra_failed_or_unverified_rows_are_rejected(self):
        for mode in ("missing","duplicate","extra","failed","oracle"):
            rows,design=fixture()
            if mode=="missing":rows.pop()
            if mode=="duplicate":rows[-1]=deepcopy(rows[0])
            if mode=="extra":rows.append(deepcopy(rows[0]))
            if mode=="failed":rows[0]["ok"]=False
            if mode=="oracle":rows[0]["oracle_verified"]=False
            with self.subTest(mode=mode),self.assertRaises(ValueError):summarize(rows,design)

    def test_case_corpus_resource_run_engine_clock_cache_and_delivery_mixing_fail(self):
        for field in ("case_id","corpus_id","block_id","resource_id","resource_identity",
                      "run_id","engine","clock","cache","delivery"):
            rows,design=fixture();rows[0][field]="changed"
            with self.subTest(field=field),self.assertRaises(ValueError):summarize(rows,design)

    def test_zero_boolean_float_nan_time_and_phase_relabelling_fail(self):
        for value in (0,-1,True,1.0,float("nan")):
            rows,design=fixture();rows[0]["total_ns"]=value
            with self.subTest(value=value),self.assertRaises(ValueError):summarize(rows,design)
        rows,design=fixture();rows[0]["phase"]="measurement"
        with self.assertRaises(ValueError):summarize(rows,design)

    def test_design_cannot_remove_scene_variant_block_or_warmup_after_observation(self):
        for mode in ("scene","variant","block","duplicate_block","warmup","overlap","case"):
            rows,design=fixture()
            if mode=="scene":design["scenes"].pop()
            if mode=="variant":design["variants"].pop()
            if mode=="block":design["blocks"].pop()
            if mode=="duplicate_block":design["blocks"][-1]=deepcopy(design["blocks"][0])
            if mode=="warmup":design["warmup_repeats"]=[0]
            if mode=="overlap":design["measured_repeats"][0]=1
            if mode=="case":design["blocks"][1]["case_id"]="f"*64
            with self.subTest(mode=mode),self.assertRaises(ValueError):summarize(rows,design)

    def test_resource_observations_never_normalize_wall_ratio(self):
        rows,design=fixture();expected=summarize(rows,design)["cells"][0]["R"]
        for row in rows:row["resource_observations"]={"cpu_ns":10**12,"sched_wait_ns":10**15,"throttled_ns":10**10}
        self.assertEqual(summarize(rows,design)["cells"][0]["R"],expected)

    def test_order_of_rows_does_not_change_paired_primary_statistics(self):
        rows,design=fixture();first=summarize(rows,design)
        rows.reverse();second=summarize(rows,design)
        self.assertEqual(first["cells"],second["cells"])
        self.assertNotEqual(first["all_rows"],second["all_rows"])

    def test_different_cross_block_resources_are_explicit_not_causal_equivalence(self):
        rows,design=fixture();design["blocks"][1]["resource_id"]="f"*64
        for row in rows:
            if row["scene_id"]==design["scenes"][0] and row["variant"]=="candidate":row["resource_id"]="f"*64
        comparison=summarize(rows,design)["scene_comparisons"][0]
        self.assertFalse(comparison["same_resource_identity"])
        self.assertIn("descriptive",comparison["interpretation"])

    def test_historical_minimum_is_whole_block_and_grouped_by_exact_protocol(self):
        _,design=fixture();source=design["blocks"][0]
        def block(name,fs,io):
            return {"case_id":source["case_id"],"corpus_id":source["corpus_id"],"block_id":name,
                "arm_protocols":deepcopy(source["arm_protocols"]),"samples_ns":{"FS-index":fs,"IoTDB-FS":io},
                "source_evidence":{"artifact":"old-"+name+".json","sha256":"f"*64}}
        old=block("a",[100,100],[200,200]);new=block("b",[1000,1000],[3000,3000])
        changed=block("c",[100,100],[1,1]);changed["arm_protocols"]["IoTDB-FS"]["engine"]="native-hdf5-vfd"
        groups=historical_minima([old,new,changed],arms=design["arms"])
        self.assertEqual(len(groups),2)
        ordinary=next(g for g in groups if len(g["source_blocks"])==2)
        self.assertEqual(ordinary["minimum_R"],2)
        self.assertEqual(ordinary["minimum_block"]["block_id"],"a")
        self.assertFalse(ordinary["minimum_block"]["resource_controlled"])
        self.assertFalse(ordinary["strict_current_run_AB"])

    def test_historical_missing_evidence_protocol_arm_or_duplicates_fail(self):
        _,design=fixture();source=design["blocks"][0]
        base={"case_id":source["case_id"],"corpus_id":source["corpus_id"],"block_id":"old",
            "arm_protocols":deepcopy(source["arm_protocols"]),"samples_ns":{"FS-index":[10],"IoTDB-FS":[20]},
            "source_evidence":{"artifact":"old.json","sha256":"a"*64}}
        for mode in ("evidence","clock","samples","duplicate"):
            block=deepcopy(base)
            if mode=="evidence":block.pop("source_evidence")
            if mode=="clock":block["arm_protocols"]["FS-index"].pop("clock")
            if mode=="samples":block["samples_ns"]["FS-index"].append(10)
            with self.subTest(mode=mode),self.assertRaises(ValueError):
                historical_minima([block,deepcopy(block)] if mode=="duplicate" else [block],arms=design["arms"])


if __name__=="__main__":
    unittest.main()
