"""Frozen Section 4.2 Arrow and scientific-coordinate input identities.

No publication or queries occur at import time. Registry identity hashes must
still be verified against bytes and exact catalog entries by the supervisor.
"""
from copy import deepcopy

BASE = "/data_01/zzy/iotdb-fs-exp/public-arrow-baseline"


def source_registry():
    return {
        "arrow_ett": dict(format="arrow", raw=BASE + "/arrow/ETTm1-none-8192.arrows",
            logical_path="/ETTm1-none-8192.arrows", version=1790670158165,
            bytes=1955584, channels=7, codec="none",
            sha256="cbd4833ce525c32dd883bf6e1b174f2d2b2ab7241889358c1a3325883c2b179b"),
        "arrow_e370": dict(format="arrow", raw=BASE + "/arrow/UCI_Electricity_370-none-8192.arrows",
            logical_path="/UCI_Electricity_370-none-8192.arrows", version=1790670177863,
            bytes=207613480, channels=370, codec="none",
            sha256="536c3c72afb04f09daadf860ef021719bb4f90515c10a6710bd35d78de18fa0f"),
        "arrow_e370_lz4": dict(format="arrow", raw=BASE + "/arrow-query-variants-20261001/UCI_Electricity_370-lz4-8192.arrows",
            logical_path="/query-ext-20261001/UCI_Electricity_370-lz4.arrows", version=1790755242327,
            bytes=93676296, channels=370, codec="lz4",
            sha256="b39aabba25e389b06197bb81bd5b4fdd553320bce456939c3b2ecda0b619b03f",
            manifest=BASE + "/arrow-query-extended-combined-20261001/source-manifest.json"),
        "geos": dict(format="hdf5", raw=BASE + "/geos-pilot-20261006/raw/geos-20110702-0900.nc4",
            logical_path="/geos-pilot-b66607b30410/file1.nc4", version=1791287459566,
            bytes=445984379, database="h09probe1790939889dbfc1f",
            sha256="fd763bf40ed34e0e4f91b9da1bd94e272e2145c95a0323b292f7b48b143a723e",
            manifest=BASE + "/geos-semantic-prod-20261006/ingest-0900-1200-v5/pilot.json",
            date="2011-07-02T09:00:00", dataset="/T", dtype="<f4", source_shape=[1,42,721,1152],
            chunk_shape=[1,1,91,144], roles=["time","pressure","latitude","longitude"],
            coordinate_datasets=["/time","/lev","/lat","/lon"],
            attribute="standard_name", attribute_value="air_temperature", view="physical"),
        "oisst": dict(format="hdf5", raw=BASE + "/hdf5-sc-perf-20261006/raw/oisst-avhrr-v02r01.20240101.nc",
            logical_path="/h09-scperf-f6509fc361c9/day01.nc", version=1791262231728,
            bytes=1528923, database="h09probe1790939889dbfc1f",
            sha256="fd2f772cbe1633b20ab0c53ed76b874406f933bbd97062d611827c5d8a73bc4c",
            manifest=BASE + "/hdf5-sc-perf-20261006/final-seven-day/campaign.json",
            date="2024-01-01", dataset="/sst", dtype="<i2", source_shape=[1,1,720,1440],
            chunk_shape=[1,1,720,1440], roles=["time","vertical","latitude","longitude"],
            coordinate_datasets=["/time","/zlev","/lat","/lon"],
            attribute="long_name", attribute_value="Daily sea surface temperature", view="science"),
    }


def case_registry():
    cases = []
    for ident, source, children, begin, length in [
        ("A-F4-ETT7-C1-R128", "arrow_ett", [0], 37, 128),
        ("A-F4-E370-C1-R128", "arrow_e370", [0], 37, 128),
        ("A-F4-E370-C370-R128", "arrow_e370", list(reversed(range(370))), 37, 128),
        ("A-F4-E370-C370-R4096", "arrow_e370", list(reversed(range(370))), 0, 4096),
    ]:
        cases.append(dict(id=ident, source=source, format="arrow", batch=0, row=0,
                          field="target", children=children, begin=begin, length=length))
    for suffix, origin, shape, chunks, selectors in [
        ("ALL", [0,0,0,0], [1,42,721,1152], 2688, {}),
        ("P750", [0,10,0,0], [1,1,721,1152], 64, {"pressure":[749.,751.]}),
        ("R256", [0,10,352,568], [1,1,16,16], 4,
            {"pressure":[749.,751.],"latitude":[-2.,2.],"longitude":[-2.5,2.5]}),
        ("R1", [0,10,360,576], [1,1,1,1], 1,
            {"pressure":[749.,751.],"latitude":[-.001,.001],"longitude":[-.001,.001]}),
    ]:
        cases.append(dict(id="H-F8-GEOS-" + suffix, source="geos", format="hdf5",
            origin=origin, shape=shape, chunks=chunks, selectors=selectors,
            coordinates=[], selection_coordinates_verified=True,
            output_contract="owned Float32 values and uint8 mask; coordinates select and verify ROI, not repeated per value"))
    for suffix, origin, shape, selectors in [
        ("R1", [0,0,360,720], [1,1,1,1], {"latitude":[0.,.25],"longitude":[180.,180.25]}),
        ("R64", [0,0,356,720], [1,1,8,8], {"latitude":[-1.,1.],"longitude":[180.,182.]}),
    ]:
        cases.append(dict(id="H-F8-OISST-" + suffix, source="oisst", format="hdf5",
            origin=origin, shape=shape, chunks=1, selectors=selectors, coordinates=[]))
    for ident, children, begin, length in [
        ("A-F9-E370-C1-R128", [0], 37, 128),
        ("A-F9-E370-C370-R4096", list(reversed(range(370))), 0, 4096),
    ]:
        for codec, source in [("none","arrow_e370"),("lz4","arrow_e370_lz4")]:
            cases.append(dict(id=ident, variant=codec, source=source, format="arrow", batch=0,
                row=0, field="target", children=deepcopy(children), begin=begin, length=length))
    return cases
