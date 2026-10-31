# HDF5 / GRIB multi-file gradient feasibility — 2026-10-13

This is a metadata feasibility audit, not a performance experiment. It answers whether existing raw datasets can support independent candidate count (N), matching-file count (K), units per file (D), and directory-depth gradients.

## Evidence and scope

Host: `root@192.168.99.49`. Raw experiment base: `/data_01/zzy/iotdb-fs-exp`.
We inspected 34 existing input paths: 16 complete GEOS time files, 7 OISST daily files, 2 GWOSC detector files, 2 GFS files, 4 ERA5 files, and 3 controlled HDF5 files. Runtime mount views, test fixtures, duplicate aliases, and truncated GEOS downloads are not additional independent inputs.

[metadata.json](https://github.com/SpriCoder/IoTDB-FS-C/blob/6e637945e13e107709aba733eb00ded350629750/experiments/multi_file/feasibility-20261013/metadata.json) records the actual counts, schema, chunks, headers, paths, and free space. HDF5 inspection used h5py 3.12.1 / HDF5 1.14.4; GRIB inspection used native `grib_ls -x`, without payload decoding. This audit did not recompute full-file source hashes, publish data, create new databases, or change raw originals. Prior correctness manifests remain separate evidence.

The inspected roots are bounded; this is not a claim that every file on both data partitions has been inventoried. This audit checks file-format feasibility and existing query contracts; it does not claim that a new multi-file generator or runner has already been validated.

## Definitions

- **N:** distinct candidate file paths, one published version per path. Record distinct scientific observations separately from replicas.
- **K_file:** files containing at least one matching dataset/message. **K_unit:** matching datasets/messages. **K_fragment:** physical chunks/messages read. All three must be reported; a fixed K_file alone does not fix result size.
- **D_data:** numeric scientific datasets per HDF5 file, or messages per GRIB file. Also record total HDF5 objects/datasets and chunks. A GRIB message and a HDF5 dataset do not necessarily have equal payload size or semantic extent.
- **depth:** number of directory components below the experimental root, excluding the filename. HDF5 internal group depth is a separate factor; GRIB has no corresponding internal group tree.

## Existing data and feasibility

| Source | Inspected structure | N gradient | K gradient | D gradient | External depth |
|---|---|---|---|---|---|
| GEOS-5.7.2 | 16 three-hour UTC files, 7,142,773,610 B; 14 science datasets + 4 coordinate datasets per file | Immediate 1/4/8/16 subsets; this public sample collection does not supply a larger genuine N | Time interval selects 0..N files; shared variables, pressure levels, and global coverage do not add file-level selectivity | Fixed 14 science datasets in originals; requires derived files or controlled generation | Can reorganize copies without changing contents |
| OISST v2.1 | 7 consecutive daily files, about 1.5 MB each; 4 science datasets + 4 coordinates | Public daily archive can supply many genuine files; only 7 are local inputs here | Time can vary selection in principle; production UTC directory is currently GEOS-specific | Fixed 4 science datasets in originals | Can reorganize copies |
| GWOSC GW150914 | 2 detector files, about 1 MB each; 1 strain array, 2 quality masks, and metadata/string datasets, 15 datasets total | Current event pair is small; broader public archive exists | Time/detector selection possible, but those metadata datasets are not automatically dataset-attribute predicates | Strain count remains 1; total dataset count 15 is not 15 equivalent scientific arrays | Can reorganize copies; existing internal groups are a separate property |
| GFS cloud mixing ratio | 2 files, 22 and 4,598 messages; 1 variable, 22 pressure levels; larger file has 209 forecast steps | Can split message streams; genuine larger corpus needs additional cycles/products | Existing variable predicate is constant; level/forecast-time predicates work; file-level K depends on grouping | Message grouping can vary D without repacking | Can reorganize copies or newly grouped streams |
| ERA5 pressure/surface | Four local files in d0/d1; monthly pressure file has 1,984 messages = 4 variables × 4 levels × 124 times | Message regrouping supplies a better real-data pool, but N and D remain coupled | Real variable/level/time diversity; grouping determines K_file | Message grouping varies D | Can reorganize new grouped streams, preserving protected originals |
| Existing controlled HDF5 | 3 size tiers, each only /T; Float32, shuffle + gzip-1, chunks [1,1,91,144] | Current files are size tiers, not a candidate-count corpus | No heterogeneous cross-file variable/time labels | D=1 in all tiers; current generator changes array length, not dataset count | Needs a new tree materialization step |
| Proposed controlled corpus | Fully allocated numeric datasets or valid ecCodes messages; not yet generated | Can control N explicitly | Can place matching units in exactly K files and verify with a native oracle | Can control D with separate physical objects/messages | Can independently materialize balanced tree depths |

GEOS /T is [1,42,721,1152], with chunks [1,1,91,144]: 2,688 chunks. All 16 files share the same structural schema and the /T dimension scales /time, /lev, /lat, /lon. Their time values are each zero in different reference units; comparing raw /time[0] would incorrectly treat them as the same instant. Parsing the complete units produces 16 distinct UTC instants, 10,800 seconds apart.

Every inspected OISST value dataset is [1,1,720,1440], with a single chunk of that full shape. These files are useful for directory scale and format compatibility, but point/ROI queries cannot demonstrate spatial chunk pruning on the unchanged originals.

GFS uses the same 1440 × 721 regular latitude/longitude grid across inspected messages, but **packing is not uniform**. The larger file contains 209 constant grid_simple messages and 4,389 grid_complex_spatial_differencing messages; bitsPerValue also varies. Splitting must preserve and report this distribution rather than calling all messages a single packing configuration.

ERA5 pressure files use a 1440 × 721 grid and grid_simple / 16-bit packing. The d0 pressure file has 128 messages over 8 times; d1 has 1,984 over 124 times. Both contain t/u/v/z and levels 500/700/850/1000 hPa. Surface files contain 32 and 496 messages respectively, with 10u/10v/2t/msl. The d0 and d1 dates overlap; do not count them as independent runs or indiscriminately concatenate them.

## Why reorganization alone is insufficient

For a fixed pool of M unique messages, full files with D messages satisfy N × D ≤ M. The monthly ERA5 pressure pool therefore supports:

| D messages/file | Full files from M=1,984 |
|---:|---:|
| 1 | 1,984 |
| 16 | 124 |
| 64 | 31 |

It cannot provide N=1,024 and D=64 without at least 65,536 messages. A GFS pool of 4,598 messages gives 287 full D=16 files plus one 6-message remainder. The remainder must be excluded or explicitly classified, not silently treated as D=16.

Byte-range copying at complete GRIB message boundaries can preserve original encoding. The official [ecCodes grib_copy documentation](https://confluence.ecmwf.int/spaces/ECC/pages/45747226/grib_copy) supports copying and grouping by metadata keys. A future regrouping run must verify original versus copied message SHA-256, message count, selection identities, and native decoded result checksums. This audit did not run that transformation.

HDF5 variable/level partitioning can reorganize real arrays into new physical datasets, but creating new files requires rebuilding valid dimension-scale references and verifying values. Copying datasets independently can leave invalid cross-object references. Renaming /T or copying it 128 times does not create 128 real variables. Hard-link aliases of one object also do not constitute D distinct physical arrays.

Replicating a file increases N and may increase K simultaneously. Modifying only a pathname creates no new scientific time. Such data must be labeled replicas, not additional public observations.

Changing D while retaining per-dataset geometry changes total file size. Two useful controls are (1) constant per-unit geometry/encoding, with increased total payload, and (2) constant total payload, with changed partitioning. Neither can claim all three of D, per-unit size, and total file size stayed constant.

## Recommended preparation

1. **Keep genuine-data validation:** GEOS 16-file time discovery and physical chunk pruning; ERA5 variable/level/time predicates. Preserve existing GRIB raw files and databases.
2. **Build an independent controlled gradient corpus:** begin with N=64/256/1,024, K_file=0/1/16 plus selected fractions, D_data=1/8/32/128, and external depth=1/4/8. Use one-factor gradients first rather than a large full Cartesian product. These are proposed settings, not completed workloads.
3. **Use actual physical units:** fully allocate HDF5 numeric arrays and GRIB message payloads. Keep deterministic seeds, dtype, geometry, filter/packing configuration, and per-unit size explicit. For HDF5, fixed-length variable labels and scalar time/pressure/region attributes can exercise existing attribute predicates without declaring synthetic data to be GEOS. Scientific coordinate queries require their own valid coordinate evidence.
4. **Control K separately:** place matching units in exactly K_file files and control matches per selected file. Use enough distractor metadata combinations to prevent every file matching the same predicate. Verify K_file/K_unit/K_fragment from native raw readers before ingestion.
5. **Reorganize external trees as independent variants:** keep the same payload and predicates across depths, with both semantically meaningful and weakly correlated directory arrangements. FS may exploit meaningful names. Fix leaf occupancy where practical and report total directories, path lengths and tree breadth.
6. **Extend genuine N only where needed:** OISST is an inexpensive public file-count control; additional GFS cycles/products supply richer GRIB inputs. Neither expansion independently varies D. Additional HDF5 formats such as HIRDLS are not required just to create these gradients and would introduce separate semantic-binding work.

Formal paired FS / IoTDB-FS variants must consume the same frozen source set. Do not replace an FS copy with symlinks/hard links or mounted result views and then interpret that as equivalent publication/storage work. Record derived-data construction and equivalent FS persistent-index construction separately from online queries.

## Runtime and capacity boundaries

The HDF5 discovery request defaults to max_sources=1,024 and max_datasets=1,024. Its validated maxima are 4,096 candidate sources, 65,536 result datasets, and 256 MiB catalog bytes. N=4,096 can be configured within the existing contract, but needs explicit candidate/result/catalog budgets and a capacity pilot. A request cap is not automatic pagination. Candidate scanning and authorization still precede local GEOS time pruning, so a small K alone does not mean only K manifests were examined.

The verified UTC directory requires the GEOS profile and /T evidence. Neither OISST nor synthetic data acquires this directory merely by changing filenames or attaching a fake Source attribute. Generalized coverage requires a separate verified profile/contract.

At inspection, /data_01 had approximately 3.73 TB free and /data_02 0.385 TB free (decimal). An average GEOS file is about 446 MB. Hypothetical 1,024-file and 4,096-file replicas would consume roughly 457 GB and 1.83 TB in raw copies alone. FS raw copies plus IoTDB publication, layouts, logs and experiment variants need substantially more. Compact controlled inputs should precede any large corpus expansion; all large variants should not be stored simultaneously.

## Reproduction and public source references

The three inspection scripts in this directory are the metadata probes used on the server. Execute them using:

```sh
env -u LD_LIBRARY_PATH -u PYTHONPATH HDF5_PLUGIN_PRELOAD=:: \
  /data_01/zzy/iotdb-fs-exp/IoTDB-FS-C/experiments/single_file/work/session-20261012/venv/bin/python \
  inspect_primary.py
```

Run inspect_era5_time.py and inspect_small_public.py with the same environment. They use the recorded server raw paths, read metadata only, and print JSON. The archived metadata.json consolidates their outputs; it is not a performance timing log.

Primary public references checked on 2026-10-13:

- [NASA GEOS-5.7.2 sample collection](https://gmao.gsfc.nasa.gov/gmaoftp/ops/GEOS572_Sample_Data/inst3_3d_asm_Np/): 16 time files in this collection.
- [NOAA OISST v2.1](https://www.ncei.noaa.gov/products/optimum-interpolation-sst): daily archive from 1981 to present; v2.1 netCDF4.
- [NOAA GFS data access](https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast): four cycles per day and multiple parameter sets.
- [ecCodes message copying](https://confluence.ecmwf.int/spaces/ECC/pages/45747226/grib_copy): metadata-selective copying and grouping.
- [GWOSC public strain-file API](https://gwosc.org/api/v2/strain-files?format=api): detector, GPS interval, and sample-rate filtering; not evidence that all archive releases match our current binding.
