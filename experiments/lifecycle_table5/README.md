# Table 5: matched three-path lifecycle extension

This is a new 2026-10-31 campaign; the accepted 2026-10-26 records remain intact.
It reuses all five immutable inputs and their point-query oracles from
`section42_20261026.read_matrix.main_cases`: GRIB 22/4,598 messages and HDF5
`[t,1,728,1152]` with t=2/160/1024. The HDF5 path is **iotdb-indexed**, matching
the original lifecycle table, rather than the native-HDF5 collection profile.

The full matrix is 5 sources × 3 arms × Q=1/10/100 × 3 repetitions = 135 directly
timed lifecycles and 4,995 verified queries. The arms are FS-scan, FS-index and
IoTDB-FS. Arm and Q orders rotate by repetition. Every cell copies the original
bytes to a new destination. Content, exact source identity and owned numerical
outputs align; implementations retain their own publication/version machinery.

The outer clock runs continuously from copy entry through file fsync/close,
source binding, index extraction and FULL SQLite commit where applicable, to
the last owned query result. SHA/oracle validation and parent-directory sync
follow the clock, as in the original table. Service and empty catalog setup
precede it. Catalog setup is separately recorded once per repetition; the
table therefore measures incremental publication into initialized services,
not a cold application installation. Final checkpoint/database flush is outside
this original copy-to-result clock. No durability stronger than these concrete
acknowledgments is inferred.

Each arm block begins and ends with a flush of only the experiment database,
followed by a recorded quiescence check (three consecutive two-second windows,
DataNode CPU <=0.10 core and write_bytes <=1 MiB/window, timeout 180 seconds).
This time is reported outside copy-to-result latency. It limits immediate
carryover into the next arm; periodic resource samples also retain any
background activity that starts later.
Service/catalog initialization and post-clock directory synchronization are
also recorded separately. JVM limits, listening PID, release JAR hashes and
the filesystems of local copies, database data and WAL are read from live state.

FS-index uses `fs_index.lifecycle.Catalog`: all GRIB message headers and native
offsets, or the explicitly named HDF5 dataset descriptor, persisted with exact
source/version identity. Each appended source scans only that source. Each
query reads the SQLite catalog and opens/closes a fresh native file handle;
it caches neither encoded messages nor decoded values. All original native
readers and numerical result validation remain shared with FS-scan. This large
native-file profile is distinct from the fixed-grid v2 update harness; this
table tests new-file registration, while M1 tests repeated changed versions.

Qualification of a separate database deployment passed authentication but its
license rejected schema/write operations. That owned deployment was stopped;
no license was altered. The measured campaign instead uses a new database and
owner namespace on the existing licensed DataNode, retaining its 28/4 GiB
heap/direct limits. The original table used 15,232/2,176 MiB. All three paths are
therefore freshly measured in the new campaign; old values are not spliced in.
Client, native readers, FUSE and the DataNode share CPUs 2–3 and one two-core quota;
the query leaf is limited to 2 GiB, swap disabled. ConfigNode is a separate
control-plane service, as in the original setup. OS and application caches
remain enabled. The host's actual current memory and software identities are
recorded; no claim that the intervening host state is identical is made.

`database.py` preserves the failed isolated deployment and its safe lifecycle,
and reads the private shared connection binding without changing service config.
`supervise.py` keeps an external recovery process and restores
cgroups/affinities on success or failure. `run.py` retains raw clocks, outputs'
validation, source hashes, resource snapshots, execution profiles and source
closure. `summary.json` contains arithmetic means, medians, ranges and all
three observations. Paper values use the arithmetic mean of lifecycle walls.

Qualification runs are excluded. A failed attempt remains in its own directory;
changes require a new frozen runner snapshot and run identifier. Public archives
exclude private connection/configuration files and private service/client logs.
