# 5. SIGMOD DEMO
```bash
bash /data/iotdb-deploy/object/datanode1/iotdb/sbin/start-cli.sh -h 101.6.42.165 -sql_dialect table

bash /root/fs/IoTDB-FS-C/scripts/dev/start.sh
bash /root/fs/IoTDB-FS-C/scripts/dev/stop.sh

# Mount IoTDB-FS at the mount point
./build/bin/iotdb_fs /mnt/dfs

# Scenario 1 - Transparent Ingestion
# 1.1 Ingest two GRIB2 files into the mounted directory
cp /root/demo-data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
getfattr -d -m - /mnt/dfs/show.grib2
cp /root/demo-data/KWBCGFS_CMRA_100_2024031900_GLB_0_0_0P25.grib2 /mnt/dfs/show2.grib2
getfattr -d -m - /mnt/dfs/show2.grib2
# 1.2 Verify ingestion results in the latest-view namespace
ls -l /mnt/dfs

# Scenario 2 - In-situ Subset Access
# 2.1 Read the full file (POSIX-style access)
ls -l '/mnt/dfs/show.grib2'
cat '/mnt/dfs/show.grib2' | wc -c
# 2.2 Read a single GRIB message via an extended filename
ls -l '/mnt/dfs/show.grib2?function=read&level=500'
cat '/mnt/dfs/show.grib2?function=read&level=500' | wc -c

# Scenario 3 - Declarative Cross-file Analytics
./build/bin/iotdb_fs cross-file-client \
  --file-query=shortName=clwmr \
  --inner-query=level=500

# Scenario 4 - Time-Travel Management
cp /root/demo-data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
# 4.1 Get latest-version view
ls -l /mnt/dfs
# 4.2 Get full-history view
ls -la /mnt/dfs 

# Unmount IoTDB-FS from the mount point
fusermount -u /mnt/dfs
```