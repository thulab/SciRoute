#!/bin/bash
mkdir -p /mnt/dfs/2024031800
mkdir -p /mnt/dfs/2024031806
mkdir -p /mnt/dfs/2024031812
mkdir -p /mnt/dfs/forecast

cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800* /mnt/dfs/2024031800
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_105_2024031800* /mnt/dfs/2024031800
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031806* /mnt/dfs/2024031806
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_105_2024031806* /mnt/dfs/2024031806
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031812* /mnt/dfs/2024031812
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_105_2024031812* /mnt/dfs/2024031812

cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031818_GLB_0_0_0P25.grib2 /mnt/dfs/forecast/
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031818_GLB_0_0_0P25.grib2 /mnt/dfs/forecast/
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031818_GLB_0_0_0P25.grib2 /mnt/dfs/forecast/