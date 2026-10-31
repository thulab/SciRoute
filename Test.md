# 1. 客户端连接

```bash
/root/spricoder/object/sbin/start-cli.sh -h 192.168.130.38 -sql_dialect table
bash /data/iotdb-deploy/object/datanode1/iotdb/sbin/start-cli.sh -h 101.6.42.165 -sql_dialect table
bash /data/iotdb-deploy/1c1d-open/datanode1/iotdb/sbin/start-cli.sh -sql_dialect table
# 编译
mvn clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests -Diotdb-tools-thrift.version=0.14.1.1-glibc223-SNAPSHOT
mvn clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests -Diotdb-tools-thrift.version=0.14.1.1-old-glibc-SNAPSHOT
```

# 2. IoTDB-FS 启动
```bash
./iotdb_fs /mnt/dfs
./iotdb_fs -o default_permissions -f -d /mnt/dfs
./build/bin/iotdb_fs -o default_permissions -f -d /mnt/dfs
fusermount -u /mnt/dfs
```

# 3. 正确性验证
```bash
# 集成测试环境
g++ -std=c++17 -O2 -o build/testWrite tests/integration/testWrite.cpp
g++ -std=c++17 -O2 -o build/testQuery tests/integration/testQuery.cpp
./build/testWrite
./build/testQuery
g++ -std=c++17 -O2 \
  tests/integration/write_grib.cpp src/formats/grib/gribUtils.cpp \
  -I/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/include \
  $(pkg-config --cflags eccodes) \
  -L/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
  -liotdb_session \
  $(pkg-config --libs eccodes) \
  -Wl,-rpath,/root/fs/client-cpp-2.0.5-SNAPSHOT-cpp-linux-x86_64/lib \
  -o build/testWriteGrib
./build/testWriteGrib /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./show.grib2
./build/testWriteGrib /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 ./show2.grib2

# 网格场写入
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./show.grib2
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 ./show2.grib2
cp /root/zhy/qxj-data/data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./show.grib2

# 整场读取（新语法：function 链式调用）
ll 'show.grib2?function=read&level=500&dtype=f64'
ll '.show_1767867559239.grib2?function=read&level=500&dtype=f64'
cat 'show.grib2?function=read&level=500&dtype=f64' | wc -c

# 读取原始 GRIB message（二进制）
ll 'show.grib2?function=read&type=binary&level=500'
ll '.show_1767867559239.grib2?function=read&type=binary&level=500'
cat 'show.grib2?function=read&type=binary&level=500' | wc -c

# 空间裁剪（read + crop）
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' | wc -c
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64' | wc -c

# 空间插值（read + interp；interp 默认 bilinear）
ll 'show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32'
cat 'show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32' | wc -c

# 空间裁剪 + 空间插值（read + crop + interp）
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' | wc -c


# 大文件 6G
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_AVO_100_2024031818_GLB_1_2_0P25.grib2 ./big1.grib2
cp /root/qxj-data/GRIB-Test-3/KWBCGFS_O3MR_100_2024031818_GLB_1_2_0P25.grib2 ./big1.grib2
cp /root/qxj-data/GRIB-Test/KWBCGFS_VVP_100_2024031800_GLB_1_2_0P25.grib2 ./big.grib2

unrar l GRIB测试数据-1.rar | awk 'NR>8 && NF>1 {print $NF, $1}' | numfmt --to=iec

```

# 4. 性能测试
```bash
g++ -std=c++17 tests/integration/write_test.cpp -o build/write_test
g++ -std=c++17 tests/integration/read_test.cpp -o build/read_test

./build/write_test --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 --dst=/mnt/dfs/concurrent1 --tasks=100 --concurrency=10
./build/write_test --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --dst=/mnt/dfs/concurrent1 --tasks=1 --concurrency=1
./build/write_test --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --dst=/mnt/dfs/concurrent2 --tasks=2 --concurrency=2
./build/write_test --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --dst=/mnt/dfs/concurrent3 --tasks=3 --concurrency=3
./build/write_test --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --dst=/mnt/dfs/concurrent4 --tasks=4 --concurrency=4

cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 /mnt/dfs

./build/read_test --src=/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --tasks=1 --concurrency=1
./build/read_test --src=/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --tasks=2 --concurrency=2
./build/read_test --src=/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --tasks=3 --concurrency=3

./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500&dtype=f64' --tasks=100 --concurrency=1
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500&dtype=f64' --tasks=200 --concurrency=2
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500&dtype=f64' --tasks=300 --concurrency=3
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500&dtype=f64' --tasks=500 --concurrency=5
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500&dtype=f64' --tasks=700 --concurrency=7
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500&dtype=f64' --tasks=1000 --concurrency=10

./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' --tasks=100 --concurrency=1
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' --tasks=200 --concurrency=2
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' --tasks=300 --concurrency=3
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' --tasks=500 --concurrency=5
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' --tasks=700 --concurrency=7
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' --tasks=1000 --concurrency=10

./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' --tasks=100 --concurrency=1
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' --tasks=200 --concurrency=2
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' --tasks=300 --concurrency=3
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' --tasks=500 --concurrency=5
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' --tasks=700 --concurrency=7
./build/read_test --src='/mnt/dfs/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' --tasks=1000 --concurrency=10

g++ -std=c++17 tests/integration/write_test_ftp.cpp -o build/write_test_ftp -leccodes
./build/write_test_ftp --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --ftp-url=ftp://127.0.0.1/upload --ftp-user=ftpuser --ftp-pass='!20000901' --tasks=1 --concurrency=1 --prefix=grib4
./build/write_test_ftp --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --ftp-url=ftp://127.0.0.1/upload --ftp-user=ftpuser --ftp-pass='!20000901' --tasks=2 --concurrency=2 --prefix=grib5
./build/write_test_ftp --src=/root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 --ftp-url=ftp://127.0.0.1/upload --ftp-user=ftpuser --ftp-pass='!20000901' --tasks=3 --concurrency=3 --prefix=grib6

g++ -std=c++17 tests/integration/read_test_ftp.cpp -o build/read_test_ftp -leccodes
./build/read_test_ftp --ftp-url=ftp://127.0.0.1/upload/grib63799920885356_0_0.grib2 --ftp-user=ftpuser --ftp-pass='!20000901' --tasks=10 --concurrency=1

cat > codex <<'EOF'
#!/usr/bin/env bash
export HTTPS_PROXY="http://127.0.0.1:7890"
export HTTP_PROXY="http://127.0.0.1:7890"
export NO_PROXY="127.0.0.1,localhost"
# ------------------------------------------------
HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec "$HERE/codex.real" "$@"
EOF

docker run -d \
  --name mysql57 \
  -e MYSQL_ROOT_PASSWORD='!Zhy20000901' \
  -p 3306:3306 \
  -v /srv/mysql57:/var/lib/mysql \
  -v /root/qxj-data:/var/lib/mysql-files/grib-src \
  --restart unless-stopped \
  mysql:5.7


mysql -h 127.0.0.1 -P 3306 -u root -p

g++ -std=c++17 tests/integration/write_test_mysql.cpp -o build/write_test_mysql -leccodes
SELECT path, OCTET_LENGTH(data) FROM `blob`;
g++ -std=c++17 tests/integration/read_test_mysql.cpp -o build/read_test_mysql -leccodes

./build/write_test_mysql \
  --src=/var/lib/mysql-files/grib-src/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' --db=concurrent1 \
  --inode-table=inode --blob-table=blob \
  --tasks=1 --concurrency=1 --prefix=bench_
./build/write_test_mysql \
  --src=/var/lib/mysql-files/grib-src/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' --db=concurrent2 \
  --inode-table=inode --blob-table=blob \
  --tasks=2 --concurrency=2 --prefix=bench_
./build/write_test_mysql \
  --src=/var/lib/mysql-files/grib-src/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' --db=concurrent3 \
  --inode-table=inode --blob-table=blob \
  --tasks=3 --concurrency=3 --prefix=bench_

./build/write_test_mysql \
  --src=/var/lib/mysql-files/grib-src/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' --db=concurrent1 \
  --inode-table=inode --blob-table=blob \
  --tasks=1 --concurrency=1 --prefix=bench_
./build/write_test_mysql \
  --src=/var/lib/mysql-files/grib-src/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' --db=concurrent2 \
  --inode-table=inode --blob-table=blob \
  --tasks=2 --concurrency=2 --prefix=bench_
./build/write_test_mysql \
  --src=/var/lib/mysql-files/grib-src/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' --db=concurrent3 \
  --inode-table=inode --blob-table=blob \
  --tasks=3 --concurrency=3 --prefix=bench_

./build/read_test_mysql \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' \
  --db=concurrent1 --blob-table=blob \
  --path-prefix=/perf/bench_1761302794967_0_0.grib2 \
  --tasks=1 --concurrency=1

./build/read_test_mysql \
  --host=127.0.0.1 --user=root --pass='!Zhy20000901' \
  --db=concurrent1 --blob-table=blob \
  --path-prefix=/perf/bench_1761318018728_0_0.grib2 \
  --tasks=1 --concurrency=1


cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./show.grib2

ll 'show.grib2?function=read&level=500&dtype=f64'
cat 'show.grib2?function=read&level=500&dtype=f64' | wc -c

ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' | wc -c
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64' | wc -c

ll 'show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32'
cat 'show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32' | wc -c
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' | wc -c


% 数据写入
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./show.grib2
% 第二次数据写入（另一个版本）
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 ./show.grib2
% 第三次数据写入（另一个文件）
cp /root/qxj-data/GRIB-Test-1/KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2 ./show.grib2

% 最新版本
ls 
% 全部版本
ls -a

% 读取局部数据
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' | wc -c
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64' | wc -c

% 使用命令行跨文件查询
select time, data from blob table1 join (select last(time) last_time, path from blob group by path where key1 = value1) table2 on table1.time = table2.b

select time, path, data from inode table1 join (select last(time) last_time, path logical_path from inode where shortName = 'clwmr' group by path) table2 on table1.time = table2.last_time
select time, data from inode table1 join (select last(time) last_time, path from inode group by path) table2 on table1.time = table2.last_time

bash /data/iotdb-deploy/object/datanode1/iotdb/sbin/start-cli.sh -h 101.6.42.165 -sql_dialect table

bash /root/fs/IoTDB-FS-C/scripts/dev/start.sh
bash /root/fs/IoTDB-FS-C/scripts/dev/stop.sh

# Mount IoTDB-FS at the mount point
./build/bin/iotdb_fs /mnt/dfs

# Scenario 1 - Transparent Ingestion
# 1.1 Ingest two GRIB2 files into the mounted directory
cp /root/demo-data/KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2 /mnt/dfs/show.grib2
getfattr -d -m - show.grib2
cp /root/demo-data/KWBCGFS_CMRA_100_2024031900_GLB_0_0_0P25.grib2 /mnt/dfs/show2.grib2
getfattr -d -m - show2.grib2
# 1.2 Verify ingestion results in the latest-view namespace
ls -l /mnt/dfs

# Scenario 2 - In-situ Subset Access
# 2.1 Read the full file (POSIX-style access)
ls -l '/mnt/dfs/show.grib2'
cat '/mnt/dfs/show.grib2' | wc -c
# 2.2 Read a single GRIB message via an extended filename
ls -l '/mnt/dfs/show.grib2?function=read&level=500'
cat '/mnt/dfs/show.grib2?function=read&level=500' | wc -c

# 整场读取
ll 'show.grib2?function=read&level=500&dtype=f64'
cat 'show.grib2?function=read&level=500&dtype=f64' | wc -c

# 读取原始 GRIB message（二进制）
ll 'show.grib2?function=read&type=binary&level=500'
cat 'show.grib2?function=read&type=binary&level=500' | wc -c

# 空间裁剪（read + crop）
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&dtype=f64' | wc -c
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=20&lon1=120&dtype=f64' | wc -c

# 空间插值（read + interp；interp 默认 bilinear）
ll 'show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32'
cat 'show.grib2?function=read&level=500?function=interp&dlon=0.25&dlat=0.25&dtype=f32' | wc -c

# 空间裁剪 + 空间插值（read + crop + interp）
ll 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64'
cat 'show.grib2?function=read&level=500?function=crop&lat0=30&lon0=100&lat1=10&lon1=140?function=interp&outNi=200&outNj=160&dtype=f64' | wc -c


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

# Orable DBFS
```bash
su - oracle
dbfs_client -o allow_root dbfs_user@//127.0.0.1:1521/XEPDB1 /mnt/oracle
fusermount -u /mnt/oracle
```