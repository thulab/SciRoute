"""M0 functional gates. Real files and independent processes; no performance claims."""
from contextlib import contextmanager
from itertools import product
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import sqlite3
import tempfile
import time
import unittest

import h5py
import numpy as np

from fs_index.maintenance import Reader, Writer
from fs_index.native import NativeError
from fs_index.tests.test_native import LIBRARY, TARGET, make_grib


@contextmanager
def sql_connection(path):
    connection=sqlite3.connect(path)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def child_prepare_failure(library,root):
    with Reader(library,root) as r:
        with sql_connection(Path(root)/"index.sqlite") as c:
            c.execute("ALTER TABLE units RENAME TO temporarily_unavailable")
        try:
            r.select("index",{"kind":"latest"},TARGET,["/hdf5"])
        except NativeError:
            pass
        else:
            raise AssertionError("prepare should fail")
        with sql_connection(Path(root)/"index.sqlite") as c:
            c.execute("ALTER TABLE temporarily_unavailable RENAME TO units")
        assert len(r.select("index",{"kind":"latest"},TARGET,["/hdf5"])["units"])==1


def hdf(path, version=1, *, level=500, size=4096, dtype=np.float32, finite=True):
    with h5py.File(path, "w") as f:
        values = np.arange(size, dtype=dtype) + version * 10000
        if not finite:
            values[0] = np.nan
        d = f.create_dataset("field", data=values)
        for k, v in {**TARGET, "level": level}.items():
            d.attrs[k] = np.bytes_(v) if isinstance(v, str) else np.int64(v)


def child_publish(library, root, request, point=None):
    if point:
        os.environ["FS_INDEX_TEST_KILL_AT"] = point
    with Writer(library, root) as writer:
        writer.publish(**request)


def child_sequence(library, root, requests, channel):
    with Writer(library, root) as writer:
        channel.send("ready")
        channel.recv()
        for request in requests:
            receipt = writer.publish(**request)
            channel.send(receipt["result"]["version"])
        channel.send("done")
    channel.close()


@unittest.skipUnless(LIBRARY, "FS_INDEX_NATIVE_LIBRARY required")
class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = self.root / "store"

    def writer(self, **kwargs):
        w = Writer(LIBRARY, self.store, **kwargs)
        self.addCleanup(w.close)
        return w

    def reader(self):
        r = Reader(LIBRARY, self.store)
        self.addCleanup(r.close)
        return r

    def request(self, fmt, version, *, logical=None, matching=True, op=None):
        path = self.root / (fmt + str(version) + ("-yes" if matching else "-no"))
        if fmt == "grib":
            make_grib(path, version, 0 if matching else 3)
        else:
            hdf(path, version, level=500 if matching else 700)
        return dict(input=path, logical_path=logical or "/" + fmt, format=fmt,
            version=version, ordinal=version, expected_version=version-1, op_id=op or fmt+str(version))

    def rows(self, reader, fmt, kind="latest", mode="index", ordinals=None):
        choice = dict(kind=kind)
        if ordinals is not None:
            choice["ordinals"] = ordinals
        return reader.select(mode, choice, TARGET, ["/"+fmt])

    def test_incremental_equivalence_history_and_old_locators_both_formats(self):
        w = self.writer(create=True)
        r = self.reader()  # A retained empty reader must discover newly added paths.
        for fmt in ("grib", "hdf5"):
            first = w.publish(**self.request(fmt, 1))
            old = self.rows(r, fmt)["units"][0]
            before = r.read(old)[0].copy()
            second = w.publish(**self.request(fmt, 2, matching=False))
            self.assertEqual(second["metrics"]["extracted_files"], 1)
            self.assertEqual(self.rows(r, fmt)["units"], [])
            exact = self.rows(r, fmt, "ordinal", ordinals=[1])["units"]
            np.testing.assert_array_equal(r.read(exact[0])[0], before)
            np.testing.assert_array_equal(r.read(old)[0], before)
            for selection in ({"kind":"latest"}, {"kind":"all"}, {"kind":"ordinal","ordinals":[1]}, {"kind":"between","ordinals":[1,2]}):
                self.assertEqual(r.select("scan",selection,TARGET,["/"+fmt]), r.select("index",selection,TARGET,["/"+fmt]))
            self.assertLess(first["result"]["source_id"], second["result"]["source_id"])
        w.close()
        with Reader(LIBRARY, self.store) as reopened:
            self.assertEqual(reopened.status()["generation"], 4)
            self.assertEqual(reopened.status()["refreshed_sources"], 4)
            np.testing.assert_array_equal(reopened.read(old)[0], before)

    def test_no_rebuild_or_old_source_extraction_and_query_plan(self):
        w = self.writer(create=True)
        first = w.publish(**self.request("hdf5",1))
        db = self.store/"index.sqlite"
        inode = db.stat().st_ino
        with sql_connection(db) as c:
            old = c.execute("SELECT * FROM units").fetchall()
        second = w.publish(**self.request("hdf5",2))
        self.assertEqual(inode,db.stat().st_ino)
        with sql_connection(db) as c:
            self.assertEqual(old,c.execute("SELECT * FROM units WHERE source=?",(first["result"]["source_id"],)).fetchall())
        self.assertEqual(second["metrics"]["extracted_files"],1)
        self.assertEqual(second["metrics"]["extraction_file_opens"],1)
        self.assertEqual(second["metrics"]["extracted_units"],1)
        r = self.reader()
        plan = r.explain({"kind":"latest"},TARGET,["/hdf5"])
        self.assertTrue(any("predicate_" in p["detail"] for p in plan["plan"]))
        self.rows(r,"hdf5")
        after = r.status()
        self.rows(r,"hdf5")
        self.assertEqual(r.status()["refreshed_sources"],after["refreshed_sources"])

    def test_idempotency_content_conflict_and_expected_version(self):
        w = self.writer(create=True)
        request = self.request("hdf5",1)
        first = w.publish(**request)
        again = w.publish(**request)
        self.assertTrue(again["replayed"])
        self.assertEqual(first["result"],again["result"])
        self.assertEqual(w.status()["generation"],1)
        for update in ({"logical_path":"/else"},{"expected_version":1},{"version":2}):
            with self.assertRaisesRegex(NativeError,"operation request conflict"):
                w.publish(**{**request,**update})
        hdf(request["input"],99)
        with self.assertRaisesRegex(NativeError,"operation request conflict"):
            w.publish(**request)
        second = self.request("hdf5",2)
        with self.assertRaisesRegex(NativeError,"expected version conflict"):
            w.publish(**{**second,"expected_version":0})
        with self.assertRaisesRegex(NativeError,"must increase"):
            w.publish(**{**second,"ordinal":1})
        self.assertEqual(w.status()["sources"],1)
        w.publish(**second)
        self.assertEqual(w.status()["generation"],2)

    def test_single_writer_lock_closed_handles_and_schema_rejection(self):
        w = self.writer(create=True)
        with self.assertRaisesRegex(NativeError,"another writer"):
            Writer(LIBRARY,self.store)
        with self.assertRaisesRegex(NativeError,"must be new"):
            Writer(LIBRARY,self.store,create=True)
        w.close()
        with self.assertRaisesRegex(NativeError,"closed"):
            w.status()
        with sql_connection(self.store/"index.sqlite") as c:
            c.execute("PRAGMA user_version=99")
        for cls in (Reader,Writer):
            with self.assertRaisesRegex(NativeError,"schema"):
                cls(LIBRARY,self.store)

    def test_source_only_native_path_uses_same_committed_versions(self):
        w = self.writer(create=True,indexed=False)
        r = self.reader()
        for version in (1,2):
            receipt = w.publish(**self.request("hdf5",version))
            self.assertEqual(receipt["metrics"]["extracted_files"],0)
            rows = self.rows(r,"hdf5",mode="scan")
            self.assertEqual(rows["units"][0]["version"],version)
            values,mask=r.read(rows["units"][0])
            np.testing.assert_array_equal(values,np.arange(4096,dtype=np.float32)+version*10000)
            self.assertTrue(mask.all())
        self.assertEqual(w.status()["units"],0)
        with self.assertRaisesRegex(NativeError,"no index"):
            self.rows(r,"hdf5")
        self.assertEqual(self.rows(r,"hdf5","ordinal",mode="scan",ordinals=[1])["units"][0]["version"],1)
        w.close()
        with self.assertRaisesRegex(NativeError,"mode mismatch"):
            Writer(LIBRARY,self.store,indexed=True)

    def test_admission_rejects_unsupported_hdf_units_without_partial_publish(self):
        w=self.writer(create=True)
        for i,options in enumerate((dict(size=16),dict(dtype=np.float64),dict(finite=False))):
            path=self.root/("bad"+str(i));hdf(path,**options)
            with self.assertRaises(NativeError):
                w.publish(path,logical_path="/bad",format="hdf5",version=1,ordinal=1,expected_version=0,op_id="bad"+str(i))
            self.assertEqual(w.status()["sources"],0)
            self.assertEqual(w.status()["generation"],0)
        w.publish(**self.request("hdf5",1))
        self.assertEqual(w.status()["orphan_files"],3)
        self.assertFalse(w.checkpoint()["busy"])

    def test_transaction_failure_rolls_back_and_retries_same_source(self):
        w=self.writer(create=True)
        with sql_connection(self.store/"index.sqlite") as c:
            c.execute("CREATE TRIGGER fail_unit BEFORE INSERT ON units BEGIN SELECT RAISE(ABORT,'injected SQL failure'); END")
        request=self.request("hdf5",1)
        with self.assertRaisesRegex(NativeError,"injected SQL failure"):
            w.publish(**request)
        self.assertEqual(w.status()["generation"],0)
        self.assertEqual(w.status()["sources"],0)
        self.assertEqual(w.status()["orphan_files"],1)
        with sql_connection(self.store/"index.sqlite") as c:
            c.execute("DROP TRIGGER fail_unit")
        w.publish(**request)
        self.assertEqual(w.status()["orphan_files"],0)
        self.assertEqual(w.status()["generation"],1)

    def test_external_hdf_storage_cannot_escape_immutable_source_binding(self):
        w=self.writer(create=True)
        source=self.root/"external-source";hdf(source)
        for kind in ("link","virtual","external"):
            path=self.root/kind
            with h5py.File(path,"w") as f:
                if kind=="link":
                    f["field"]=h5py.ExternalLink(str(source),"field")
                else:
                    if kind=="virtual":
                        layout=h5py.VirtualLayout(shape=(4096,),dtype=np.float32)
                        layout[:]=h5py.VirtualSource(str(source),"field",shape=(4096,))
                        d=f.create_virtual_dataset("field",layout)
                    else:
                        d=f.create_dataset("field",shape=(4096,),dtype=np.float32,
                            external=[(str(self.root/"external-raw"),0,h5py.h5f.UNLIMITED)])
                        d[:]=np.arange(4096,dtype=np.float32)
                    for key,value in TARGET.items():
                        d.attrs[key]=np.bytes_(value) if isinstance(value,str) else np.int64(value)
            with self.assertRaisesRegex(NativeError,"file-local"):
                w.publish(path,logical_path="/external",format="hdf5",version=1,ordinal=1,expected_version=0,op_id=kind)
            self.assertEqual(w.status()["generation"],0)

    def test_select_exception_releases_snapshot_and_locator_binding(self):
        w=self.writer(create=True)
        w.publish(**self.request("hdf5",1))
        r=self.reader();row=self.rows(r,"hdf5")["units"][0]
        for update in ({"source_id":0},{"store_id":"other"},{"generation":0},{"version":2}):
            with self.assertRaises(NativeError):r.read({**row,**update})
        p=Path(row["native_path"]);data=p.read_bytes();stamp=p.stat()
        p.chmod(0o644)
        p.write_bytes(data+b"damage")
        with self.assertRaises(NativeError):self.rows(r,"hdf5")
        self.assertFalse(w.checkpoint()["busy"])
        p.write_bytes(data);os.utime(p,ns=(stamp.st_atime_ns,stamp.st_mtime_ns));p.chmod(0o444)
        self.assertEqual(len(self.rows(r,"hdf5")["units"]),1)
        with self.assertRaises(NativeError):r.select("index",{"kind":"latest"},TARGET,["/unknown"])
        w.publish(**self.request("hdf5",2))
        self.assertEqual(self.rows(r,"hdf5")["generation"],2)
        checkpoint=w.checkpoint()
        self.assertFalse(checkpoint["busy"])
        self.assertEqual(checkpoint["wal_bytes"],0)

    def test_process_crash_recovery_at_all_publication_boundaries(self):
        ctx=mp.get_context("spawn")
        for fmt in ("hdf5","grib"):
            for point in ("file_sync","rename","directory_sync","before_commit","after_commit"):
                with self.subTest(format=fmt,point=point):
                    self.store=self.root/(fmt+"-"+point)
                    request=self.request(fmt,1)
                    with Writer(LIBRARY,self.store,create=True):pass
                    process=ctx.Process(target=child_publish,args=(LIBRARY,self.store,request,point))
                    process.start();process.join(20)
                    if process.is_alive():process.kill();process.join();self.fail("fault child timed out")
                    self.assertEqual(process.exitcode,-signal.SIGKILL)
                    with Reader(LIBRARY,self.store) as first_reader:
                        self.assertEqual(first_reader.status()["generation"],int(point=="after_commit"))
                        if point=="after_commit":
                            self.assertEqual(len(self.rows(first_reader,fmt)["units"]),1)
                    with Writer(LIBRARY,self.store) as recovered:
                        state=recovered.status();self.assertEqual(state["generation"],int(point=="after_commit"))
                        self.assertEqual(state["staging_files"],0)
                        result=recovered.publish(**request)
                        self.assertEqual(result["replayed"],point=="after_commit")
                        self.assertEqual(recovered.status()["generation"],1)
                        self.assertEqual(recovered.status()["sources"],1)
                        self.assertEqual(recovered.status()["orphan_files"],0)
                        self.assertFalse(recovered.checkpoint()["busy"])
                    with Reader(LIBRARY,self.store) as r:
                        row=self.rows(r,fmt)["units"][0]
                        np.testing.assert_array_equal(r.read(row)[0].ravel(),np.arange(4096,dtype=np.float32)+10000)

    def test_independent_writer_and_reader_overlap_without_mixed_versions(self):
        ctx=mp.get_context("spawn")
        for fmt,mode in product(("hdf5","grib"),("index","scan")):
            with self.subTest(format=fmt,mode=mode):
                self.store=self.root/(fmt+"-"+mode+"-overlap")
                with Writer(LIBRARY,self.store,create=True,indexed=(mode=="index")) as w:w.publish(**self.request(fmt,1))
                requests=[self.request(fmt,i) for i in range(2,8)]
                parent,child=ctx.Pipe()
                process=ctx.Process(target=child_sequence,args=(LIBRARY,self.store,requests,child))
                process.start();child.close()
                try:
                    self.assertTrue(parent.poll(15));self.assertEqual(parent.recv(),"ready")
                    with Reader(LIBRARY,self.store) as r:
                        old=self.rows(r,fmt,mode=mode)["units"][0];parent.send("go")
                        acknowledged=1;last=1;complete=False;queries=0;deadline=time.monotonic()+20
                        while not complete and time.monotonic()<deadline:
                            while parent.poll():
                                event=parent.recv()
                                if event=="done":complete=True;break
                                acknowledged=event
                            row=self.rows(r,fmt,mode=mode)["units"][0]
                            self.assertGreaterEqual(row["version"],acknowledged)
                            self.assertGreaterEqual(row["version"],last);last=row["version"]
                            values,mask=r.read(row)
                            np.testing.assert_array_equal(values.ravel(),np.arange(4096,dtype=np.float32)+last*10000)
                            self.assertTrue(mask.all());queries+=1
                            self.assertFalse(r.status()["space_snapshot_atomic"])
                        self.assertTrue(complete);self.assertGreater(queries,0)
                        self.assertEqual(self.rows(r,fmt,mode=mode)["units"][0]["version"],7)
                        np.testing.assert_array_equal(r.read(old)[0].ravel(),np.arange(4096,dtype=np.float32)+10000)
                    process.join(5);self.assertEqual(process.exitcode,0)
                finally:
                    if process.is_alive():process.kill();process.join()
                    parent.close()

    def test_prepare_failure_is_an_error_not_a_null_statement_crash(self):
        with Writer(LIBRARY,self.store,create=True) as w:
            w.publish(**self.request("hdf5",1))
        process=mp.get_context("spawn").Process(target=child_prepare_failure,args=(LIBRARY,self.store))
        process.start();process.join(15)
        if process.is_alive():process.kill();process.join();self.fail("prepare-failure child timed out")
        self.assertEqual(process.exitcode,0)
        with Writer(LIBRARY,self.store) as w:
            self.assertFalse(w.checkpoint()["busy"])

    def test_space_accounting_and_synchronization_receipt(self):
        w=self.writer(create=True)
        receipt=w.publish(**self.request("hdf5",1))
        metrics=receipt["metrics"]
        for name in ("copy_hash_ns","file_sync_ns","directory_sync_ns","extract_validate_ns","commit_ns","total_ns"):
            self.assertGreater(metrics[name],0)
        self.assertGreaterEqual(metrics["total_ns"],sum(metrics[k] for k in ("copy_hash_ns","file_sync_ns","directory_sync_ns","extract_validate_ns","commit_ns")))
        state=w.status()
        self.assertTrue(state["space_snapshot_atomic"])
        self.assertEqual(state["data_bytes"],metrics["copied_bytes"])
        self.assertGreater(state["database_bytes"],0)
        self.assertGreater(state["wal_bytes"],0)
        self.assertEqual(state["orphan_bytes"],0)
        self.assertFalse(w.checkpoint()["busy"])
        self.assertEqual(w.status()["wal_bytes"],0)
        self.assertGreater(w.status()["observed_wal_peak_bytes"],0)


if __name__ == "__main__":
    unittest.main()
