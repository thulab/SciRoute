"""Persistent catalog contract tests; real native-format gates run in the harness."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from fs_index.lifecycle import Catalog


class HdfReader:
    def open_file(self, path):
        return Path(path)

    def close_file(self, handle):
        pass

    def describe(self, handle, dataset):
        return dict(dataset=dataset, source_shape=[1024,1,728,1152], dtype="<f4")

    def _verify_metadata(self, spec, metadata):
        if "source_shape" in spec and spec["source_shape"] != metadata["source_shape"]:
            raise ValueError("shape differs")

    def read(self, spec, metadata=None):
        if metadata is None:
            raise AssertionError("query rediscovered metadata")
        # A fresh result on each call; the persistent index stores only metadata.
        return dict(discovery_ns=1, total_ns=2), {"values": [7], "metadata": metadata}


class LifecycleCatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "catalog.sqlite"
        self.source = self.root / "source.h5"
        self.source.write_bytes(b"fixed immutable bytes")
        self.digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.spec = dict(raw=str(self.source), logical_path="/science/data", version=1, format_id="hdf5", dataset="/T")
        self.reader = HdfReader()

    def test_reopen_persisted_lookup_and_owned_queries(self):
        with Catalog(self.path, create=True) as catalog:
            receipt = catalog.register(self.spec, self.reader, sha256=self.digest)
            self.assertEqual(receipt["metrics"]["extracted_files"],1)
            self.assertEqual(receipt["catalog_units"],1)
            self.assertEqual(catalog.connection.execute("PRAGMA synchronous").fetchone()[0],2)
            self.assertGreater(catalog.stats()["index_bytes"],0)
        with Catalog(self.path) as catalog:
            row, one = catalog.read(self.reader,self.spec)
            _, two = catalog.read(self.reader,self.spec)
            self.assertIsNot(one["values"],two["values"])
            self.assertTrue(row["persistent_index"])
            self.assertFalse(row["handles_reused"])
            self.assertEqual(one["metadata"]["source_shape"],[1024,1,728,1152])
            self.assertEqual(catalog.checkpoint()["index_files_bytes"]["-wal"],0)

    def test_detect_replacement_and_missing_exact_version(self):
        with Catalog(self.path,create=True) as catalog:
            catalog.register(self.spec,self.reader,sha256=self.digest)
            with self.assertRaisesRegex(ValueError,"absent"):
                catalog.lookup(dict(self.spec,version=2))
            replacement=self.root/"replacement"
            replacement.write_bytes(self.source.read_bytes())
            replacement.replace(self.source)
            with self.assertRaisesRegex(ValueError,"changed"):
                catalog.lookup(self.spec)

    def test_new_version_appends_without_reextracting_old_source(self):
        with Catalog(self.path,create=True) as catalog:
            catalog.register(self.spec,self.reader,sha256=self.digest)
            second=self.root/"second.h5"
            second.write_bytes(b"new version bytes")
            new_spec=dict(self.spec,raw=str(second),version=2)
            with patch.object(self.reader,"describe",wraps=self.reader.describe) as describe:
                catalog.register(new_spec,self.reader,sha256=hashlib.sha256(second.read_bytes()).hexdigest(),version=2)
                self.assertEqual(describe.call_count,1)
            self.assertEqual(catalog.stats()["sources"],2)
            self.assertEqual(catalog.lookup(self.spec),catalog.lookup(new_spec))
            with self.assertRaises(sqlite3.IntegrityError):
                catalog.register(self.spec,self.reader,sha256=self.digest)
            self.assertEqual(catalog.stats()["sources"],2)

    def test_all_grib_predicates_query_persisted_inventory(self):
        rows=[dict(offset=0,length=9,shortName="t",typeOfLevel="surface",level=0,step=0),
              dict(offset=9,length=12,shortName="u",typeOfLevel="surface",level=0,step=3)]
        spec=dict(self.spec,format_id="grib",filter={"shortName":"t","step":0})
        with Catalog(self.path,create=True) as catalog, patch("fs_index.lifecycle._grib_inventory",return_value=rows):
            receipt=catalog.register(spec,object(),sha256=self.digest)
            self.assertEqual(receipt["catalog_units"],2)
        with Catalog(self.path) as catalog:
            second=catalog.lookup(dict(spec,filter={"shortName":"u","step":3}))
            self.assertEqual(second["selected"][0]["offset"],9)
            with self.assertRaisesRegex(ValueError,"one selected"):
                catalog.lookup(dict(spec,filter={"level":0}))
            with self.assertRaisesRegex(ValueError,"unsupported"):
                catalog.lookup(dict(spec,filter={"invented":1}))

    def test_extraction_failure_and_source_mutation_do_not_publish(self):
        with Catalog(self.path,create=True) as catalog:
            with self.assertRaisesRegex(ValueError,"shape"):
                catalog.register(dict(self.spec,source_shape=[1]),self.reader,sha256=self.digest)
            self.assertEqual(catalog.stats()["sources"],0)
            original=self.reader.describe
            def mutate(handle,dataset):
                self.source.write_bytes(b"changed during extraction")
                return original(handle,dataset)
            with patch.object(self.reader,"describe",side_effect=mutate):
                with self.assertRaisesRegex(ValueError,"changed"):
                    catalog.register(self.spec,self.reader,sha256=self.digest)
            self.assertEqual(catalog.stats()["sources"],0)

    def test_creation_does_not_overwrite_and_reopen_checks_profile(self):
        with Catalog(self.path,create=True):
            pass
        with self.assertRaises(FileExistsError):
            Catalog(self.path,create=True)
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE configuration SET protocol='other'")
        with self.assertRaisesRegex(ValueError,"profile"):
            Catalog(self.path)


if __name__ == "__main__":
    unittest.main()
