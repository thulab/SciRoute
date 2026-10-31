"""Dependency integrity and extraction tests; no network or SDK is required."""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock
import urllib.error
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "install_dependencies.py"
SPEC = importlib.util.spec_from_file_location("install_dependencies", SCRIPT)
deps = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deps)


class DependencyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def make_tar(self, entries):
        path = self.root / "source.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for name, kind, content in entries:
                member = tarfile.TarInfo(name)
                if kind == "file":
                    data = content.encode()
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
                else:
                    member.type = {"symlink": tarfile.SYMTYPE, "hardlink": tarfile.LNKTYPE,
                                   "device": tarfile.CHRTYPE, "directory": tarfile.DIRTYPE}[kind]
                    member.linkname = content
                    archive.addfile(member)
        return path

    def make_zip(self, entries):
        path = self.root / "sdk.zip"
        with zipfile.ZipFile(path, "w") as archive:
            for name, contents, mode in entries:
                member = zipfile.ZipInfo(name)
                member.create_system = 3
                member.external_attr = mode << 16
                archive.writestr(member, contents)
        return path

    def fake_prefix(self):
        prefix = self.root / "installed"
        files = {
            "iotdb/include/Common.h": b"SDK types",
            "iotdb/lib/libiotdb_session.so": b"SDK library",
            "include/eccodes_version.h": b'#define ECCODES_VERSION_STR "2.44.0"\n',
            "lib/pkgconfig/eccodes.pc": b"Version: 2.44.0\n",
            "lib/pkgconfig/gtest.pc": b"Version: 1.14.0\n",
            "lib/libeccodes.so": b"ecCodes library",
            "lib/libgtest.a": b"GTest library",
            "lib/libgmock.a": b"GMock library",
            "lib/cmake/GTest/GTestConfig.cmake": b"GTest configuration",
            "include/hdf5.h": b"HDF5 C API",
            "include/H5pubconf.h": b"#ifndef H5_HAVE_THREADSAFE\n# define H5_HAVE_THREADSAFE 1\n#endif\n#define H5_HAVE_FILTER_DEFLATE 1\n",
            "lib/libhdf5.so": b"HDF5 shared library",
            "bin/h5dump": b"HDF5 tool",
            "share/doc/hdf5/COPYING": b"HDF5 license",
            "share/doc/hdf5/COPYING_LBNL_HDF5": b"LBNL HDF5 license",
            "lib/cmake/hdf5/hdf5-config.cmake": (
                b'set (${HDF5_PACKAGE_NAME}_VERSION_STRING 1.14.4)\n'
                b'set (${HDF5_PACKAGE_NAME}_BUILD_SHARED_LIBS YES)\n'
                b'set (${HDF5_PACKAGE_NAME}_BUILD_STATIC_LIBS NO)\n'
                + b''.join(f'set (${{HDF5_PACKAGE_NAME}}_{name} ON)\n'.encode()
                           for name in ("ENABLE_THREADSAFE", "ENABLE_Z_LIB_SUPPORT"))
                + b''.join(f'set (${{HDF5_PACKAGE_NAME}}_{name} OFF)\n'.encode()
                           for name in ("ENABLE_PARALLEL", "BUILD_CPP_LIB",
                                        "BUILD_FORTRAN", "BUILD_JAVA", "BUILD_HL_LIB",
                                        "ENABLE_PLUGIN_SUPPORT", "ENABLE_SZIP_SUPPORT"))),
            "include/arrow/api.h": b"Arrow API",
            "lib/libarrow.so": b"Arrow shared library",
            "lib/cmake/Arrow/ArrowConfig.cmake": b'set(ARROW_VERSION "21.0.0")\n',
            "lib/cmake/Arrow/ArrowOptions.cmake": (b'set(ARROW_BUILD_SHARED "ON")\n'
                                                   b'set(ARROW_IPC "ON")\n'
                                                   b'set(ARROW_WITH_LZ4 "ON")\n'
                                                   b'set(ARROW_WITH_ZSTD "ON")\n'),
            "lib/cmake/Arrow/ArrowTargets.cmake": b"Arrow imported target",
            "share/doc/arrow/LICENSE.txt": b"Apache-2.0",
            "share/doc/arrow/NOTICE.txt": b"Apache Arrow notice",
            "share/eccodes/definitions/boot.def": b"definitions",
            "share/eccodes/samples/GRIB1.tmpl": b"GRIB1",
            "share/eccodes/samples/GRIB2.tmpl": b"GRIB2",
        }
        for name, data in files.items():
            target = prefix / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        lock = {"sources": {"hdf5": {"version": "1.14.4"}, "arrow": {"version": "21.0.0"},
                            "eccodes": {"version": "2.44.0"},
                            "googletest": {"version": "1.14.0"}},
                "sdk": {"files": {name.removeprefix("iotdb/"): hashlib.sha256(data).hexdigest()
                                  for name, data in files.items() if name.startswith("iotdb/")}}}
        self.write_marker(prefix, lock)
        return prefix, lock

    def write_marker(self, prefix, lock):
        marker = {"lock_sha256": deps.lock_digest(lock), "files": deps.tree_manifest(prefix)}
        (prefix / deps.MARKER_NAME).write_text(json.dumps(marker))

    def test_sdk_extracts_regular_headers_and_libraries(self):
        archive = self.make_zip([("include/Common.h", "types", stat.S_IFREG | 0o644),
                                 ("lib/libiotdb_session.so", "library", stat.S_IFREG | 0o755)])
        target = self.root / "sdk"
        deps.extract_sdk(archive, target)
        self.assertEqual((target / "include/Common.h").read_text(), "types")
        self.assertEqual((target / "include/Common.h").stat().st_mode & 0o777, 0o644)
        self.assertEqual((target / "lib/libiotdb_session.so").stat().st_mode & 0o777, 0o755)

    def test_sdk_rejects_traversal_absolute_paths_and_unexpected_roots(self):
        for path in ("../outside", "/tmp/outside", "include/../../outside", "bin/tool", "include\\outside"):
            with self.subTest(path=path):
                archive = self.make_zip([(path, "bad", stat.S_IFREG | 0o644)])
                with self.assertRaises(deps.DependencyError):
                    deps.extract_sdk(archive, self.root / "sdk")
                self.assertFalse((self.root / "outside").exists())

    def test_sdk_rejects_symbolic_links_before_writing_files(self):
        archive = self.make_zip([("include/Common.h", "good", stat.S_IFREG | 0o644),
                                 ("lib/escape", "../../outside", stat.S_IFLNK | 0o777)])
        with self.assertRaises(deps.DependencyError):
            deps.extract_sdk(archive, self.root / "sdk")
        self.assertFalse((self.root / "sdk/include/Common.h").exists())

    def test_source_extracts_internal_directory_and_file_links(self):
        archive = self.make_tar([("source/definitions/table", "file", "data"),
                                 ("source/definitions/alias", "symlink", "table"),
                                 ("source/share/tables", "symlink", "../definitions")])
        extracted = deps.extract_source(archive, self.root / "source", "source")
        self.assertEqual((extracted / "share/tables/alias").read_text(), "data")

    def test_source_rejects_traversal_and_wrong_root(self):
        for name in ("source/../../outside", "/source/file", "other/file"):
            with self.subTest(name=name):
                archive = self.make_tar([(name, "file", "bad")])
                with self.assertRaises(deps.DependencyError):
                    deps.extract_source(archive, self.root / "source", "source")

    def test_source_rejects_links_outside_source_tree(self):
        for target in ("/tmp/outside", "../../outside", "../other"):
            with self.subTest(target=target):
                archive = self.make_tar([("source/link", "symlink", target)])
                with self.assertRaises(deps.DependencyError):
                    deps.extract_source(archive, self.root / "source", "source")

    def test_source_rejects_link_chain_escape(self):
        archive = self.make_tar([("source/base/link", "symlink", "../"),
                                 ("source/escape", "symlink", "base/link/../outside")])
        with self.assertRaisesRegex(deps.DependencyError, "outside"):
            deps.extract_source(archive, self.root / "source", "source")

    def test_source_rejects_link_cycles(self):
        archive = self.make_tar([("source/a", "symlink", "b"),
                                 ("source/b", "symlink", "a")])
        with self.assertRaisesRegex(deps.DependencyError, "cycle"):
            deps.extract_source(archive, self.root / "source", "source")

    def test_source_cannot_overwrite_file_through_symlink_parent(self):
        archive = self.make_tar([("source/directory", "symlink", "other"),
                                 ("source/directory/child", "file", "bad")])
        with self.assertRaisesRegex(deps.DependencyError, "conflicts"):
            deps.extract_source(archive, self.root / "source", "source")

    def test_source_rejects_hardlinks_devices_and_duplicate_members(self):
        entries = [[("source/file", "hardlink", "source/other")],
                   [("source/device", "device", "")],
                   [("source/file", "file", "one"), ("source/file", "file", "two")]]
        for members in entries:
            with self.subTest(members=members):
                archive = self.make_tar(members)
                with self.assertRaises(deps.DependencyError):
                    deps.extract_source(archive, self.root / "source", "source")

    def test_extraction_rejects_existing_content_and_symlink_destinations(self):
        archive = self.make_zip([("include/Common.h", "bad", stat.S_IFREG | 0o644)])
        target = self.root / "existing"
        target.mkdir()
        (target / "keep").write_text("user data")
        with self.assertRaises(deps.DependencyError):
            deps.extract_sdk(archive, target)
        self.assertEqual((target / "keep").read_text(), "user data")
        link = self.root / "link"
        link.symlink_to(target)
        with self.assertRaises(deps.DependencyError):
            deps.extract_sdk(archive, link)

    def test_only_https_urls_without_userinfo_are_allowed(self):
        for url in ("http://example.com/sdk.zip", "file:///tmp/sdk.zip", "https:///sdk.zip",
                    "https://user:password@example.com/sdk.zip", "https://example.com/sdk.zip#fragment"):
            with self.subTest(url=url), self.assertRaises(deps.DependencyError):
                deps.validate_url(url)
        self.assertEqual(deps.validate_url("https://example.com/sdk.zip?signature=secret"),
                         "https://example.com/sdk.zip?signature=secret")

    def test_http_redirect_is_rejected(self):
        request = deps.urllib.request.Request("https://example.com/sdk.zip")
        with self.assertRaises(deps.DependencyError):
            deps.HTTPSOnlyRedirect().redirect_request(request, None, 302, "Found", {},
                                                       "http://example.com/sdk.zip")

    def test_corrupt_download_never_replaces_existing_verified_file(self):
        target = self.root / "sdk.zip"
        target.write_bytes(b"keep")
        response = io.BytesIO(b"corrupt")
        response.geturl = lambda: "https://example.com/sdk.zip"
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(deps.urllib.request, "build_opener", return_value=opener):
            with self.assertRaisesRegex(deps.DependencyError, "SHA-256"):
                deps.download_verified("https://example.com/sdk.zip", target, "0" * 64)
        self.assertEqual(target.read_bytes(), b"keep")
        self.assertFalse((self.root / "sdk.zip.partial").exists())

    def test_valid_download_is_authenticated_and_atomically_installed(self):
        data = b"locked source archive"
        response = io.BytesIO(data)
        response.geturl = lambda: "https://example.com/sdk.zip"
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(deps.urllib.request, "build_opener", return_value=opener):
            deps.download_verified("https://example.com/sdk.zip", self.root / "sdk.zip",
                                   hashlib.sha256(data).hexdigest())
        self.assertEqual((self.root / "sdk.zip").read_bytes(), data)

    def test_download_errors_do_not_expose_signed_urls(self):
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.URLError("https://example.com/?signature=secret")
        with mock.patch.object(deps.urllib.request, "build_opener", return_value=opener):
            with self.assertRaises(deps.DependencyError) as failure:
                deps.download_verified("https://example.com/?signature=secret", self.root / "sdk.zip", "0" * 64)
        self.assertNotIn("secret", str(failure.exception))

    def test_restored_prefix_verifies_without_network_and_is_idempotent(self):
        prefix, lock = self.fake_prefix()
        deps.verify_prefix(prefix, lock)
        with mock.patch.object(deps, "download_verified") as download:
            deps.install(prefix, lock, None, None, 4)
        download.assert_not_called()

    def test_sdk_tampering_is_rejected_even_with_a_forged_cache_manifest(self):
        prefix, lock = self.fake_prefix()
        (prefix / "iotdb/lib/libiotdb_session.so").write_bytes(b"tampered")
        self.write_marker(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "SDK files differ"):
            deps.verify_prefix(prefix, lock)

    def test_sdk_extra_library_and_missing_header_are_rejected(self):
        prefix, lock = self.fake_prefix()
        extra = prefix / "iotdb/lib/libunexpected.so"
        extra.write_text("unexpected")
        with self.assertRaisesRegex(deps.DependencyError, "SDK files differ"):
            deps.verify_prefix(prefix, lock)
        extra.unlink()
        (prefix / "iotdb/include/Common.h").unlink()
        with self.assertRaisesRegex(deps.DependencyError, "SDK files differ"):
            deps.verify_prefix(prefix, lock)

    def test_non_sdk_corruption_and_external_symlinks_are_rejected(self):
        prefix, lock = self.fake_prefix()
        (prefix / "lib/libeccodes.so").write_text("tampered")
        with self.assertRaisesRegex(deps.DependencyError, "manifest"):
            deps.verify_prefix(prefix, lock)
        (prefix / "outside").symlink_to(self.root / "other")
        with self.assertRaisesRegex(deps.DependencyError, "escapes"):
            deps.verify_prefix(prefix, lock)

    def test_changed_lock_and_incomplete_prefix_are_rejected(self):
        prefix, lock = self.fake_prefix()
        lock["compiler"] = "different compiler"
        with self.assertRaisesRegex(deps.DependencyError, "different lock"):
            deps.verify_prefix(prefix, lock)
        (prefix / deps.MARKER_NAME).unlink()
        with self.assertRaisesRegex(deps.DependencyError, "marker"):
            deps.verify_prefix(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "nonempty or incomplete"):
            deps.install(prefix, lock, None, None, 4)

    def test_installed_version_and_runtime_data_are_checked(self):
        prefix, lock = self.fake_prefix()
        header = prefix / "include/eccodes_version.h"
        original = header.read_text()
        header.write_text('#define ECCODES_VERSION_STR "2.25.0"')
        self.write_marker(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "ecCodes version"):
            deps.verify_prefix(prefix, lock)
        header.write_text(original)
        (prefix / "share/eccodes/samples/GRIB2.tmpl").unlink()
        self.write_marker(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "Required dependency file"):
            deps.verify_prefix(prefix, lock)

    def test_arrow_version_codec_options_and_notice_are_checked(self):
        prefix, lock = self.fake_prefix()
        options = prefix / "lib/cmake/Arrow/ArrowOptions.cmake"
        options.write_text(options.read_text().replace('ARROW_WITH_ZSTD "ON"',
                                                       'ARROW_WITH_ZSTD "OFF"'))
        self.write_marker(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "ARROW_WITH_ZSTD"):
            deps.verify_prefix(prefix, lock)
        options.write_text(options.read_text().replace('ARROW_WITH_ZSTD "OFF"',
                                                       'ARROW_WITH_ZSTD "ON"'))
        (prefix / "share/doc/arrow/NOTICE.txt").unlink()
        self.write_marker(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "Required Arrow file"):
            deps.verify_prefix(prefix, lock)


    def test_hdf5_version_threading_filters_and_license_are_checked(self):
        prefix, lock = self.fake_prefix()
        config = prefix / "lib/cmake/hdf5/hdf5-config.cmake"
        original = config.read_text()
        for old, new, message in (("VERSION_STRING 1.14.4", "VERSION_STRING 1.14.5", "VERSION_STRING"),
                                  ("ENABLE_THREADSAFE ON", "ENABLE_THREADSAFE OFF", "ENABLE_THREADSAFE"),
                                  ("ENABLE_PARALLEL OFF", "ENABLE_PARALLEL ON", "ENABLE_PARALLEL"),
                                  ("ENABLE_Z_LIB_SUPPORT ON", "ENABLE_Z_LIB_SUPPORT OFF", "ENABLE_Z_LIB_SUPPORT")):
            with self.subTest(message=message):
                config.write_text(original.replace(old, new))
                self.write_marker(prefix, lock)
                with self.assertRaisesRegex(deps.DependencyError, message):
                    deps.verify_prefix(prefix, lock)
        config.write_text(original)
        (prefix / "share/doc/hdf5/COPYING").unlink()
        with self.assertRaisesRegex(deps.DependencyError, "Required HDF5 file"):
            deps.verify_hdf5(prefix, lock)

    def test_hdf5_header_must_actually_enable_threadsafe_and_deflate(self):
        prefix, lock = self.fake_prefix()
        (prefix / "include/H5pubconf.h").write_text("/* #undef H5_HAVE_THREADSAFE */\n")
        with self.assertRaisesRegex(deps.DependencyError, "H5_HAVE_THREADSAFE"):
            deps.verify_hdf5(prefix, lock)

    def test_hdf5_only_cli_does_not_use_sdk_or_replace_existing_prefix(self):
        with mock.patch.object(deps, "install_hdf5") as install, mock.patch.object(deps, "install") as full:
            self.assertEqual(deps.main(["--prefix", str(self.root / "hdf5"), "--only", "hdf5",
                                        "--cc", "gcc", "--cxx", "g++", "--verify-only"]), 0)
        self.assertEqual(install.call_args.args[4:7], ("gcc", "g++", True))
        full.assert_not_called()
        prefix = self.root / "occupied"
        prefix.mkdir()
        (prefix / "keep").write_text("data")
        with self.assertRaisesRegex(deps.DependencyError, "nonempty"):
            deps.install_hdf5(prefix, {}, 4)
        self.assertEqual((prefix / "keep").read_text(), "data")

    def test_hdf5_only_authenticates_archive_before_build(self):
        archive = self.root / "hdf5.tar.gz"
        archive.write_bytes(b"wrong archive")
        lock = {"sources": {"hdf5": {"sha256": "0" * 64}}}
        with mock.patch.object(deps.platform, "system", return_value="Linux"), \
                mock.patch.object(deps.shutil, "which", return_value="/usr/bin/tool"), \
                mock.patch.object(deps, "run_cmake") as build:
            with self.assertRaisesRegex(deps.DependencyError, "SHA-256"):
                deps.install_hdf5(self.root / "hdf5", lock, 4, archive)
        build.assert_not_called()
        self.assertFalse((self.root / "hdf5" / deps.MARKER_NAME).exists())

    def test_hdf5_old_version_is_rejected_after_downgrade(self):
        prefix, lock = self.fake_prefix()
        config = prefix / "lib/cmake/hdf5/hdf5-config.cmake"
        config.write_text(config.read_text().replace("VERSION_STRING 1.14.4", "VERSION_STRING 1.14.6"))
        self.write_marker(prefix, lock)
        with self.assertRaisesRegex(deps.DependencyError, "VERSION_STRING; expected 1.14.4"):
            deps.verify_prefix(prefix, lock)

    def test_missing_sdk_fails_before_download_or_build(self):
        with mock.patch.object(deps, "download_verified") as download:
            with self.assertRaisesRegex(deps.DependencyError, "IOTDB_SDK_URL"):
                deps.install(self.root / "prefix", {}, None, None, 4)
        download.assert_not_called()
        self.assertFalse((self.root / "prefix").exists())

    def test_local_sdk_digest_is_checked_before_source_download(self):
        archive = self.root / "sdk.zip"
        archive.write_bytes(b"wrong SDK")
        with mock.patch.object(deps.platform, "system", return_value="Linux"), \
                mock.patch.object(deps.platform, "machine", return_value="x86_64"), \
                mock.patch.object(deps.shutil, "which", return_value="/usr/bin/mock"), \
                mock.patch.object(deps, "download_verified") as download:
            with self.assertRaisesRegex(deps.DependencyError, "SHA-256"):
                deps.install(self.root / "prefix", {"sdk": {"sha256": "0" * 64}}, None, archive, 4)
        download.assert_not_called()

    def test_failed_build_does_not_write_completion_marker(self):
        prefix = self.root / "prefix"
        archive = self.root / "sdk.zip"
        archive.write_bytes(b"SDK")
        lock = {"sdk": {"sha256": hashlib.sha256(b"SDK").hexdigest()},
                "sources": {name: {"version": "1", "url": "https://example.com/source.tar.gz",
                                   "sha256": "0" * 64, "archive_root": name}
                            for name in ("ecbuild", "eccodes", "googletest")}}
        with mock.patch.object(deps.platform, "system", return_value="Linux"), \
                mock.patch.object(deps.platform, "machine", return_value="x86_64"), \
                mock.patch.object(deps.shutil, "which", return_value="/usr/bin/mock"), \
                mock.patch.object(deps, "download_verified"), \
                mock.patch.object(deps, "extract_source", return_value=self.root), \
                mock.patch.object(deps, "extract_sdk"), mock.patch.object(deps, "verify_sdk"), \
                mock.patch.object(deps, "run_cmake", side_effect=subprocess.CalledProcessError(2, "cmake")):
            with self.assertRaises(subprocess.CalledProcessError):
                deps.install(prefix, lock, None, archive, 4)
        self.assertFalse((prefix / deps.MARKER_NAME).exists())

    def test_cli_verify_only_returns_nonzero_for_missing_install(self):
        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            result = deps.main(["--prefix", str(self.root / "missing"), "--verify-only"])
        self.assertEqual(result, 1)
        self.assertIn("marker", stderr.getvalue())

    def test_cli_reads_sdk_url_from_environment(self):
        with mock.patch.dict(os.environ, {"IOTDB_SDK_URL": "https://example.com/sdk.zip"}), \
                mock.patch.object(deps, "install") as install:
            self.assertEqual(deps.main(["--prefix", str(self.root / "prefix")]), 0)
        self.assertEqual(install.call_args.args[2], "https://example.com/sdk.zip")

    def test_cli_local_archive_does_not_use_environment_url(self):
        with mock.patch.dict(os.environ, {"IOTDB_SDK_URL": "https://example.com/sdk.zip"}), \
                mock.patch.object(deps, "install") as install:
            self.assertEqual(deps.main(["--prefix", str(self.root / "prefix"),
                                        "--sdk-archive", str(self.root / "sdk.zip")]), 0)
        self.assertIsNone(install.call_args.args[2])


if __name__ == "__main__":
    unittest.main()
