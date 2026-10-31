#!/usr/bin/env python3
"""Build the locked CI dependencies, or verify a restored dependency prefix.

Only the Python standard library is needed. The SDK URL is deliberately supplied
by the repository configuration: its digest and installed files remain locked.
"""

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile


LOCK_PATH = Path(__file__).with_name("dependencies.lock.json")
MARKER_NAME = ".iotdbfs-dependencies.json"
MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024
HDF5_CMAKE_OPTIONS = [
    "-DBUILD_SHARED_LIBS=ON", "-DBUILD_STATIC_LIBS=OFF",
    "-DHDF5_ENABLE_THREADSAFE=ON", "-DHDF5_ENABLE_PARALLEL=OFF",
    "-DHDF5_BUILD_CPP_LIB=OFF", "-DHDF5_BUILD_FORTRAN=OFF", "-DHDF5_BUILD_JAVA=OFF",
    "-DHDF5_BUILD_HL_LIB=OFF", "-DHDF5_BUILD_TOOLS=ON",
    "-DHDF5_BUILD_EXAMPLES=OFF", "-DBUILD_TESTING=OFF",
    "-DHDF5_ENABLE_Z_LIB_SUPPORT=ON", "-DHDF5_ENABLE_SZIP_SUPPORT=OFF",
    "-DHDF5_ENABLE_PLUGIN_SUPPORT=OFF", "-DHDF5_ALLOW_EXTERNAL_SUPPORT=NO",
    "-DHDF5_ENABLE_ROS3_VFD=OFF", "-DHDF5_ENABLE_HDFS=OFF",
    "-DHDF5_ENABLE_SUBFILING_VFD=OFF", "-DHDF5_ENABLE_MIRROR_VFD=OFF",
    "-DHDF5_ENABLE_DIRECT_VFD=OFF", "-DDEFAULT_API_VERSION=v114",
    "-DHDF5_INSTALL_CMAKE_DIR=lib/cmake/hdf5",
    "-DHDF5_INSTALL_DATA_DIR=share/doc/hdf5",
]


class DependencyError(RuntimeError):
    """A dependency could not be authenticated, installed, or verified."""


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def check_digest(path, expected):
    if sha256_file(path) != expected:
        raise DependencyError(f"SHA-256 mismatch for {Path(path).name}")


def validate_url(url):
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.fragment):
        raise DependencyError("Dependency URLs must use HTTPS without credentials or fragments")
    return url


class HTTPSOnlyRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        validate_url(new_url)
        return super().redirect_request(request, response, code, message, headers, new_url)


def download_verified(url, destination, expected):
    """Never expose signed download URLs in logs, and reject HTTP redirects."""
    validate_url(url)
    destination = Path(destination)
    temporary = destination.with_suffix(destination.suffix + ".partial")
    opener = urllib.request.build_opener(HTTPSOnlyRedirect())
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "iotdbfs-ci-dependencies/1"})
        with opener.open(request, timeout=60) as response, temporary.open("wb") as stream:
            validate_url(response.geturl())
            size = 0
            for block in iter(lambda: response.read(1024 * 1024), b""):
                size += len(block)
                if size > MAX_DOWNLOAD_BYTES:
                    raise DependencyError("Dependency download exceeds the size limit")
                stream.write(block)
        check_digest(temporary, expected)
        temporary.replace(destination)
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        # URLError strings can contain a private or signed URL.
        raise DependencyError(f"Download failed for {destination.name} ({type(error).__name__})") from None
    finally:
        temporary.unlink(missing_ok=True)


def archive_path(name):
    """Archive paths are POSIX paths even when this script is imported elsewhere."""
    if not name or "\\" in name or "\x00" in name:
        raise DependencyError(f"Unsafe archive path: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise DependencyError(f"Unsafe archive path: {name!r}")
    return path


def internal_link_target(member_path, link_name, root_name):
    if not link_name or "\\" in link_name or "\x00" in link_name:
        raise DependencyError("Unsafe archive link")
    target = PurePosixPath(link_name)
    if target.is_absolute():
        raise DependencyError("Archive links must stay inside their source tree")
    parts = list(member_path.parent.parts)
    for part in target.parts:
        if part == "..":
            if len(parts) <= 1:
                raise DependencyError("Archive link escapes its source tree")
            parts.pop()
        elif part != ".":
            parts.append(part)
    if not parts or parts[0] != root_name:
        raise DependencyError("Archive link escapes its source tree")


def empty_destination(destination):
    destination = Path(destination)
    if destination.is_symlink() or (destination.exists() and any(destination.iterdir())):
        raise DependencyError(f"Extraction destination must be an empty directory: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    return destination.resolve()


def extract_source(archive, destination, root_name):
    """Extract regular files first, then verified internal symbolic links.

    ecCodes definitions and ecbuild use internal links. Delaying their creation
    prevents subsequent members from being written through symlink parents.
    Hard links and device entries are not present in the locked sources.
    """
    destination = empty_destination(destination)
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        seen = set()
        links = []
        for member in members:
            relative = archive_path(member.name)
            if relative.parts[0] != root_name or str(relative) in seen:
                raise DependencyError("Unexpected source root or duplicate archive member")
            seen.add(str(relative))
            if member.issym():
                internal_link_target(relative, member.linkname, root_name)
                links.append((relative, member.linkname))
            elif not (member.isdir() or member.isfile()):
                raise DependencyError("Unsupported source archive entry")
        for member in members:
            relative = archive_path(member.name)
            target = destination.joinpath(*relative.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as incoming, target.open("xb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                target.chmod(0o755 if member.mode & 0o111 else 0o644)
        for relative, link in links:
            target = destination.joinpath(*relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() or target.is_symlink():
                raise DependencyError("Archive link conflicts with another member")
            target.symlink_to(link)
        for relative, _ in links:
            link_path = destination.joinpath(*relative.parts)
            try:
                resolved = link_path.resolve()
            except RuntimeError:
                raise DependencyError("Archive contains a symbolic link cycle") from None
            if not resolved.is_relative_to(destination / root_name):
                raise DependencyError("Archive link resolves outside its source tree")
            # Python 3.13 may return a still-unresolved path for a symlink loop
            # when strict=False. Check the installed link graph strictly as well.
            try:
                link_path.resolve(strict=True)
            except RuntimeError:
                raise DependencyError("Archive contains a symbolic link cycle") from None
            except OSError as error:
                if error.errno == errno.ELOOP:
                    raise DependencyError("Archive contains a symbolic link cycle") from None
                raise DependencyError("Archive link target is missing or inaccessible") from None
    return destination / root_name


def extract_sdk(archive, destination):
    destination = empty_destination(destination)
    with zipfile.ZipFile(archive) as source:
        seen = set()
        for member in source.infolist():
            relative = archive_path(member.filename)
            mode = member.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            if (relative.parts[0] not in {"include", "lib"} or str(relative) in seen
                    or file_type not in {0, stat.S_IFDIR, stat.S_IFREG}
                    or (file_type == stat.S_IFDIR and not member.is_dir())):
                raise DependencyError("Unexpected SDK member, duplicate path, or symbolic link")
            seen.add(str(relative))
        for member in source.infolist():
            relative = archive_path(member.filename)
            target = destination.joinpath(*relative.parts)
            mode = member.external_attr >> 16
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.open(member) as incoming, target.open("xb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                target.chmod(0o755 if mode & 0o111 else 0o644)


def tree_manifest(root):
    """Hash files and record links without following directory symlinks."""
    root = Path(root).resolve()
    result = {}
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in sorted(directories + files):
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if relative == MARKER_NAME:
                continue
            if path.is_symlink():
                try:
                    resolved = path.resolve()
                except RuntimeError:
                    raise DependencyError(f"Dependency link cycle: {relative}") from None
                if not resolved.is_relative_to(root):
                    raise DependencyError(f"Dependency link escapes prefix: {relative}")
                result[relative] = {"link": os.readlink(path)}
            elif path.is_file():
                result[relative] = {"sha256": sha256_file(path)}
            elif not path.is_dir():
                raise DependencyError(f"Unsupported dependency file: {relative}")
    return result


def verify_sdk(prefix, lock):
    actual = tree_manifest(Path(prefix) / "iotdb")
    expected = {name: {"sha256": digest} for name, digest in lock["sdk"]["files"].items()}
    if actual != expected:
        changed = sorted(name for name in actual.keys() | expected.keys()
                         if actual.get(name) != expected.get(name))
        raise DependencyError("SDK files differ from dependencies.lock.json: " + ", ".join(changed[:4]))


def verify_versions(prefix, lock):
    prefix = Path(prefix)
    expected = lock["sources"]
    version_header = (prefix / "include/eccodes_version.h").read_text()
    if not re.search(r'#define\s+ECCODES_VERSION_STR\s+"' + re.escape(expected["eccodes"]["version"]) + '"', version_header):
        raise DependencyError("Installed ecCodes version differs from the lock")
    for package, relative in (("eccodes", "lib/pkgconfig/eccodes.pc"),
                              ("googletest", "lib/pkgconfig/gtest.pc")):
        text = (prefix / relative).read_text()
        if not re.search(r"^Version:\s*" + re.escape(expected[package]["version"]) + r"\s*$", text, re.MULTILINE):
            raise DependencyError(f"Installed {package} pkg-config version differs from the lock")
    for relative in ("lib/libeccodes.so", "lib/libgtest.a", "lib/libgmock.a",
                     "lib/cmake/GTest/GTestConfig.cmake", "share/eccodes/definitions/boot.def",
                     "share/eccodes/samples/GRIB1.tmpl", "share/eccodes/samples/GRIB2.tmpl"):
        if not (prefix / relative).is_file():
            raise DependencyError(f"Required dependency file is missing: {relative}")
    for relative in ("include/arrow/api.h", "lib/libarrow.so",
                     "lib/cmake/Arrow/ArrowConfig.cmake",
                     "lib/cmake/Arrow/ArrowOptions.cmake",
                     "lib/cmake/Arrow/ArrowTargets.cmake", "share/doc/arrow/LICENSE.txt",
                     "share/doc/arrow/NOTICE.txt"):
        if not (prefix / relative).is_file():
            raise DependencyError(f"Required Arrow file is missing: {relative}")
    arrow_config = (prefix / "lib/cmake/Arrow/ArrowConfig.cmake").read_text()
    if not re.search(r'^set\(ARROW_VERSION "' + re.escape(expected["arrow"]["version"])
                     + r'"\)', arrow_config, re.MULTILINE):
        raise DependencyError("Installed Arrow version differs from the lock")
    arrow_options = (prefix / "lib/cmake/Arrow/ArrowOptions.cmake").read_text()
    for option in ("ARROW_BUILD_SHARED", "ARROW_IPC", "ARROW_WITH_LZ4", "ARROW_WITH_ZSTD"):
        if not re.search(r'^set\(' + option + r' "ON"\)', arrow_options, re.MULTILINE):
            raise DependencyError(f"Installed Arrow is missing required option: {option}")

    verify_hdf5(prefix, lock)


def verify_hdf5(prefix, lock):
    """Check the installed serial C library profile, independently of the SDK."""
    prefix = Path(prefix)
    for relative in ("include/hdf5.h", "include/H5pubconf.h", "lib/libhdf5.so",
                     "lib/cmake/hdf5/hdf5-config.cmake", "share/doc/hdf5/COPYING",
                     "share/doc/hdf5/COPYING_LBNL_HDF5",
                     "bin/h5dump"):
        if not (prefix / relative).is_file():
            raise DependencyError(f"Required HDF5 file is missing: {relative}")
    config = (prefix / "lib/cmake/hdf5/hdf5-config.cmake").read_text()
    # The upstream installed config retains the literal ${HDF5_PACKAGE_NAME} prefix.
    def setting(name, value):
        pattern = (r"set\s*\(\s*\$\{HDF5_PACKAGE_NAME\}_" + name
                   + r"\s+\"?" + re.escape(value) + r"\"?\s*\)")
        if not re.search(pattern, config):
            raise DependencyError(f"Installed HDF5 has an unexpected {name}; expected {value}")
    setting("VERSION_STRING", lock["sources"]["hdf5"]["version"])
    setting("BUILD_SHARED_LIBS", "YES")
    setting("BUILD_STATIC_LIBS", "NO")
    for name in ("ENABLE_THREADSAFE", "ENABLE_Z_LIB_SUPPORT"):
        setting(name, "ON")
    for name in ("ENABLE_PARALLEL", "BUILD_CPP_LIB", "BUILD_FORTRAN",
                 "BUILD_JAVA", "BUILD_HL_LIB", "ENABLE_PLUGIN_SUPPORT", "ENABLE_SZIP_SUPPORT"):
        setting(name, "OFF")
    header = (prefix / "include/H5pubconf.h").read_text()
    for name in ("H5_HAVE_THREADSAFE", "H5_HAVE_FILTER_DEFLATE"):
        if not re.search(r"^#\s*define\s+" + name + r"\s+1\s*$", header, re.MULTILINE):
            raise DependencyError(f"Installed HDF5 header lacks {name}")


def install_hdf5(prefix, lock, jobs, source_archive=None, cc="gcc-11", cxx="g++-11",
                 verify_only=False):
    """Build just HDF5 into a separate prefix; never mutate an existing bundle."""
    prefix = Path(prefix)
    if prefix.is_symlink():
        raise DependencyError("Dependency prefix must not be a symbolic link")
    if (prefix / MARKER_NAME).is_file():
        marker = json.loads((prefix / MARKER_NAME).read_text())
        if marker.get("component") != "hdf5" or marker.get("lock_sha256") != lock_digest(lock):
            raise DependencyError("HDF5 prefix has a different component or lock")
        if marker.get("files") != tree_manifest(prefix):
            raise DependencyError("Installed HDF5 files differ from the installation manifest")
        verify_hdf5(prefix, lock)
        return
    if verify_only:
        raise DependencyError("HDF5 prefix has no completed installation marker")
    if prefix.exists() and any(prefix.iterdir()):
        raise DependencyError("Dependency prefix is nonempty or incomplete; choose a fresh --prefix")
    if platform.system() != "Linux":
        raise DependencyError("The H00 HDF5 dependency profile requires Linux")
    for executable in ("cmake", cc, cxx):
        if not shutil.which(executable):
            raise DependencyError(f"Missing build tool: {executable}")
    dependency = lock["sources"]["hdf5"]
    with tempfile.TemporaryDirectory(prefix="iotdbfs-hdf5-build-") as temporary:
        work = Path(temporary)
        if source_archive:
            archive = Path(source_archive).resolve()
            check_digest(archive, dependency["sha256"])
        else:
            archive = work / "hdf5.tar.gz"
            download_verified(dependency["url"], archive, dependency["sha256"])
        source = extract_source(archive, work / "source", dependency["archive_root"])
        prefix.mkdir(parents=True, exist_ok=True)
        run_cmake(source, work / "build", prefix, jobs, HDF5_CMAKE_OPTIONS, cc=cc, cxx=cxx)
        shutil.copy2(source / "COPYING_LBNL_HDF5", prefix / "share/doc/hdf5/COPYING_LBNL_HDF5")
        verify_hdf5(prefix, lock)
        manifest = {"component": "hdf5", "lock_sha256": lock_digest(lock),
                    "compiler": {"cc": cc, "cxx": cxx}, "files": tree_manifest(prefix)}
        (prefix / MARKER_NAME).write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    print(f"Installed and verified HDF5 prefix: {prefix}", flush=True)


def lock_digest(lock):
    return hashlib.sha256(json.dumps(lock, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def verify_prefix(prefix, lock):
    prefix = Path(prefix)
    marker_path = prefix / MARKER_NAME
    if not marker_path.is_file() or marker_path.is_symlink():
        raise DependencyError("Dependency prefix has no completed installation marker")
    marker = json.loads(marker_path.read_text())
    if marker.get("lock_sha256") != lock_digest(lock):
        raise DependencyError("Dependency prefix was built with a different lock; use a fresh prefix")
    # This check uses the checked-in lock, not an editable cache marker.
    verify_sdk(prefix, lock)
    if marker.get("files") != tree_manifest(prefix):
        raise DependencyError("Installed dependency files differ from the completed installation manifest")
    verify_versions(prefix, lock)


def run_cmake(source, build, prefix, jobs, options, *, cc="gcc-11", cxx="g++-11"):
    command = ["cmake", "-S", str(source), "-B", str(build),
               "-DCMAKE_BUILD_TYPE=Release", f"-DCMAKE_INSTALL_PREFIX={prefix}",
               "-DCMAKE_INSTALL_LIBDIR=lib", "-DINSTALL_LIB_DIR=lib",
               f"-DCMAKE_C_COMPILER={cc}", f"-DCMAKE_CXX_COMPILER={cxx}",
               "-DFETCHCONTENT_FULLY_DISCONNECTED=ON", f"-DCMAKE_PREFIX_PATH={prefix}"]
    subprocess.run(command + options, check=True)
    subprocess.run(["cmake", "--build", str(build), "--parallel", str(jobs)], check=True)
    subprocess.run(["cmake", "--install", str(build)], check=True)


def install(prefix, lock, sdk_url, sdk_archive, jobs):
    prefix = Path(prefix)
    if (prefix / MARKER_NAME).exists():
        verify_prefix(prefix, lock)
        print(f"Verified existing dependency prefix: {prefix}", flush=True)
        return
    if prefix.exists() and any(prefix.iterdir()):
        raise DependencyError("Dependency prefix is nonempty or incomplete; choose a fresh --prefix")
    if not sdk_archive and not sdk_url:
        raise DependencyError("IoTDB SDK is not available: set repository variable IOTDB_SDK_URL "
                              "to its HTTPS URL, or supply --sdk-archive for a local verified SDK")
    if sdk_url:
        validate_url(sdk_url)
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        raise DependencyError("The locked SDK requires Linux x86_64")
    for executable in ("cmake", "gcc-11", "g++-11"):
        if not shutil.which(executable):
            raise DependencyError(f"Missing build tool: {executable}")

    with tempfile.TemporaryDirectory(prefix="iotdbfs-ci-build-") as temporary:
        work = Path(temporary)
        if sdk_archive:
            archive = Path(sdk_archive).resolve()
            check_digest(archive, lock["sdk"]["sha256"])
        else:
            archive = work / "iotdb-sdk.zip"
            download_verified(sdk_url, archive, lock["sdk"]["sha256"])
        sources = {}
        # Authenticate every source before executing any build commands.
        for name, dependency in lock["sources"].items():
            print(f"Fetching locked {name} {dependency['version']}", flush=True)
            tarball = work / f"{name}.tar.gz"
            download_verified(dependency["url"], tarball, dependency["sha256"])
            sources[name] = extract_source(tarball, work / name, dependency["archive_root"])
        prefix.mkdir(parents=True, exist_ok=True)
        extract_sdk(archive, prefix / "iotdb")
        verify_sdk(prefix, lock)
        run_cmake(sources["ecbuild"], work / "build-ecbuild", prefix, jobs,
                  ["-DENABLE_TESTS=OFF"])
        run_cmake(sources["eccodes"], work / "build-eccodes", prefix, jobs,
                  [f"-Decbuild_DIR={prefix}/lib/cmake/ecbuild", "-DBUILD_SHARED_LIBS=ON",
                   "-DENABLE_FORTRAN=OFF", "-DENABLE_NETCDF=OFF", "-DENABLE_TESTS=OFF",
                   "-DENABLE_EXAMPLES=OFF", "-DENABLE_JPG=ON", "-DENABLE_AEC=ON",
                   "-DENABLE_INSTALL_ECCODES_DEFINITIONS=ON", "-DENABLE_INSTALL_ECCODES_SAMPLES=ON"])
        run_cmake(sources["googletest"], work / "build-googletest", prefix, jobs,
                  ["-DBUILD_SHARED_LIBS=OFF", "-DBUILD_GMOCK=ON", "-DINSTALL_GTEST=ON",
                   "-Dgtest_build_tests=OFF", "-Dgmock_build_tests=OFF"])
        run_cmake(sources["arrow"] / "cpp", work / "build-arrow", prefix, jobs,
                  ["-DARROW_BUILD_SHARED=ON", "-DARROW_BUILD_STATIC=OFF", "-DARROW_IPC=ON",
                   "-DARROW_WITH_LZ4=ON", "-DARROW_WITH_ZSTD=ON",
                   "-DARROW_DEPENDENCY_SOURCE=BUNDLED", "-DARROW_MIMALLOC=OFF",
                   "-DARROW_JEMALLOC=OFF", "-DARROW_WITH_BACKTRACE=OFF",
                   "-DARROW_SIMD_LEVEL=NONE", "-DARROW_RUNTIME_SIMD_LEVEL=NONE",
                   "-DARROW_BUILD_TESTS=OFF", "-DARROW_BUILD_BENCHMARKS=OFF",
                   "-DARROW_COMPUTE=OFF", "-DARROW_FILESYSTEM=OFF",
                   "-DARROW_WITH_UTF8PROC=OFF", "-DARROW_WITH_RE2=OFF",
                   "-DFETCHCONTENT_FULLY_DISCONNECTED=OFF"])
        run_cmake(sources["hdf5"], work / "build-hdf5", prefix, jobs, HDF5_CMAKE_OPTIONS)
        shutil.copy2(sources["hdf5"] / "COPYING_LBNL_HDF5", prefix / "share/doc/hdf5/COPYING_LBNL_HDF5")
        verify_versions(prefix, lock)
        manifest = {"lock_sha256": lock_digest(lock), "files": tree_manifest(prefix)}
        # A marker is written only after every build, install and verification succeeds.
        (prefix / MARKER_NAME).write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        verify_prefix(prefix, lock)
    print(f"Installed and verified dependency prefix: {prefix}", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    sdk = parser.add_mutually_exclusive_group()
    sdk.add_argument("--sdk-url", help="HTTPS SDK URL (defaults to IOTDB_SDK_URL)")
    sdk.add_argument("--sdk-archive", type=Path, help="local SDK archive with the locked SHA-256")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--verify-only", action="store_true", help="verify restored files against the lock and installation manifest")
    parser.add_argument("--only", choices=("hdf5",), help="install only HDF5 into an isolated prefix")
    parser.add_argument("--source-archive", type=Path, help="local locked HDF5 archive with --only hdf5")
    parser.add_argument("--cc", default="gcc-11", help="C compiler for isolated HDF5 build")
    parser.add_argument("--cxx", default="g++-11", help="C++ compiler for isolated HDF5 build")
    args = parser.parse_args(argv)
    try:
        if args.jobs < 1:
            raise DependencyError("--jobs must be positive")
        if args.prefix.is_symlink():
            raise DependencyError("Dependency prefix must not be a symbolic link")
        prefix = args.prefix.resolve()
        lock = json.loads(LOCK_PATH.read_text())
        if args.only:
            if args.sdk_url or args.sdk_archive:
                raise DependencyError("--only hdf5 does not accept SDK arguments")
            install_hdf5(prefix, lock, args.jobs, args.source_archive, args.cc, args.cxx,
                         args.verify_only)
        elif args.source_archive or args.cc != "gcc-11" or args.cxx != "g++-11":
            raise DependencyError("--source-archive/--cc/--cxx require --only hdf5")
        elif args.verify_only:
            verify_prefix(prefix, lock)
            print(f"Verified dependency prefix: {prefix}")
        else:
            sdk_url = args.sdk_url or (None if args.sdk_archive else os.environ.get("IOTDB_SDK_URL")) or lock["sdk"].get("url")
            install(prefix, lock, sdk_url, args.sdk_archive, args.jobs)
        return 0
    except (DependencyError, OSError, ValueError, tarfile.TarError, zipfile.BadZipFile,
            subprocess.CalledProcessError) as error:
        print(f"[dependencies] ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
