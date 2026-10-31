"""Independent section 4.2 F6 copy/publication→query lifecycles.

No process or resource manipulation occurs here. The supervisor provides one
writable private owner context per campaign, with a separate namespace and
native_file_root. All original raw inputs are immutable. Every (source,Q,arm)
has a new destination, its own full copy and an independently executed Q session.
"""
from __future__ import annotations

from pathlib import Path
import hashlib
import re

try:
    from . import read_matrix as rm, sessions, writes
    from .read_matrix_native import NativeHdf5, NativeGrib, need
except ImportError:
    import read_matrix as rm
    import sessions
    import writes
    from read_matrix_native import NativeHdf5, NativeGrib, need


BASE_CASES = ("G-F1-M22", "G-F1-M4598", "H-F1-T2", "H-F1-T160", "H-F1-T1024")
Q_VALUES = (1, 10, 100)
EXPECTED_LIFECYCLES = 90
EXPECTED_QUERIES = 3330


def normalize_campaigns(campaigns):
    need(isinstance(campaigns, tuple) and bool(campaigns) and
         all(type(c) is int and 0 <= c <= 2 for c in campaigns) and
         len(set(campaigns)) == len(campaigns), "lifecycle campaigns must be distinct global indices 0/1/2")
    return campaigns


def campaigns_for_stage(stage):
    if stage in ("all", "lifecycle"):
        return (0, 1, 2)
    if stage in ("lifecycle-c0", "lifecycle-c1", "lifecycle-c2"):
        return (int(stage[-1]),)
    raise ValueError("stage does not declare a lifecycle campaign scope: "+str(stage))


def lifecycle_namespace(base_namespace, work, campaign):
    """Isolate every run without editing the sealed publication plan."""
    normalize_campaigns((campaign,))
    need(re.fullmatch(r"vd_[a-z0-9_]{1,48}", base_namespace) is not None,
         "invalid lifecycle namespace base")
    name = Path(work).name
    need(bool(name), "lifecycle work directory must identify the independent run")
    suffix = "_life%d_" % campaign + hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    result = base_namespace[:51-len(suffix)]+suffix
    need(re.fullmatch(r"vd_[a-z0-9_]{1,48}", result) is not None,
         "derived lifecycle namespace invalid")
    return result


def _native_locator(destination: Path, source: dict) -> dict:
    stat = destination.stat()
    need(stat.st_size == source["bytes"], "new filesystem copy size differs")
    return dict(logical_path=str(destination), exact_visible=True,
                format_id=source["format_id"], version=None,
                filesystem_identity=dict(device=stat.st_dev, inode=stat.st_ino,
                                         bytes=stat.st_size, mtime_ns=stat.st_mtime_ns),
                immutable_source_sha256=source["sha256"],
                scientific_version_selection="N/A for native filesystem copy")


def _product_locator(context: dict, destination: Path, case: dict) -> dict:
    mount = Path(context["mount"])
    logical = "/"+str(destination.relative_to(mount))
    # The declaration builder requires a positive Exact version. This local
    # placeholder is replaced by Latest before any request is sent; the actual
    # returned immutable version is then selected and confirmed explicitly.
    declaration = (rm.discovery_request(dict(case, logical_path=logical, version=1))
                   if case["format_id"] == "grib" else None)
    return writes.exact_visibility_locator(
        control=lambda request: rm.control(context["socket"], request),
        logical_path=logical, format_id=case["format_id"], format_request=declaration)


def collect_lifecycles(repo, work, publication_context_factory, emit_lifecycle,
                       *, campaigns=(0, 1, 2)):
    """Emit 30 independent lifecycles / 1,110 queries per declared campaign.

    The default retains all three campaigns (90 lifecycles / 3,330 queries).
    A single-campaign restoring run preserves its global method/Q rotation;
    campaign 1 or 2 is never relabelled campaign 0.

    publication_context_factory(campaign) yields one new writable private owner
    mount, configured iotdb-indexed, without borrowing query-stage namespaces.
    emit_lifecycle receives keyword arguments case, method, campaign, q,
    destination, source, locator, query_runner, query_audit, context. It must
    execute each plan immediately, while its supplied context is mounted.
    query_runner(exact_identity,deferred_audits) executes the whole new Q session.
    The sink may use run_lifecycle_plan to obtain directly measured clocks and
    pending owned results. It validates all pending results after timing stops.
    """
    campaigns = normalize_campaigns(campaigns)
    repo, work = Path(repo), Path(work)
    registry = rm.main_cases(repo)
    by_id = {case["case_id"]: case for case in registry["cases"]}
    need(all(case in by_id for case in BASE_CASES), "F6 point cases missing")
    native_root = work/"locallife"
    native_root.mkdir(exist_ok=False)
    hdf5_library = repo/"experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310"
    grib_library = Path("/lib/x86_64-linux-gnu/libeccodes.so.0")
    lifecycle_count = query_count = 0
    for campaign in campaigns:
        fs_root = native_root/f"c{campaign}"
        fs_root.mkdir()
        methods = ("native FS", "IoTDB-FS") if campaign % 2 == 0 else ("IoTDB-FS", "native FS")
        q_order = Q_VALUES[campaign:] + Q_VALUES[:campaign]
        for method in methods:
            if method == "native FS":
                context_manager = _unmounted()
            else:
                context_manager = publication_context_factory(campaign)
            with context_manager as context:
                if context is not None:
                    mount = Path(context["mount"])
                    root = mount/f"section42-lifecycle-c{campaign}"
                    need(not root.exists(), "private lifecycle logical prefix already exists")
                    root.mkdir()
                for base_id in BASE_CASES:
                    base = by_id[base_id]
                    source = registry["sources"][base["source_id"]]
                    ident = base["source_id"]
                    case = dict(base, case_id=ident.replace("G-", "G-F6-").replace("H-", "H-F6-"),
                                base_case_id=base_id)
                    native = None
                    if method == "native FS":
                        native = (NativeHdf5(hdf5_library) if case["format_id"] == "hdf5"
                                  else NativeGrib(grib_library))
                    for q in q_order:
                        name = f"{ident}-q{q}"+Path(source["raw"]).suffix
                        destination = (fs_root/name if context is None else root/name)
                        state = {}
                        if method == "native FS":
                            locator = lambda path, current=source: _native_locator(path, current)
                            def query_runner(identity, deferred_audits, current=case,
                                             reader=native, count=q, target=destination, holder=state):
                                actual = dict(current, raw=str(target), logical_path=str(target), version=None)
                                holder["case"] = actual
                                return sessions.run_native_session(reader=reader, spec=actual, q=count,
                                    prepare_reuse=0, deferred_audits=deferred_audits)
                        else:
                            locator = lambda path, mounted=context, current=case: _product_locator(mounted, path, current)
                            def query_runner(identity, deferred_audits, current=case,
                                             mounted=context, count=q, holder=state):
                                actual = dict(current, **identity)
                                holder["case"] = actual
                                return sessions.run_iotdb_session(context=mounted, spec=actual, q=count,
                                    read_iotdb=rm.read_iotdb, deferred_audits=deferred_audits)
                        def query_audit(row, owned, holder=state):
                            need("case" in holder, "lifecycle queries were not executed")
                            return rm.validate_owned(holder["case"], owned)
                        emit_lifecycle(case=case, method=method, campaign=campaign, q=q,
                            destination=destination, source=source, locator=locator,
                            query_runner=query_runner, query_audit=query_audit, context=context)
                        lifecycle_count += 1
                        query_count += q
    need(lifecycle_count == 30*len(campaigns) and query_count == 1110*len(campaigns),
         "F6 collection omitted lifecycles or independent queries")
    return lifecycle_count


class _unmounted:
    def __enter__(self):
        return None

    def __exit__(self, kind, value, traceback):
        return False


def run_lifecycle_plan(*, case, method, campaign, q, destination, source,
                       locator, query_runner, query_audit, context=None):
    """Execute one plan; return (serializable row, pending owned audit payloads).

    Root must call query_audit(record,owned) for every returned pair, after this
    helper returns. Full copied-file verification also occurs after the directly
    measured lifecycle stops. No old publication receipt enters any timing.
    """
    pending = []
    result = writes.run_lifecycle(source=source["raw"], destination=destination,
        expected_bytes=source["bytes"], expected_sha256=source["sha256"], locator=locator,
        run_queries=lambda identity: query_runner(identity, pending))
    need(result["session"]["q"] == q and len(pending) == q,
         "actual lifecycle session omitted queries")
    need(result["includes_query_callback_audit_overhead"] is False,
         "oracle work entered directly measured lifecycle")
    result.update(case_id=case["case_id"], base_case_id=case["base_case_id"],
                  source_id=case["source_id"], method=method, campaign=campaign, q=q,
                  prepare_reuse=0, audit_pending_count=len(pending))
    return result, pending
