"""Section 4.2 F5 collection using current native and product interfaces.

This module starts no service and changes no resource or source state. The
supervisor supplies exact publication bindings, private mount contexts, and the
evidence sink. Every Q is a full independent session. Product daemons and their
caches persist across Q and cases within each campaign/method; an independent
session wall clock does not imply a cache reset. Historical fixed typed prepared
queries and independent encoded-cache toggles are not used.
"""
from __future__ import annotations

from pathlib import Path
from contextlib import nullcontext

try:
    from . import sessions
    from .read_matrix import main_cases, read_iotdb, validate_owned
    from .read_matrix_native import NativeHdf5, NativeGrib, need
except ImportError:
    import sessions
    from read_matrix import main_cases, read_iotdb, validate_owned
    from read_matrix_native import NativeHdf5, NativeGrib, need


METHODS = ("native FS", "iotdb-indexed", "native-hdf5")
BASE_CASES = ("G-F1-M4598", "H-F1-T160")
Q_VALUES = (1, 10, 100)
EXPECTED_SESSIONS = 63
EXPECTED_QUERIES = 2331


def collect_sessions(repo, bindings_by_campaign, mount_factory, emit_session):
    """Collect 63 independent sessions / 2,331 fresh owned query results.

    mount_factory(campaign, block_id, method) returns one resource-bound context
    per campaign/product method: six product mounts for the whole collection.
    The same daemon and caches remain across that method's cases and Q values;
    every Q nevertheless executes its own complete, directly timed new session.
    emit_session takes
    keyword arguments case, method, campaign, q, prepare_reuse, run, audit,
    context. It invokes run(deferred_audits), records the actual session clocks,
    then calls audit(record, payload) after the session closes and timing stops.
    Library discovery, constructors, profile harvesting and hash checks belong
    outside the session. No Q=100 prefix substitutes for Q=1 or Q=10.
    """
    repo = Path(repo)
    need(len(bindings_by_campaign) == 3, "session collection requires three campaigns")
    hdf5_library = repo/"experiments/single_file/work/session-20261012/hdf5-prefix/lib/libhdf5.so.310"
    grib_library = Path("/lib/x86_64-linux-gnu/libeccodes.so.0")
    session_count = query_count = mount_count = 0
    for campaign in range(3):
        registry = main_cases(repo, bindings_by_campaign[campaign])
        selected = {case["case_id"]: case for case in registry["cases"]
                    if case["case_id"] in BASE_CASES}
        need(set(selected) == set(BASE_CASES), "F5 base cases missing or ambiguous")
        method_order = METHODS[campaign:] + METHODS[:campaign]
        q_order = Q_VALUES[campaign:] + Q_VALUES[:campaign]
        reuse_order = (0, 1) if campaign % 2 == 0 else (1, 0)
        for method in method_order:
            if method == "native FS":
                manager = nullcontext(None)
            else:
                manager = mount_factory(campaign, f"session-all-{method}", method)
                mount_count += 1
            with manager as context:
                for base_id in BASE_CASES:
                    base = selected[base_id]
                    need(isinstance(base.get("logical_path"), str) and
                         type(base.get("version")) is int and base["version"] > 0,
                         "session exact publication binding missing")
                    if method == "native-hdf5" and base["format_id"] != "hdf5":
                        continue
                    for q in q_order:
                        prefix = "G" if base["format_id"] == "grib" else "H"
                        case = dict(base, case_id=f"{prefix}-F5-Q{q}", base_case_id=base_id)
                        audit = lambda row, owned, current=case: validate_owned(current, owned)
                        if method == "native FS":
                            for reuse in reuse_order:
                                # Native preparation objects/handles remain new
                                # for every actual independent Q session.
                                native = (NativeHdf5(hdf5_library) if case["format_id"] == "hdf5"
                                          else NativeGrib(grib_library))
                                emit_session(case=case, method=method, campaign=campaign, q=q,
                                    prepare_reuse=reuse,
                                    run=lambda deferred_audits, current=case, reader=native,
                                               count=q, mode=reuse:
                                        sessions.run_native_session(reader=reader, spec=current,
                                            q=count, prepare_reuse=mode,
                                            deferred_audits=deferred_audits),
                                    audit=audit, context=None)
                                session_count += 1
                                query_count += q
                        else:
                            emit_session(case=case, method=method, campaign=campaign, q=q,
                                prepare_reuse=0,
                                run=lambda deferred_audits, current=case, mounted=context, count=q:
                                    sessions.run_iotdb_session(context=mounted, spec=current,
                                        q=count, read_iotdb=read_iotdb,
                                        deferred_audits=deferred_audits),
                                audit=audit, context=context)
                            session_count += 1
                            query_count += q
    need(session_count == EXPECTED_SESSIONS and query_count == EXPECTED_QUERIES,
         "session collection omitted independent sessions or queries")
    need(mount_count == 6, "session collection must retain six campaign/method product daemons")
    return session_count
