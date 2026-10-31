import unittest

import sessions


class SessionTests(unittest.TestCase):
    def test_independent_q_counts_and_release(self):
        for reuse in (0, 1):
            for q in sessions.Q_VALUES:
                calls = {"open": 0, "close": 0, "read": 0, "audit": 0}
                def prepare():
                    calls["open"] += 1
                    return calls["open"]
                def read(state, index):
                    calls["read"] += 1
                    return {"discovery_ns": 0}, (state, index)
                def release(state):
                    calls["close"] += 1
                def audit(row, payload):
                    self.assertEqual(calls["close"], 1 if reuse else q)
                    calls["audit"] += 1
                result = sessions.run_session(q=q, prepare_reuse=reuse,
                    prepare=prepare, read_owned=read, release=release, audit=audit)
                self.assertEqual(calls, {"open": 1 if reuse else q,
                                        "close": 1 if reuse else q, "read": q, "audit": q})
                self.assertGreaterEqual(result["session_wall_ns"], result["query_sum_ns"])
                self.assertTrue(all(r["total_ns"] == r["discovery_ns"] + r["value_and_delivery_ns"]
                                    for r in result["observations"]))

    def test_failure_closes_and_never_audits_partial_results(self):
        for reuse in (0, 1):
            closed = []
            def fail(state, index):
                raise RuntimeError("query failure")
            with self.assertRaisesRegex(RuntimeError, "query failure"):
                sessions.run_session(q=10, prepare_reuse=reuse, prepare=lambda: 1,
                    read_owned=fail, release=closed.append,
                    audit=lambda *args: self.fail("partial session audited"))
            self.assertEqual(closed, [1])

    def test_deferred_audits_keep_owned_result_until_after_release(self):
        payload = object()
        pending = []
        result = sessions.run_session(q=1, prepare_reuse=1, prepare=lambda: None,
            read_owned=lambda state, index: ({}, payload), release=lambda state: None,
            deferred_audits=pending, audit=lambda *args: self.fail("audit executed inline"))
        self.assertIs(pending[0][1], payload)
        self.assertTrue(result["audit_deferred_to_caller"])


if __name__ == "__main__":
    unittest.main()
