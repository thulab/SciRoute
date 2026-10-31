"""Protocol invariants independent of the native libraries and remote services."""
import unittest
from .run import query_kind, expected

class ProtocolTests(unittest.TestCase):
    def test_history_fraction(self):
        for q,wanted in ((1,0),(10,1),(100,10)):
            self.assertEqual(sum(query_kind(q,i)=='v1' for i in range(q)),wanted)

    def test_latest_first_and_public_bindings(self):
        from experiments.fixed_matrix.client import PREDICATE
        entries=[]; bindings={}; current={i:2 for i in range(8)}
        for ordinal in (1,2):
            for i in range(8):
                bindings[i,ordinal]=100*ordinal+i
                attrs=dict(PREDICATE)
                if i>1:attrs['level']=700
                entries.append(dict(file_id=i,ordinal=ordinal,logical_path='/f'+str(i),units=[
                    dict(dataset='/u',attributes=attrs,oracle={'marker':ordinal})]))
        latest=expected(entries,current,'latest',bindings,'hdf5')
        first=expected(entries,current,'v1',bindings,'hdf5')
        self.assertEqual([m['binding']['version'] for m in latest],[200,201])
        self.assertEqual([m['binding']['version'] for m in first],[100,101])
        self.assertEqual([m['native_oracle']['marker'] for m in first],[1,1])
        # v2 no longer matches: Latest must not fall back to matching v1.
        entries[8]['units'][0]['attributes']['level']=700
        with self.assertRaisesRegex(RuntimeError,'fixed two selected'):
            expected(entries,current,'latest',bindings,'hdf5')

if __name__=='__main__':unittest.main()
