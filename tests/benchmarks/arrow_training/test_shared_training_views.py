import unittest
import numpy as np
from run_shared_training_views import demand_units, normalized, verify, views


class SharedViewsTests(unittest.TestCase):
    def test_disjoint_control_has_no_shared_step_blocks(self):
        item=dict(layout='step_major',batches=[dict(batch=i,start_step=i*128,end_step=(i+1)*128)
                  for i in range(600)])
        control=demand_units(item,views('disjoint'))
        overlap=demand_units(item,views('overlap'))
        self.assertEqual(control['typed_private_fragment_units'],control['typed_shared_fragment_units'])
        self.assertLess(overlap['typed_shared_fragment_units'],overlap['typed_private_fragment_units'])

    def test_channel_tiles_and_batches_have_different_sharing(self):
        item=dict(layout='channel_major',batches=[dict(batch=0,start_step=0,end_step=8192,
            rows=[dict(row_in_batch=0,start_step=0,end_step=8192)])])
        specs=[dict(length=64,horizon=24,channels=[0],anchors=[[4000]]),
               dict(length=96,horizon=48,channels=[1],anchors=[[4000]])]
        units=demand_units(item,specs)
        self.assertEqual(units['fs_shared_batch_units'],1)
        self.assertEqual(units['typed_shared_fragment_units'],2)
        self.assertEqual(units['typed_private_fragment_units'],2)

    def test_reversed_projection_masks_and_float_bits(self):
        spec=dict(length=1,channels=[2,0])
        raw=np.array([[[7.,1.],[13.,float('nan')]]],dtype=np.float32)
        mask=np.array([[[True,False],[True,True]]])
        actual=normalized(raw,mask,spec,np.array([1.,2.,3.]),np.array([2.,3.,2.]))
        np.testing.assert_array_equal(actual.input_values,np.array([[[2.,0.]]],dtype=np.float32))
        self.assertTrue(np.isnan(actual.target_values[0,0,1]))
        self.assertTrue(actual.target_valid[0,0,1])
        verify(actual,actual)
        self.assertFalse(actual.input_valid[0,0,1])


if __name__=='__main__':unittest.main()
