"""Owned IPC/provenance and bounded transport regression checks (no service)."""
import hashlib
import json
from pathlib import Path
import socket
import struct
import threading
import unittest

import numpy as np
import pyarrow as pa

try:
    from . import read_matrix as reader
except ImportError:
    import read_matrix as reader


def sha(data):
    return hashlib.sha256(data).hexdigest()


class ReadMatrixTests(unittest.TestCase):
    def grib(self, binary=False):
        values=b'GRIB-unit-7777' if binary else np.array([1.,-0.],dtype='<f4')
        lat,lon=np.array([0.,.25],dtype='<f8'),np.array([1.,1.],dtype='<f8')
        mask=np.ones(1 if binary else 2,dtype='u1')
        spec=dict(format_id='grib',view='binary' if binary else 'values',
            logical_path='/f.grib2',version=123,oracle=dict(rows=mask.size,
            value_sha256=sha(values if binary else values.tobytes()),mask_sha256=sha(mask.tobytes()),
            latitude_sha256=sha(b'' if binary else lat.tobytes()),
            longitude_sha256=sha(b'' if binary else lon.tobytes()),messages=[dict(shape=[2,1])]))
        arrays=([pa.array([values],type=pa.binary())] if binary else
                [pa.array(lat),pa.array(lon),pa.array(values)])
        batch=pa.record_batch(arrays,names=['x'+str(i) for i in range(len(arrays))])
        meta={b'iotdbfs.sources':b'1',b'iotdbfs.source.0.path':b'/f.grib2',
              b'iotdbfs.source.0.version':b'123',b'iotdbfs.source.0.format_id':b'grib',
              b'iotdbfs.domain.dimension.0.shape':b'2',b'iotdbfs.domain.dimension.1.shape':b'1'}
        stream=pa.BufferOutputStream()
        with pa.ipc.new_stream(stream,batch.schema) as writer:
            writer.write_batch(batch,custom_metadata=meta)
        return spec,bytearray(stream.getvalue().to_pybytes())

    def test_values_coordinates_mask_remain_owned_after_input_overwrite(self):
        spec,payload=self.grib()
        result=reader.extract_owned(payload,spec)
        payload[:]=b'X'*len(payload)
        self.assertTrue(reader.validate_owned(spec,result)['oracle_verified'])
        self.assertEqual(result['values'].view('<u4').tolist(),[0x3f800000,0x80000000])

    def test_binary_is_owned_and_source_version_is_checked(self):
        spec,payload=self.grib(True)
        result=reader.extract_owned(payload,spec)
        payload[:]=b'X'*len(payload)
        self.assertTrue(reader.validate_owned(spec,result)['oracle_verified'])
        result['metadata'][0][b'iotdbfs.source.0.version']=b'124'
        with self.assertRaisesRegex(ValueError,'provenance'):
            reader.validate_owned(spec,result)

    def test_invalid_mask_or_values_fail_oracle(self):
        spec,payload=self.grib()
        result=reader.extract_owned(payload,spec)
        result['mask'][0]=0
        with self.assertRaisesRegex(ValueError,'oracle hash'):
            reader.validate_owned(spec,result)

    def test_empty_or_truncated_ipc_is_not_success(self):
        spec,payload=self.grib()
        for invalid in (b'',payload[:len(payload)//2]):
            with self.assertRaises(Exception):
                reader.extract_owned(invalid,spec)

    def test_fetch_rejects_outside_route_before_connecting(self):
        with self.assertRaisesRegex(ValueError,'escaped'):
            reader.fetch(dict(socket='/no-socket',mount=Path('/tmp/owned')),
                         dict(path='/tmp/unowned/result'))

    def test_short_control_frame_is_error(self):
        sender,receiver=socket.socketpair()
        try:
            sender.sendall(b'\0\0')
            sender.shutdown(socket.SHUT_WR)
            with self.assertRaisesRegex(ValueError,'short'):
                reader._receive(receiver,4)
        finally:
            sender.close()
            receiver.close()

    def test_huge_control_frame_rejected_without_body_allocation(self):
        sender,receiver=socket.socketpair()
        try:
            sender.sendall(struct.pack('>I',1024*1024+1))
            with self.assertRaisesRegex(ValueError,'budget'):
                reader._response(receiver)
        finally:
            sender.close()
            receiver.close()

    def test_exact_sources_explicitly_selects_only_requested_format(self):
        for fmt in ('grib','hdf5'):
            result=reader.exact_sources(dict(logical_path='/source',version=123,format_id=fmt))
            self.assertEqual(result['versions'],dict(kind='exact',exact=123))
            self.assertEqual(result['formats'],dict(adapters='listed',adapter_ids=[fmt],
                                                   include_byte_passthrough=False))

    def test_top_level_server_error_retains_code_phase_and_response(self):
        response=dict(ok=False,code=10,phase=3,message='format catalog requires one listed adapter')
        payload=json.dumps(response).encode()
        sender,receiver=socket.socketpair()
        try:
            sender.sendall(struct.pack('>I',len(payload))+payload)
            with self.assertRaisesRegex(ValueError,'code=10, phase=3, message=format catalog') as raised:
                reader._response(receiver)
            self.assertEqual(raised.exception.response,response)
        finally:
            sender.close()
            receiver.close()


if __name__=='__main__':
    unittest.main()
