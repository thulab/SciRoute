"""Experimental, version-pinned derived Arrow block reader (not a FUSE route).

Both local mmap and IoTDB payload ranges feed exactly the same Arrow decoder,
byte-bounded block cache, and owned [time, channel] window assembler. The remote
helper validates the published immutable version once at open. This deliberately
measures a prepared training adapter, not the production per-request query,
authorization, scheduling, or result-IPC pipeline.
"""
from __future__ import annotations
from collections import OrderedDict
from dataclasses import dataclass
import base64
from bisect import bisect_right
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import time
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc


def recv_exact(connection, count):
    result = bytearray(count)
    view = memoryview(result)
    position = 0
    while position < count:
        n = connection.recv_into(view[position:])
        if not n:
            raise ConnectionError('derived range helper closed early')
        position += n
    return result


def recv_header(connection):
    size = struct.unpack('>I', recv_exact(connection, 4))[0]
    if not 0 < size <= 1024 * 1024:
        raise ValueError('invalid derived response header')
    value = json.loads(recv_exact(connection, size))
    if value.get('ok') is not True:
        raise RuntimeError(value.get('error', 'derived range request failed'))
    return value


@dataclass(frozen=True)
class DecodedBlock:
    """One cache entry per physical IPC batch, without rewriting row layout.

    A source batch can contain several channel-major rows. Keep their strided
    [time, channel] views separately: concatenating them would covertly give the
    original source a step-major decoded cache and obscure the layout comparison.
    """
    rows: tuple
    starts: tuple

    def __iter__(self):
        for _, _, values, valid in self.rows:
            yield values
            yield valid


class DerivedWindowReader:
    def __init__(self, manifest, *, mode='local', helper=None, config=None,
                 env=None, log=None, max_cache_bytes=1024**3):
        started = time.perf_counter()
        self.manifest = manifest
        self.layout = manifest['layout']
        if self.layout not in ('channel_major', 'step_major'):
            raise ValueError('unknown derived layout')
        self.channels = manifest['channels']
        self.total_steps = manifest['total_steps']
        self.blocks = manifest['batches']
        self.starts = [b['start_step'] for b in self.blocks]
        end = 0
        for i, b in enumerate(self.blocks):
            if b['batch'] != i or b['start_step'] != end or b['end_step'] <= end:
                raise ValueError('derived blocks must exactly cover ordered steps')
            if b['offset'] < 0 or b['length'] <= 0 or b['offset']+b['length'] > manifest['bytes']:
                raise ValueError('invalid indexed IPC byte range')
            if 'rows' in b:
                row_end = b['start_step']
                for row_index, row in enumerate(b['rows']):
                    if (row['row_in_batch'] != row_index or row['start_step'] != row_end or
                            row['end_step'] <= row_end or row['channels'] != self.channels):
                        raise ValueError('invalid indexed batch row coverage')
                    row_end = row['end_step']
                if not b['rows'] or row_end != b['end_step']:
                    raise ValueError('incomplete indexed batch row coverage')
            end = b['end_step']
        if end != self.total_steps or self.channels <= 0 or max_cache_bytes < 0:
            raise ValueError('invalid derived coverage/cache budget')
        self.schema = ipc.read_schema(pa.BufferReader(base64.b64decode(manifest['schema_ipc_base64'])))
        self.mode = mode
        self.cache = OrderedDict()
        self.cache_bytes = 0
        self.max_cache_bytes = max_cache_bytes
        self.source = self.connection = self.process = self.log_handle = None
        self.metrics = dict(read_calls=0, read_ranges=0, read_bytes=0,
            batch_decodes=0, decoded_bytes=0, cache_hits=0, cache_evictions=0,
            io_seconds=0., remote_read_seconds=0., decode_seconds=0., assemble_seconds=0.,
            read_windows_calls=0, peak_cache_bytes=0)
        try:
            if mode in ('local', 'read', 'fuse'):
                path = manifest['mounted'] if mode == 'fuse' else manifest['local']
                if Path(path).stat().st_size != manifest['bytes']:
                    raise ValueError('derived byte size changed')
                self.source = pa.memory_map(path, 'r') if mode == 'local' else pa.OSFile(path, 'rb')
            elif mode == 'iotdb':
                self.connection, child = socket.socketpair()
                self.connection.settimeout(300)
                self.log_handle = open(log or os.devnull, 'ab')
                try:
                    self.process = subprocess.Popen([str(helper), '--config', str(config),
                        '--logical', manifest['logical'], '--version', str(manifest['version']),
                        '--size', str(manifest['bytes']), '--fd', str(child.fileno())],
                        pass_fds=(child.fileno(),), env=env,
                        stdout=self.log_handle, stderr=subprocess.STDOUT)
                finally:
                    child.close()
                self.ready = recv_header(self.connection)
            else:
                raise ValueError('unknown read mode')
        except BaseException:
            self.close()
            raise
        self.setup_seconds = time.perf_counter()-started

    def close(self):
        self.cache.clear()
        self.cache_bytes = 0
        if self.source is not None:
            self.source.close()
            self.source = None
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.process is not None:
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=10)
            self.process = None
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None

    def _read_wave(self, ranges):
        start = time.perf_counter()
        if self.mode == 'iotdb':
            body = json.dumps({'ranges': [dict(offset=o, length=n) for o,n in ranges]},
                              separators=(',', ':')).encode()
            self.connection.sendall(struct.pack('>I', len(body))+body)
            header = recv_header(self.connection)
            if header['lengths'] != [n for _,n in ranges]:
                raise ValueError('derived response lengths differ')
            data = [pa.py_buffer(recv_exact(self.connection, n)) for _,n in ranges]
            self.metrics['remote_read_seconds'] += header['read_ns']/1e9
        else:
            data = []
            for offset, length in ranges:
                self.source.seek(offset)
                buffer = self.source.read_buffer(length)
                if len(buffer) != length:
                    raise ValueError('short derived local read')
                data.append(buffer)
        self.metrics['read_calls'] += 1 if self.mode == 'iotdb' else len(ranges)
        self.metrics['read_ranges'] += len(ranges)
        self.metrics['read_bytes'] += sum(n for _,n in ranges)
        self.metrics['io_seconds'] += time.perf_counter()-start
        return data

    def _read(self, ranges):
        # Only the IoTDB transport has an 8 MiB request bound. Splitting mmap
        # ranges and joining them would unnecessarily copy original large batches
        # and penalize the filesystem control. Decoder/cache/assembly stay shared.
        if self.mode != 'iotdb' or sum(n for _, n in ranges) <= 8*1024**2:
            return self._read_wave(ranges)
        outputs = []
        for offset, size in ranges:
            pieces = []
            done = 0
            while done < size:
                count = min(size-done, 8*1024**2)
                pieces.extend(self._read_wave([(offset+done, count)]))
                done += count
            if len(pieces) == 1:
                outputs.append(pieces[0])
            else:
                outputs.append(pa.py_buffer(b''.join(pieces)))
        return outputs

    def _decode(self, index, buffer):
        started = time.perf_counter()
        batch = ipc.read_record_batch(ipc.read_message(pa.BufferReader(buffer)), self.schema)
        b = self.blocks[index]
        rows = b.get('rows', [b])
        if batch.num_rows != len(rows):
            raise ValueError('indexed message row count mismatch')
        decoded = []
        for row_index, row in enumerate(rows):
            start, end = row['start_step'], row['end_step']
            steps = end-start
            if (batch.column('start_step')[row_index].as_py() != start or
                    batch.column('end_step')[row_index].as_py() != end):
                raise ValueError('indexed message coverage mismatch')
            outer = batch.column('target')[row_index]
            if not outer.is_valid:
                raise ValueError('null indexed parent')
            leaf = outer.values
            if self.layout == 'channel_major':
                if len(leaf) != self.channels or leaf.null_count:
                    raise ValueError('invalid channel list')
                offsets = leaf.offsets.to_numpy()
                if not np.all(np.diff(offsets)==steps):
                    raise ValueError('ragged indexed channels')
                leaf = leaf.values.slice(int(offsets[0]), steps*self.channels)
            if leaf.type != pa.float32() or len(leaf) != steps*self.channels:
                raise ValueError('invalid indexed values')
            values = leaf.fill_null(0).to_numpy(zero_copy_only=False)
            valid = leaf.is_valid().to_numpy(zero_copy_only=False)
            shape = (steps, self.channels) if self.layout=='step_major' else (self.channels, steps)
            values, valid = values.reshape(shape), valid.reshape(shape)
            if self.layout == 'channel_major':
                values, valid = values.T, valid.T
            decoded.append((start, end, values, valid))
        result = DecodedBlock(tuple(decoded), tuple(row[0] for row in decoded))
        self.metrics['batch_decodes'] += 1
        self.metrics['decoded_bytes'] += sum(array.nbytes for array in result)
        self.metrics['decode_seconds'] += time.perf_counter()-started
        return result

    def read_windows(self, starts, length, channels=None):
        channels = tuple(range(self.channels)) if channels is None else tuple(channels)
        if not starts or length <= 0 or not channels or len(set(channels))!=len(channels) or any(c<0 or c>=self.channels for c in channels):
            raise ValueError('invalid window request')
        demand = set()
        for start in starts:
            if start<0 or start+length>self.total_steps:
                raise ValueError('window outside source coverage')
            first, last = bisect_right(self.starts,start)-1, bisect_right(self.starts,start+length-1)-1
            demand.update(range(first,last+1))
        pinned = {}
        for i in sorted(demand):
            if i in self.cache:
                pinned[i] = self.cache[i]
                self.cache.move_to_end(i)
                self.metrics['cache_hits'] += 1
        missing = sorted(demand-pinned.keys())
        # Merge adjacent complete messages; bound each transport wave by bytes
        # and range count. A two-window request normally needs <=4 small blocks.
        waves, groups, total = [], [], 0
        for i in missing:
            b = self.blocks[i]
            if b['length'] > 128*1024**2:
                raise ValueError('single derived block exceeds I/O bound')
            if groups and (total+b['length']>8*1024**2 or len(groups)>=64):
                waves.append(groups); groups=[]; total=0
            if groups and groups[-1][0]+groups[-1][1]==b['offset']:
                groups[-1][1] += b['length']; groups[-1][2].append(i)
            else:
                groups.append([b['offset'],b['length'],[i]])
            total += b['length']
        if groups: waves.append(groups)
        for groups in waves:
            buffers = self._read([(o,n) for o,n,_ in groups])
            for (offset,_,indices), buffer in zip(groups,buffers):
                for i in indices:
                    b=self.blocks[i]
                    value=self._decode(i,buffer.slice(b['offset']-offset,b['length']))
                    pinned[i]=value
                    size=sum(a.nbytes for a in value)
                    if size<=self.max_cache_bytes:
                        while self.cache and self.cache_bytes+size>self.max_cache_bytes:
                            _,old=self.cache.popitem(last=False)
                            self.cache_bytes-=sum(a.nbytes for a in old)
                            self.metrics['cache_evictions']+=1
                        self.cache[i]=value;self.cache_bytes+=size
                        self.metrics['peak_cache_bytes']=max(self.metrics['peak_cache_bytes'],self.cache_bytes)
        started=time.perf_counter()
        values=np.empty((len(starts),length,len(channels)),dtype=np.float32)
        valid=np.empty(values.shape,dtype=np.bool_)
        selection=slice(None) if channels==tuple(range(self.channels)) else list(channels)
        for k,start in enumerate(starts):
            step=start
            while step<start+length:
                i=bisect_right(self.starts,step)-1;b=self.blocks[i]
                block=pinned[i]
                row=block.rows[bisect_right(block.starts,step)-1]
                row_start,row_end,data,mask=row
                stop=min(start+length,row_end)
                source=slice(step-row_start,stop-row_start)
                dest=slice(step-start,stop-start)
                values[k,dest,:]=data[source,selection]
                valid[k,dest,:]=mask[source,selection]
                step=stop
        self.metrics['assemble_seconds']+=time.perf_counter()-started
        self.metrics['read_windows_calls']+=1
        return values,valid
