# H00 generated fixtures

Generate into a temporary or evidence directory; generated HDF5 binaries are not
source fixtures and must not be committed:

```sh
python tests/fixtures/hdf5/generate_h00.py --output /tmp/iotdbfs-h00-fixtures
python -m unittest discover -s tests/benchmarks/hdf5_query -p test_h00_fixtures.py -v
```

Requires the selected experiment environment's NumPy and h5py with HDF5 DEFLATE,
shuffle, Fletcher32, and virtual-dataset support. The generator installs nothing
and performs no network access. It emits 55 small files (about 0.3 MB with the
tested toolchain), plus `manifest.json`. Keep the whole directory together.

The matrix covers rank 1/2/3 Float32/Float64 in both byte orders, contiguous and
chunked layouts, all nonempty combinations of the three filters, a mixed file
with one chunk skipping all three filters, partial edge chunks, user blocks of
512/4096 bytes (both contiguous and compressed chunked), default/explicit fill for unallocated contiguous storage and
partially allocated chunks, zero extent, and hard-link aliases with a cycle.
Negative profiles cover undefined fill, scalar/null dataspaces, compact layout,
integer/variable-length types, external storage, soft/external links, and VDS.
A separate corrupted Fletcher32 chunk must produce a corruption error.
The minimal binding example has `/a` with shape `[5,7,9]`, chunks `[2,3,4]`,
and selection `/a[1:5,2:7,3:9]`, alongside `/b` with another shape and values.
Its selections declare `excluded_datasets: ["/b"]`; the unified runner checks
that the traced payload ranges do not intersect `/b`'s allocated chunks.
Unsupported does not mean the HDF5 library cannot read a feature; it is the H00
adapter profile's expected boundary.

`manifest.json` uses `schema_version: iotdbfs-hdf5-h00-fixtures/1`. Each file lists
its relative path, SHA256, byte size, expected support/reason, datasets, and
selections. Each selection names an exact dataset path, zero-based `start`, and
`count` per dimension. Its `expected` object records shape, canonical little-endian
NumPy dtype, and SHA256 of **C-order little-endian decoded values**. Unsupported
selections have no value oracle. A selection's `support` is authoritative:
the undefined-fill chunked file also contains a supported selection wholly
inside its allocated chunk. Corruption selections additionally declare
`expected_status: corrupt`; they are not unsupported-format tests.
File-byte digests and selected-value digests are
different concepts. Big-endian results must be converted before digesting.

Data are generated from `-3.25 + sum(index[axis] * 17**axis / 8)`. Dataset recipes
also state the fill value and written boxes where applicable. Expected selection
digests are computed from global coordinates and these declarations, without
reading the generated dataset. Tests separately compare scalar arithmetic,
HDF5 reads, and canonical bytes. Missing chunks/storage remain truly unallocated;
the generator does not write a prefilled dense array for those tests. Undefined
fill has no expected bytes, because reading those holes has no stable value
contract.

File/group/dataset time tracking is disabled. Tests cross a one-second timestamp
boundary and require complete file/manifest equality within the same toolchain.
The manifest records Python/NumPy/h5py/HDF5 versions and host byte order; binary
stability across HDF5 or compression-library versions is not promised. The
undefined-fill fixture calls the public `H5Pset_fill_value(..., NULL)` through the
already-loaded h5py extension because h5py's Python setter cannot express NULL.
It never loads a second HDF5 runtime. Failure to access this API is an environment
failure, not a reason to silently replace undefined fill with zero.
