# Experiment entry points

These drivers accompany the scientific-file query implementation. They are
separate from the product build and do not run automatically. Read each protocol
before use: full runs require native dependencies, a compatible IoTDB service,
private input data/configuration, and the specified resource isolation.

| Workload | Entry point |
| --- | --- |
| Fixed cross-file and historical query matrix | [fixed_matrix](fixed_matrix/README.md) |
| Source/case/run identity | [identity contract](../docs/contracts/experiment-identity.md) |
| Incremental FS-scan / FS-index / IoTDB-FS maintenance | [maintenance_lifecycle](maintenance_lifecycle/README.md) |
| Queries over already stored data; HDF5 retained handles | [resident_table5](resident_table5/README.md) |
| Complete import/publication traces | [lifecycle_table5](lifecycle_table5/README.md) |
| Native readers, data preparation and selection/session controls | [single_file](single_file/README.md) |
| Corpus builders and version/resource helpers | [multi_file](multi_file/README.md), [cross_version](cross_version/README.md) |

Some historical modules remain because current drivers import their corpus,
resource-control, publication, or native-reader helpers. A historical directory
name does not establish compatibility with the current binary. Frozen source
and binary checks must not be bypassed to rerun an old experiment.

Generated workspaces/results, obsolete diagnostic probes and their dedicated
tests, feasibility notes, and retired source-copy indexes are excluded from Git.
Datasets and full archived runs are not bundled; existing driver documentation
states their dependencies and original timing boundaries. Import traces and
queries over already stored data measure different costs and must stay separate.
