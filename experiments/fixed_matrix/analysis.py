"""Fail-closed descriptive analysis of the approved primary fixed matrix.

Three retained-process campaigns are time blocks, not independent machines.
Ratios are quotients of within-campaign warm medians. No pooled significance,
equivalence, cold-disk, or tail-latency guarantee is inferred from these data.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
import statistics

ARMS = ('FS-scan', 'FS-index', 'IoTDB-FS')
_SHA = re.compile(r'[0-9a-f]{64}\Z')


def _need(condition, reason):
    if not condition:
        raise ValueError(reason)


def _sha(value, name):
    _need(isinstance(value, str) and _SHA.fullmatch(value),
          name + ' must be SHA-256')


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      separators=(',', ':'), allow_nan=False)


def _absolute(values):
    return {'count': len(values), 'median_ns': statistics.median(values),
            'mean_ns': statistics.mean(values), 'min_ns': min(values),
            'max_ns': max(values), 'samples_ns': list(values)}


def _distribution(values):
    return {'median': statistics.median(values), 'min': min(values),
            'max': max(values), 'campaign_values': list(values)}


def _resource(value, name):
    _need(isinstance(value, dict), name + ' must be an object')
    _need(type(value.get('monotonic_ns')) is int and value['monotonic_ns'] > 0,
          name + ' requires monotonic timestamp')
    groups = value.get('cgroups')
    _need(isinstance(groups, dict) and {'parent', 'query', 'database'} <= set(groups),
          name + ' requires actual cgroup counters')
    for role, group in groups.items():
        _need(isinstance(role, str) and role and isinstance(group, dict),
              name + ' has malformed cgroup entry')
        _need(isinstance(group.get('path'), str) and group['path'].startswith('/'),
              name + ' requires cgroup path')
        files = group.get('files')
        _need(isinstance(files, dict) and
              isinstance(files.get('cpu.stat'), str) and
              re.search(r'(?:^|\n)usage_usec \d+(?:\n|$)', files['cpu.stat']) and
              isinstance(files.get('memory.current'), str) and
              files['memory.current'].isdigit(),
              name + ' lacks readable CPU/memory evidence')


def _preparation(value):
    _need(isinstance(value, dict) and value,
          'each corpus requires its preparation receipt')
    _need(isinstance(value.get('artifact'), str) and value['artifact'],
          'preparation artifact reference required')
    _sha(value.get('sha256'), 'preparation receipt SHA')
    # Receipt itself includes index generation time and its resource series;
    # reference it once per corpus rather than charging every selected case.
    _canonical(value)


def summarize(rows, catalog, bindings, *, resource_ids,
              preparation_refs, completion):
    """Require exactly 3,456 complete rows, then report R/Rd/Rq per campaign.

    bindings[group_id] is {corpus_id, case_id, oracle_sha256}. Optional run_ids
    maps string campaign labels to {arm: SHA}. Each campaign otherwise requires
    coherent, unique per-case/arm run identities. resource_ids is either a
    campaign->SHA mapping (preferred) or a set of allowed SHAs. Evidence remains
    in the original row stream, which this analysis hashes without rewriting.
    """
    _need(isinstance(catalog, dict) and isinstance(catalog.get('groups'), list),
          'frozen catalog required')
    groups = [g for g in catalog['groups'] if g.get('collection_tier') == 'primary']
    _need(len(groups) == 32, 'approved primary catalog must contain exactly 32 groups')
    by_group = {g['group_id']: g for g in groups}
    _need(len(by_group) == 32, 'duplicate group IDs in catalog')
    _need(catalog.get('arms') == list(ARMS), 'catalog arm names/order differ')
    _need(catalog.get('repetitions', {}).get('campaigns') == 3 and
          catalog['repetitions'].get('warmup') == 2 and
          catalog['repetitions'].get('measured') == 10,
          'catalog requires three campaigns, two warmups and ten measurements')
    _need(isinstance(completion, dict) and all(completion.get(k) is True for k in
          ('complete', 'original_service_restored', 'resources_restored')),
          'complete service/resource restoration evidence required')
    _need(isinstance(bindings, dict) and set(bindings) == set(by_group),
          'exactly one binding per primary group required')
    _need(isinstance(preparation_refs, dict), 'preparation_refs must be an object')
    case_groups = {}
    for group_id, binding in bindings.items():
        _need(isinstance(binding, dict), 'binding must be an object')
        for name in ('corpus_id', 'case_id', 'oracle_sha256'):
            _sha(binding.get(name), name)
        _need(binding['case_id'] not in case_groups,
              'distinct registered primary groups must have distinct cases')
        case_groups[binding['case_id']] = group_id
        _preparation(preparation_refs.get(binding['corpus_id']))
    _need(isinstance(rows, list) and len(rows) == 3456,
          'complete primary matrix requires exactly 3,456 rows')
    if isinstance(resource_ids, dict):
        declared_resources = {str(k): v for k, v in resource_ids.items()}
        _need(len(declared_resources) == len(resource_ids) == 3,
              'exactly three explicit campaign resource identities required')
        for value in declared_resources.values():
            _sha(value, 'resource_id')
        campaigns = sorted(declared_resources)
        allowed_resources = set(declared_resources.values())
    else:
        _need(isinstance(resource_ids, (set, frozenset)) and resource_ids,
              'resource_ids requires a campaign mapping or nonempty allowed set')
        for value in resource_ids:
            _sha(value, 'resource_id')
        allowed_resources = set(resource_ids)
        campaigns = sorted({str(row.get('campaign')) for row in rows})
        _need(len(campaigns) == 3, 'exactly three campaigns required')
        declared_resources = {}
    index, run_by_block, block_by_run = {}, {}, {}
    for row in rows:
        _need(isinstance(row, dict), 'row must be an object')
        _canonical(row)
        gid, arm, rep = row.get('group_id'), row.get('arm'), row.get('repeat')
        campaign = str(row.get('campaign'))
        _need(gid in by_group and arm in ARMS and type(rep) is int and 0 <= rep < 12
              and campaign in campaigns, 'unknown group, arm, campaign or repeat')
        key = (gid, campaign, arm, rep)
        _need(key not in index, 'duplicate query row')
        _need(row.get('phase') == ('warmup' if rep < 2 else 'measurement'),
              'warmup/measurement phase mismatch')
        _need(all(row.get(name) is True for name in
                  ('ok', 'oracle_verified', 'mode_verified')),
              'failed/unverified rows cannot enter the complete analysis')
        binding = bindings[gid]
        for name in ('case_id', 'corpus_id', 'oracle_sha256'):
            _need(row.get(name) == binding[name], 'row differs from frozen ' + name)
        run_id = row.get('run_id')
        _sha(run_id, 'run_id')
        block = (gid, campaign, arm)
        _need(run_by_block.setdefault(block, run_id) == run_id,
              'run ID changed within a case/campaign/arm')
        _need(block_by_run.setdefault(run_id, block) == block,
              'run ID reused for a different case/campaign/arm')
        if 'run_ids' in binding:
            _need(run_id == binding['run_ids'].get(campaign, {}).get(arm),
                  'run ID differs from preregistered binding')
        resource_id = row.get('resource_id')
        _need(resource_id in allowed_resources, 'unregistered resource identity')
        _need(declared_resources.setdefault(campaign, resource_id) == resource_id,
              'resource identity changed within campaign')
        for name in ('total_ns', 'discovery_ns', 'value_ns'):
            _need(type(row.get(name)) is int and row[name] > 0,
                  name + ' must be a positive integer')
        _need(row['total_ns'] == row['discovery_ns'] + row['value_ns'],
              'query clocks must satisfy T = d + q exactly')
        fmt = by_group[gid]['format']
        expected_mode = ('native-hdf5' if fmt == 'hdf5' else
                         'iotdb-indexed' if arm == 'IoTDB-FS' else 'native-grib')
        _need(row.get('execution_mode') == expected_mode,
              'execution mode differs from fixed format route')
        before, after = row.get('resource_before'), row.get('resource_after')
        _resource(before, 'resource_before')
        _resource(after, 'resource_after')
        _need(after['monotonic_ns'] > before['monotonic_ns'],
              'resource envelope timestamps must increase')
        _need(set(before['cgroups']) == set(after['cgroups']) and
              all(before['cgroups'][g]['path'] == after['cgroups'][g]['path']
                  for g in before['cgroups']),
              'resource envelope group membership changed')
        index[key] = row
    expected = {(g, c, a, r) for g in by_group for c in campaigns
                for a in ARMS for r in range(12)}
    _need(set(index) == expected, 'missing matrix cell; no exclusions allowed')
    cases = []
    for gid, group in by_group.items():
        blocks = []
        for campaign in campaigns:
            absolute = {}
            for arm in ARMS:
                selected = [index[gid, campaign, arm, r] for r in range(12)]
                absolute[arm] = {'run_id': run_by_block[gid, campaign, arm],
                    'measured': {name: _absolute([row[name] for row in selected[2:]])
                        for name in ('total_ns', 'discovery_ns', 'value_ns')},
                    'warmup': {name: _absolute([row[name] for row in selected[:2]])
                        for name in ('total_ns', 'discovery_ns', 'value_ns')}}
            ratios = {}
            for denominator in ('FS-index', 'FS-scan'):
                ratios[denominator] = {
                    label: absolute['IoTDB-FS']['measured'][name]['median_ns'] /
                           absolute[denominator]['measured'][name]['median_ns']
                    for label, name in (('R', 'total_ns'), ('Rd', 'discovery_ns'),
                                        ('Rq', 'value_ns'))}
            blocks.append({'campaign': campaign, 'resource_id': declared_resources[campaign],
                           'arms': absolute, 'ratios': ratios})
        cases.append({'group_id': gid, 'format': group['format'],
            **{k: bindings[gid][k] for k in ('corpus_id', 'case_id', 'oracle_sha256')},
            'table_id': group['table_id'], 'axes': deepcopy(group['axes']),
            'campaigns': blocks,
            'ratios': {denominator: {label: _distribution([
                    block['ratios'][denominator][label] for block in blocks])
                for label in ('R', 'Rd', 'Rq')}
                for denominator in ('FS-index', 'FS-scan')},
            'preparation_reference': deepcopy(preparation_refs[bindings[gid]['corpus_id']]),
            'x': None, 'y': None, 'best_complete_primary_R': False})
    minima = {}
    for fmt in ('grib', 'hdf5'):
        candidates = [case for case in cases if case['format'] == fmt]
        _need(len(candidates) == 16, 'primary matrix requires 16 cases per format')
        minimum = min(c['ratios']['FS-index']['R']['median'] for c in candidates)
        winners = [c for c in candidates if c['ratios']['FS-index']['R']['median'] == minimum]
        for winner in winners:
            winner['best_complete_primary_R'] = True
        minima[fmt] = {'R': minimum, 'group_ids': [c['group_id'] for c in winners],
                       'selection': 'minimum across all 16 complete primary cases; ties retained'}
    return {'protocol': 'iotdbfs-fixed-matrix-analysis/1', 'complete': True,
        'catalog_sha256': hashlib.sha256(_canonical(catalog).encode()).hexdigest(),
        'records_sha256': hashlib.sha256(_canonical(rows).encode()).hexdigest(),
        'query_records': len(rows), 'warmup_records': 576,
        'measured_records': 2880, 'campaigns': campaigns,
        'resource_ids': declared_resources, 'completion': deepcopy(completion),
        'cases': cases, 'format_minima': minima,
        'notes': ['Ratios divide within-campaign warm medians; no cross-run minima or pooled samples.',
            'The three campaigns are retained-process time blocks on one host.',
            'Envelope resource counters include audit/profile work outside T; no query-exclusive CPU attribution.',
            'No statistical significance, equivalence, cold-disk, or p95 guarantee is claimed.',
            'Index preparation is referenced once per corpus, not folded into each query.',
            'Historic timings excluded. Future implementation columns x/y remain empty.']}
