#!/usr/bin/env python3
"""Prepare Ecoscape YAMLs offline, then run/observe one repetition. Standard library only."""
import argparse
import csv
from datetime import datetime
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent
SERVER = 'https://192.168.48.195:443'
JOB = 'monitoring/kafka-pod-monitor-series'
API = 'ecoscape.cau-se.de/v1alpha1'


def kubectl(*args, timeout=20):
    """Run kubectl and return its output, raising an error on failure."""
    p = subprocess.run(['kubectl', f'--request-timeout={max(10, timeout - 5)}s', *args],
                       text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(p.stderr.strip())
    return p.stdout


def get(kind, *args, namespace='default'):
    """Fetch Kubernetes resources as parsed JSON."""
    return json.loads(kubectl('-n', namespace, 'get', kind, *args, '-o', 'json'))


def save(path, value):
    """Write a value to an indented JSON file."""
    path.write_text(json.dumps(value, indent=2) + '\n')


def query(url, expression, now):
    """Query Prometheus for one finite value, or return None if unavailable."""
    params = urllib.parse.urlencode(
        {'query': expression, 'time': now, 'timeout': '8s'})
    with urllib.request.urlopen(url.rstrip('/') + '/api/v1/query?' + params, timeout=10) as r:
        data = json.load(r)
    if data.get('status') != 'success' or data.get('warnings'):
        raise RuntimeError(str(data))
    if data['data']['resultType'] != 'vector':
        raise RuntimeError('Expected an instant vector')
    values = data['data']['result']
    if not values:
        return None
    if len(values) != 1:
        raise RuntimeError(
            'Query returned multiple series; select one scrape job/target')
    value = float(values[0]['value'][1])
    return value if math.isfinite(value) else None


def queries(uc, job=JOB):
    # Only input consumer lag is an outcome metric; the others validate its source.
    """Build Prometheus queries for input lag and source health."""
    lag = f'kafka_consumergroup_lag{{consumergroup="theodolite-uc{uc}-application-0.0.1",job="{job}",topic="input"}}'
    return {
        'input_lag': f'sum(clamp_min({lag},0))',
        'negative_partitions': f'sum({lag} < bool 0)',
        'scrape_up': f'up{{job="{job}"}}',
        'scrape_seconds': f'scrape_duration_seconds{{job="{job}"}}',
        'source_age_seconds': f'time() - min(timestamp({lag}))',
    }


def prepare(args):
    """Create a run directory with experiment manifests and metadata."""
    source = args.source.resolve()
    args.directory.mkdir(parents=True, exist_ok=False)
    name = f'measure-uc{args.uc}-{args.fault}-{uuid.uuid4().hex[:8]}'
    scenario = f'uc{args.uc}-' + \
        ('cpu-stress-4w' if args.fault == 'cpu-stress' else args.fault)
    chaos = (source / 'faults' / (scenario + '.chaosphase.yaml')).read_text()
    # Unique ChaosPhase; custom NetworkChaos keeps its existing name. Runs are sequential.
    phase = name + '-fault'
    chaos = chaos.replace('name: esp-' + scenario, 'name: ' + phase, 1)
    (args.directory / 'chaos.yaml').write_text(chaos)
    (args.directory / 'topology.yaml').write_text((source / 'topology.yaml').read_text())
    maps = []
    for role in ('sut', 'load', 'monitor'):
        data = {}
        if role != 'monitor':
            directory = source / 'workloads' / f'resources-{role}-uc{args.uc}'
            data = {p.name: p.read_text()
                    for p in sorted(directory.glob('*.yaml'))}
            if not data:
                raise RuntimeError(f'No manifests in {directory}')
        maps.append({'apiVersion': 'v1', 'kind': 'ConfigMap',
                     'metadata': {'name': name + '-' + role, 'namespace': 'default'}, 'data': data})
    save(args.directory / 'configmaps.json',
         {'apiVersion': 'v1', 'kind': 'List', 'items': maps})
    text = (source / 'experiment.template.yaml').read_text()
    for token, value in {'EXP': name, 'CHAOSPHASE': phase, 'CM_SUT': name + '-sut',
                         'CM_LOAD': name + '-load', 'CM_MON': name + '-monitor',
                         'GROUP': f'theodolite-uc{args.uc}-application-0.0.1', 'SCRAPE_JOB': args.scrape_job}.items():
        text = text.replace('__' + token + '__', value)
    if re.search(r'__[A-Z_]+__', text):
        raise RuntimeError('Unresolved template token')
    (args.directory / 'experiment.yaml').write_text(text)
    save(args.directory / 'run.json', {'name': name, 'uc': args.uc, 'fault': args.fault,
                                       'source': str(source), 'scrape_job': args.scrape_job,
                                       'queries': queries(args.uc, args.scrape_job)})
    print(f'Prepared {args.directory}; review these files, then use run.')


def preflight():
    """Check the target cluster for active experiments and leftover resources."""
    server = kubectl('config', 'view', '--minify', '-o',
                     'jsonpath={.clusters[0].cluster.server}')
    if server != SERVER:
        raise RuntimeError(f'Wrong cluster: {server}; expected {SERVER}')
    active = [e['metadata']['name'] for e in get('experiments')['items']
              if e.get('status', {}).get('phase') not in ('Succeeded', 'Failed')]
    if active:
        raise RuntimeError(f'Experiments still active/pending: {active}')
    for ns in ('two-zone-edge-munich', 'two-zone-edge-cloud'):
        if get('pods', '-l', 'ecoscape=true', namespace=ns)['items']:
            raise RuntimeError(
                f'Workload/probe pods remain in {ns}; finish teardown first')
    chaos = get('networkchaos,stresschaos', '-A')['items']
    if chaos:
        raise RuntimeError(
            'Chaos objects remain; finish previous experiment/topology teardown first')
    topics = kubectl('-n', 'kafka', 'exec', 'kafka-cluster-dual-role-0', '--',
                     'bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list').splitlines()
    remaining = [t for t in topics if t in ('input', 'output', 'configuration', 'aggregation-feedback')
                 or re.fullmatch(r'theodolite-uc[1-4]-application-0\.0\.1-.*-(changelog|repartition)', t)]
    groups = kafka('kafka-consumer-groups.sh', '--list').splitlines()
    remaining += [g for g in groups if re.fullmatch(
        r'theodolite-uc[1-4]-application-0\.0\.1', g)]
    if remaining:
        raise RuntimeError(
            f'Previous/shared benchmark topics or consumer groups remain: {remaining}. See README cleanup.')


def kafka(tool, *args):
    # Broker CLI startup and group enumeration may exceed 20s. Retry reads only.
    """Run a Kafka CLI command, retrying timed-out reads once."""
    readonly = '--list' in args or '--describe' in args
    for attempt in range(2 if readonly else 1):
        try:
            output = kubectl('-n', 'kafka', 'exec', 'kafka-cluster-dual-role-0', '--',
                             'bin/' + tool, '--bootstrap-server', 'localhost:9092', *args,
                             timeout=60)
            if '--delete' in args and re.search(r'Error:|Exception|could not be deleted|Deletion of some', output,
                                                re.I):
                raise RuntimeError(output.strip())
            return output
        except (subprocess.TimeoutExpired, RuntimeError) as error:
            if not readonly or attempt or 'timed out' not in str(error).lower() and 'deadline' not in str(
                    error).lower():
                raise
            time.sleep(2)


def settle_group(group, timeout=180):
    """Only delete an inactive named group, then verify absence repeatedly."""
    deadline = time.monotonic() + timeout
    absent = 0
    while time.monotonic() < deadline:
        groups = kafka('kafka-consumer-groups.sh', '--list').splitlines()
        if group not in groups:
            absent += 1
            if absent == 3:
                return
        else:
            absent = 0
            state = kafka('kafka-consumer-groups.sh',
                          '--describe', '--group', group, '--state')
            lines = [line.split() for line in state.splitlines()
                     if line.startswith(group + ' ')]
            # Kafka renders state and member count as the final two columns.
            inactive = any(len(
                row) >= 3 and row[-2] in ('Empty', 'Dead') and row[-1] == '0' for row in lines)
            if inactive:
                print(f'Deleting inactive consumer group {group}', flush=True)
                kafka('kafka-consumer-groups.sh', '--delete', '--group', group)
            else:
                print(
                    f'Waiting for inactive consumer group {group}: {state.strip()}', flush=True)
        time.sleep(5)
    raise RuntimeError(
        f'Cleanup timeout: consumer group {group} did not remain absent')


def clean(args):
    """Explicit cleanup of this run only; no shared topic deletion or IaC changes."""
    config = json.loads((args.directory / 'run.json').read_text())
    name = config['name']
    server = kubectl('config', 'view', '--minify', '-o',
                     'jsonpath={.clusters[0].cluster.server}')
    if server != SERVER:
        raise RuntimeError(f'Wrong cluster: {server}')
    experiments = get('experiments')['items']
    other = [e['metadata']['name'] for e in experiments if e['metadata']['name'] != name
             and e.get('status', {}).get('phase') not in ('Succeeded', 'Failed')]
    if other:
        raise RuntimeError(f'Other experiments active/pending: {other}')
    own = [e for e in experiments if e['metadata']['name'] == name]
    print(
        f'Cleanup: {name}, its manifest ConfigMaps/ChaosPhase, and UC{config["uc"]} internal topics/consumer group')
    if args.remove_orphan_topics:
        print(
            'Also remove orphan input/output/configuration/feedback topics declared in this run, after ownership and consumer checks.')
    if not args.execute:
        print('Preview only. Add --execute to perform this cleanup.')
        return
    saved = json.loads((args.directory / 'results' /
                       'experiment.json').read_text())
    if saved['metadata']['name'] != name:
        raise RuntimeError('Saved experiment does not match run.json')
    if own:
        if own[0]['metadata']['uid'] != saved['metadata']['uid']:
            raise RuntimeError(
                'Experiment UID differs from recorded run; refusing deletion')
        kubectl('-n', 'default', 'delete', 'experiment',
                name, '--wait=true', '--timeout=10s')
    # Wait for operator teardown before removing Kafka state.
    deadline = time.monotonic() + 180
    while any(get('pods', '-l', 'ecoscape=true', namespace=ns)['items']
              for ns in ('two-zone-edge-munich', 'two-zone-edge-cloud')):
        if time.monotonic() >= deadline:
            raise RuntimeError('Cleanup timeout: workload pods still present')
        time.sleep(5)
    group = f'theodolite-uc{config["uc"]}-application-0.0.1'
    settle_group(group)
    if args.remove_orphan_topics:
        maps = json.loads(
            (args.directory / 'configmaps.json').read_text())['items']
        data = next(cm['data']
                    for cm in maps if cm['metadata']['name'] == name + '-sut')
        candidates = []
        for topic in ('input', 'output', 'configuration', 'aggregation-feedback'):
            manifest = data.get(topic + '-topic.yaml', '')
            if (re.search(r'^kind: KafkaTopic\s*$', manifest, re.M)
                    and re.search(r'^  name: ' + re.escape(topic) + r'\s*$', manifest, re.M)
                    and re.search(r'^  namespace: kafka\s*$', manifest, re.M)):
                candidates.append(topic)
        managed = get('kafkatopics', namespace='kafka')['items']
        names = {t.get('spec', {}).get(
            'topicName', t['metadata']['name']) for t in managed}
        if names.intersection(candidates):
            raise RuntimeError(
                'A declared topic still has a KafkaTopic object; wait for operator teardown')
        present = set(kafka('kafka-topics.sh', '--list').splitlines())
        candidates = sorted(present.intersection(candidates))
        # Only enumerate groups when orphan topics actually need removal.
        descriptions = kafka('kafka-consumer-groups.sh',
                             '--describe', '--all-groups') if candidates else ''
        if any(set(line.split()).intersection(candidates) for line in descriptions.splitlines()):
            raise RuntimeError(
                'Consumer groups still reference declared topics; clean groups first without --remove-orphan-topics')
        present = set(kafka('kafka-topics.sh', '--list').splitlines())
        for topic in sorted(present.intersection(candidates)):
            print('Deleting orphan from recorded run:', topic, flush=True)
            kafka('kafka-topics.sh', '--delete',
                  '--topic', '^' + re.escape(topic) + '$')
        deadline = time.monotonic() + 30
        while set(kafka('kafka-topics.sh', '--list').splitlines()).intersection(candidates):
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    'Declared topics persist or are recreated; inspect active clients')
            time.sleep(2)
    group = f'theodolite-uc{config["uc"]}-application-0.0.1'
    topics = kafka('kafka-topics.sh', '--list').splitlines()
    for topic in topics:
        if topic.startswith(group + '-') and topic.endswith(('-changelog', '-repartition')):
            # Kafka --topic is a regex; quote the observed exact topic name.
            kafka('kafka-topics.sh', '--delete',
                  '--topic', '^' + re.escape(topic) + '$')
    settle_group(group)
    for kind, names in [('configmap', [name + '-' + r for r in ('sut', 'load', 'monitor')]),
                        ('chaosphase', [name + '-fault'])]:
        kubectl('-n', 'default', 'delete', kind, *names,
                '--ignore-not-found', '--timeout=10s')
    print('Cleanup verified: consumer group absent in three consecutive checks.')


def injected(item):
    """Check whether Chaos Mesh reports fault injection into the SUT."""
    conditions = {c['type']: c['status']
                  for c in item.get('status', {}).get('conditions', [])}
    records = item.get('status', {}).get(
        'experiment', {}).get('containerRecords', [])
    return (conditions.get('Selected') == conditions.get('AllInjected') == 'True'
            and any('titan-ccp-aggregation' in r.get('id', '') and r.get('phase') == 'Injected'
                    for r in records))


def wait_for_scrape(url, timeout=60, job=JOB):
    """Require two healthy checks; transient startup scrape failure is not a run failure."""
    deadline = time.monotonic() + timeout
    consecutive = 0
    detail = 'not checked'
    print(
        f'Waiting for Kafka scrape readiness (up to {timeout}s)...', flush=True)
    while time.monotonic() < deadline:
        try:
            value = query(url, f'up{{job="{job}"}}', time.time())
            detail = f'up={value}'
            consecutive = consecutive + 1 if value == 1 else 0
            if consecutive == 2:
                print('Kafka scrape ready (two successful checks).', flush=True)
                return
        except Exception as error:
            detail = str(error)
            consecutive = 0
        time.sleep(min(5, max(0, deadline - time.monotonic())))
    raise RuntimeError(
        f'Kafka scrape not consistently healthy after {timeout}s: {detail}. No experiment started.')


def run(args):
    """Start an experiment, record observations, and check result quality."""
    config = json.loads((args.directory / 'run.json').read_text())
    name = config['name']
    preflight()
    wait_for_scrape(args.prometheus, job=config.get('scrape_job', JOB))
    # Refuse reuse before any cluster mutation or output overwrite.
    out = args.directory / 'results'
    out.mkdir(exist_ok=False)
    for filename in ('topology.yaml', 'chaos.yaml', 'configmaps.json'):
        kubectl('apply', '-f', str(args.directory / filename))
    kubectl('create', '-f', str(args.directory / 'experiment.yaml'))
    print(f'Running {name}; results: {out}', flush=True)
    print(f'Cancellation/timeout leaves the experiment running. Stop with: kubectl -n default delete experiment {name}',
          flush=True)
    deadline = time.monotonic() + args.timeout
    problems = set()
    counts = {'PreChaosMeasurement': 0, 'ChaosMeasurement': 0}
    proof = False
    last = None
    finished = False
    with (out / 'samples.csv').open('w', newline='') as f, (out / 'status.jsonl').open('w') as statuses:
        writer = csv.DictWriter(
            f, fieldnames=['time', 'phase', 'subphase', 'repetition', *config['queries']])
        writer.writeheader()
        while time.monotonic() < deadline:
            tick = time.monotonic()
            now = time.time()
            experiment = get('experiment', name)
            status = experiment.get('status', {})
            phase, sub = status.get('phase', ''), status.get('subPhase', '')
            snapshot = {'time': now, 'experiment': status}
            if (phase, sub) != last:
                print(phase, sub, flush=True)
                last = phase, sub
            row = {'time': now, 'phase': phase, 'subphase': sub,
                   'repetition': status.get('repetitionsCompleted', 0)}
            if sub in counts:
                for metric, expression in config['queries'].items():
                    try:
                        row[metric] = query(args.prometheus, expression, now)
                    except Exception as e:
                        row[metric] = None
                        problems.add(f'{metric}: {e}')
                # First 60 seconds are warmup, saved but not validated.
                start = status.get('subPhaseStartTime')
                age = now - \
                    datetime.fromisoformat(start.replace(
                        'Z', '+00:00')).timestamp() if start else 0
                if age >= 60:
                    counts[sub] += 1
                    for metric in config['queries']:
                        if row.get(metric) is None:
                            problems.add(f'{sub}: missing {metric}')
                    if row.get('scrape_up') != 1 or (row.get('source_age_seconds') or 0) > 6:
                        problems.add(f'{sub}: missing/stale Kafka scrape')
                    if (row.get('negative_partitions') or 0) > 0:
                        problems.add(
                            f'{sub}: uninitialized/negative input offsets')
                pods = get('pods', '-l', 'app=titan-ccp-aggregation',
                           namespace='two-zone-edge-munich')['items']
                snapshot['pods'] = pods
                load = get('pods', '-l', 'app=titan-ccp-load-generator',
                           namespace='two-zone-edge-cloud')['items']
                snapshot['load_pods'] = load
                if age >= 60 and (not load or any(p.get('status', {}).get('phase') != 'Running' for p in load)):
                    problems.add(f'{sub}: load generator not running')
                if any(c.get('restartCount', 0) for p in load for c in
                       p.get('status', {}).get('containerStatuses', [])):
                    problems.add('Load generator restarted')
                if age >= 60 and (not pods or not all(any(c.get('type') == 'Ready' and c.get('status') == 'True'
                                                          for c in p.get('status', {}).get('conditions', [])) for p in
                                                      pods)):
                    problems.add(f'{sub}: SUT not ready')
                if any(c.get('restartCount', 0) for p in pods for c in
                       p.get('status', {}).get('containerStatuses', [])):
                    problems.add(
                        'SUT restarted; inspect status.jsonl for OOM/failure')
                if sub == 'ChaosMeasurement':
                    kind = 'stresschaos' if config['fault'] == 'cpu-stress' else 'networkchaos'
                    chaos = get(kind, '-A')['items']
                    custom_name = f"fault-uc{config['uc']}-{config['fault']}"
                    relevant = [c for c in chaos if
                                c['metadata']['name'] == custom_name] if kind == 'networkchaos' else [
                        c for c in chaos if
                        'two-zone-edge-munich' in c.get('spec', {}).get('selector', {}).get('namespaces', [])]
                    snapshot['faults'] = relevant
                    if any(injected(c) for c in relevant):
                        proof = True
                    elif age >= 60:
                        problems.add(
                            'Fault not fully injected during settled chaos window')
                snapshot['observation_end'] = time.time()
                snapshot['experiment_after'] = get(
                    'experiment', name).get('status', {})
                writer.writerow(row)
                f.flush()
            statuses.write(json.dumps(snapshot) + '\n')
            statuses.flush()
            save(out / 'experiment.json', experiment)
            if phase in ('Succeeded', 'Failed'):
                finished = True
                if phase != 'Succeeded':
                    problems.add('Ecoscape failed')
                break
            time.sleep(max(0, args.interval - (time.monotonic() - tick)))
    if not finished:
        problems.add('Timed out; experiment may still be running')
    if not proof:
        problems.add('No confirmed SUT fault injection')
    if any(n < 3 for n in counts.values()):
        problems.add('Fewer than three settled samples in a measurement phase')
    save(out / 'quality.json', {'passed_basic_checks': not problems, 'problems': sorted(problems),
                                'settled_samples': counts, 'fault_injected': proof,
                                'note': 'Injection status does not establish physical fault magnitude. No end-to-end latency is measured.'})
    if finished:
        collect(args)
    reviewed = review_quality(args.directory, json.loads(
        (out / 'quality.json').read_text()))
    save(out / 'quality-reviewed.json', reviewed)
    print(json.dumps(reviewed, indent=2))
    print(
        f'After reviewing results, teardown: kubectl -n default delete experiment {name}')
    return 0 if reviewed['passed_basic_checks'] else 1


def collect(args):
    """Export historical Prometheus evaluations, independently of kubectl polling speed."""
    out = args.directory / 'results'
    config = json.loads((args.directory / 'run.json').read_text())
    experiment = json.loads((out / 'experiment.json').read_text())
    if experiment.get('status', {}).get('phase') not in ('Succeeded', 'Failed'):
        raise RuntimeError('Collect requires a terminal recorded experiment')
    snapshots = [json.loads(line) for line in (
        out / 'status.jsonl').read_text().splitlines()]
    starts = {}
    for snap in snapshots:
        state = snap['experiment']
        phase = state.get('subPhase')
        if phase in ('PreChaosMeasurement', 'ChaosMeasurement'):
            starts[phase] = datetime.fromisoformat(
                state['subPhaseStartTime'].replace('Z', '+00:00')).timestamp()
    if len(starts) != 2:
        raise RuntimeError('Missing recorded measurement phase boundaries')
    metrics = config['queries']
    rows, raw = [], {}
    duration = experiment['spec']['duration']
    for phase, key in [('PreChaosMeasurement', 'chaosDelay'), ('ChaosMeasurement', 'measurementDuration')]:
        start = starts[phase]
        # Mid-bin evaluations avoid counting the cleanup boundary as measurement.
        times = [
            start + t + 2.5 for t in range(0, duration[key], 5) if t + 2.5 < duration[key]]
        phase_rows = {t: {'time': t, 'phase': 'Running',
                          'subphase': phase, 'repetition': 0} for t in times}
        for metric, expression in metrics.items():
            params = urllib.parse.urlencode(
                {'query': expression, 'start': times[0], 'end': times[-1], 'step': 5, 'timeout': '25s'})
            with urllib.request.urlopen(args.prometheus.rstrip('/') + '/api/v1/query_range?' + params,
                                        timeout=35) as response:
                data = json.load(response)
            raw[phase + '/' + metric] = data
            if data.get('status') != 'success' or data.get('warnings') or data['data']['resultType'] != 'matrix':
                raise RuntimeError(
                    f'Invalid range response: {phase}/{metric}: {data}')
            series = data['data']['result']
            if len(series) > 1:
                raise RuntimeError(
                    f'Multiple series for {metric}; refusing ambiguous data')
            values = {float(t): float(v)
                      for t, v in (series[0]['values'] if series else [])}
            for t in times:
                value = values.get(t)
                phase_rows[t][metric] = value if value is not None and math.isfinite(
                    value) else None
        rows.extend(phase_rows.values())
    # Raw local observations remain in samples.csv. Publish only complete exports.
    target = out / 'samples-prometheus.csv'
    temp = target.with_suffix('.tmp')
    with temp.open('w', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=['time', 'phase', 'subphase', 'repetition', *metrics])
        writer.writeheader()
        writer.writerows(rows)
    save(out / 'prometheus-range.json', raw)
    temp.replace(target)
    print(
        f'Exported {len(rows)} scheduled evaluations to {target}', flush=True)
    return recheck(args)


def review_quality(directory, quality):
    """Same explicit coverage rule for old and new runs; never fill missing values."""
    out = directory / 'results'
    config = json.loads((directory / 'run.json').read_text())
    metrics = set(config['queries'])
    phases = {'PreChaosMeasurement': 150, 'ChaosMeasurement': 120}

    # Only replace metric-availability verdicts. Preserve injection/health/terminal failures.
    def availability(problem):
        # With a server export, coverage below validates sample count and spacing.
        # The legacy local-poll count is no longer a metric-availability gate.
        """Identify availability problems that sample coverage checks replace."""
        if (
                out / 'samples-prometheus.csv').exists() and problem == 'Fewer than three settled samples in a measurement phase':
            return True
        return (any(problem.startswith(m + ':') for m in metrics)
                or any(problem == f'{ph}: missing {m}' for ph in phases for m in metrics)
                or any(problem == f'{ph}: missing/stale Kafka scrape' for ph in phases))

    problems = [p for p in quality['problems'] if not availability(p)]
    warnings, coverage = [], {}
    snapshots = [json.loads(line) for line in (
        out / 'status.jsonl').read_text().splitlines()]
    starts = {s['experiment'].get('subPhase'): datetime.fromisoformat(
        s['experiment']['subPhaseStartTime'].replace('Z', '+00:00')).timestamp()
        for s in snapshots if s['experiment'].get('subPhaseStartTime')}
    # Rebuild only readiness/injection verdicts from observations within a stable phase.
    # Historical snapshots lack an end time: next poll is a conservative upper bound.
    transient = {f'{ph}: SUT not ready' for ph in phases} | {f'{ph}: load generator not running' for ph in phases} | {
        'Fault not fully injected during settled chaos window'}
    rebuilt, checked = set(), set()
    for i, snap in enumerate(snapshots):
        status = snap['experiment']
        phase = status.get('subPhase')
        if phase not in phases or phase not in starts:
            continue
        age = snap['time'] - starts[phase]
        if age < 60:
            continue
        end = snap.get(
            'observation_end', snapshots[i + 1]['time'] if i + 1 < len(snapshots) else float('inf'))
        after = snap.get('experiment_after')
        stable = (end < starts[phase] + phases[phase]
                  and (after is None or (after.get('phase') == status.get('phase')
                                         and after.get('subPhase') == phase
                                         and after.get('subPhaseStartTime') == status.get('subPhaseStartTime'))))
        if not stable:
            warnings.append(
                f'{phase}: health observation at +{age:.1f}s overlaps phase end; not used for readiness/injection verdict')
            continue
        pods, load = snap.get('pods'), snap.get('load_pods')
        if pods is None or load is None:
            continue
        checked.add(phase)
        if not pods or not all(any(c.get('type') == 'Ready' and c.get('status') == 'True'
                                   for c in p.get('status', {}).get('conditions', [])) for p in pods):
            rebuilt.add(f'{phase}: SUT not ready')
        if not load or any(p.get('status', {}).get('phase') != 'Running' for p in load):
            rebuilt.add(f'{phase}: load generator not running')
        if phase == 'ChaosMeasurement' and not any(injected(f) for f in snap.get('faults', [])):
            rebuilt.add('Fault not fully injected during settled chaos window')
    if checked == set(phases):
        problems = [p for p in problems if p not in transient] + \
            sorted(rebuilt)
    else:
        problems.append('Insufficient phase-stable health observations')

    samples = out / 'samples-prometheus.csv'
    if not samples.exists():
        samples = out / 'samples.csv'
    with samples.open() as stream:
        rows = list(csv.DictReader(stream))

    def finite(value):
        """Return whether a value can be parsed as a finite number."""
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError):
            return False

    for phase, duration in phases.items():
        if phase not in starts:
            problems.append(f'{phase}: phase start missing')
            continue
        start = starts[phase]
        # Five-second bins account for absent rows as well as explicit empty cells.
        bins = [False] * ((duration - 60) // 5)
        valid_times = []
        for row in rows:
            age = float(row['time']) - start
            if row['subphase'] != phase or not 60 <= age < duration:
                continue
            valid = (all(finite(row.get(m)) for m in metrics)
                     and float(row['scrape_up']) == 1
                     and 0 <= float(row['source_age_seconds']) <= 6
                     and float(row['negative_partitions']) == 0)
            if valid:
                bins[int((age - 60) // 5)] = True
                valid_times.append(age)
        missing = bins.count(False)
        longest = streak = 0
        for valid in bins:
            streak = 0 if valid else streak + 1
            longest = max(longest, streak)
        times = [60, *sorted(valid_times), duration]
        gap = max(b - a for a, b in zip(times, times[1:]))
        coverage[phase] = {'expected_bins': len(bins), 'valid_bins': sum(bins),
                           'missing_bins': missing, 'longest_missing_bins': longest,
                           'max_gap_seconds': round(gap, 2)}
        if missing / len(bins) > 0.10 or longest > 1 or gap > 15:
            problems.append(
                f'{phase}: insufficient coverage ({missing}/{len(bins)} bins missing, max gap {gap:.1f}s)')
        elif missing:
            warnings.append(
                f'{phase}: {missing}/{len(bins)} missing five-second bins; values left missing')
    return {**quality, 'policy': 'lag-coverage-v2-phase-check-v4-export-count', 'sample_source': samples.name,
            'passed_basic_checks': not problems,
            'problems': sorted(set(problems)), 'warnings': warnings, 'coverage': coverage,
            'policy_details': 'Per phase after 60s: >=90% five-second bins valid, at most one consecutive missing bin, no gap >15s. No interpolation. Health and injection checks unchanged. With a Prometheus export, local polling counts are diagnostic only.'}


def recheck(args):
    """Reassess a saved run and write its updated quality report."""
    quality = json.loads(
        (args.directory / 'results' / 'quality.json').read_text())
    reviewed = review_quality(args.directory, quality)
    save(args.directory / 'results' / 'quality-reviewed.json', reviewed)
    print(json.dumps(reviewed, indent=2))
    return 0 if reviewed['passed_basic_checks'] else 1


def main():
    """Parse command-line arguments and execute the requested run operation."""
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser(
        'prepare', help='Offline only: snapshot existing workload YAMLs and prepare one run')
    p.add_argument('uc', type=int, choices=range(1, 5))
    p.add_argument('fault', choices=[
                   'packet-loss', 'network-delay', 'cpu-stress'])
    p.add_argument('directory', type=Path)
    p.add_argument('--source', type=Path, default=HERE / 'kubernetes',
                   help='Kubernetes configuration root containing workloads, faults and templates')
    p.add_argument('--scrape-job', default=JOB)
    p = sub.add_parser(
        'run', help='Start Ecoscape and record metrics; does not delete existing resources')
    p.add_argument('directory', type=Path)
    p.add_argument('--prometheus', default='http://kube1-1:30093')
    p.add_argument('--interval', type=float, default=5)
    p.add_argument('--timeout', type=float, default=900)
    p = sub.add_parser(
        'clean', help='Preview cleanup for one prepared run; --execute performs it')
    p.add_argument('directory', type=Path)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--remove-orphan-topics', action='store_true',
                   help='Also remove recorded declared topics only when no KafkaTopic or consumer group references them')
    p = sub.add_parser('recheck',
                       help='Assess saved data with documented missing-sample tolerance; preserve original report')
    p.add_argument('directory', type=Path)
    p = sub.add_parser(
        'collect', help='Export historical lag from Prometheus and recheck; preserve local samples')
    p.add_argument('directory', type=Path)
    p.add_argument('--prometheus', default='http://kube1-1:30093')
    args = parser.parse_args()
    if args.command == 'run' and (args.interval < 2 or args.timeout <= 0):
        parser.error('interval must be >= 2 seconds and timeout > 0')
    try:
        return {'prepare': prepare, 'run': run, 'clean': clean, 'recheck': recheck, 'collect': collect}[args.command](
            args)
    except (Exception, KeyboardInterrupt) as e:
        print(f'Stopped: {e or "interrupted"}. Existing experiments are not deleted; inspect cluster status.',
              file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
