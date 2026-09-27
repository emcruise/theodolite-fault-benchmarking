#!/usr/bin/env python3
"""Four gated pilots, then 48 UC1–UC4 runs. Preserve every attempt; retry coverage failures once."""
import argparse
import csv
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
JOB = 'monitoring/kafka-pod-monitor-series'


def save(path, data):
    """Write JSON through a temporary file and replace the target atomically."""
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(data, indent=2) + '\n')
    temp.replace(path)


def coverage_only(quality):
    """Check whether all reported problems concern insufficient coverage."""
    problems = quality.get('problems', [])
    return bool(problems) and all(': insufficient coverage (' in p for p in problems)


def baseline(directory, uc):
    """Operational pilot gate, fixed before running; does not establish scientific validity."""
    out = directory / 'results'
    starts = [json.loads(line)['experiment']
              for line in (out/'status.jsonl').read_text().splitlines()]
    from datetime import datetime
    start = next(datetime.fromisoformat(s['subPhaseStartTime'].replace('Z', '+00:00')).timestamp()
                 for s in starts if s.get('subPhase') == 'PreChaosMeasurement')
    with (out/'samples-prometheus.csv').open() as f:
        rows = [r for r in csv.DictReader(f) if r['subphase'] == 'PreChaosMeasurement'
                and float(r['time']) >= start+60 and r['input_lag']
                and r['scrape_up'] == '1.0' and r['negative_partitions'] == '0.0'
                and r['source_age_seconds'] and 0 <= float(r['source_age_seconds']) <= 6]
    if len(rows) < 3:
        raise RuntimeError('Too few valid baseline observations')
    xs = [float(r['time'])-start for r in rows]
    ys = [float(r['input_lag']) for r in rows]
    mx, my = sum(xs)/len(xs), sum(ys)/len(ys)
    slope = sum((x-mx)*(y-my)
                for x, y in zip(xs, ys))/sum((x-mx)**2 for x in xs)
    nominal_rate = {1: 1000, 2: 1000, 3: 1000, 4: 1000}[uc]
    limit = max(100, 0.05*nominal_rate)
    return {'slope_messages_per_second': slope, 'maximum_absolute_slope': limit,
            'passed': abs(slope) <= limit, 'valid_samples': len(rows),
            'note': 'Operational pilot gate: absolute OLS slope <= max(100, 5% nominal input/s), last 90 baseline seconds. Both rising backlog and ongoing catch-up fail. Nominal input is configured, not measured.'}


def main():
    """Prepare, start, or resume a series of pilots and measurement runs."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path,
                   help='New series directory, or existing directory for --resume')
    p.add_argument('--run', action='store_true',
                   help='Prepare if needed, execute pilots, then all measurements')
    p.add_argument('--resume', action='store_true',
                   help='Continue existing attempts; never overwrite or blindly restart a recorded experiment')
    args = p.parse_args()
    ROOT = args.directory.resolve()
    if args.resume and not (ROOT/'progress.json').exists():
        p.error('--resume requires an existing prepared series')
    ROOT.mkdir(parents=True, exist_ok=True)
    # Shared lock with the original batch runner; blocks concurrent benchmark series.
    lock = (HERE/'series.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    progress = ROOT/'progress.json'
    frozen = ROOT/'runner'
    if not progress.exists():
        frozen.mkdir(exist_ok=False)
        shutil.copytree(HERE/'kubernetes', ROOT/'kubernetes')
        shutil.copy2(HERE/'measure.py', frozen/'measure.py')
        tasks = [{'uc': uc, 'fault': 'network-delay', 'pilot': True,
                  'directory': f'pilot-uc{uc}', 'attempts': []} for uc in (1, 2, 3, 4)]
        tasks += [{'uc': uc, 'fault': fault, 'pilot': False, 'directory': f'uc{uc}-{fault}-r{rep}', 'attempts': []}
                  for uc in (1, 2, 3, 4) for fault in ('packet-loss', 'network-delay', 'cpu-stress') for rep in range(1, 5)]
        for task in tasks:
            task['state'] = 'pending'
            directory = ROOT/task['directory']
            if not directory.exists():
                subprocess.run([sys.executable, str(frozen/'measure.py'), 'prepare', str(task['uc']), task['fault'], str(directory),
                                '--source', str(ROOT/'kubernetes'), '--scrape-job', JOB], check=True)
            task['attempts'] = [task['directory']]
        state = {'state': 'prepared', 'tasks': tasks,
                 'policy': 'one separate retry only for coverage failure; pilot baseline gate; 90s pause; frozen workload and runner'}
        save(progress, state)
    state = json.loads(progress.read_text())
    if not args.run and not args.resume:
        print('Prepared four pilots and 48 measurements. Start with --run.')
        lock.close()
        return 0
    if state['state'] == 'completed':
        print('Already completed. No experiment started.')
        lock.close()
        return 0
    if state['state'] not in ('prepared', 'stopped'):
        raise RuntimeError(
            'Runner may still be active or interrupted; inspect progress and cluster before recovery')

    def invoke(directory, command, *extra):
        """Run a frozen runner command and append its output to the batch log."""
        with (directory/'batch.log').open('a') as log:
            return subprocess.run([sys.executable, '-u', str(frozen/'measure.py'), command, str(directory), *extra],
                                  stdout=log, stderr=subprocess.STDOUT).returncode
    state['state'] = 'running'
    state.pop('error', None)
    save(progress, state)
    try:
        for index, task in enumerate(state['tasks']):
            if task['state'] == 'passed':
                continue
            task['state'] = 'running'
            state['current'] = task['directory']
            save(progress, state)
            while True:
                directory = ROOT/task['attempts'][-1]
                print(
                    f'[{index+1}/{len(state["tasks"])}] {directory.name}', flush=True)
                out = directory/'results'
                if out.exists():
                    if not (out/'quality.json').exists():
                        raise RuntimeError(
                            f'{directory.name}: incomplete recorded experiment; inspect before continuing')
                    if not (out/'quality-reviewed.json').exists():
                        code = invoke(directory, 'collect')
                    else:
                        # Preserve already-exported server data and reports.
                        code = 0
                else:
                    code = invoke(directory, 'run')
                quality = json.loads((out/'quality-reviewed.json').read_text()
                                     ) if (out/'quality-reviewed.json').exists() else None
                if (out/'experiment.json').exists():
                    if invoke(directory, 'clean', '--execute', '--remove-orphan-topics'):
                        raise RuntimeError(
                            f'{directory.name}: cleanup failed; see batch.log')
                task['quality'] = quality
                if not quality:
                    raise RuntimeError(
                        f'{directory.name}: startup/observer/export failed (exit {code}); see batch.log')
                if quality['passed_basic_checks']:
                    if code:
                        raise RuntimeError(
                            f'{directory.name}: command failed despite quality report; inspect log')
                    report = baseline(directory, task['uc'])
                    save(out/'baseline-diagnostic.json', report)
                    task['baseline'] = report
                    if task['pilot'] and not report['passed']:
                        raise RuntimeError(
                            f'{directory.name}: pilot baseline not settled ({report["slope_messages_per_second"]:.1f} msg/s); series not started')
                    task['state'] = 'passed'
                    save(progress, state)
                    break
                if not coverage_only(quality) or len(task['attempts']) >= 2:
                    raise RuntimeError(
                        f'{directory.name}: validation failed ({len(task["attempts"])} recorded attempt(s)); see quality-reviewed.json')
                retry = ROOT/(task['directory']+'-retry1')
                # Snapshot the exact prepared manifests, changing only unique run object names.
                cfg = json.loads((directory/'run.json').read_text())
                import uuid
                old = cfg['name']
                new = old+'-retry-'+uuid.uuid4().hex[:6]
                retry.mkdir(exist_ok=False)
                for file in ('run.json', 'experiment.yaml', 'configmaps.json', 'chaos.yaml', 'topology.yaml'):
                    (retry/file).write_text((directory /
                                             file).read_text().replace(old, new))
                task['attempts'].append(retry.name)
                save(progress, state)
                print(
                    'Coverage failure: preserved first attempt; one new attempt after 90s.', flush=True)
                time.sleep(90)
            if index+1 < len(state['tasks']):
                print('Pause 90 s', flush=True)
                time.sleep(90)
        state['state'] = 'completed'
    except BaseException as e:
        state['state'] = 'stopped'
        state['error'] = str(e) or type(e).__name__
        print(state['error'], flush=True)
    save(progress, state)
    lock.close()
    return 0 if state['state'] == 'completed' else 1


if __name__ == '__main__':
    sys.exit(main())
