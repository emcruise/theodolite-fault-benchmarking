import csv
import statistics as st
from pathlib import Path
from pprint import pp

BASE = Path(__file__).parent.resolve() / 'results/'
loss30_delay250_cpu2 = BASE / 'series/loss30-delay250-cpu2/'
loss10_delay500_cpu4_part1 = BASE / 'uniform-1000-20260913'
loss10_delay500_cpu4_part2 = BASE / 'recovery-20260912'

def get_measurements(path):
    """
    Extract the ChaosMeasurementPhase and PreChaosMeasurementPhase from one prometheus result file and format 
    the data for further processing.
    """
    def format_phase(phase):
                # Let values stabilize
                phase = phase[12:]
                # Remove broken measurements
                phase = [row for row in phase if row['input_lag'] != '']
                phase = [{**row, 
                          'input_lag': float(row['input_lag']), 
                          'time': float(row['time'])
                         } for row in phase]
                return phase
    
    with open(path, 'r') as f:
        row = list(csv.DictReader(f))
        chaos_phase = [r for r in row if r['subphase'] == 'ChaosMeasurement']
        pre_chaos_phase = [r for r in row if r['subphase'] == 'PreChaosMeasurement']
        chaos_phase = sorted(chaos_phase, key=lambda x: float(x['time']))
        pre_chaos_phase = sorted(pre_chaos_phase, key=lambda x: float(x['time']))
        chaos_phase = format_phase(chaos_phase)
        pre_chaos_phase = format_phase(pre_chaos_phase)
        return pre_chaos_phase, chaos_phase


def get_mean(phase: list[dict]):
    return st.mean([float(r['input_lag']) for r in phase if r['input_lag'] != ''])


def get_delta_mean(phases):
    pre_chaos_phase, chaos_phase = phases
    return get_mean(chaos_phase) - get_mean(pre_chaos_phase)


def get_median_lag(use_case: str, fault: str):
    paths = get_paths(use_case, fault)
    means = [get_delta_mean(get_measurements(path)) for path in paths]
    means.sort()
    return (means[1] + means[2]) / 2


def get_median_slope(use_case: str, fault: str):
    paths = get_paths(use_case, fault)
    means = [get_delta_slope(get_measurements(path)) for path in paths]
    means.sort()
    return (means[1] + means[2]) / 2


def get_slope(phase):
    """
    Get Slope via simple ordinary least squares regression.
    """
    slope, _ = st.linear_regression([row['time'] for row in phase], [row['input_lag'] for row in phase])
    return slope


def get_delta_slope(phases):
    pre_chaos_phase, chaos_phase = phases
    return get_slope(chaos_phase) - get_slope(pre_chaos_phase)


def get_standard_deviations(use_case: str, fault: str):
    """
    Get Standard Deviation for the change in mean lag and change in mean slope.
    """
    paths = get_paths(use_case, fault)
    mean_values = [get_delta_mean(get_measurements(path)) for path in paths]
    slope_values = [get_delta_slope(get_measurements(path)) for path in paths]
    return st.stdev(slope_values), st.stdev(mean_values)


def get_paths(use_case: str, fault: str):
    """
    Get paths of the result measurements. They're in weird positions due to connection issues and resumed measurements.
    """
    if fault == 'packet-loss-30' or fault == 'network-delay-250' or fault == 'cpu-stress-2':
        # Remove - fault strength at the end
        fault = '-'.join(fault.split('-')[:2])
        return [loss30_delay250_cpu2 / '{}-{}-r{}/results/samples-prometheus.csv'.format(use_case, fault, round) for round in list(range(1,5))]
    fault = '-'.join(fault.split('-')[:2])
    if use_case == 'uc3':
        return [loss10_delay500_cpu4_part2 / '{}-{}-r{}/results/samples-prometheus.csv'.format(use_case, fault, round) for round in list(range(1,5))]
    return [loss10_delay500_cpu4_part1 / '{}-{}-r{}/results/samples-prometheus.csv'.format(use_case, fault, round) for round in list(range(1,5))]

def main():
    faults = ['cpu-stress-2', 'cpu-stress-4', 'network-delay-250', 'network-delay-500', 'packet-loss-10',
              'packet-loss-30']
    use_cases = ['uc1', 'uc2', 'uc3', 'uc4']
    summary = []
    for uc in use_cases:
        for fault in faults:
            summary.append({'use_case': uc,
                            'fault': fault,
                            'median-lag': get_median_lag(uc, fault),
                            'median-slope': get_median_slope(uc, fault),
                            'sd-lag': get_standard_deviations(uc, fault)[1],
                            'sd-slope': get_standard_deviations(uc, fault)[0]})
    pp(summary)


if __name__ == '__main__':
    main()
