# Theodolite Fault Benchmarking

This repository contains the experiment scripts, Kubernetes configurations and
96 measurement runs used in the bachelor thesis. It studies input consumer lag
in four Theodolite Kafka Streams use cases under packet loss, network delay and
CPU stress. EcoScape controls the experiments, and Chaos Mesh injects the faults.

## Contents

- `evaluate.py`: calculates the statistics used in the thesis tables.
- `measure.py`: prepares, runs, collects and cleans up individual experiments.
- `series.py`: prepares and runs four pilots followed by 48 measurement runs.
- `kubernetes/`: workload manifests, fault scenarios, monitoring and experiment templates.
- `results/`: the 96 selected runs with their configurations and quality reports.
  See [results/README.md](results/README.md) for the two parameter series and
  [selected-96.json](results/selected-96.json) for the complete selection.

The workload manifests are adapted from Theodolite. Application and load-generator
images use version `v0.10.3`.

## Evaluate the measurements

Use Python 3.10 or newer. No extra Python packages or cluster access are needed.
Run this command from the repository root:

```sh
python3 evaluate.py
```

The script prints the median and sample standard deviation of the change in mean
lag and OLS slope for each configuration. Changes are calculated per run before
combining the four repetitions. For the thesis tables, round lag values to whole
messages and slopes to one decimal place.

The script uses the saved `samples-prometheus.csv` files. It skips the first
60 seconds of each phase using the five-second sample grid and excludes empty lag
values. It does not repeat the full quality checks stored with the measurements.
The results describe changes between baseline and fault phases, not isolated
causal effects. No separate no-fault control runs are included.

## Prepare or run experiments

The runner requires an existing Kubernetes cluster with EcoScape, Chaos Mesh,
Kafka with Kafka Exporter, Schema Registry and Prometheus Operator. Running
experiments also requires `kubectl` and access to the cluster and Prometheus.
The included addresses, selectors and API safety check target the original
cluster and must be reviewed before use elsewhere.

Prepare one experiment offline:

```sh
python3 measure.py prepare 1 network-delay runs/uc1-delay-r1
```

Inspect the generated files before starting it:

```sh
python3 measure.py run runs/uc1-delay-r1
```

Use `python3 measure.py --help` and `python3 series.py --help` for further commands.
Run only one experiment or series at a time. Stopping a local runner does not
necessarily stop an experiment already running in Kubernetes.
