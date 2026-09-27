# Selected measurements

This directory contains exactly the 96 runs used in the thesis: four use cases,
three faults, four repetitions, in each of two parameter series.

- `uniform-1000-20260913/`: original UC1, UC2 and UC4 runs.
- `recovery-20260912/`: original UC3 runs.
- `series/loss30-delay250-cpu2/`: all four use cases in the second series.
- `selected-96.json`: complete selection and original directory names.

Original settings are 10% loss, 500ms delay and four CPU workers. The second
series uses 30% loss, 250ms delay and two CPU workers. Delay jitter is 100ms;
CPU load is 100. Faults were injected separately.

