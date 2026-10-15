# Benchmarks

- `ab.py` checks a change for regressions. It loads `src/flowlet/functional.py` from two revisions side by side and runs them in turns, in one process, so machine drift hits both equally.

  ```sh
  uv run python benchmark/ab.py              # HEAD vs. the working tree
  uv run python benchmark/ab.py master HEAD  # any two revisions
  ```

- `compare.py` times flowlet against native executors and asyncio, on I/O- and CPU-bound scenarios plus no-op cases that isolate per-item overhead. If [parlane](https://github.com/SuitaShigeo/parlane) is installed in the environment, its rows are included too.

  ```sh
  uv run python benchmark/compare.py
  ```

When items do real work, expect flowlet within a few percent of native. On no-op items, most of the gap is each item crossing between the event loop and a thread, so compare against `asyncio run_in_executor` rather than `concurrent.futures`. The rest is flowlet's task per item and its feeder.
