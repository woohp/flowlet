# Benchmarks

- `ab.py` checks a change for regressions. It loads `src/flowlet/functional.py` from two revisions side by side and runs them interleaved in one process, so machine drift hits both equally.

  ```sh
  uv run python benchmark/ab.py              # HEAD vs working tree
  uv run python benchmark/ab.py master HEAD  # any two revisions
  ```

- `compare.py` puts flowlet beside native executors/asyncio and [parlane](https://github.com/SuitaShigeo/parlane), on parlane's own scenarios plus no-op cases that isolate per-item overhead.

  ```sh
  uv run --with parlane python benchmark/compare.py
  ```

Expect flowlet within a few percent of native whenever items do real work. On no-op items, the gap is the cost of each item crossing between the event loop and a thread (compare against `asyncio run_in_executor`, not `concurrent.futures`), plus flowlet's task-per-item feeder.
