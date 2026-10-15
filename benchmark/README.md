# Benchmarks

- `ab.py` compares two versions of `src/flowlet/functional.py` for performance regressions. It alternates between them in one process to reduce machine drift.

  ```sh
  uv run python benchmark/ab.py              # HEAD vs. the working tree
  uv run python benchmark/ab.py master HEAD  # any two revisions
  ```

- `compare.py` times flowlet against native executors and asyncio. I/O- and CPU-bound scenarios measure real work; no-op cases isolate per-item overhead. Install [parlane](https://github.com/SuitaShigeo/parlane) to include it in the comparison.

  ```sh
  uv run python benchmark/compare.py
  ```

In our benchmark runs (M1 Pro, Python 3.14), flowlet took about 1.5–6% longer than native on the real-work scenarios. For no-op thread calls, most overhead comes from crossing between the event loop and a worker thread. Compare with `asyncio run_in_executor`, which pays that cost too, rather than `concurrent.futures`. Flowlet also pays for a feeder and a task per item.
