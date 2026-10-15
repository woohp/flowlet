"""Interleaved in-process A/B of `functional.py` between two revisions.

    uv run python benchmark/ab.py [BASE [TARGET]] [--rounds N]

BASE defaults to HEAD and TARGET to the working tree. Both versions are loaded
side by side as standalone modules and run in alternation, so machine drift
affects them equally; each cell reports the best of `--rounds` runs.
"""

import argparse
import asyncio
import importlib.util
import subprocess
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parent.parent
SOURCE = "src/flowlet/functional.py"
N = 50_000

type Case = Callable[[], Awaitable[object]]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("base", nargs="?", default="HEAD")
    parser.add_argument("target", nargs="?", help="git revision (default: working tree)")
    parser.add_argument("--rounds", type=int, default=11)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        versions = {
            args.base: load(args.base, Path(tmp) / "base.py"),
            args.target or "worktree": load(args.target, Path(tmp) / "target.py"),
        }
        asyncio.run(run(versions, args.rounds))


def load(rev: str | None, scratch: Path) -> ModuleType:
    """Load `functional.py` at git `rev` (written to `scratch`), or from the working tree when `rev` is None."""
    path = ROOT / SOURCE
    if rev is not None:
        path = scratch
        path.write_bytes(
            subprocess.run(["git", "show", f"{rev}:{SOURCE}"], cwd=ROOT, check=True, capture_output=True).stdout
        )
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cases(f: ModuleType, ordered: bool) -> dict[str, Case]:
    return {
        "map c1": lambda: f.collect(f.map(lambda x: x, concurrency=1, ordered=ordered)(range(N))),
        "map c4": lambda: f.collect(f.map(lambda x: x, concurrency=4, ordered=ordered)(range(N))),
        "filter c4": lambda: f.collect(f.filter(lambda x: x % 2 == 0, concurrency=4, ordered=ordered)(range(N))),
        "flat_map c4": lambda: f.collect(f.flat_map(lambda x: [x] * 5, concurrency=4, ordered=ordered)(range(N // 5))),
    }


async def run(versions: dict[str, ModuleType], rounds: int) -> None:
    base_name, target_name = versions
    for ordered in (False, True):
        best: dict[tuple[str, str], float] = {}
        for _ in range(rounds):
            for name, module in versions.items():
                for case, build in cases(module, ordered).items():
                    start = time.perf_counter()
                    await build()
                    elapsed = time.perf_counter() - start
                    best[case, name] = min(best.get((case, name), elapsed), elapsed)

        print(f"--- {'ordered' if ordered else 'unordered'} (best of {rounds}) ---")
        for case in cases(versions[base_name], ordered):
            b, t = best[case, base_name], best[case, target_name]
            print(
                f"{case:<12} {base_name}={b * 1e3:7.1f}ms  {target_name}={t * 1e3:7.1f}ms  {(t / b - 1) * 100:+5.1f}%"
            )


if __name__ == "__main__":
    main()
