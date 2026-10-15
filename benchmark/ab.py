"""Compare `functional.py` at two revisions, interleaved in one process.

    uv run python benchmark/ab.py [BASE [TARGET]] [--rounds N]

BASE defaults to HEAD and TARGET to the working tree. Both versions load side by
side as standalone modules and take turns, so machine drift hits them equally.
Each cell is the best of `--rounds` runs.
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
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("base", nargs="?", default="HEAD")
    parser.add_argument(
        "target", nargs="?", help="git revision (default: working tree)"
    )
    parser.add_argument("--rounds", type=int, default=11)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        base = (args.base, load(args.base, Path(tmp) / "base.py"))
        target = (args.target or "worktree", load(args.target, Path(tmp) / "target.py"))
        asyncio.run(run(base, target, args.rounds))


def load(rev: str | None, scratch: Path) -> ModuleType:
    """Load `functional.py` from git `rev` (via `scratch`), or the worktree if None."""
    path = ROOT / SOURCE
    if rev is not None:
        path = scratch
        path.write_bytes(
            subprocess.run(
                ["git", "show", f"{rev}:{SOURCE}"],
                cwd=ROOT,
                check=True,
                capture_output=True,
            ).stdout
        )
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cases(f: ModuleType, ordered: bool) -> dict[str, Case]:
    return {
        "map c1": lambda: f.collect(
            f.map(lambda x: x, concurrency=1, ordered=ordered)(range(N))
        ),
        "map c4": lambda: f.collect(
            f.map(lambda x: x, concurrency=4, ordered=ordered)(range(N))
        ),
        "filter c4": lambda: f.collect(
            f.filter(lambda x: x % 2 == 0, concurrency=4, ordered=ordered)(range(N))
        ),
        "flat_map c4": lambda: f.collect(
            f.flat_map(lambda x: [x] * 5, concurrency=4, ordered=ordered)(range(N // 5))
        ),
    }


async def run(
    base: tuple[str, ModuleType], target: tuple[str, ModuleType], rounds: int
) -> None:
    versions = (base, target)
    for ordered in (False, True):
        best: dict[tuple[str, int], float] = {}
        for round_ in range(rounds):
            # Alternate who goes first, so neither version always gets the same slot.
            for index in (0, 1) if round_ % 2 == 0 else (1, 0):
                for case, build in cases(versions[index][1], ordered).items():
                    start = time.perf_counter()
                    await build()
                    elapsed = time.perf_counter() - start
                    best[case, index] = min(best.get((case, index), elapsed), elapsed)

        print(f"--- {'ordered' if ordered else 'unordered'} (best of {rounds}) ---")
        for case in cases(base[1], ordered):
            b, t = best[case, 0], best[case, 1]
            before = f"{base[0]}={b * 1e3:7.1f}ms"
            after = f"{target[0]}={t * 1e3:7.1f}ms"
            print(f"{case:<12} {before}  {after}  {(t / b - 1) * 100:+5.1f}%")


if __name__ == "__main__":
    main()
