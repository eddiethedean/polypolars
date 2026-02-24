"""
Simple benchmarks for build_dataframe with and without chunk_size.

Run: python -m benchmarks.bench_build
Or: pytest benchmarks/ -v (if pytest-benchmark is installed).
"""

import sys
import time
from pathlib import Path
from typing import Optional

# Allow importing polypolars
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataclasses import dataclass

from polypolars import polars_factory


@polars_factory
@dataclass
class Row:
    id: int
    name: str
    value: float
    flag: bool


def bench_build(size: int, chunk_size: Optional[int] = None) -> float:
    start = time.perf_counter()
    if chunk_size:
        Row.build_dataframe(size=size, chunk_size=chunk_size)
    else:
        Row.build_dataframe(size=size)
    return time.perf_counter() - start


def main():
    sizes = [1_000, 10_000, 50_000]
    print("build_dataframe (no chunking)")
    for n in sizes:
        t = bench_build(n)
        print(f"  size={n:>6}  {t:.3f}s")
    print("build_dataframe (chunk_size=5000)")
    for n in sizes:
        t = bench_build(n, chunk_size=5000)
        print(f"  size={n:>6}  {t:.3f}s")


if __name__ == "__main__":
    main()
