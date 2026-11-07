#!/usr/bin/env python3
"""Run one map task by hand and look at what it leaves on disk."""
import os
import shutil
import sys

import minimr

DATA = "data"
work = "peek.work"
shutil.rmtree(work, ignore_errors=True)
os.makedirs(f"{work}/_tmp")
books = [f"{DATA}/{b}.txt" for b in ("moby-dick", "pride-and-prejudice")]
splits = minimr.make_splits(books, 1 << 20)
print("splits:", *splits, sep="\n  ")

opts = dict(job="wordcount", work=work, reducers=4, combiner=False,
            partitioner="crc32", fail_rate=0.0, seed=0,
            spill_records=int(sys.argv[1]) if len(sys.argv) > 1 else 250_000)
counters = minimr.map_task((0, 1, splits[0], opts))
print("\ncounters of map task 0:", dict(counters))
for name in sorted(os.listdir(f"{work}/map-0000")):
    path = f"{work}/map-0000/{name}"
    print(f"{path}: {os.path.getsize(path):,} bytes")
print("\nfirst records of part0 (sorted by key):")
with open(f"{work}/map-0000/part0") as f:
    for _ in range(4):
        print(" ", f.readline().rstrip())
shutil.rmtree(work)
