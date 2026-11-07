#!/usr/bin/env python3
"""minimr: a MapReduce engine in plain Python (Recitation 3).

    python3 minimr.py wordcount ../../../labs/hadoop/data/*.txt --out out-wc
    python3 wordcount.py map < f | sort | python3 wordcount.py reduce
"""
import argparse
import hashlib
import heapq
import importlib
import json
import os
import random
import shutil
import sys
import time
import zlib
from collections import Counter
from itertools import groupby
from multiprocessing import Pool

MAX_ATTEMPTS = 4  # mapreduce.map.maxattempts


class InjectedCrash(RuntimeError):
    pass


# ---------- 1. input splits ----------
def make_splits(paths, split_size):
    splits = []
    for p in paths:
        size = os.path.getsize(p)
        for start in range(0, size, split_size):
            splits.append((p, start, min(start + split_size, size)))
    return splits


def read_split(path, start, end):
    """Lines that start in (start, end]; split 0 also gets line 0."""
    with open(path, "rb") as f:
        f.seek(start)
        if start:
            f.readline()  # partial line: the previous split owns it
        while f.tell() <= end:
            line = f.readline()
            if not line:
                break
            yield line.decode("utf-8", "replace").rstrip("\n")


# ---------- 2. partitioning ----------
def hash_partition(key, n):
    # Not hash(): it is salted per process, so workers would disagree.
    return zlib.crc32(json.dumps(key).encode()) % n


def builtin_partition(key, n):
    return hash(key) % n


PARTITIONERS = {"crc32": hash_partition, "builtin": builtin_partition}


def resolve_partitioner(name):
    """'crc32', 'builtin' or 'module:function' for your own partitioner."""
    if ":" not in name:
        return PARTITIONERS[name]
    mod, fn = name.split(":")
    return getattr(importlib.import_module(mod), fn)


# ---------- 3. spill files: sorted runs of JSON lines ----------
def encode(k, v):
    return (json.dumps([k, v]) + "\n").encode()


def write_run(path, records):
    nbytes = 0
    with open(path, "wb") as f:
        for k, v in records:
            b = encode(k, v)
            f.write(b)
            nbytes += len(b)
    return nbytes


def read_run(path):
    with open(path, "rb") as f:
        for line in f:
            k, v = json.loads(line)
            yield (tuple(k) if isinstance(k, list) else k), v


def by_key(kv):
    return kv[0]


def combine(records, combiner, counters):
    """records: sorted (k, v) pairs; apply the combiner per key."""
    for k, grp in groupby(records, key=by_key):
        vals = [v for _, v in grp]
        counters["combine input records"] += len(vals)
        for out in combiner(k, iter(vals)):
            counters["combine output records"] += 1
            yield out


# ---------- 4. map task ----------
def map_task(spec):
    m, attempt, split, opts = spec
    job = importlib.import_module(opts["job"])
    path, start, end = split
    os.environ["mapreduce_map_input_file"] = path  # as Hadoop Streaming does
    combiner = getattr(job, "combiner", None) if opts["combiner"] else None
    partition = resolve_partitioner(opts["partitioner"])
    n = opts["reducers"]
    rng = random.Random(f"{opts['seed']}/{m}/{attempt}")
    crash = rng.random() < opts["fail_rate"]
    crash_at = rng.randint(1, 1000) if crash else None

    tmp = os.path.join(opts["work"], "_tmp", f"map-{m:04d}-attempt-{attempt}")
    os.makedirs(tmp)
    c = Counter()
    buf = [[] for _ in range(n)]
    runs = 0

    def spill():
        nonlocal runs
        for r in range(n):
            buf[r].sort(key=by_key)
            spill_path = os.path.join(tmp, f"spill{runs}-part{r}")
            if combiner:
                c["map output bytes"] += sum(len(encode(*x)) for x in buf[r])
                write_run(spill_path, combine(buf[r], combiner, c))
            else:
                c["map output bytes"] += write_run(spill_path, buf[r])
            buf[r] = []
        runs += 1

    def emit(pairs):
        for k, v in pairs:
            buf[partition(k, n)].append((k, v))
            c["map output records"] += 1

    for line in read_split(path, start, end):
        c["map input records"] += 1
        if c["map input records"] == crash_at:
            raise InjectedCrash(f"map {m} attempt {attempt} crashed")
        emit(job.mapper(line))
        if sum(map(len, buf)) >= opts["spill_records"]:
            spill()
    emit(getattr(job, "cleanup", lambda: ())())  # flush per-task state
    spill()
    c["spills"] += runs
    # One sorted run per partition, as Hadoop merges before the shuffle.
    for r in range(n):
        spills = [f"{tmp}/spill{s}-part{r}" for s in range(runs)]
        if runs == 1:
            os.rename(spills[0], os.path.join(tmp, f"part{r}"))
            continue
        merged = heapq.merge(*map(read_run, spills), key=by_key)
        if combiner:
            merged = combine(merged, combiner, c)
        write_run(os.path.join(tmp, f"part{r}"), merged)
        for f in spills:
            os.remove(f)
    final = os.path.join(opts["work"], f"map-{m:04d}")
    try:
        os.rename(tmp, final)  # commit: one attempt becomes visible
    except OSError:
        shutil.rmtree(tmp)  # another attempt already committed
    return c


# ---------- 5. reduce task ----------
def fmt(x):
    return x if isinstance(x, str) else json.dumps(x)


def reduce_task(spec):
    r, n_maps, opts = spec
    job = importlib.import_module(opts["job"])
    group = getattr(job, "group", None) or (lambda k: k)
    c = Counter()
    files = [os.path.join(opts["work"], f"map-{m:04d}", f"part{r}")
             for m in range(n_maps)]
    c["reduce shuffle bytes"] = sum(os.path.getsize(f) for f in files)
    stream = heapq.merge(*(read_run(f) for f in files), key=by_key)
    tmp = os.path.join(opts["out"], f"_tmp-part-r-{r:05d}")
    with open(tmp, "w") as out:
        for gk, grp in groupby(stream, key=lambda kv: group(kv[0])):
            c["reduce input groups"] += 1

            def values(grp=grp):
                for _, v in grp:
                    c["reduce input records"] += 1
                    yield v
            for k, v in job.reducer(gk, values()):
                out.write(f"{fmt(k)}\t{fmt(v)}\n")
                c["reduce output records"] += 1
    os.rename(tmp, os.path.join(opts["out"], f"part-r-{r:05d}"))
    return r, c


# ---------- 6. the master ----------
def run(job, inputs, out, reducers=4, workers=4, split_size=1 << 20,
        combiner=False, partitioner="crc32", fail_rate=0.0, seed=0,
        spill_records=250_000):
    opts = dict(job=job, out=out, work=out + ".work", reducers=reducers,
                combiner=combiner, partitioner=partitioner,
                fail_rate=fail_rate, seed=seed, spill_records=spill_records)
    for d in (out, opts["work"]):
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(os.path.join(opts["work"], "_tmp"))
    os.makedirs(out)
    splits = make_splits(inputs, split_size)
    counters = Counter(splits=len(splits))
    per_reducer = [0] * reducers
    t0 = time.perf_counter()
    with Pool(workers) as pool:
        attempts = {m: 1 for m in range(len(splits))}

        def submit(m):
            spec = (m, attempts[m], splits[m], opts)
            return pool.apply_async(map_task, (spec,))
        pending = {m: submit(m) for m in attempts}
        while pending:
            for m, res in list(pending.items()):
                if not res.ready():
                    continue
                del pending[m]
                try:
                    counters.update(res.get())
                except InjectedCrash as e:
                    counters["failed map attempts"] += 1
                    if attempts[m] == MAX_ATTEMPTS:
                        raise RuntimeError(f"job failed: {e}") from None
                    print(f"{e}: re-executing")
                    attempts[m] += 1  # re-execute from the input split
                    pending[m] = submit(m)
            time.sleep(0.005)
        specs = [(r, len(splits), opts) for r in range(reducers)]
        for r, c in pool.imap_unordered(reduce_task, specs):
            counters.update(c)
            per_reducer[r] = c["reduce input records"]
    counters["wall ms"] = round((time.perf_counter() - t0) * 1000)
    shutil.rmtree(opts["work"])
    return counters, per_reducer


def digest(out):
    """sha256 over the sorted output lines: equal digests = equal results."""
    lines = []
    for name in sorted(os.listdir(out)):
        with open(os.path.join(out, name)) as f:
            lines += f.readlines()
    return hashlib.sha256("".join(sorted(lines)).encode()).hexdigest()[:16]


# ---------- 7. Streaming: the same functions behind stdin/stdout ----------
def streaming(mapper, reducer, combiner=None, cleanup=lambda: ()):
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    mode = sys.argv[1] if len(sys.argv) == 2 else None
    if mode == "map":
        for line in sys.stdin:
            for k, v in mapper(line.rstrip("\n")):
                sys.stdout.write(f"{fmt(k)}\t{json.dumps(v)}\n")
        for k, v in cleanup():
            sys.stdout.write(f"{fmt(k)}\t{json.dumps(v)}\n")
    elif mode in ("combine", "reduce"):
        dumps = json.dumps
        fn, out = (reducer, fmt) if mode == "reduce" else (combiner, dumps)
        pairs = (line.rstrip("\n").split("\t", 1) for line in sys.stdin)
        for k, grp in groupby(pairs, key=lambda kv: kv[0]):
            for k2, v in fn(k, (json.loads(v) for _, v in grp)):
                sys.stdout.write(f"{fmt(k2)}\t{out(v)}\n")
    else:
        sys.exit("usage: map | combine | reduce")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("job", help="module with mapper() and reducer(); "
                    "optional combiner(), group(), cleanup()")
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("--out", required=True)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--split-kb", type=int, default=1024)
    ap.add_argument("--combiner", action="store_true")
    ap.add_argument("--partitioner", default="crc32",
                    help="crc32, builtin or module:function")
    ap.add_argument("--fail-rate", type=float, default=0.0,
                    help="probability that a map attempt crashes")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--spill-records", type=int, default=250_000,
                    help="map buffer size, in records")
    a = ap.parse_args()
    try:
        counters, per_reducer = run(
            a.job, a.inputs, a.out, a.reducers, a.workers, a.split_kb * 1024,
            a.combiner, a.partitioner, a.fail_rate, a.seed, a.spill_records)
    except RuntimeError as e:
        sys.exit(str(e))
    for k, v in counters.items():
        print(f"{k:<24}{v:>12,}")
    print(f"{'records per reducer':<24}{per_reducer}")
    print(f"{'output sha256':<24}{digest(a.out):>12}")


if __name__ == "__main__":
    main()
