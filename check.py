"""Self-checks: python3 check.py [setup c1-c7 h1-h7 ...]  (default: everything but a live setup check).

c = in class ("Your turn"), h = at home. "setup" checks your laptop and the Hadoop cluster and
records the result in setup.json, which later checks read.
Exit status is 0 even when checks fail; it is 1 only if observations.py cannot be imported.
"""
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

import observations as obs

HERE = Path(__file__).resolve().parent
GROUPS = {"in class:": "c1 c2 c3 c4 c5 c6 c7", "at home:": "h1 h2 h3 h4 h5 h6 h7"}
# Digests (salted with the item name) of the correct observations on the three books in data/.
EXPECTED = {
    "LENGTHS": "0fb9981940d9",
    "TOP_LENGTH": "b238a781be83",
    "SPILLS": "c9eb423dd719",
    "ALPHA_PER_REDUCER": "087f211376c3",
    "PRIDE_SHUFFLE": "6da123ac5149",
    "PRIDE_SHUFFLE_COMBINED": "ed55975b9e1e",
    "FAILED_SEED5": "5d68e6157b71",
    "GAVE_UP_MAP": "96448915023c",
    "BLOCKS_1M": "9b4503dc383e",
    "LAUNCHED_MAPS_1M": "31268afe2b9d",
    "SHUFFLE_1M": "641a93505434",
    "IN_ALL_THREE": "2076dde5af16",
    "LINES_UNSORTED": "98c6ecb4013d",
    "PARTS_3": "8fd29e74be5f",
    "COMBINE_IN_SMALL": "d3770eadc472",
    "FIRST_FAILING_SEED": "52a21a80a3d7",
    "LAST_BLOCK_1M": "03bc52645f7d",
    "SLOT_RATIO": "a9d9bff82e68",
}
# First 12 hex digits of the sha1 of each file a Your turn may change, as shipped.
PRISTINE = {
    "minimr.py": "6c1722841fe2",
    "wordcount.py": "64930656c67f",
    "partitioners.py": "92da0720b9d6",
    "data/moby-dick.txt": "8e7a0efc964d",
    "data/pride-and-prejudice.txt": "a7c062daa698",
    "data/shakespeare.txt": "4783ff1befea",
}
CHECKS = {
    "c1": ["LENGTHS", "TOP_LENGTH"], "c2": ["LINES_UNSORTED"], "c3": ["ALPHA_PER_REDUCER"],
    "c4": ["PRIDE_SHUFFLE", "PRIDE_SHUFFLE_COMBINED"], "c5": ["FAILED_SEED5", "GAVE_UP_MAP"],
    "c6": ["BLOCKS_1M"], "c7": ["LAUNCHED_MAPS_1M", "SHUFFLE_1M"],
    "h1": ["IN_ALL_THREE"], "h2": ["SPILLS"], "h3": ["PARTS_3"], "h4": ["COMBINE_IN_SMALL"],
    "h5": ["FIRST_FAILING_SEED"], "h6": ["LAST_BLOCK_1M"], "h7": ["SLOT_RATIO"],
}
NAMENODE = "hadoop-lab-namenode-1"
SERVICES = ["namenode", "datanode1", "datanode2", "resourcemanager", "nodemanager", "historyserver"]
JAR = "/lab/wordcount-java/target/wordcount.jar"
SETUP = HERE / "setup.json"


def sha1(path):
    return hashlib.sha1(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()[:12]


def digest(name, value):
    # The name salts the hash, so two items with the same answer never share a digest.
    return hashlib.sha256((name + json.dumps(value, sort_keys=True)).encode()).hexdigest()[:12]


def observed(name):
    value = getattr(obs, name)
    if value is None:
        print(f"  {name}: not filled in yet")
        return False
    ok = digest(name, value) == EXPECTED[name]
    print(f"  {name} = {value!r}: {'ok' if ok else 'WRONG'}")
    return ok


def warn_if_changed():
    """Your-turn edits must be undone, or later numbers shift."""
    for name, sha in PRISTINE.items():
        p = HERE / name
        if not p.exists() or sha1(p) != sha:
            print(f"WARNING: {name} differs from the original: undo with 'git checkout {name}'")


def sh(*cmd, timeout=60):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout + r.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as e:
        return 1, str(e)


def setup_live():
    """Everything the session needs: Python, Docker, a healthy cluster, the jar, HDFS."""
    res = {"checked": time.strftime("%Y-%m-%d %H:%M"), "python": platform.python_version()}
    steps = [("python 3.9+", sys.version_info >= (3, 9), res["python"])]
    ok = shutil.which("docker") is not None
    steps.append(("docker", ok, sh("docker", "--version")[1] if ok else "not on PATH"))
    if ok:
        health = {}
        for s in SERVICES:
            rc, out = sh("docker", "inspect", "-f", "{{.State.Health.Status}}", f"hadoop-lab-{s}-1")
            health[s] = out if rc == 0 else "not running"
        bad = [f"{s}: {h}" for s, h in health.items() if h != "healthy"]
        steps.append(("cluster healthy", not bad, ", ".join(bad) or f"{len(health)} containers"))
        rc, _ = sh("docker", "exec", NAMENODE, "test", "-f", JAR)
        steps.append(("wordcount.jar", rc == 0, JAR if rc == 0 else "missing: run 'make jar' in hadoop-cluster"))
        rc, out = sh("docker", "exec", NAMENODE, "hdfs", "dfs", "-ls", "/", timeout=120)
        steps.append(("hdfs", rc == 0, "reachable" if rc == 0 else (out.splitlines() or ["no answer"])[-1]))
    for label, passed, detail in steps:
        print(f"  {label}: {'ok' if passed else 'FAIL'} ({detail})")
    res["ok"] = all(p for _, p, _ in steps)
    res["steps"] = {label: bool(p) for label, p, _ in steps}
    SETUP.write_text(json.dumps(res, indent=2) + "\n")
    print("  wrote setup.json")
    return res["ok"]


def setup_recorded():
    if not SETUP.exists():
        print("  setup.json missing: run 'python3 check.py setup' with the cluster up")
        return False
    res = json.loads(SETUP.read_text())
    print(f"  recorded {res.get('checked')}: {'ready' if res.get('ok') else 'not ready'}")
    return bool(res.get("ok"))


def run(name):
    print(name)
    if name == "setup":
        return setup_live() if LIVE else setup_recorded()
    return all([observed(n) for n in CHECKS[name]])


if __name__ == "__main__":
    every = " ".join(GROUPS.values()).split()
    wanted = sys.argv[1:] or ["setup"] + every
    if not set(wanted) <= set(every) | {"setup"}:
        sys.exit("usage: python3 check.py [setup c1-c7 h1-h7 ...]")
    LIVE = "setup" in sys.argv[1:]
    warn_if_changed()
    res = {n: run(n) for n in wanted}
    if "setup" in res:
        print(f"{'setup:':10}{'PASS' if res['setup'] else 'FAIL'}")
    for label, group in GROUPS.items():
        row = [f"{n} {'PASS' if res[n] else 'FAIL'}" for n in group.split() if n in res]
        if row:
            print(f"{label:10}" + "  ".join(row))
