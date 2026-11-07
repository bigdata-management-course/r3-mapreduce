# Recitation 3: MapReduce, by hand and on Hadoop

The code of the week-3 practice session. Seven Lecture 3 terms (map and reduce functions, shuffle and
sort, the partitioner, the combiner, re-execution, HDFS blocks, input splits and locality), each one
first seen in `minimr.py`, a MapReduce engine in 300 lines of Python, then found again in the counters
of a real Hadoop job. In class you change one thing per term and record what you see; at home you answer
new questions with the same tools. `worksheet.pdf` has everything. Not graded.

**Before the session:** do [BEFORE-CLASS.md](BEFORE-CLASS.md). The Hadoop image build and the Maven
download take 10-20 minutes and cannot happen in class.

## Setup

You need Python 3.9+, git, Docker, and a clone of the course cluster `bigdata-management-course/hadoop-cluster` (the same
cluster as Homework 1; its `INSTALL.md` covers macOS, Windows and Linux). No Python packages.

The code is in the public repository `bigdata-management-course/r3-mapreduce`: no GitHub login, no
invitation. Clone and run:

```sh
git clone https://github.com/bigdata-management-course/r3-mapreduce.git
cd r3-mapreduce
python3 check.py
```

The last three lines are

```
setup:    FAIL
in class: c1 FAIL  c2 FAIL  c3 FAIL  c4 FAIL  c5 FAIL  c6 FAIL  c7 FAIL
at home:  h1 FAIL  h2 FAIL  h3 FAIL  h4 FAIL  h5 FAIL  h6 FAIL  h7 FAIL
```

`FAIL` only means "not done yet". `setup` turns `PASS` once `python3 check.py setup` has found a healthy
cluster (BEFORE-CLASS.md). On Windows, work inside WSL2 (Ubuntu), as for the lab.

## Working on an item

1. Make the change the slide or `worksheet.pdf` asks for (a flag of `minimr.py`, one line of
   `wordcount.py`, or a Hadoop command) and run it.
2. Write the value you saw into `observations.py` and check it: `python3 check.py c2` (or `h1 h2`, or
   nothing for all). The checker compares digests and never shows the expected value.
3. Undo the change: `git checkout wordcount.py`. `check.py` warns if a shipped file still differs.
4. Rerun `python3 check.py` and read its PASS/FAIL lines: that is your feedback. Your answers stay in
   your own copy; there is nothing to push or submit.

Hadoop commands run inside the namenode container. Define once per terminal:

```sh
NN="docker exec -i hadoop-lab-namenode-1"
$NN hdfs dfs -ls          # your HDFS home, /user/hadoop
```

## Files

| File | What it is |
|---|---|
| `observations.py` | Your observations: one value per item. |
| `minimr.py` | The engine: splits, map tasks with sorted spills, partitioner, combiner, shuffle, reduce, re-execution, Streaming mode. `python3 minimr.py --help`. |
| `wordcount.py` | The job: `mapper()` and `reducer()`. Runs unchanged on minimr, in a Unix pipe, and on Hadoop Streaming. |
| `invindex.py` | A second job: inverted index, word -> books (Lecture 3, Problem 1). |
| `partitioners.py` | `alphabet`, the same range partitioner as `AlphabetPartitioner.java`. |
| `peek.py` | Runs one map task by hand and shows the files it leaves on disk. |
| `data/` | The three books of the lab, 7.4 MB. |
| `check.py` | Self-checks: `python3 check.py [setup c1-c7 h1-h7 ...]`. |
| `worksheet.pdf` | Every item: what to change, what to run, what you should see, hints. |

Outputs (`out-*`) are ignored by git; delete them whenever you like.
