# Before Monday 16 November: get the cluster ready (at home)

In the session we run real Hadoop jobs on your laptop, on the course cluster: the public repository
`bigdata-management-course/hadoop-cluster`. Building its Hadoop image downloads about 600 MB and the first Maven build about
260 MB. Neither can happen in class on shared Wi-Fi.

## 1. Install and smoke-test

Follow `INSTALL.md` in the cluster repository, https://github.com/bigdata-management-course/hadoop-cluster/blob/main/INSTALL.md,
sections 1-5: Docker (macOS, Windows with WSL2, or Linux), git, the GitHub CLI, then

```sh
git clone https://github.com/bigdata-management-course/hadoop-cluster.git
cd hadoop-cluster
make up          # first time: builds the image, then starts 6 containers (~20 s)
make jar         # first time: downloads the Hadoop client jars, builds wordcount.jar
make put-data    # the three books into HDFS
make java        # smoke test: a real WordCount job, ~20 s
```

If you did this in the 9 November session, `make up put-data` is all you need now.

`make java` must end with the ten most frequent words, starting with

```
--- top 10 words (Java) ---
the     49594
```

## 2. Record that you are ready

With the cluster still running, clone the public recitation repository (no GitHub login needed) and run the check:

```sh
git clone https://github.com/bigdata-management-course/r3-mapreduce.git
cd r3-mapreduce
python3 check.py setup
```

`check.py setup` must print `setup:    PASS`. It also writes `setup.json`, which later runs of `check.py`
read.

## 3. Stop the cluster

```sh
cd hadoop-cluster && make down
```

This frees the memory and erases HDFS; the image and the jar stay. In class, `make up put-data` takes
under a minute.

## If something fails

More cases, including Windows and WSL problems, are in INSTALL.md section 6.

| Symptom | Fix |
|---|---|
| `make up` hangs or a container is `unhealthy` | Docker is short of memory: INSTALL.md section 6, then `make down` and `make up` |
| `port is already allocated` (9870, 8088, 19888) | another program uses the port; stop it, or an old cluster: `make down` |
| `make jar` fails with a network error | retry on a better connection; the download is cached in `~/.m2` |
| `check.py setup`: `wordcount.jar: FAIL` | run `make jar` in `hadoop-cluster`, with the cluster up |
| `check.py setup`: `cluster healthy: FAIL ... not running` | `make up` in `hadoop-cluster` first |
| Apple Silicon: image build fails on `curl` | retry; the Apache mirror sometimes refuses, the build falls back to archive.apache.org |

Still stuck? Post the error on the Moodle forum before Monday, or come 10 minutes early.
