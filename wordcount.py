#!/usr/bin/env python3
"""Word count, for minimr and (unchanged) for Hadoop Streaming."""
import re

WORD = re.compile(r"[a-z]+")  # same tokenizer as labs/hadoop


def mapper(line):
    for word in WORD.findall(line.lower()):
        yield word, 1


def reducer(word, counts):
    yield word, sum(counts)


combiner = reducer  # sum is associative and commutative

if __name__ == "__main__":
    import os
    import sys
    # YARN symlinks -files into the cwd; sys.path[0] follows the link.
    sys.path.insert(0, os.getcwd())
    from minimr import streaming
    streaming(mapper, reducer, combiner)
