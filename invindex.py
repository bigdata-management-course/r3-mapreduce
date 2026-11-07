#!/usr/bin/env python3
"""Inverted index: word -> sorted documents (Lecture 3, Problem 1)."""
import os
import re

WORD = re.compile(r"[a-z]+")


def mapper(line):
    path = os.environ["mapreduce_map_input_file"]
    doc = os.path.basename(path).removesuffix(".txt")
    for word in set(WORD.findall(line.lower())):
        yield word, doc


def reducer(word, docs):
    yield word, sorted(set(docs))


def combiner(word, docs):
    for doc in set(docs):  # dedup only: output stays (word, doc)
        yield word, doc


if __name__ == "__main__":
    import sys
    # YARN symlinks -files into the cwd; sys.path[0] follows the link.
    sys.path.insert(0, os.getcwd())
    from minimr import streaming
    streaming(mapper, reducer, combiner)
