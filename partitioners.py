"""Partitioners for minimr: --partitioner partitioners:alphabet"""


def alphabet(key, n):
    # Same rule as AlphabetPartitioner.java in labs/hadoop.
    letter = max(0, min(25, ord(key[0]) - ord("a")))
    return letter * n // 26
