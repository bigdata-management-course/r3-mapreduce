"""What you observed. Replace each None with the value your run printed, then run check.py.

Write numbers as plain numbers (1336289, not "1,336,289") and lists as lists ([1, 2, 3]).
worksheet.pdf says, for each one, which command to run and what to change.
"""

# === IN CLASS: "Your turn", one per Lecture 3 term ===========================

# 1. Map and reduce functions: wordcount.py mapper emits len(word) instead of word
LENGTHS = None              # "reduce output records": how many distinct word lengths
TOP_LENGTH = None           # the word length that occurs most often

# 2. Shuffle and sort: map | reduce on moby-dick.txt, with and without the sort
LINES_UNSORTED = None  # lines of the output without the sort

# 3. Partitioner: minimr with --partitioner partitioners:alphabet --reducers 4
ALPHA_PER_REDUCER = None    # "records per reducer", the whole list

# 4. Combiner: pride-and-prejudice.txt alone, without and with --combiner
PRIDE_SHUFFLE = None        # "reduce shuffle bytes" without the combiner
PRIDE_SHUFFLE_COMBINED = None  # "reduce shuffle bytes" with the combiner

# 5. Re-execution: minimr with --fail-rate 0.5
FAILED_SEED5 = None         # "failed map attempts" with --seed 5
GAVE_UP_MAP = None          # with --seed 23: the number of the map task the job gave up on

# 6. HDFS blocks: the three books put from stdin with -D dfs.blocksize=1m
BLOCKS_1M = None            # fsck "Total blocks (validated)" of books1m

# 7. Splits and locality: Java WordCount on books1m
LAUNCHED_MAPS_1M = None     # job counter "Launched map tasks"
SHUFFLE_1M = None           # job counter "Reduce shuffle bytes"

# === AT HOME: the same tools, new questions ===================================

IN_ALL_THREE = None         # 1. words whose inverted-index entry lists all three books
SPILLS = None  # 2. the job's "spills" counter with --spill-records 30000
PARTS_3 = None              # 3. sizes in bytes of part-r-00000..00002, Java WordCount with 3 reducers
COMBINE_IN_SMALL = None  # 4. "combine input records" with --combiner --spill-records 30000
FIRST_FAILING_SEED = None   # 5. with --fail-rate 0.5: the smallest seed (0, 1, 2, ...) whose job fails
LAST_BLOCK_1M = None        # 6. length in bytes of the last block of books1m/shakespeare.txt
SLOT_RATIO = None  # 7. 1024 MB maps: "occupied slots" map time / map time, whole number
