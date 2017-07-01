from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
import os

if __name__ == "__main__":
    # Start SparkContext
    sc = SparkContext(appName="PythonWordCount")

    # Load data from S3 bucket
    lines = sc.textFile('s3n://emr-related-files/words.txt', 1)
    # Calculate word counts
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    # Print word counts
    for (word, count) in output:
        print("%s: %i" % (word, count))
    # Save word counts in S3 bucket
    counts.saveAsTextFile("s3n://emr-related-files/output.txt")
    # Stop SparkContext
    sc.stop()
