# Mention Count Program
# Nathaniel Lipsey
# CS 433 Homework 2
import sys
from pyspark import SparkContext
import re

if __name__ == "__main__":
    print("Finding popular mentions...")
    # fetch input/output from command line arguments
    if len(sys.argv) > 2:
        input_file = sys.argv[1]
        output_location = sys.argv[2]
        print("input file = " + input_file)
        print("output file = " + output_location + "popular_mentions.txt")
    else:
        print("Insufficient command line arguments were supplied. Defaulting to hard-coded paths.")
        print("To set input/output paths use: argv1 = input file path (ex. \"D:/input/file.txt\", argv2 = output folder path (ex. \"D:/output\")")
        input_file = "D:/spark/workspace/input/training_set_tweets.txt"
        output_location = "D:/spark/workspace/output/"

    # create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Mention Count")

    # read data from text file and split each line into words
    input_text = sc.textFile(input_file)

    # count the occurrence of each word, remove punctuation
    mentions = input_text.flatMap(lambda line: line.split(" "))\
        .filter(lambda w: '@' in w) \
        .map(lambda w: re.sub(r'[^\w\s]', '', w)) \
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: a + b)\
        .top(25, key=lambda x: x[1])

    # save the counts to output
    print(mentions)

    file = open(output_location + 'popular_mentions.txt', 'w')
    for mention in mentions:
        file.write("@" + mention[0])
        file.write(" - ")
        file.write(str(mention[1]))
        file.write("\n")
    file.close()

    print("All done!")
