# Most Tweeted Users Program
# Nathaniel Lipsey
# CS 433 Homework 2
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from datetime import datetime


def is_date_within_range(date_string):
    try:
        date = datetime.strptime(date_string, dt_format)
    except:
        return False

    start_date = datetime(2009, 9, 16)
    end_date = datetime(2009, 9, 20)

    if start_date <= date <= end_date:
        return True
    return False


if __name__ == "__main__":
    print("Finding most tweeted users...")
    # fetch input/output from command line arguments
    if len(sys.argv) > 2:
        tweets = sys.argv[1]
        users = sys.argv[2]
        output_location = sys.argv[3]
        print("input files = " + tweets + ", " + users)
        print("output file = " + output_location + "most_tweeted_users.txt")
    else:
        print("Insufficient command line arguments were supplied. Defaulting to hard-coded paths.")
        print(
            "To set input/output paths use: argv1, argv2 = input file path for tweets, users (ex. \"D:/input/file.txt\", argv3 = output folder path (ex. \"D:/output\")")
        tweets = "D:/spark/workspace/input/training_set_tweets.txt"
        users = "D:/spark/workspace/input/training_set_users.txt"
        output_location = "D:/spark/workspace/output/"

    # create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Tweets")
    sqlContext = SQLContext(sc)

    # read data from text file and split each line into words
    input_tweets = sc.textFile(tweets)
    input_users = sc.textFile(users)

    # Get list of user codes from LA
    la_user_codes = input_users.map(lambda line: line.split("\t")) \
        .filter(lambda x: "Los Angeles" in x[1]) \
        .map(lambda x: x[0]).collect()

    dt_format = '%Y-%m-%d %H:%M:%S'

    la_user_tweets = input_tweets.map(lambda line: line.split("\t")) \
        .filter(lambda x: x[0] in la_user_codes) \
        .filter(lambda x: is_date_within_range(x[-1])) \
        .map(lambda x: x[0]) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .top(10, key=lambda x: x[1])

    file = open(output_location + 'most_tweeted_users.txt', 'w')
    for tweet in la_user_tweets:
        file.write("User ID: ")
        file.write(tweet[0])
        file.write(" - Tweets: ")
        file.write(str(tweet[1]))
        file.write("\n")
    file.close()

    print("All done!")