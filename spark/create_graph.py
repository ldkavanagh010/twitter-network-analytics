from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from neo4j import GraphDatabase
import boto3
from shutil import rmtree
import os

def create_pair(s):
        return (s.user.id, s.in_reply_to_user_id, 1)

def aggregate_pair(a, b):
        return a + b

def print_helper(x):
	print(x)

def to_CSV_line(data):
	return ','.join(str(d) for d in data)

if __name__ == '__main__':

        spark = SparkSession.builder.appName("GRAPH MAKER").getOrCreate()
        sqlContext = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)

        df = sqlContext.read.parquet("s3a://liam-input-twitter-dataset/verified/*.parquet")

        pairs = df.rdd.map(create_pair)
        counts = pairs.reduceByKey(aggregate_pair)
        csv_lines = counts.map(to_CSV_line)
        csv_lines.saveAsTextFile("s3a://liam-input-twitter-dataset/graph")
