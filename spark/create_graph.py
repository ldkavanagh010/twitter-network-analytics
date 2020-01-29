import importlib
import sys
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import DataType
from pyspark import SparkContext
from neo4j import GraphDatabase
import boto3
from shutil import rmtree
import os

def create_pair(s):
        return ((s.user.id, s.in_reply_to_user_id), 1)

def aggregate_pair(a, b):
        return a + b

def print_helper(x):
	print(x)

def to_CSV_line(data):
	return ','.join(str(d) for d in data)

def flatten(d):
        return (d[0][0], d[0][1], d[1])

if __name__ == '__main__':

        #reload(sys)
        #sys.setdefaultencoding("utf-8")
        os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

        spark = SparkSession.builder.appName("GRAPH MAKER").getOrCreate()
        sqlctx = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)

        replies = sqlctx.read.option('encoding', 'UTF-8').parquet("s3a://liam-input-twitter-dataset/replies/*.parquet")
        replies.registerTempTable("replies")
        edges = sqlctx.sql("""SELECT id, in_reply_to_user_id as reply_id, COUNT(id) as count, SUM(favorite_count) as favorites, SUM(retweet_count) as retweets
                              FROM replies
                              GROUP BY id, reply_id""")
        edges.coalesce(1).write.csv("s3a://liam-input-twitter-dataset/edges", mode='overwrite', header=True)

        
        #pairs = replies.rdd.map(create_pair)
        #counts = pairs.reduceByKey(aggregate_pair)
        #csv_lines = counts.map(flatten)
        #csv_df = csv_lines.toDF()
        #csv_df.write.csv("s3a://liam-input-twitter-dataset/graph/", mode='overwrite')
