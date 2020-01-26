from pyspark.sql import SQLContext, SparkSession
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
	unnested = [data[0][0], data[0][1], data[1]]
	return ','.join(str(d) for d in unnested)

def uploadDirectory(path,bucketname):
        s3 = boto3.client('s3')
        for root,dirs,files in os.walk(path):
                for file in files:
                        s3.upload_file(os.path.join(root,file),bucketname,(path + '/'  + file + '.csv'))

S3_BUCKET_NAME = 'liam-input-twitter-dataset'

spark = SparkSession.builder.appName("test-app").master("local").config("dfs.client.read.shortcircuit.skip.checksum", "true").getOrCreate()
spark.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
sqlContext = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)

df = sqlContext.read.parquet("s3a://liam-input-twitter-dataset/verified_09_01/*.parquet")
if os.path.exists("/graph"):
	rmtree('graph')

pairs = df.rdd.map(create_pair)
counts = pairs.reduceByKey(aggregate_pair)
csv_lines = counts.map(to_CSV_line)
csv_lines.saveAsTextFile("graph")
uploadDirectory('graph', 'liam-input-twitter-dataset')

