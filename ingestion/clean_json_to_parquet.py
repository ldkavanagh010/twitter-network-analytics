from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from shutil import rmtree
import sys
import os
import boto3
import wget
import urllib.request
from html_parser import *
import zstandard
import pathlib

def print_helper(x):
	print("printing: " + x)

if __name__ == '__main__':
	
	# create spark environment
	os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
	spark = SparkSession.builder.appName("DATA INGESTOR").getOrCreate()
	sc = spark.sparkContext

	
	sqlctx = SQLContext(sc)
	df = sqlctx.read.option('encoding', 'UTF-8').json('s3a://liam-input-twitter-dataset/json/*')
	df.registerTempTable('tweets')

	# extract user data and write parquet to s3
	users = sqlctx.sql("""SELECT user.*
						  FROM tweets
						  WHERE in_reply_to_user_id IS NOT NULL""")
	users.registerTempTable('users')
	clean_users = sqlctx.sql("""SELECT id, name, screen_name, statuses_count, followers_count, friends_count, description
							   FROM users""").distinct()
	clean_users.write.mode('overwrite').parquet('s3a://liam-input-twitter-dataset/users')

	# extract replies data and write parquet to s3
	#replies = sqlctx.sql("""SELECT user.id, in_reply_to_user_id, favorite_count, retweet_count
	#						FROM tweets
	#						WHERE in_reply_to_user_id IS NOT NULL""")
	#replies.write.mode('overwrite').parquet('s3a://liam-input-twitter-dataset/replies')




