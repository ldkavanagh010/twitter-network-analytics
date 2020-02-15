from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import os
import yaml

# get configuration options
with open('config.yml', 'r') as ymlfile:
	cfg = yaml.safe_load(ymlfile)

# create Logger to be used across stages


# set up spark environment
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

spark = SparkSession.builder.appName("Label Propagation").config('spark.executor.heartbeatInterval', '60')\
														   		 .config("spark.redis.host", "10.0.0.6")\
	    												         .config("spark.redis.port", "6379")\
													   	         .config("spark.redis.auth", "hello")\
													   	         .config('spark.sql.session.timeZone', 'UTC')\
															 	 .config('spark.storage.memoryFraction', '0.0')\
														   		 .getOrCreate()
sc = spark.sparkContext
