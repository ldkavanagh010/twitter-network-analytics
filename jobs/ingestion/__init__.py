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

spark = SparkSession.builder.appName("INGESTION")\
							.config('spark.sql.session.timeZone', 'UTC')\
							.getOrCreate()
sc = spark.sparkContext
sqlctx = SQLContext(sc)
