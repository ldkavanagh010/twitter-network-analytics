from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import os

# get configuration options


# create Logger to be used across stages


# set up spark environment
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

spark = SparkSession.builder.appName("GRAPH").config('spark.sql.session.timeZone', 'UTC').getOrCreate()
sc = spark.sparkContext
sqlctx = SQLContext(sc)
