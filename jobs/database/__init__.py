from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark import SparkContext
import os
# create Logger to be used across stages


# set up spark environment
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
spark = SparkSession.builder.appName("Label Propagation").config('spark.sql.session.timeZone', 'UTC').getOrCreate()

