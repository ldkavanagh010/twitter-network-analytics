from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark import SparkContext
import os

# set up spark environment
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
spark = SparkSession.builder.appName("DatabaseWriter").config(
    'spark.sql.session.timeZone', 'UTC').getOrCreate()
