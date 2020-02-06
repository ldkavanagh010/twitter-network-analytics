from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from graphframes import *
import boto3
from shutil import rmtree
import os
S3_BUCKET_NAME = 'liam-input-twitter-dataset'

spark = SparkSession.builder.appName("test-app").getOrCreate()
sqlContext = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)

df = sqlContext.read.format("csv").option("header", "false").load("s3a://liam-input-twitter-dataset/graph/*")
vertices = df.select(df.columns[0]).selectExpr("_c0 as id")
edges = df.select(df.columns[0], df.columns[1], df.columns[2]).selectExpr("_c0 as src", "_c1 as dst", "_c2 as relationship")
graph = GraphFrame(vertices, edges)
communities = graph.labelPropagation(maxIter=3)
communities.persist().show(10)
