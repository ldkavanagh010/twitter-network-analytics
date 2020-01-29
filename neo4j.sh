#!/bin/bash
S3_LINK=`aws s3 presign s3://liam-input-twitter-dataset/edges/part-00000-5a7d7802-2b9b-4a90-9bd0-fcb5825deccf-c000.csv`
spark-submit --master spark://10.0.0.10:7077 \
database/graph_to_neo4j.py $S3_LINK
