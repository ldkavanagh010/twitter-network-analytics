from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import yaml


with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

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
        os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

        spark = SparkSession.builder.appName("GRAPH MAKER").config('spark.sql.session.timeZone', 'UTC')\
                                                           .getOrCreate()
        sqlctx = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)

        replies = sqlctx.read.option('encoding', 'UTF-8').parquet("s3a://" + cfg['s3']['bucket'] + "/" + cfg['s3']['retweets'] + "/*.parquet")
        replies.registerTempTable("replies")
        edges = sqlctx.sql("""SELECT id, in_reply_to_user_id as reply_id, COUNT(id) as count, SUM(favorite_count) as favorites, SUM(retweet_count) as retweets
                              FROM replies
                              GROUP BY id, reply_id""")
        edges.write.mode('overwrite').parquet("s3a://" + cfg['s3']['bucket']  + "/" + cfg['s3']['edges'])

        
        #pairs = replies.rdd.map(create_pair)
        #counts = pairs.reduceByKey(aggregate_pair)
        #csv_lines = counts.map(flatten)
        #csv_df = csv_lines.toDF()
        #csv_df.write.csv("s3a://liam-input-twitter-dataset/graph/", mode='overwrite')
