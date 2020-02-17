from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark import SparkContext



class DatabaseWriter():

	def __init__(self, cfg):
		self.cfg = cfg
		global spark
		spark = SparkSession.builder.appName("Label Propagation").config("spark.redis.host", self.cfg['redis']['host'])\
	    												         .config("spark.redis.port", self.cfg['redis']['port'])\
													   	         .config("spark.redis.auth", self.cfg['redis']['pwd'])\
													   	         .config('spark.sql.session.timeZone', 'UTC')\
														   		 .getOrCreate()
		global sc
		sc = spark.sparkContext
		global sqlctx
		sqlctx = SQLContext(sc)

	def _read_vertices(self) -> DataFrame:
		s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['labelled'])
		return sqlctx.read.parquet(s3_path)

	def _read_communities(self) -> DataFrame:
		s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['communities'])
		return sqlctx.read.parquet(s3_path)

	def _write_vertices(self, dataframe: DataFrame) -> None:
		dataframe.write.format("org.apache.spark.sql.redis").option("table", "user").option("key.column", "id").save()

	def _write_communities(self, dataframe: DataFrame) -> None:
		dataframe.write.format("org.apache.spark.sql.redis").option("table", "communities").option("key.column", "name").save()

	def write_vertices(self) -> None:
		dataframe = self._read_vertices()
		self._write_vertices(dataframe)

	def write_communities(self) -> None:
		dataframe = self._read_communities()
		self.write_communities(dataframe)
