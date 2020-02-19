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
        """Read all the labelled vertices from s3 """
        s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['labelled'])
        return sqlctx.read.parquet(s3_path)

    def _read_communities(self) -> DataFrame:
        """Read all the aggregated community data from s3 """
        s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['communities'])
        return sqlctx.read.parquet(s3_path)

    def _write_vertices(self, dataframe: DataFrame) -> None:
        """write all the user data to redis """
        dataframe.write.format("org.apache.spark.sql.redis")\
                       .option("table", "user")\
                       .option("key.column", "id").save()

    def _write_communities(self, dataframe: DataFrame) -> None:
        """write all the aggregated community data to redis """
        dataframe.write.format("org.apache.spark.sql.redis")\
                       .option("table", "communities")\
                       .option("key.column", "name").save()

    def write_vertices(self) -> None:
        """Public function to read vertices from s3 and write to redis """
        dataframe = self._read_vertices()
        self._write_vertices(dataframe)

    def write_communities(self) -> None:
        """Public function to read communities from s3 and write to redis """
        dataframe = self._read_communities()
        self._write_communities(dataframe)
