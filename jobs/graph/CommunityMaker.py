from . import sqlctx, spark, sc
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType


class CommunityMaker:

    def __init__(self, cfg):
        self.cfg = cfg

    def _read_data(self) -> DataFrame:
        """Read the user data from s3"""
        s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['labelled'])
        return sqlctx.read.parquet(s3_path)

    def _read_users(self) -> DataFrame:
        s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['users'])
        return sqlctx.read.parquet(s3_path)

    def _write_data(self, dataframe: DataFrame) -> None:
        """Write the aggregated community data back to s3 """
        s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['communities'])
        dataframe.write.mode('overwrite').parquet(s3_path)

    def _create_community_dataframe(self) -> DataFrame:
        """Create an empty Table with the schema for the results """

        # schema for the community table
        schema = StructType([StructField("name", StringType(), False),
                             StructField('influencers', ArrayType(StringType(), False), False),
                             StructField("size", IntegerType(), False)])

        communities = spark.createDataFrame([], schema)


    def _aggregate_communities(self, dataframe: DataFrame) -> None:
        """Find the overall size and the top ten influencers by follower_count from each selected community."""

        user_data = self._read_users()
        user_data.registerTempTable('users')
        dataframe.registerTempTable('labels')

        # join the label and users table to retrieve information.
        dataframe = sqlctx.sql("""SELECT labels.id, labels.label,
										  users.screen_name,
										  users.followers_count
								  FROM labels
								  JOIN users on users.id = labels.id """)

        # check only for my pre-defined communities
        communities_list = ['Bernie', 'Biden', 'Buttigieg', 'Warren', 'Trump',
                            'Clinton', 'Leftist Media', 'CNN', 'MSNBC', 'FOX',
                            'Libertarian', 'IDW', 'AltRight', 'BJP', 'Tech', 'Sports', 'Joe Rogan']


        communities = _create_community_dataframe()


        for community in communities_list:
            temp_df = dataframe.filter(dataframe.label == community)
            size = temp_df.count()
            temp_df.registerTempTable('temp')

            # gather the list of the ten most popular members of each community.
            temp_df = sqlctx.sql("""SELECT label as name, collect_list(screen_name) as influencers
									FROM (SELECT label, screen_name
										  FROM temp
										  ORDER BY followers_count DESC
										  LIMIT 10)
									GROUP BY label""")
            temp_df = temp_df.withColumn('size', lit(int(size)))
            communities = communities.union(temp_df)
        self._write_data(communities)

    def make_communities(self):
        """ This is the public interface for the Community Maker. Call this function to read the data from s3,
            and write the results back to s3.
        """
        dataframe = self._aggregate_communities(self._read_data())
