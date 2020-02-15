from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import yaml
import os


class GraphMaker:

    def __init__(self):
        pass

    def _read_data(self) -> DataFrame:
        """Read the edge data table from s3 and return them to a spark dataframe. """
        s3_path = 's3a://{}/{}/*.parquet'.format(cfg['s3']['bucket'], cfg['s3']['retweets'])
        return sqlctx.read.parquet(s3_path)

    def _write_data(self, dataframe: DataFrame, s3_path: str) -> None:
        """Write the aggregated edge data back to s3 for use with graph algorithms"""
        dataframe.write.mode('overwrite').parquet(s3_path)

    def _find_vertices(self, dataframe: DataFrame) -> None:
        """Find all unique vertices from edge combinations, processes them into the form needed for the algorithms
           INITIAL SCHEMA: {src: str, dst: str}
           FINAL SCHEMA:   {id: str, label: str, prelabelled: Bool}
        """
        # get all unique ids
        srcs = dataframe.select("src")
        dsts = dataframe.select("dst")
        verts = srcs.union(dsts).distinct()

        # add labelling columns and initialize value to False
        verts = verts.withColumn("label", verts.id).withColumn('prelabelled', lit(False))

        # join the prelabelled vertices and unlabelled vertices tables
        prelabelled_verts = sqlctx.read.json("s3a://liam-input-twitter-dataset/prelabelled.json")
        unlabelled_verts = verts.filter((~verts.id.isin([str(row.id) for row in prelabelled_verts.collect()])))
        verts = unlabelled_verts.union(prelabelled_verts)


        s3_path = 's3a://{}/{}'.format(cfg['s3']['bucket'], cfg['s3']['vertices'])
        self._write_data(verts, s3_path)

    def _find_edges(self, dataframe: DataFrame) -> None:
        """Collapse individual references between users into directed edges of a graph
           INITIAL SCHEMA: {src: str, dst: str}
           FINAL SCHEMA:   {src: str, dst: str, weight: int}
        """

        dataframe.registerTempTable('replies')

        # collapse all references into single edge weight between nodes
        edges = sqlctx.sql("""SELECT src, dst, 
                             COUNT(*) as weight
                      FROM replies
                      GROUP BY src, dst""")
        # write out to s3
        s3_path = 's3a://{}/{}'.format(cfg['s3']['bucket'], cfg['s3']['edges'])
        self._write_data(edges, s3_path)


    def make_graph(self):
        """ This is the public interface for the GraphMaker class. Run only after ingestion.
            the function will take the replies and find edges and vertices from them.
            It will write each of these to S3
        """
        dataframe = self._read_data()
        self._find_vertices(dataframe)
        self._find_edges(dataframe)
        