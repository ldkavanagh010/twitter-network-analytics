import os
import sys
import operator
from . import sqlctx, spark, sc
from pyspark.sql import DataFrame
from pyspark.sql.typing import Row, StringType, BoolType, 
from pyspark.sql.functions import lit, when, size, desc, udf
from typing import List, Tuple


#######################################################################
# SPARK UDFS
# Used in the Algorithm
#######################################################################
def find_new_label(id: str, labels: List[str], weights: List[int], label: str, prelabelled: bool) -> Row:
    """ Aggregate the labels and weights of all the *OUTGOING* edges of each vertex
            Return the label which has the largest aggregated score.
    """
    if prelabelled:
        return label
    labels = dict(zip(labels, weights))
    return max(labels.items(), key=operator.itemgetter(1))[0]



# schema for a new vertice in the graph to be used with the findLabel udf
schema = StructType([
        StructField('id', StringType(), False),
        StructField("label", StringType(), False),
        StructField("prelabelled", BooleanType(), False)])


def double_edge_weight(weight: int, prelabelled: bool) -> int:
    """ Double the weight of the edges on prelabelled vertices to increase their spread
    """
    if prelabelled:
        return weight * 2
    else:
        return weight


def set_prelabelled(label: str) -> bool:
    """ Sets the prelabelled column for each vertex, in order to ensure that 
            the chosen communities don't lose members that they have reached.
    """
    communities = ['Bernie', 'Biden', 'Buttigieg', 'Warren', 'Trump',
                   'Clinton', 'Leftist Media', 'CNN', 'MSNBC', 'FOX',
                   'Libertarian', 'IDW', 'AltRight', 'BJP', 'Tech', 'Sports', 'Joe Rogan']
    if label in communities:
        return True
    else:
        return False


spark.udf.register("findLabel", find_new_label)
spark.udf.register("double_edge_weight", double_edge_weight)
spark.udf.register("set_prelabelled", set_prelabelled)

#######################################################################


class LabelPropagation:

    def __init__(self, cfg):
        self.cfg = cfg

    def _read_data(self) -> Tuple[DataFrame, DataFrame]:
        """ Read the stored Vertex and Edge data from s3
                Return a tuple of the vertices and edges
        """
        verts_s3 = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['vertices'])
        edges_s3 = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['edges'])

        verts = sqlctx.read.parquet(verts_s3)
        edges = sqlctx.read.parquet(edges_s3)

        return (verts, edges)

    def _write_data(self, graph: Tuple[DataFrame, DataFrame]) -> None:
        """ Take the resulting vertices of the Label Propagation Algorithm
                and write them out to S3
        """
        vertices, edges = graph[0], graph[1]
        s3_path = 's3a://{}/{}'.format(self.cfg['s3']['bucket'], self.cfg['s3']['labelled'])

        # remove unecessary columns
        vertices = vertices.drop(vertices.prelabelled)
        vertices.write.mode('overwrite').parquet(s3_path)

    def _find_outgoing_edges(self, graph: Tuple[DataFrame, DataFrame]) -> Tuple[DataFrame, DataFrame]:
        """ Aggregates the outgoing edge weights and labels of the edge destination
                for each vertex in the graph. Based on the Pregel model for graph modelling.
                Returns the Modified Graph
                Initial Schemas {id: str, label: str, prelabelled: bool} {src: str, dst:str, weight: int}
                Final Schemas   {id: str, label: str, prelabelled: bool, weights: List[int], label: List[str]}
        """
        vertices, edges = graph[0], graph[1]
        vertices.registerTempTable('vertices')
        edges.registerTempTable('edges')

        associations = sqlctx.sql("""SELECT /*+ BROADCAST(vertices) */ 
											edges.src, edges.dst, 
											double_edge_weight(edges.weight, vertices.prelabelled) as weight,
											vertices.label
							  		 FROM edges
							  		 JOIN vertices on edges.dst = vertices.id """)

        associations.registerTempTable('associations')

        aggregates = sqlctx.sql("""SELECT src as id,
										  collect_list(weight) as weights,
										  collect_list(label) as labels 
								   FROM associations
								   GROUP BY src""")

        aggregates.registerTempTable('aggs')
        aggregates = sqlctx.sql("""SELECT /*+ BROADCAST(vertices) */
										  aggs.id, 
										  vertices.label,
										  vertices.prelabelled,
										  aggs.weights, aggs.labels
								   FROM aggs
								   JOIN vertices on aggs.id = vertices.id
								   """)

        return (aggregates, edges)

    def _choose_label(self, graph: Tuple[DataFrame, DataFrame]) -> Tuple[DataFrame, DataFrame]:
        """ Finds the most prominent label (by aggregated weights) on all the outgoing edges.
            It applies the given label to each vertex, and then locks in the spread of the predefined
            label groups.
            INITIAL SCHEMA: {id: str, label: str, prelabelled: bool, weights: List[int], label: List[str]}
            FINAL SCHEMA: {id: str, label: str, prelabelled: bool}
        """

        vertices, edges = graph[0], graph[1]
        vertices.registerTempTable('vertices')

        relabelled = sqlctx.sql("""SELECT id,
										  findLabel(labels, weights, label, prelabelled) as label,
										  prelabelled
					 		 			FROM vertices""")

        relabelled.registerTempTable("relabelled")

        relabelled = sqlctx.sql("""SELECT id,
										  label,
										  set_prelabelled(label) as prelabelled
									 FROM relabelled """)

        return (relabelled, edges)

    def run_algorithm(self, num_iters):
        """ Run the algorithm for the number of iterations that the class was instantiated with.
        """
        graph = self._read_data()
        for x in range(num_iters):
            graph = self._choose_label(self._find_outgoing_edges(graph))
        self._write_data(graph)
