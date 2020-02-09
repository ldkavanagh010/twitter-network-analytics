import os
import operator
import yaml
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import lit, when, size, desc, udf
from pyspark import SparkContext
from pyspark.sql.types import ArrayType


def initialize_tables(sc, sqlctx):
	edges = sqlctx.read.parquet('s3a://liam-input-twitter-dataset/edges/*.parquet')
	
	#find all unique vertex ids
	edges = edges.select('id', 'reply_id', 'count')\
				 .withColumnRenamed('id', 'src')\
				 .withColumnRenamed('reply_id', 'dst')\
				 .withColumnRenamed('count', 'weight')

	srcs = edges.select("src")
	srcs = srcs.withColumnRenamed('src', 'id')
	srcs.registerTempTable("srcs")

	dsts = edges.select("dst")
	dsts = dsts.withColumnRenamed('dst', 'id')
	dsts.registerTempTable("dsts")
	verts = sqlctx.sql("""SELECT DISTINCT id FROM srcs
						 UNION
						 SELECT DISTINCT id FROM dsts""")


	# add labelling columns and initialize their value
	verts = verts.withColumn("label", verts.id).withColumn('prelabelled', lit(False))

	# join the prelabelled vertices and unlabelled vertices tables
	prelabelled_verts = sqlctx.read.json("s3a://liam-input-twitter-dataset/prelabelled.json")
	unlabelled_verts = verts.filter((~verts.id.isin([str(row.id) for row in prelabelled_verts.collect()])))
	verts = unlabelled_verts.union(prelabelled_verts)

	return (verts, edges)

def initialize_test_tables(sc, sqlctx):
	verts_list = [('10000', '10000', False),\
				 ('20000', '20000', True),\
				 ('30000', '30000', False),\
				 ('40000', '40000', False), \
				 ('50000', '50000', False)]

	edges_list = [('10000', '50000', 1),\
				  ('20000', '50000', 1),\
				  ('30000', '50000', 1),\
				  ('40000', '50000', 2),\
				  ('40000', '20000', 1),\
				  ('50000', '20000', 1)]

	verts = sqlctx.createDataFrame(verts_list, schema=('id', 'label', 'prelabelled'))
	edges = sqlctx.createDataFrame(edges_list, schema=('src', 'dst', 'weight'))
	return (verts, edges)



def find_new_label(labels, weights, label, prelabelled):
	if prelabelled:
		return label
	labels = dict(zip(labels, weights))
	return max(labels.items(), key=operator.itemgetter(1))[0]

def double_edge_weight(weight, prelabelled):
	if prelabelled:
		return weight * 2
	else:
		return weight

def set_prelabelled(label):
	communities = ['Bernie', 'Biden', 'Buttigieg', 'Warren', 'Trump',\
					'Clinton', 'Leftist Media', 'CNN', 'MSNBC', 'FOX',\
					'Libertarian', 'IDW', 'AltRight']
	if label in communities:
		return True
	else:
		return False


def find_incoming_edges(verts, edges):

	verts.registerTempTable('vertices')
	edges.registerTempTable('edges')
	associations = sqlctx.sql("""SELECT edges.src, edges.dst, double_edge_weight(edges.weight, vertices.prelabelled) as weight, vertices.label
						  		 FROM edges
						  		 JOIN vertices on edges.dst = vertices.id """)
	associations.registerTempTable('associations')

	edges = associations.select("src", "dst", "weight")


	aggregates = sqlctx.sql("""SELECT src as id, collect_list(dst) as dsts, collect_list(weight) as weights, collect_list(label) as labels 
							   FROM associations
							   GROUP BY src""")

	aggregates.registerTempTable('aggs')
	aggregates = sqlctx.sql("""SELECT aggs.id, vertices.label, aggs.dsts, aggs.weights, aggs.labels, vertices.prelabelled
							   FROM aggs
							   JOIN vertices on aggs.id = vertices.id""")

	return aggregates, edges


def run_algorithm(graph, sc, sqlctx):
	verts, edges = graph[0], graph[1]
	aggregates, edges = find_incoming_edges(verts, edges)

	aggregates.registerTempTable('aggregates')

	relabelled = sqlctx.sql("""SELECT id, findLabel(labels, weights, label, prelabelled) as label, prelabelled
				 		 			FROM aggregates""")


	relabelled.registerTempTable("relabelled")

	relabelled = sqlctx.sql("""SELECT id, label, set_prelabelled(label) as prelabelled
								 FROM relabelled """)
	
	verts = relabelled
	return (verts, edges)

if __name__ == '__main__':
	os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
	spark = SparkSession.builder.appName("Label Propagation").config('spark.executor.heartbeatInterval', '60')\
													   		 .config("spark.redis.host", "10.0.0.6")\
    												         .config("spark.redis.port", "6379")\
												   	         .config("spark.redis.auth", "hello")\
												   	         .config('spark.sql.session.timeZone', 'UTC')\
													   		 .getOrCreate()
	sqlctx = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)

	spark.udf.register("findLabel", find_new_label)
	spark.udf.register("double_edge_weight", double_edge_weight)
	spark.udf.register("set_prelabelled", set_prelabelled)
	graph = initialize_tables(spark.sparkContext, sqlctx)
	for x in range(5):
		graph = run_algorithm(graph, spark.sparkContext, sqlctx)

	graph[0].registerTempTable("verts")
	communities = sqlctx.sql("""SELECT label as community, COUNT(*) as size
								FROM verts
								GROUP BY label""")

	graph[0].write.format("org.apache.spark.sql.redis").option("table", "user").option("key.column", "id").save()
	communities.write.format("org.apache.spark.sql.redis").option("table", "community").option("key.column", "community").save()









