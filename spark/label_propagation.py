import os
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import lit, when
from pyspark import SparkContext

def initialize_tables(sc, sqlctx):
	edges = sqlctx.read.csv('s3a://liam-input-twitter-dataset/tests/edges/*.csv')

	# find all unique vertex ids
	srcs = edges.select(edges._c0).withColumnRenamed('_c0', 'id')
	dsts = edges.select(edges._c1).withColumnRenamed('_c1', 'id')
	edges = edges.withColumnRenamed('_c0', 'src')\
				 .withColumnRenamed('_c1', 'dst')\
				 .withColumnRenamed('_c2', 'weight')\
				 .withColumnRenamed('_c3', 'favorites')\
				 .withColumnRenamed('_c4', 'retweets')
	srcs.registerTempTable('srcs')
	srcs.registerTempTable('dsts')
	verts = sqlctx.sql("""SELECT id FROM srcs
						 UNION
						 SELECT id FROM dsts""")

	# add labelling columns and initialize their value
	verts = verts.withColumn("label", verts.id).withColumn('prelabelled', lit(False))
	print(verts)#.show(vertical=True)

	# join the prelabelled vertices and unlabelled vertices tables
	prelabelled = [('216776631','1',True)]
	prelabelled_verts = sqlctx.createDataFrame(prelabelled, schema=('id', 'label', 'prelabelled'))
	unlabelled_verts = verts.filter((~verts.id.isin([str(row.id) for row in prelabelled_verts.collect()])))
	verts = unlabelled_verts.union(prelabelled_verts)

	#add initial label information to edges table
	#edges.registerTempTable('edges')
	#erts.registerTempTable('vertices')
	#edges = sqlctx.sql("""SELECT src, dst, weight, favorites, retweets, label, prelabelled
	#					  FROM edges
	#					  JOIN vertices on id=src""")

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

def run_algorithm(graph, sc, sqlctx, NumIters):
	
	verts = graph[0]
	edges = graph[1]

	for x in range(NumIters):
		verts.show()
		verts.registerTempTable('vertices')
		edges.registerTempTable('edges')

		# find the aggregates of incoming labels across edges to any given vertex
		label_aggregates = sqlctx.sql("""SELECT dst, label, sum(weight) as res
					  				    FROM vertices
					  					JOIN edges on vertices.id = edges.src
					  					GROUP BY dst, label""")

		# find the maximum of these aggregates and save that label
		label_aggregates.registerTempTable('LabelAggregates')
		new_labels = sqlctx.sql("""SELECT *
								  FROM LabelAggregates WHERE (dst, res) IN
								  (SELECT dst, max(res)
								  FROM LabelAggregates
								  GROUP BY dst)""")

		# break any ties between the label aggregates
		new_labels.registerTempTable('tiedlabels')
		break_ties = sqlctx.sql("""SELECT *
								   FROM tiedlabels
								   WHERE (dst, label) IN
								   (SELECT dst, max(label)
								   FROM tiedlabels
								   GROUP BY dst) """)

		# apply the new labels to each vert
		break_ties.registerTempTable('labels')
		relabelled_verts = sqlctx.sql("""SELECT id, labels.label, prelabelled
										 FROM vertices
										 JOIN labels on vertices.id = labels.dst
										 WHERE vertices.prelabelled = false """)

		# replace each vertex with its newly labelled version (if it exists)
		unchanged_verts = verts.filter((~verts.id.isin([str(row.id) for row in relabelled_verts.collect()])))
		verts = unchanged_verts.union(relabelled_verts)
		verts.show()
		



if __name__ == '__main__':
	os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
	spark = SparkSession.builder.appName("GRAPH MAKER").getOrCreate()
	sqlctx = SQLContext(sparkSession=spark, sparkContext = spark.sparkContext)
	run_algorithm(initialize_test_tables(spark.sparkContext, sqlctx), spark.sparkContext, sqlctx, 5)










