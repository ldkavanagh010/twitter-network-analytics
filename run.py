"""
	THIS IS THE SCRIPT FOR RUNNING ALL PARTS OF THE BACKEND.
	run in the command line with spark-submit run.py [all/ingestion/graph/database]
	or you can run various substages: [upload/clean], [make/lpa], [vertices/communities]
"""
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import argparse
import sys
import yaml

with open('config.yml', 'r') as ymlfile:
	cfg = yaml.safe_load(ymlfile)


def ingest_data(substage):
	""" Run the Ingestion stage of the pipeline.
		Download Data + Upload Raw to S3 -> Clean Data to S3
	"""
	from jobs.ingestion.DataCleaner import DataCleaner
	from jobs.ingestion.FileUploader import FileUploader
	if substage == 'all' or substage == 'ingestion':
		file_uploader = FileUploader(cfg)
		#file_uploader.upload()
		cleaner = DataCleaner(cfg)
		cleaner.clean()
	elif substage == 'download':
		file_uploader = FileUploader(cfg)
		#file_uploader.upload()
	elif substage == 'clean':
		cleaner = DataCleaner(cfg)
		cleaner.clean()
	else:
		print('INVALID STAGE NAME')




def process_graph(substage):
	""" Run the Graphing stage of the pipeline
		Retrieve retweet/quote references, aggregate edges -> run LPA -> Aggregate Community Data
	"""
	from jobs.graph.GraphMaker import GraphMaker
	from jobs.graph.LabelPropagation import LabelPropagation
	if substage == 'all' or substage == 'graph':
		graph = GraphMaker(cfg)
		graph.make_graph()
		lpa = LabelPropagation(cfg)
		lpa.run_algorithm()
	elif substage == 'make':
		graph = GraphMaker(cfg)
		graph.make_graph()
	elif substage == 'lpa':
		lpa = LabelPropagation(cfg)
		lpa.run_algorithm(10)
	else:
		print('INVALID STAGE NAME')

def database_write(substage):
	""" Writes data to database
		Vertices to DB -> Communities to DB
	"""
	from jobs.database.DatabaseWriter import DatabaseWriter
	if substage == 'all' or substage == 'database':
		db_writer = DatabaseWriter(cfg)
		db_writer.write_vertices()
		db_writer.write_communities()
	elif substage == 'vertices':
		db_writer = DatabaseWriter(cfg)
		db_writer.write_vertices()
	elif substage == 'communities':
		db_writer = DatabaseWriter(cfg)
		db_writer.write_communities()
	else:
		print('INVALID STAGE NAME')

def run(stage):
	""" Logic to determine which stage of the pipeline to run, based on input
	"""
	if stage == 'all':
		ingest_data(stage)
		process_graph(stage)
		database_write(stage)
		return
	else:

		switch = { 
				   'ingestion':ingest_data,
				   'upload':ingest_data,
				   'clean':ingest_data,
				   'graph':process_graph,
				   'make': process_graph,
				   'lpa': process_graph,
				   'database':database_write,
				   'vertices': database_write,
				   'communities':database_write,

		}
		switch.get(stage, lambda: 'Invalid')(stage)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('stage',\
						 help="""Input the Stage of the Pipeline you want to run\n
						 		 [all/ingestion/graph/database]\n
						 		 or substages [upload/clean/make/lpa/vertices/communities]""")

	args = parser.parse_args()
	sys.path.insert(0, 'jobs.zip')
	run(args.stage)