from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from shutil import rmtree
import sys
import os
import boto3
import wget
import urllib.request
from html_parser import *
import zstandard
import pathlib

#filtered = df.select("user","id", "in_reply_to_user_id", "in_reply_to_screen_name", "quoted_status_id", "is_quote_status",\
#                	"lang", "quoted_status", "retweeted_status", "reply_count", "retweet_count").filter(df.lang == "en")

def print_helper(x):
	print("printing: " + x)

def _get_urls():
		""" fetch urls from the data sources to process
		"""
		html_handler = HTMLHandler('http://files.pushshift.io/twitter/verified_feed/')
		# remove duplicates and return
		return list(set(html_handler.get_links()))

def _uncompress_file(file):
		""" uncompress a file and return the path to the new file
		"""
		input_file = pathlib.Path(file)
		input_file_str = str(input_file)
		output_path = input_file.stem
		output_path_str = str(output_path)

		if not os.path.exists(output_path_str):
			with open(input_file_str, 'rb') as compressed:
				decomp = zstandard.ZstdDecompressor()
				with open(output_path, 'wb') as destination:
					print("DECOMPRESSING: " + output_path_str)
					decomp.copy_stream(compressed, destination)
		# delete the zipped file
		os.remove(input_file_str)
		return output_path

def _download_and_unzip_file(url, s3_files):
		""" fetches the file to work with from its source and uncompresses it
		"""
		# From URL construct the destination path and filenames
		file_name = os.path.basename(urllib.parse.urlparse(url).path)
		file_path = pathlib.Path(file_name)

		if ('json/' + str(pathlib.Path(file_name).stem)) in s3_files:
			return
		# Check if the file has already been downloaded.
		if not os.path.exists(str(file_path)):
			# Download and write to file.
			print("DOWNLOADING: " + str(file_path))
			wget.download(url)
		# unzip compressed files
		_upload_json_to_s3(_uncompress_file(file_path))

def _upload_json_to_s3(path):
	""" Uploads json data to s3
	"""
	s3 = boto3.client('s3')
	s3.upload_file(path, 'liam-input-twitter-dataset', ('json/' + path))
	os.remove(path)

class DataIngestor:

	def __init__(self, bucket_name):
		""" Initializes the spark environment and other class variables for the data transformations
		"""
		self.bucket_name = bucket_name

	

	def _clean_data():
		""" Removes fields from the data and reformats it for writing to parquet
		"""
		pass



	def ingest(self, sc):
		"""
		This is the public method to interface with the DataIngestor class, first instantiate an instance of this class,
		with ... and ..., then call the ingest function. The ingest function will download the data, process it from json into
		a spark dataframe, clean the data, write it to parquet files and finally upload it to s3
		"""
		#['http://files.pushshift.io/twitter/verified_feed/TW_verified_2019-05-29.zst']
		#sc.parallelize(urls).foreach(print_helper)
		#jsons = files.map(lambda path: self._read_file(path))

		#sql = SQLContext(sc)
		#json = sql.read.json()
		#json.show(n = 10, vertical =True)
		#jsons.explode()

if __name__ == '__main__':
	
	#spark = SparkSession.builder.appName("DATA INGESTOR").getOrCreate()
	#os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
	sc = SparkContext()
	sql = SQLContext(sc)
	#sc.addFile(os.path.abspath('twitter-network-analytics/ingestion/html_parser.py'))
	urls = _get_urls()
	s3 = boto3.resource('s3')
	bucket = s3.Bucket('liam-input-twitter-dataset')
	s3_files = list()
	for obj in bucket.objects.filter(Prefix='json/'):
		s3_files.append(obj.key)

	sc.parallelize(urls).foreach(lambda url: _download_and_unzip_file(url, s3_files))
	#d = DataIngestor(sys.argv[1])
	#d.ingest(sc)





