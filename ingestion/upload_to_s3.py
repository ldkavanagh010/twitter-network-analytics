def get_urls():
	""" fetch urls from the data sources to process
	"""
	html_handler = HTMLHandler('http://files.pushshift.io/twitter/verified_feed/')
	# remove duplicates and return
	return list(set(html_handler.get_links()))

def uncompress_file(file):
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

def download_and_write_file(url, s3_files):
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
	upload_json_to_s3(uncompress_file(file_path))

def upload_json_to_s3(path):
	""" Uploads json data to s3
	"""
	s3 = boto3.client('s3')
	s3.upload_file(path, 'liam-input-twitter-dataset', ('json/' + path))
	os.remove(path)


if __name__ == '__main':
	os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
	spark = SparkSession.builder.appName("DATA INGESTOR").getOrCreate()
	sc = spark.sparkContext
	sc.addFile(os.path.abspath('twitter-network-analytics/ingestion/html_parser.py'))

	# find urls from webpage
	urls = get_urls()

	# get list of files from s3
	s3 = boto3.resource('s3')
	bucket = s3.Bucket('liam-input-twitter-dataset')
	s3_files = list()
	for obj in bucket.objects.filter(Prefix='json/'):
		s3_files.append(obj.key)
	
	sc.parallelize(urls).foreach(lambda url: download_and_write_file(url, s3_files))



