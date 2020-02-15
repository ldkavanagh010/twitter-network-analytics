from html_parser import *
from shutil import rmtree
import zstandard
import tarfile
import pathlib
import yaml
import boto3
import os
import sys

with open('config.yml', 'r') as ymlfile:
	cfg = yaml.safe_load(ymlfile)

def get_urls():
	""" fetch download urls from the data sources to process
	"""
	html_handler = HTMLHandler()
	links = []
	for site in cfg['download_urls']:
		links.append(html_handler.get_links(site))
	#flatten list structure
	links = [link for website in links for link in website]
	# remove duplicates and return
	return list(set(links))

def uncompress_zst(file):
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
				decomp.copy_stream(compressed, destination)
	# delete the zipped file
	os.remove(input_file_str)
	return output_path

def uncompress_tar(file):
	tf = tarfile.open(file)
	tf.extractall()



def download_file(url, s3_files):
	""" Fetches files from the source websites, to be fed to the uncompressor.
	"""
	# From URL construct the destination path and filenames
	file_name = os.path.basename(urllib.parse.urlparse(url).path)
	file_path = pathlib.Path(file_name)

	if (cfg['s3']['input'] + '/' + str(pathlib.Path(file_name).stem)) in s3_files:
		return 
	# Check if the file has already been downloaded.
	if not os.path.exists(str(file_path)):
		# Download and write to file.
		wget.download(url)
	# unzip compressed files
	return file_path

def upload_json_to_s3(path):
	""" Uploads json data to s3
	"""
	s3 = boto3.client('s3')
	s3.upload_file(path, cfg['s3']['bucket'], (cfg['s3']['input'] + '/' + path))
	os.remove(path)

if __name__ == '__main__':


	# find urls from webpage
	urls = get_urls()

	# get list of files from s3
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(cfg['s3']['bucket'])
	s3_files = list()

	# check if pushshift files are already downloaded
	for obj in bucket.objects.filter(Prefix= cfg['s3']['input'] + '/'):
		s3_files.append(obj.key)
	
	for url in urls:
		compressed_path = download_file(url, s3_files)
		uncompressed_paths = uncompress_zst(compressed_path)
		upload_json_to_s3(uncompressed_paths)



