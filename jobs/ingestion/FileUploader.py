from typing import List
from . import cfg
from .html_parser import HTMLHandler
from shutil import rmtree
import zstandard
import tarfile
import pathlib
import boto3
import os
import sys

class FileUploader:

	def __init__(self):
		self.s3_files = self._get_s3_files()
		self.urls = self._get_download_links()

	def _get_download_links(self) -> List[str]:
		""" Scrape all the file links from webpages
		"""
		handler = HTMLHandler()
		sites = cfg['download_links']
		return handler.get_urls(sites)


	def _get_s3_files(self) -> List[str]:
		""" Find all files in ingestion folder of s3 bucket 
		"""
		s3 = boto3.resource('s3')
		bucket = s3.Bucket(cfg['s3']['bucket'])
		files = list()
		# get file keys from s3 bucket folder
		for obj in bucket.objects.filter(Prefix= cfg['s3']['input'] + '/'):
			files.append(obj.key)
		return files


	def _delete_file(path: str) -> None:
		""" Tries to delete the file at given path, writes to logs on success or failure
		"""
		os.remove(input_file_str)

	def _uncompress_zst(self, file:str) -> str:
		""" Takes an string path to a file compressed in zst format, decompresses it and
			returns a string path to the uncompressed file(s). Will delete the compressed file
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

		return output_path

	def _uncompress_tar(file: str) -> str:
		""" Takes an string path to a file compressed in tar format, decompresses it and
			returns a string path to the uncompressed file(s). Will delete the compressed file
		"""
		tf = tarfile.open(file)
		tf.extractall()
		return ""



	def _download_file(self, url:str, s3_files:List[str]) -> str:
		""" Fetches files from the source websites, to be fed to the uncompressor.
		"""
		# From URL construct the destination path and filenames
		file_name = os.path.basename(urllib.parse.urlparse(url).path)
		file_path = pathlib.Path(file_name)

		if '{}/{}'.format(cfg['s3']['input'], str(pathlib.Path(file_name).stem)) in s3_files:
			return
		# Check if the file has already been downloaded.
		if not os.path.exists(str(file_path)):
			# Download and write to file.
			wget.download(url)
		# unzip compressed files
		return file_path

	def _check_file_exists(path:str):
		""" Checks whether file exists in S3 already, in order to avoid download duplicate
		"""
		pass

	def upload_file(self, path:str) -> None:
		""" Uploads unzipped files to s3
		"""
		s3_path = '{}/{}'.format(cfg['s3']['input'], path)
		s3 = boto3.client('s3')
		s3.upload_file(path, cfg['s3']['bucket'], s3_path)

		#deletes file from local host.
		os.remove(path)


		# find urls from webpage
		urls = get_urls()
		
		for url in urls:
			compressed_path = download_file(url, s3_files)
			uncompressed_paths = uncompress_zst(compressed_path)
			upload_json_to_s3(uncompressed_paths)



