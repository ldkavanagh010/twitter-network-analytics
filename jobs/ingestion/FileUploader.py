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
        for obj in bucket.objects.filter(Prefix=cfg['s3']['input'] + '/'):
            files.append(obj.key)
        return files

    def _delete_file(path: str) -> None:
        """ deletes the file at given path.
        """

        os.remove(os.abspath(path))

    def _uncompress_zst(self, file: str) -> str:
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
        self._delete_file(file)

        return output_path

    def _download_file(self, url: str, s3_files: List[str]) -> str:
        """ Fetches files from the source websites, to be fed to the uncompressor.
        """
        # From URL construct the destination path and filenames
        file_name = os.path.basename(urllib.parse.urlparse(url).path)
        file_path = pathlib.Path(file_name)

        # Check if the file has already been downloaded.
        if not os.path.exists(str(file_path)):
            # Download and write to file.
            wget.download(url)
        # unzip compressed files
        return file_path

    def _check_file_exists(path: str):
        """ Checks whether file exists in S3 already, in order to avoid download duplicate
        """
        pass

    def _upload_file(self, path: str) -> None:
        """ Uploads unzipped files to s3
        """
        s3_path = '{}/{}'.format(cfg['s3']['input'], path)
        s3 = boto3.client('s3')
        s3.upload_file(path, cfg['s3']['bucket'], s3_path)
        # deletes file from local host.
        self.delete_file(path)


        def ingest():
        """This is the public interface for the file uploader. Call ingest(), to begin downloading the historical tweets
           and uploading them to your s3 bucket.
        """

        # find urls from webpage
        urls = get_urls()

                

        # download the file from the url, uncompress it and upload it to s3.
        for url in urls:
            # do not download if file is already accounted for
            url_file_name = url.split('/')[-1][:-4]
            if '{}/{}'.format(cfg['s3']['input'], url_file_name) in s3_files:
                continue

            compressed_path = download_file(url, s3_files)
            uncompressed_path = uncompress_zst(compressed_path)
            upload_file(uncompressed_path)
