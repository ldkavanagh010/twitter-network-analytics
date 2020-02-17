import urllib3
from html.parser import HTMLParser
from typing import List

class MyParser(HTMLParser):
	hrefs = list()

	def handle_starttag(self, tag, attrs):
		# Only parse the 'anchor' tag.
		if tag == "a":
			# Check the list of defined attributes.
			for name, value in attrs:
				# If href is defined, grab the links that have .zst or .tar file format
				if name == "href" and ((value[-3:] == 'zst') or (value[-3:] == 'tar')):
					if value[-3:] == 'zst':
						self.hrefs.append((value, 'zst'))
					if value[-3:] == 'tar':
						self.hrefs.append((value, 'tar'))



class HTMLHandler():

	def __init__(self):
		self.http = urllib3.PoolManager()
		self.parser = MyParser()
		self.parser.hrefs = list()

	def _get_html(self, website):
		"""Retrieve the HTML for one page of a website to be parsed for page links """
		return self.http.request('GET', website).data.decode('utf-8')

	def _get_links(self, website):
		"""Scrape the webpage for download links, and return all the archives"""
		links = []
		# scrape links from website
		self.parser.feed(self._get_html(website))
		# format hrefs for use with wget
		for href in self.parser.hrefs:
			if href[1] == 'zst':
				links.append(href[0].replace(".", website, 1))
			else:
				links.append(website + href[0])
		return links

	def get_urls(sites: List[str]) -> List[str]:
		""" This is the public interface for the HTMLHandler class
			Given a list of pages to scrape links from, return all links of a certain type
		"""
		links = []
		for site in sites:
			links.append(self._get_links(site))
		#flatten list structure
		links = [link for website in links for link in website]
		# remove duplicates and return
		return list(set(links))