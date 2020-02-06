import urllib3
from html.parser import HTMLParser

class MyParser(HTMLParser):
	hrefs = list()

	def handle_starttag(self, tag, attrs):
		# Only parse the 'anchor' tag.
		if tag == "a":
			# Check the list of defined attributes.
			for name, value in attrs:
				# If href is defined, print it.
				if name == "href" and ((value[-3:] == 'zst') or (value[-3:] == 'tar')):
					self.hrefs.append(value)


class HTMLHandler():

	def __init__(self, website):
		self.http = urllib3.PoolManager()
		self.website = website
		self.links = []

	def _get_html(self):
		return self.http.request('GET', self.website).data.decode('utf-8')

	def get_links(self):
		parser = MyParser()
		parser.feed(self._get_html())
		for href in parser.hrefs:
			self.links.append(href.replace(".", self.website, 1))
		return self.links