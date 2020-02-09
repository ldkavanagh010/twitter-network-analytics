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
					if value[-3:] == 'zst':
						self.hrefs.append((value, 'zst'))
					if value[-3:] == 'tar':
						self.hrefs.append((value, 'tar'))



class HTMLHandler():

	def __init__(self):
		self.http = urllib3.PoolManager()

	def _get_html(self, website):
		return self.http.request('GET', website).data.decode('utf-8')

	def get_links(self, website):
		links = []
		parser = MyParser()
		parser.hrefs = list()
		parser.feed(self._get_html(website))
		for href in parser.hrefs:
			if href[1] == 'zst':
				links.append(href[0].replace(".", website, 1))
			else:
				links.append(website + href[0])
		return links