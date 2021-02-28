#!/usr/bin/env python

# Author: Genevieve LaLonde

"""
Pull data from an external file server. 
Store in json or raw html format, or decode to text.

Example sources:
http://rbi.ddns.net/getBreadCrumbData - json
http://rbi.ddns.net/getStopEvents - html or txt
"""

from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
from datetime import date
import argparse

def initialize():

	parser = argparse.ArgumentParser()
	parser.add_argument("-u", "--url", 
		dest="url", 
		default="http://isitsnowinginpdx.com/",
		help="URL of JSON data to download",
		required=True)
	parser.add_argument("-x", "--file-extension",
		dest="extension",
		default="html",
		help="The file extension. Default = html.")
	parser.add_argument("-r", "--raw_html",
		help="Store raw html instead of decoding it.",
		dest="raw",
		action="store_true")
	args = parser.parse_args()
	return args

if __name__ == '__main__':

	ssl._create_default_https_context = ssl._create_unverified_context

	args = initialize()
	raw = args.raw
	extension = args.extension
	url = args.url
	
	if raw and extension == 'html':
		page = urlopen(url)
		text = page.read().decode("utf8")
		page.close()
	else:
		soup = BeautifulSoup(urlopen(url), 'lxml')
		text = soup.get_text()
		extension = 'txt'
	dataNameList = url.split('/')
	file = open(str(date.today()) + '_' + dataNameList[len(dataNameList)-1] + '.' + extension,"w+")
	file.write(text)
	file.write('\n')
	file.close()
