#!/usr/bin/env python

# Author: Genevieve LaLonde

"""
Project Part 1 Section B Task Description

1. Install "urllib" python package on your VM.
2. Make an HTTP request to a web server using "urllib".
3. Retrieve the results.
4. Write the data to a file and exit.

Step C
1. Set daily job with cron: 'crontab -e' and '0 10 * * * /usr/bin/python3 /absolute/path/to/pp1-b.py'
1b. TODO Run twice daily.
* Requires: adjust cron time and include hour in output filename.
X. TODO Make your data pipeline reliable and tolerant of unexpected computer systems shortages and problems.
 * VM crashes
 * network failures
 * limited storage space
 * etc.

Step E will include:
TODO
1. Parse the JSON data.
2. Send individual records to a Kafka topic.
"""

from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
from datetime import date

ssl._create_default_https_context = ssl._create_unverified_context

url = "http://rbi.ddns.net/getBreadCrumbData"
soup = BeautifulSoup(urlopen(url), 'lxml')
text = soup.get_text()

#str(date.today()) + 'BreadCrumbData' + ".json"
file = open(str(date.today()) + 'BreadCrumbData' + ".json","w+")
file.write(text)
file.write('\n')
file.close()
