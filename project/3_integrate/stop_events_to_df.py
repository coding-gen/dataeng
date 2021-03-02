import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import re
from datetime import date, datetime 

# from pylab import rcParams
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
# %matplotlib inline

ssl._create_default_https_context = ssl._create_unverified_context

url = 'http://rbi.ddns.net/getStopEvents'
soup = BeautifulSoup(urlopen(url), 'lxml')

def clean(raw):
	return BeautifulSoup(str(raw), "lxml").get_text()

# Get the date from the header.
date_head = soup.find_all('h1')
assert(len(date_head) == 1)
timestamp = clean(date_head[0])
date_match = re.compile('.*for (\d{4}-\d{2}-\d{2}).*')
timestamp = re.match(date_match,timestamp).group(1)
timestamp = datetime.strptime(timestamp,'%Y-%m-%d').date()

# Get all the trip IDs in order
trip_headers = soup.find_all('h3')
trip_match = re.compile('.*trip ([0-9]+).*')
trip_ids = []
for trip_id in trip_headers:
	trip_id = clean(trip_id)
	trip_ids.append(int(re.match(trip_match,trip_id).group(1)))

tables = soup.find_all("table")

# Assume all the tables have the same columns.
columns = ['date', 'trip_id']
attributes = tables[0].find_all("th")
clen = len(attributes)
for attribute in attributes:
	columns.append(clean(attribute))

# For each table, prep it for the dataframe, along with the date and trip_id for that table.
rowlist = []
trip_i = 0
clen = len(attributes)
column_i = 0
for table in tables:
	table_data_blob = table.find_all("td")
	row = []
	for attribute in table_data_blob:
			if column_i % clen == 0:
				rowlist.append(row)
				row = [timestamp,trip_ids[trip_i]]
			row.append(clean(attribute))
			column_i += 1

# Create a data frame with those columns.
df = pd.DataFrame(rowlist, columns=columns)
