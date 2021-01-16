
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# %matplotlib inline
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import re

ssl._create_default_https_context = ssl._create_unverified_context

"""
import os, ssl

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
getattr(ssl, '_create_unverified_context', None)):
	ssl._create_default_https_context = ssl._create_unverified_context
"""

url = "http://www.hubertiming.com/results/2017GPTR10K"
# html = urlopen(url)
# print(html)

# params: format, parser
# soup = BeautifulSoup(html, 'lxml')
soup = BeautifulSoup(urlopen(url), 'lxml')

# to check type, example output
# print("Type is:", type(soup))
# eg: bs4.BeautifulSoup

# Get the title
title = soup.title
# print("Title is:", title)
# eg: <title>2017 Intel Great Place to Run 10K \\ Urban Clash Games Race Results</title>

# Print out the text
text = soup.get_text()
# print(soup.text)


"""
 < a > - hyperlinks
 < table > - tables
 < tr > - table rows
 < th > - table headers
 < td > - table cells
"""

# soup.find_all('a')

"""
print("Anchor links are: ")
all_links = soup.find_all("a")
for link in all_links:
	print(link.get("href"))
"""

# Print the first 10 rows for sanity check
rows = soup.find_all('tr')
# print(rows[:10])


# Clean with BS
# print("Table Data Cells: ")
for row in rows:
	row_td = row.find_all('td')
	cleantext = BeautifulSoup(str(row_td), "lxml").get_text()
# print(cleantext)
# print("Type of data:", type(row_td))

# Clean with regex
list_rows = []
for row in rows:
	row_td = row.find_all('td')
	match_expression = re.compile('<.*?>')
	clean = (re.sub(match_expression, '', str(row_td)))
	list_rows.append(clean)
# print(clean)
# print(type(clean))

df = pd.DataFrame(list_rows)
# print('Data Frame first 10 rows:')
# print(df.head(10))

"""
# This doesn't work.
df = pd.DataFrame(cleantext)
print('Data Frame first 10 rows:')
print(df.head(10))
"""

df_explode = df[0].str.split(',', expand = True)
df_explode[0] = df_explode[0].str.strip('[')
df_explode[9] = df_explode[9].str.strip(']')
# print(df_explode.head(10))

# Headers
col_labels = soup.find_all('th')
all_header = []
clean_headers = BeautifulSoup(str(col_labels), "lxml").get_text()
all_header.append(clean_headers)
#print(all_header)

# print('Data Frame Header')
df_header = pd.DataFrame(all_header)
df_header_split = df_header[0].str.split(',', expand = True)
# print(df_header_split.head())

full_table = pd.concat([df_header_split, df_explode])
full_table = full_table.rename(columns = full_table.iloc[0])
# print(full_table.head(10))

# More Info
# print(full_table.info())
# print(full_table.shape)

# Drop NULLs
full_table_no_null = full_table.dropna(axis=0, how='any')
# print(full_table_no_null.info())
# print(full_table_no_null.shape)

# Remove duplicate header row
full_table_good_header = full_table_no_null.drop(full_table_no_null.index[0])
# print(full_table_no_null.head())

# Fix formatting, remove list start/end
full_table_good_header.rename(columns={'[Place': 'Place'},inplace=True)
full_table_good_header.rename(columns={' Team]': 'Team'},inplace=True)
# print(full_table_no_null.head())


full_table_good_header['Team'] = full_table_good_header['Team'].str.strip(']')
print('Final table')
print(full_table_good_header.head())

# ===

# Average finish time (min)

time_list = full_table_good_header[' Chip Time'].tolist()

time_mins = []
for i in time_list:
	if len(i.split(':')) == 2 :
		h = 0
		m, s = i.split(':')
	else:
		h, m, s = i.split(':')

	math = (int(h) * 3600 + int(m) * 60 + int(s))/60
	time_mins.append(math)
# print(time_mins)

full_table_good_header['Runner_mins'] = time_mins
print('Average finish time column fixed:')
print(full_table_good_header.head())
	















































