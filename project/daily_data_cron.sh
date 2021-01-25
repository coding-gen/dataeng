#!/bin/sh

# Linux
wget=/usr/local/bin/wget
$wget http://rbi.ddns.net/getBreadCrumbData -O ~/developer/dataeng/daily_data/getBreadCrumbData_`date +"%Y-%m-%d"`.json
