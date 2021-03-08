#!/usr/bin/python
#
# Author: Genevieve LaLonde

# Purpose
# Produce messages directly from server to kafka.
# Flushes are batched, to speed up production.
# Can produce only specified count of messages, helpful for testing and development. 

# With thanks to Apache Confluent Kafka Client Examples
# URL: https://github.com/confluentinc/examples
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================


import argparse
from bs4 import BeautifulSoup
import ccloud_lib
from confluent_kafka import Producer, KafkaError
from datetime import date, datetime 
import json
import os
import pandas as pd
import re
import ssl
from urllib.request import urlopen

delivered_records = 0

def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Produce StopEvent data messages \
                  to Confluent Cloud")
    parser.add_argument(
        '-n', '--message-number',
        dest='message_count',
        help='Number of messages to upload to topic',
        default=100000000000
        )
    required = parser.add_argument_group('required arguments')
    required.add_argument("-f", "--config-file", 
        dest="config_file", 
        default=f"{os.environ['HOME']}/.confluent/librdkafka.config",
        help="The path to the Confluent Cloud configuration file",
        required=True)
    required.add_argument('-t',
                          dest="topic",
                          help="topic name",
                          required=True)
    args = parser.parse_args()
    return args

def clean(raw):
    return BeautifulSoup(str(raw), "lxml").get_text()

def get_date(date_head):
    # Get the date from the header.
    assert(len(date_head) == 1)
    header = date_head[0].get_text().split()[4]
    return datetime.strptime(header,'%Y-%m-%d').timestamp()

def get_trip_ids(trip_headers):
    # Get all the trip IDs in order
    trip_match = re.compile('.*trip ([0-9]+).*')
    trip_ids = []
    for trip_id in trip_headers:
        trip_id = trip_id.get_text()
        trip_ids.append(int(re.match(trip_match,trip_id).group(1)))
    return trip_ids

def get_columns(attributes):
    # Assume all the tables have the same columns.
    # Wait to handle date and trip ID until producing.
    columns = []
    for attribute in attributes:
        columns.append(attribute.get_text())
    return columns

def acked(err, msg):
    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).

    global delivered_records

    """
    Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        if delivered_records % 100 == 0:
            print("Produced 100 records to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

def main():
    # Read arguments and configurations and initialize
    args = parse_args()
    config_file = args.config_file
    topic = args.topic
    message_count = int(args.message_count)
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'http://rbi.ddns.net/getStopEvents'
    soup = BeautifulSoup(urlopen(url), 'lxml')

    # Parse the table pieces
    timestamp = get_date(soup.find_all('h1'))
    trip_ids = get_trip_ids(soup.find_all('h3'))

    tables = soup.find_all("table")

    columns = get_columns(tables[0].find_all("th"))

    # For each table, 
    # For each row:
    # Produce a record with that row of data, along with the date and trip_id for that table.
    clen = len(columns)
    produced_records=0
    record_key = "StopEvent"

    # Iterate over all tables
    for trip_index in range(len(tables)):
        table_data_list = tables[trip_index].find_all("td")
        rowdict = {}

        # Iterate over all the table data
        # It is in a giant list, so use index to determine row endings.
        # Walk the columns and the table data at the same time.
        for index in range(len(table_data_list)):

            # Only produce a limited number of messages, for testing.
            if produced_records >= message_count:
                producer.flush()
                break
            rowdict[columns[index % clen]] = table_data_list[index].get_text()

            # Produce a complete row from the dictionary.
            # Using modulo allows us to walk columns and data at the same time.
            if (index % clen) == (clen - 1):
                rowdict['date'] = timestamp
                rowdict['trip_id'] = trip_ids[trip_index]
                producer.produce(topic, key=record_key, value=json.dumps(rowdict), on_delivery=acked)
                rowdict = {}
                produced_records += 1

            # Check for message count, flush periodically.
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
            producer.poll(0)
            if produced_records % 1800 == 0:
                producer.flush()

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))

if __name__ == '__main__':
    main()
    
