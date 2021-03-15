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

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import argparse
from datetime import date, datetime


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Confluent Python Client example to produce messages \
                  to Confluent Cloud")
    parser.add_argument(
        '-b', '--backup-to-file',
        dest='backup',
        default=False,
        help="Also store a local backup of the data to the current directory. Ignored if ingesting from backup file already.",
        action="store_true") 
    parser.add_argument(
        '--force',
        dest='force_ingest',
        default=False,
        help="Force ingest even if this data has already been produced.",
        action="store_true") 
    parser.add_argument(
        '-n', '--message-number',
        dest='message_count',
        default=100000000000,
        help='Number of messages to upload to topic'
        )
    parser.add_argument(
        '-s', '--source-file',
        dest='source',
        default=None,
        help='Path to source file, instead of ingesting from server.'
        )
    required = parser.add_argument_group('required arguments')
    required.add_argument('-f',
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=True)
    required.add_argument('-t',
                          dest="topic",
                          help="topic name",
                          required=True)
    args = parser.parse_args()

    return args

def ingested(date):
    # Check if data for this date has already been ingested, as per the log. 
    try:
        log = open('breadcrumbs.log', 'r')
    except:
        return False
    for line in log:
        log_cols = line.split()
        if log_cols[0] ==  str(date) and log_cols[2] == 'Finished':
            log.close()
            return True
    log.close()
    return False

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = parse_args()
    backup = args.backup
    force_ingest = args.force_ingest
    message_count = int(args.message_count)
    source = args.source
    topic = args.topic
    config_file = args.config_file
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

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            if delivered_records % 100000 == 0:
                print("Produced 100,000 records to topic {} partition [{}] @ offset {}"
                    .format(msg.topic(), msg.partition(), msg.offset()))
    if source:
        file_object = open(source)
        original_data = json.load(file_object)
    else:
        ssl._create_default_https_context = ssl._create_unverified_context
        url = "http://rbi.ddns.net/getBreadCrumbData"
        soup = BeautifulSoup(urlopen(url), 'lxml')
        text = soup.get_text()
        original_data = json.loads(text)
        if backup:
            file = open(str(date.today()) + '_BreadCrumbData' + ".json","w+")
            file.write(text)
            file.write('\n')
            file.close()

    #TODO should really name this something else, since date is imported too.
    date = datetime.strptime(original_data[0]['OPD_DATE'],'%d-%b-%y').date()

    # Don't ingest this data if we already have, unless we're forcing or testing a smaller dataset.
    assert (force_ingest or not ingested(date) or message_count < 100000000000), \
    "This data has already been ingested, skipping.\n" \
    + "Force with --force. For testing purposes, reduce the message count."
        
    log = open('breadcrumbs.log', 'a+')
    log.write(f'{date} Info: Start producing BreadCrumb data.\n')

    record_key = "breadcrumb"
    flushed_records = 0
    for record_data in original_data:
        record_value = json.dumps(record_data)

    flushed_records=0
    record_key = "BreadCrumb"
    for record_data in original_data:
        if flushed_records >= message_count:
            producer.flush()
            break
        record_value = json.dumps(record_data)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
        flushed_records+=1
        if flushed_records%1800 == 0:
            producer.flush()
            
    producer.flush()

    # Only log complete if we did the full dataset.
    if message_count == 100000000000:
        log.write(f'{date} Info: Finished producing all BreadCrumb data.\n')

    log.write(f'{date} Info: Produced {delivered_records} BreadCrumb messages.\n')
    log.close()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
