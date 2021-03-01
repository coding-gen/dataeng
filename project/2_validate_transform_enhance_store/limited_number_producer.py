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


"""
Optional: Run this producer daily at noon with cron:
crontab -e
0 12 * * * /usr/bin/python3 /home/ubuntu/bread_producer_from_server.py -f ~/.confluent/librdkafka.config -t topic-test
confirm: crontab -l
"""

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import argparse

def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Confluent Python Client example to produce messages \
                  to Confluent Cloud")
    parser._action_groups.pop()
    parser.add_argument(
        '-n', '--message-number',
        dest='message_count',
        help='Number of messages to upload to topic',
        default=100000000000
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

if __name__ == '__main__':

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
            if delivered_records % 100 == 0:
                print("Produced 100 records to topic {} partition [{}] @ offset {}"
                    .format(msg.topic(), msg.partition(), msg.offset()))

    ssl._create_default_https_context = ssl._create_unverified_context
    url = "http://rbi.ddns.net/getBreadCrumbData"
    soup = BeautifulSoup(urlopen(url), 'lxml')
    text = soup.get_text()
    original_data = json.loads(text)

    flushed_records=0
    record_key = "breadcrumb"
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
            

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
