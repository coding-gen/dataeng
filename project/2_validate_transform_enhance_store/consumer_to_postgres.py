#!/usr/bin/env python
#
# Author: Genevieve LaLonde

# Purpose:
# Consume messages to file, and maintain JSON formatting.

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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer, KafkaError
import json
import ccloud_lib
from datetime import date
import argparse
import time
import psycopg2
import re
import csv

def initialize():

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--config-file", 
        dest="config_file", 
        help="path to Confluent Cloud configuration file",
        required=True)
    parser.add_argument("-t", "--topic-name",
        dest="topic",
        help="topic name",
        required=True)
    parser.add_argument("-c", "--create-table", 
        dest="create_table",
        help="Whether to create the table or not.",
        action="store_true")
    parser.add_argument("-H", "--host", 
        dest="host",
        help="host of the database, default localhost.")
    parser.add_argument("-d", "--database", 
        dest="db",
        help="Database to use.")
    parser.add_argument("-u", "--user", 
        dest="user",
        help="User to connect to the database as.")
    parser.add_argument("-p", "--password", 
        dest="pw",
        help="Password for connecting to the database.")
    args = parser.parse_args()
    return args

# connect to the database
def dbconnect(host='0', db='bus_data', user='ctran', pw='bread_bytes'):
    connection = psycopg2.connect(
        host="localhost",
        database=db,
        user=user,
        password=pw)
    connection.autocommit = True
    return connection

def row2vals(row):
  # handle the null vals
  for key in row:
    if not row[key]:
      row[key] = 0
    #row['County'] = row['County'].replace('\'','')  # strip out single quotes within literals

  ret = f"""
       {Year},                          -- Year
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop

  """
  return ret

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLstmt(rowlist):
  cmdlist = []
  for row in rowlist:
    valstr = row2vals(row)
    cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
    cmdlist.append(cmd)
  return cmdlist

def insert_messages(conf,topic,conn,create_table):
        # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })
    # Subscribe to topic
    consumer.subscribe([topic])

    #rlis = read in data
    cmdlist = getSQLstmt(rlis)

    # Process messages
    # TODO switch this to append instead of overwrite
    file = open(str(date.today()) + '_consumed_BreadCrumbData' + ".json","w+")
    consumed_messages = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
                """
                # This errored on "code" 
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))
                """
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                # Each line of data (breadcrumb) is a record value.
                file.write(str(json.loads(record_value)))
                consumed_messages += 1

                """
                # This was too verbose.
                print("Consumed record with key {} and value {}"
                      .format(record_key, record_value))
                """
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        # Maintain json formatting too.
        file.close()
        consumer.close()
        print('Consumed {} messages.'.format(consumed_messages))


if __name__ == '__main__':

    """
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)
    cmdlist = getSQLcmnds(rlis)

    if CreateDB:
      createTable(conn)

    load(conn, cmdlist)

    """

    # Read arguments and configurations and initialize
    args = initialize()
    config_file = args.config_file
    create_table = args.create_table
    topic = args.topic
    host = args.host
    db = args.db
    user = args.user
    pw = args.pw
    conf = ccloud_lib.read_ccloud_config(config_file)
    
    conn = dbconnect()

    insert_messages(conf,topic,conn,create_table)


