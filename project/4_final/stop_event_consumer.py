#!/usr/bin/env python
#
# Author: Genevieve LaLonde

# Purpose:
# Consume messages from kafka, validate, and insert to postgres.

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
from datetime import date, datetime, timedelta
import argparse
import time
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import re
import csv
import os

EventRows = set()
validations = {'trip_id': -1, 'vehicle_n': {}, 'aim': {}, 'service_k': {}}

def initialize():

    # TODO add an argument for testing, which uses staging tables instead.
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--config-file", 
        dest="config_file", 
        default=f"{os.environ['HOME']}/.confluent/librdkafka.config",
        help="The path to the Confluent Cloud configuration file")
    parser.add_argument("-t", "--topic-name",
        dest="topic",
        default='test',
        help="topic name",
        required=True)
    parser.add_argument("-H", "--host", 
        dest="host",
        default='127.0.0.1',
        help="The host of the database, default localhost.")
    parser.add_argument("-d", "--database", 
        dest="db",
        default='test_db',
        help="Database to use.")
    parser.add_argument("-u", "--user", 
        dest="user",
        default='root',
        help="User to connect to the database as.")
    parser.add_argument("-p", "--password", 
        dest="pw",
        default='',
        help="Password for connecting to the database.")
    args = parser.parse_args()
    return args

# connect to the database
def dbconnect(host='0', db='ctran', user='vancouver', pw='washington'):
    connection = psycopg2.connect(
        host=host,
        database=db,
        user=user,
        password=pw)
    connection.autocommit = True
    return connection


def transform(row_dict):
    # Repackage values as needed for target table meaning.

    # Set nulls to python none type
    for key in row_dict:
        if not row_dict[key]:
            if key in ['direction', 'x_coordinate']:
                row_dict[key] = '0'
            else:
                row_dict[key] = None

    # Change direction [0,1] to ['Out','Back']
    # TODO after the fixer is ready, set this after it.
    switcher = {0: 'Out', 1: 'Back'}
    row_dict['direction'] = switcher[int(row_dict['direction'])]

    switcher = {'W': 'Weekday', 'S': 'Saturday', 'U': 'Sunday'}
    row_dict['service_key'] = switcher[row_dict['service_key']]


    return row_dict


def valid_row(row_dict):
    # Do some simple value checking, skip insertion for invalid values.
    # Validate values even if they won't be inserted to the table, 
    # because invalidity in other attributes of the record cast the whole record into doubt.

    # Existence Assertion
    # Each message has a trip ID and timestamp.
    if not row_dict['trip_id']:
        return False

    if not row_dict['date']:
        return False

    # Limit Assertions
    # CTran operates in Vancouver and Portland
    # The longitude should be between Hillsboro and Sandy
    long = float(row_dict['x_coordinate'])
    if long < -122.938094 or long > -122.257624:
        return False

    # The latitude should be between Tigard and La Center.
    lat = float(row_dict['y_coordinate'])
    if lat < 45.426648 or lat > 45.862359:
        return False

    # Intra-record Assertion
    # The bus arrives at the stop before it leaves it. 
    # At the same time is also normal, but it cannot be after.
    if int(row_dict['arrive_time']) > int(row_dict['leave_time']):
        return False

    """
    # TODO: test this. commenting out for now
    # TODO: add route number too

    # Inter-record Assertion
    # All records where the trip ID is the same have the same vehicle number, direction, and service key.
    # On first occurrence of trip, set the other values.
    if row_dict['trip_id'] != validations['trip_id']:
        validations = {'trip_id': int(row_dict['trip_id']), \
                        'vehicle_n': {int(row_dict['vehicle_number']): 1}, \
                        'aim': {int(row_dict['direction']): 1}, \
                        'service_k': {int(row_dict['service_key']): 1}}

    
    # If any of these values not in the dict already, there was an issue. Fix it.
    # Otherwise, increment the count of times that value has been seen.
    else:
        if not validations['vehicle_n'][int(row_dict['vehicle_number'])]:
            validations['vehicle_n'][int(row_dict['vehicle_number'])] = 1
            row_dict['vehicle_number'] = fix_data(row_dict['vehicle_number'], 'vehicle_n')
        else:
            validations['vehicle_n'][int(row_dict['vehicle_number'])] += 1

        if not validations['aim'][int(row_dict['direction'])]:
            validations['aim'][int(row_dict['direction'])] = 1
            row_dict['direction'] = fix_data(row_dict['direction'],'aim')
        else:
            validations['aim'][int(row_dict['direction'])] += 1

        if not validations['service_k'][int(row_dict['service_key'])]:
            validations['service_k'][int(row_dict['service_key'])] = 1
            row_dict['service_key'] = fix_data(row_dict['service_k'],'service_k')
        else:
            validations['service_k'][int(row_dict['service_key'])] += 1
    """

    return True


def fix_data(current_value, kind = 'vehicle_n', ):
    # For the given attribute, return the value that has been seen more for this trip. 
    # If it's a tie, return the same value.
    # Maintain the type of the current value.
    for attribute in validations[kind]:
        if validations[kind][attribute] > validations[kind][int(current_value)]:
            current_value = str(validations[kind][attribute])
    return current_value


def validate_dataset():

    # Summary Assertion
    # No records from the future: maximum date in the data is today.

    # SERVICE_KEY should be the same for all the data, since it is all from the same day. 
    # This holds, for trips that continue into the small hours. Their schedule is still for the previous day.

    pass

def store(row_dict):
    # Prep the data for insertion in batch.

    global EventRows

    # Trip id is at the end for the SQL statement condition check.
    EventRows.add(
        (
            int(row_dict['route_number']),
            int(row_dict['vehicle_number']),
            row_dict['service_key'],
            row_dict['direction'],
            int(row_dict['trip_id'])
        )
    )
    return

def insert(conn):
    global EventRows

    # Convert the set of unique trips to a list for execute_batch
    # TODO: this may be unnecessary, needs testing.
    UniqueTrips = []
    for element in EventRows:
        UniqueTrips.append(element)

    with conn.cursor() as cursor:
        trip_cmd = sql.SQL("UPDATE Trip SET route_id = %s, vehicle_id = %s, service_key = %s, direction = %s WHERE trip_id = %s;")
        execute_batch(cursor,trip_cmd,UniqueTrips)

    # Clear the inserted values
    EventRows = set()

def consume(conf,topic,conn,db):
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

    # Consume by inserting to postgres.
    consumed_messages = 0
    fail_count = 0
    skipped_rows = 0
    inserted_bread_rows = 0
    inserted_trip_rows = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            # Check for message
            if msg is None:
                if fail_count >= 5:
                    # Insert records from the final batch.
                    insert(conn)
                    inserted_trip_rows += len(EventRows)
                    print("No new messages in 5 seconds, closing.")
                    break
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                fail_count += 1
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Reset if new messages were received.
                fail_count = 0 

                # Parse Kafka message
                assert msg.key().decode('UTF-8') == "StopEvent"
                record_value = msg.value()
                # Each row of StopEvent data is one record value.
                row_dict = json.loads(record_value)
                print(f"route right after load: {row_dict['route_number']}")
                row_dict = transform(row_dict)
                # Check if the row is valid
                if valid_row(row_dict):
                    store(row_dict)
                else:
                    skipped_rows += 1

                # Insert in batches of up to 1k rows.
                if len(EventRows) >= 1000:
                    insert(conn)
                    inserted_trip_rows += len(EventRows)

                consumed_messages += 1

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        print(f"Consumed {consumed_messages} messages.")
        print(f"Inserted {inserted_trip_rows} unique rows to `{db}.Trip`.")
        print(f"Updated {inserted_trip_rows} unique rows in `{db}.Trip`.")
        print(f"Skipped {skipped_rows} messages due to data validation.")

def main():
    args = initialize()
    topic = args.topic
    host = args.host
    db = args.db
    user = args.user
    pw = args.pw
    config_file = args.config_file

    conf = ccloud_lib.read_ccloud_config(config_file)
    conn = dbconnect(host,db,user,pw)

    consume(conf,topic,conn,db)


if __name__ == '__main__':
    main()

