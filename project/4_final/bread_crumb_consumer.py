#!/usr/bin/env python
#
# Author: Genevieve LaLonde

# Purpose:
# Consume JSON BreadCrumb messages from kafka, validate, and insert to postgres.

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

BreadCrumbRows = []
TripRows = set()
date = datetime.strptime('2020-01-01', '%Y-%m-%d').date()

def initialize():
    # TODO alphabetize and set required.
    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required arguments')

    parser.add_argument("-c", "--create-tables", 
        dest="create_tables",
        default=True,
        help="Creates the tables if they don't exist yet, without overwriting any existing table.",
        action="store_true")
    required.add_argument("-d", "--database", 
        dest="db",
        help="Database to use.",
        required=True)
    required.add_argument("-f", "--config-file", 
        dest="config_file", 
        default=f"{os.environ['HOME']}/.confluent/librdkafka.config",
        help="The path to the Confluent Cloud configuration file",
        required=True)
    parser.add_argument("-H", "--host", 
        dest="host",
        default='127.0.0.1',
        help="The host of the database, default localhost.")
    parser.add_argument("-p", "--password", 
        dest="pw",
        default='',
        help="Password for connecting to the database.")
    parser.add_argument("-s", "--staging",
        dest="staging",
        default=False,
        help="Only load to staging tables, but don't insert to the dataset. Use for testing.",
        action="store_true")
    required.add_argument("-t", "--topic-name",
        dest="topic",
        default='test',
        help="topic name",
        required=True)
    parser.add_argument("-u", "--user", 
        dest="user",
        default='root',
        help="User to connect to the database as.")
    parser.add_argument("-x", "--truncate-tables", 
        dest="truncate_tables",
        default=False,
        help="Truncates tables before inserting new messages.",
        action="store_false")

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

def createTables(conn):

  with conn.cursor() as cursor:
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS TripStaging (
            trip_id integer,
            route_id integer,
            vehicle_id integer,
            service_key service_type,
            direction tripdir_type,
            PRIMARY KEY (trip_id)
        );
        CREATE TABLE IF NOT EXISTS BreadCrumbStaging (
            tstamp timestamp,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer,
            FOREIGN KEY (trip_id) REFERENCES TripStaging
        );
      """)

def truncateTables(conn):

  with conn.cursor() as cursor:
    cursor.execute(f"""
        TRUNCATE TABLE BreadCrumbStaging;
        TRUNCATE TABLE TripStaging CASCADE;
      """)

def produce_completed(date):
    # Check if the full dataset for this date was produced. 
    # If not, there was some issue, so skip this run.
    # This allows messages to expire in kafka.
    # Cron will call the produce and consume again at the next interval.
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


def transform(row_dict):
    # Repackage values as needed for target table meaning.
    global date 

    # Set nulls to python none type
    for key in row_dict:
        if not row_dict[key]:
            if key in ['GPS_LONGITUDE']:
                row_dict[key] = '0'
            else:
                row_dict[key] = None

    # Generate a timestamp from the date and seconds offset.
    local_date = datetime.strptime(row_dict['OPD_DATE'],'%d-%b-%y')
    date = local_date.date()
    row_dict['TIMESTAMP'] = local_date + timedelta(seconds = int(row_dict['ACT_TIME']))

    # Overwrite the date in correct format, to prevent need to convert it again.
    row_dict['OPD_DATE'] = local_date

    # Determine if it is a weekday/weekend schedule from the date
    row_dict['SERVICE_KEY'] = None

    # Standin to determine the general out/in direction of the trip.
    row_dict['TRIP_DIRECTION'] = None

    # Standin to determine the route id of the trip.
    row_dict['ROUTE_ID'] = None

    return row_dict


def validate_row(row_dict,log):
    # Do some simple value checking, skip insertion for invalid values.
    global date
    # Existence Assertion
    # Each message has a trip ID and timestamp.
    if not row_dict['EVENT_NO_TRIP']:
        log.write(f'{date} Warn: Missing trip ID value in record: {row_dict}\n')
        return False

    if not row_dict['TIMESTAMP']:
        log.write(f'{date} Warn: Missing timestamp value in record: {row_dict}\n')
        return False

    # Limit Assertions
    # Breadcrumb Direction = ''  or between 0 and 359.
    if row_dict['DIRECTION']:
        int_direction = int(row_dict['DIRECTION'])
        if int_direction > 359 or int_direction < 0:
            log.write(f'{date} Warn: Invalid direction value in record: {row_dict}\n')
            return False

    # Speed = '' or between 0 and 200.
    if row_dict['VELOCITY']:
        int_speed = int(row_dict['VELOCITY'])
        if int_speed > 200 or int_speed < 0:
            log.write(f'{date} Warn: Invalid velocity value in record: {row_dict}\n')
            return False

    # CTran operates in Vancouver and Portland
    # The longitude should be between Hillsboro and Sandy
    long = float(row_dict['GPS_LONGITUDE'])
    if long < -122.938094 or long > -122.257624:
        log.write(f'{date} Warn: Invalid longitude value in record: {row_dict}\n')
        return False

    # The latitude should be between Tigard and La Center.
    lat = float(row_dict['GPS_LATITUDE'])
    if lat < 45.426648 or lat > 45.9:
        log.write(f'{date} Warn: Invalid latitude value in record: {row_dict}\n')
        return False

    # Intra-record Assertion
    # Generated timestamp less than 48 hours after date.
    if (row_dict['TIMESTAMP'] - row_dict['OPD_DATE']) > timedelta(days = 2):
        log.write(f'{date} Warn: Invalid generated timestamp value in record: {row_dict}\n')
        return False

    return True

def fix_data():
    # if a date is very different from the rest of the dataset, adjust it
    pass

def validate_dataset():
    # For each trip, there is only one vehicle id 
    # note: a vehicle ID could have multiple trips.

    # Limit Assertions
    # All values of trip_direction in {0,1}.

    # Summary Assertion
    # No records from the future: maximum date in the data is today.

    # SERVICE_KEY should be the same for almost all the data. 
    # Some may be on the next day (when time offset is large).
    # fix_data()

    pass

def store(row_dict):
    # Prep the data for insertion in batch.

    global BreadCrumbRows
    global TripRows

    BreadCrumbRows.append(
        (
            row_dict['TIMESTAMP'],
            row_dict['GPS_LATITUDE'],
            row_dict['GPS_LONGITUDE'],
            row_dict['DIRECTION'],
            row_dict['VELOCITY'],
            row_dict['EVENT_NO_TRIP']
        )
    )

    TripRows.add(
        (
            row_dict['EVENT_NO_TRIP'],
            row_dict['ROUTE_ID'],
            row_dict['VEHICLE_ID'],
            row_dict['SERVICE_KEY'],
            row_dict['TRIP_DIRECTION']
        )
    )
    return

def insert(conn):
    global BreadCrumbRows
    global TripRows

    with conn.cursor() as cursor:
        bread_cmd = sql.SQL("INSERT INTO BreadCrumbStaging VALUES (%s,%s,%s,%s,%s,%s);")
        trip_cmd = sql.SQL("INSERT INTO TripStaging VALUES (%s,%s,%s,%s,%s) on conflict do nothing;")
        execute_batch(cursor,trip_cmd,TripRows)
        execute_batch(cursor,bread_cmd,BreadCrumbRows)

    bread_inserted = len(BreadCrumbRows)
    trip_inserted = len(TripRows)

    # Clear the inserted values
    BreadCrumbRows = []
    TripRows = set()

    return bread_inserted, trip_inserted

def consume(conf,topic,conn):
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
                    bread_inserted, trip_inserted = insert(conn)
                    inserted_bread_rows += bread_inserted
                    inserted_trip_rows += trip_inserted
                    # As a final action move rows to the real tables.
                    if not staging:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                INSERT INTO Trip SELECT * FROM TripStaging;
                                INSERT INTO BreadCrumb SELECT * FROM BreadCrumbStaging;
                                DROP TABLE BreadCrumbStaging;
                                DROP TABLE TripStaging;
                                """)
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
                assert msg.key().decode('UTF-8') == "BreadCrumb"
                record_key = msg.key()

                record_value = msg.value()
                # Each row of breadcrumb data is one record value.
                row_dict = json.loads(record_value)
                row_dict = transform(row_dict)

                if consumed_messages == 0:
                    global date
                    assert produce_completed(date) or staging, \
                    'Messages did not finish producing for this date. ' \
                    + 'Skipping for now. To override for testing use --staging.' \
                    'Retry at next cron interval.'
                    log = open('breadcrumbs.log', 'a+')
                    log.write(f'{date} Info: Start consuming BreadCrumb data.\n')
                # Check if the row is valid
                if validate_row(row_dict,log):
                    store(row_dict)
                else:
                    skipped_rows += 1

                # Insert in batches of up to 10k rows.
                if len(BreadCrumbRows) >= 10000:
                    bread_inserted, trip_inserted = insert(conn)
                    inserted_bread_rows += bread_inserted
                    inserted_trip_rows += trip_inserted


                consumed_messages += 1

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        log = open('breadcrumbs.log', 'a+')
        if staging:
            print(f"Consumed {consumed_messages} messages.")
            print(f"Inserted {inserted_bread_rows} rows to `{db}.BreadCrumbStaging`.")
            print(f"Inserted {inserted_trip_rows} unique rows to `{db}.TripStaging`.")
            print(f"Skipped {skipped_rows} messages due to data validation.")
            log.write(f'{date} Info: Inserted {inserted_bread_rows} rows to `{db}.BreadCrumbStaging`.\n')
            log.write(f'{date} Info: Inserted {inserted_trip_rows} unique rows to `{db}.TripStaging`.\n')
            log.write(f'{date} Info: Skipped {skipped_rows} messages due to data validation.\n')
        else:
            print(f"Consumed {consumed_messages} messages.")
            print(f"Inserted {inserted_bread_rows} rows to `{db}.BreadCrumb`.")
            print(f"Inserted {inserted_trip_rows} unique rows to `{db}.Trip`.")
            print(f"Skipped {skipped_rows} messages due to data validation.")
            log.write(f'{date} Info: Inserted {inserted_bread_rows} rows to `{db}.BreadCrumb`.\n')
            log.write(f'{date} Info: Inserted {inserted_trip_rows} unique rows to `{db}.Trip`.\n')
            log.write(f'{date} Info: Skipped {skipped_rows} messages due to data validation.\n')
        log.close()


if __name__ == '__main__':

    args = initialize()
    topic = args.topic
    host = args.host
    db = args.db
    user = args.user
    pw = args.pw
    create_tables = args.create_tables
    truncate_tables = args.truncate_tables
    staging = args.staging
    if not staging:
        truncate_tables = True
    config_file = args.config_file

    conf = ccloud_lib.read_ccloud_config(config_file)
    conn = dbconnect(host,db,user,pw)

    if create_tables:
      createTables(conn)
    if truncate_tables or not staging:
      truncateTables(conn)
    
    consume(conf,topic,conn)
