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


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

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

    # Process messages
    # TODO switch this to append instead of overwrite
    file = open(str(date.today()) + '_consumed_BreadCrumbData' + ".json","w+")
    consumed_messages = 0
    previous_row = 0
    # Recreating json formatting.
    file.write('[\n')
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
                # Add commas to maintain json formatting.
                if previous_row:
                    file.write(',\n')
                else:
                    previous_row = 1
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
        file.write(']\n')
        file.close()
        consumer.close()
        print('Consumed {} messages.'.format(consumed_messages))