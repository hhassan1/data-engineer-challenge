#!/usr/bin/env python3
import argparse
import sys
import threading
import time

from confluent_kafka import Consumer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    parser.add_argument('--broker', default="localhost:9093")
    args = parser.parse_args()
    return args.topic, args.broker

def create_consumer(server, group_id = 1):
    return Consumer({
        'bootstrap.servers': server,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })

if __name__ == "__main__":
    topic, server = parse_args()
    consumer = create_consumer(server)
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print(f"Received: {msg.value().decode('utf-8')}")

