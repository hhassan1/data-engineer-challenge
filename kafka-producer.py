#!/usr/bin/env python3
import argparse
import logging
import sys
import time
import threading

from confluent_kafka import Producer
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)



def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    parser.add_argument('--broker', default="localhost:9093")
    args = parser.parse_args()
    return args.topic, args.broker

def create_producer(server):
    return Producer({'bootstrap.servers': server})

counter = 0
done = False

def run():
    global counter
    global done
    topic, server = parse_args()
    producer = create_producer(server)
    for i, line in enumerate(sys.stdin):
        producer.poll(0)
        if i % 25000 == 0:
            producer.flush()
        counter += 1
        producer.produce(topic, line.encode('utf-8'))
    done = True


def metric():
    global counter
    global done
    while not done:
        pre = counter
        pre_time = time.time()
        time.sleep(1)
        post = counter
        post_time = time.time()
        print(f"Send: {(post - pre)/(post_time - pre_time)} m/s")

t = threading.Thread(target=run)
t.start()
metric()
