#!/usr/bin/env python3
import argparse
import json
import random
import sys
import time

from bloom_filter import BloomFilter
from collections import deque
from confluent_kafka import Consumer, Producer
from queue import SimpleQueue
from datetime import timedelta
from threading import Thread



class StreamReader(Thread):
    MAX_SHUTDOWN_COUNTER = 6
    POLL_TIMEOUT = 1.0

    def __init__(self, server, topic, group_id):
        self._consumer = Consumer({
            'bootstrap.servers': server,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self._consumer.subscribe([topic])
        self._shutdown_counter = 0
        self._queue = SimpleQueue()
        super().__init__()

    def fetch(self, block=True, timeout=None):
        while True:
            msg = self._consumer.poll(StreamReader.POLL_TIMEOUT)
            if msg is None or msg.error():
                self._shutdown_counter += 1
                if self._shutdown_counter == StreamReader.MAX_SHUTDOWN_COUNTER:
                    break
            else:
                yield msg.value().decode('utf-8')

    def run(self):
        for message in self.fetch():
            self._queue.put_nowait(message)
        self._queue.put_nowait(None)

    def queue(self):
        return self._queue


class StatisticsHolder:
    MAX_DELAY = 5

    def __init__(self, start_ts, end_ts):
        self.counter = 0
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.filter = BloomFilter(max_elements=20000, error_rate=0.01)

    def add(self, uid):
        if uid not in self.filter:
            self.counter += 1
            self.filter.add(uid)

    def __contains__(self, ts):
        return ts >= self.start_ts and ts < self.end_ts

    def closed_by(self, ts):
        return ts >= self.end_ts + StatisticsHolder.MAX_DELAY


class StatisticsReporter:

    def __init__(self, producer, unit="minutes", topic="output_topic"):
        self.producer = producer
        self.unit = unit
        self.delta = timedelta(**{unit:1}).total_seconds()
        self.stats = deque()
        self.topic = topic
    
    def process_message(self, message):
        ts = message["ts"]
        uid = message["uid"]
        pop_cnt = -1
        found = False
        for i, s in enumerate(self.stats):
            if ts in s:
                s.add(uid)
                found = True
                break
            elif s.closed_by(ts):
                pop_cnt = i

        if not found:
            start_time = ts - (ts % self.delta)
            end_time = start_time + self.delta
            s = StatisticsHolder(start_time, end_time)
            s.add(uid)
            self.stats.append(s)

        for _ in range(pop_cnt+1):
            s = self.stats.popleft()
            producer.poll(0)
            producer.produce(self.topic, f"{self.unit},{s.start_ts},{s.counter}".encode('utf-8'))
            producer.flush()
            

class StreamStatisticsRunner(Thread):

    def __init__(self, queue, reporters):
        self._queue = queue
        self._reporters = reporters
        self.counter = 0
        super().__init__()


    def run(self):
        while True:
            message = self._queue.get()
            if message is None:
                break
            for reporter in self._reporters:
                reporter.process_message(json.loads(message))
            self.counter += 1

class MessagesPerSecondMetric(Thread):

    def __init__(self, stream_statistics_runner):
        self.ssr = stream_statistics_runner
        super().__init__()

    def run(self):
        while True:
            pre = self.ssr.counter
            pre_time = time.time()
            time.sleep(1)
            post = self.ssr.counter
            post_time = time.time()
            print(f"Process: {(post - pre)/(post_time - pre_time)} m/s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')
    parser.add_argument('--broker')
    parser.add_argument('--delay')
    args = parser.parse_args()
    sr = StreamReader(args.broker, args.input, str(random.random()))
    producer = Producer({'bootstrap.servers': args.broker})
    ssr = StreamStatisticsRunner(sr.queue(), [StatisticsReporter(producer, "minutes", args.output)])
    StatisticsHolder.MAX_DELAY = int(args.delay)
    mpsm = MessagesPerSecondMetric(ssr)
    sr.start()
    ssr.start()
    mpsm.start()

