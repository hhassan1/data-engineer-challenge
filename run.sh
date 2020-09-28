#!/bin/bash

source env/bin/activate

export LINES=1000000
export RATE=4000
export UNIQUE_USERS=100000
export DELAY=5
export PROB_DELAY=1

export PORT=9093
export INPUT_TOPIC=input_topic
export OUTPUT_TOPIC=output_topic
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

pip install -r pip-requirements
docker-compose down --volumes
docker-compose up -d

sleep 15

./data-generator -c ${LINES} -r ${RATE} -n ${UNIQUE_USERS} -d ${DELAY} -p ${PROB_DELAY} | ./kafka-producer.py --broker localhost:${PORT} --topic ${INPUT_TOPIC} &
./measure_stream_statistics.py --broker localhost:${PORT} --input ${INPUT_TOPIC} --output ${OUTPUT_TOPIC} --delay ${DELAY} &
./kafka-consumer.py --broker localhost:${PORT} --topic ${OUTPUT_TOPIC} &

sleep infinity

