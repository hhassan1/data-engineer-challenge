# Data engineering challenge for Doodle

## Applicant's information

Name: Hussein Hassan Harrirou
Applying for: Data engineering at Doodle in Zurich, Switzerland
Date: 28.09.2020

## Running the code

The prerequisites are having docker-compose and python>=3.7 installed.

Then simply run `bash run.sh`

## Motivation

The solution I provide monitors the number of unique users per minute, waiting until receiving a message 5 seconds after the minute has ended to commit the result of the interval. This is, for minute 0s-59s, the algorithm will commit the result once a message with timestamp >= 1m05s is received (due to the given constraint of 99.9% of messages arriving with at most 5 seconds delay).

The number of unique users is computed using a bloom filter per interval. Bloom filters are probabilistic data structures that will ensure that we never double-count a user, but may fail to consider unseen users. This will give us lower-bound estimations.

The choice of using bloom filters was made in order to minimize the space-complexity of the algorithm. This is usually critical for systems that record many metrics in long-running streams. Another option would have been to record all the users per interval using a hash set, which would work for intervals with small number of unique users. I considered that a bloom filter would be an adequate choice as we have already accepted that we can lose a 0.1% of the results due to delays.


## Outputting the data

As implied before, the constraint of 99.9% messages arriving with a delay below 5 seconds made me choose to emit the interval statistic once a message 5 seconds above the interval limit is received. I think it is possible to have a timer to force the emission of the message in case no messages are received for a long time, but I didn't want to complicate my code on this version.

## Error estimation

Given that the hash-collision probability is independent from the >5s delay probability, the percentage of uncounted users should be on the order of 1% + 99.9% * 1% < 2%. This is true if the estimated number of unique users per interval is correct (I have used a number of 50000 unique users per minute as an upper threshold)

## Speed analysis

I have used pv to measure the speed of all the joint-points of the pipeline. The main metric I used was the frame/second (lines/second), which I measured using the tool `pv`, available in Linux. I measured the number of lines per second when producing the input data and when reading data from Kafka. I have also measured the number of lines per second processed by my unique-user-per-minute recorder.

Given that every computer is different, absolute metrics are not relevant. I will thus only mention relative comparations. On the pipeline I offer here, I experienced a 4x slowdown of the processed lines per second compared to the input lines per second. Of this, half of the delay I attribute it to JSON parsing, and the other half to business logic (mostly operations on the bloom filter). The delay on JSON parsing was also checked against the well-proven tool 'jq', which did show a similar slowdown rate. The slowdown caused by the data structure is closely related to the chosen error tolerance, as a smaller error tollerance will increase the space-use and the computation time of the underlying hash functions.

## Reporting metrics

The scripts print their frames/second on stdout. 'Send' messages correspond to the Kafka line producer, ' Process' corresponds to the unique-users-per-minute script, and 'Received' will send the statistic (unique-users-per-interval). These outputs can be redirected to separate files by modifying 'run.sh' if wanted.

## Serialization

JSON is definitely not the most ideal format for this specific problem. Although there exist some parsers that will extract the target value and skip the remaining JSON string, this is not common in most widely available libraries. In my experience, a format such as Protobuf of Capnproto would make extracting the uid and ts much faster. A binary fomrat would also make the size of the input smaller and make network transmission faster.

## Scalability

A clear approach to scale this solution would be to process the frames in parallel by separating them in different partitions. To ensure that no double-counting happens, the partition choice should be done by hashing the uid of the frame. The results of each parallel worker can be stored in an in-memory database (i.e. Redis) and combined once all the results for a given interval have been emitted.
