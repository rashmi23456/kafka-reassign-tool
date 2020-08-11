# kafka-reassign-tool
A helper script for Kafka to make it easier changing replicas for existing topics

Help:
```
usage: kafka-reassign-tool.py [-h] [--kafka-home KAFKA_HOME]
                              [--zookeeper ZOOKEEPER] [--throttle THROTTLE]
                              [--retry-after RETRY_AFTER] [--debug]
                              input

positional arguments:
  input                 File containing partition assignment

optional arguments:
  -h, --help            show this help message and exit
  --kafka-home KAFKA_HOME
                        Root directory of the Kafka installation. Default:
                        /usr/share/varadhi-kafka
  --zookeeper ZOOKEEPER
                        The connection string for the zookeeper connection. If
                        not specified, an attempt to read it from Kafka config
                        file is made
  --throttle THROTTLE   Replication throttle in B/s. If not given,
                        throttle.json file will be loaded. Format is a json array containing integers.
  --retry-after RETRY_AFTER
                        Retry duration in sec after which the tool should look
                        for completion status again
  --debug               For debug logs
```

Command:
```
python kafka-reassign_tool.py --input input_assignment --throttle [1000000,10000000] --retry-after 30
```

Content of `input_assignment`:
```
[
    [
        {
            "topic": "varadhi_nfr",
            "partition": 0,
            "to": [
                11,
                12,
                13
            ]
        },
        {
            "topic": "varadhi_nfr",
            "partition": 1,
            "to": [
                12,
                13,
                11
            ]
        }
    ],
    [
        {
            "topic": "varadhi_nfr",
            "partition": 2,
            "to": [
                13,
                11,
                12
            ]
        },
        {
            "topic": "varadhi_nfr",
            "partition": 3,
            "to": [
                21,
                22,
                23
            ]
        }
    ]
]
```

## Note
**Input Assignment** file contains List of List of assignments.
For the above input, partition 0 & 1 are migrated together. After their migration completes, the tool will automatically move to partition 2 & 3.

**Throttle** file or param is json array of integers (B/sec). For example [100, 1000, 10000].
The tool will start with the first throttle limit and will keep setting the next throttle limit after retry seconds.


Example output:
```
Reading /usr/share/varadhi-kafka/config/server.properties
Using zookeeper URL: zk-1:2181,zk-2:2181,zk-3:2181/varadhi/kafka
varadhi_nfr-0 completed successfully
varadhi_nfr-1 completed successfully
varadhi_nfr-2 starting reassignment of partitions
varadhi_nfr-2 started reassignment of partitions
varadhi_nfr-2 is still in progress. Retrying after 30 seconds
varadhi_nfr-2 is still in progress. Retrying after 30 seconds
varadhi_nfr-2 is still in progress. Retrying after 30 seconds
varadhi_nfr-2 is still in progress. Retrying after 30 seconds
varadhi_nfr-2 is still in progress. Retrying after 30 seconds
varadhi_nfr-2 is still in progress. Retrying after 30 seconds
varadhi_nfr-2 completed successfully
```
