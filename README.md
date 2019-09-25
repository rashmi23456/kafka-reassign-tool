# kafka-reassign-tool
A helper script for Kafka to make it easier changing replicas for existing topics

Help:
```
Usage: reassign_tool [options]
        --kafka-home <dir>           Root directory of the Kafka installation
                                       (standard Kafka scripts must be under bin/ directory there)
                                       Default: /usr/share/varadhi-kafka
        --zookeeper <url>            The connection string for the zookeeper connection
                                       If not specified, and attempt to read it from Kafka config file is made
        --input <file>               File containing partition assignment
        --throttle <throttle>        replication throttle in B/s
        --retry-after <retry>        retry duration in sec after which the tool should look for completion status again
        --debug                      for debug logs
```

Command:
```
ruby reassign_tool.rb --input input_assignment --throttle 200000000 --retry-after 30
```

Content of `input_assignment`:
```
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
	},
	{
		"topic": "varadhi_nfr",
		"partition": 2,
		"to": [
			13,
			11,
			12
		]
	}
]
```

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
