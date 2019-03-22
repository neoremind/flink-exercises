https://github.com/neoremind/kafka-example

```
bin/kafka-topics.sh --create --zookeeper hadoop-master:2181 --replication-factor 3 --partitions 3 --topic test-topic1
```

```
$ bin/kafka-topics.sh --describe --zookeeper hadoop-master:2181 --topic test-topic1
Topic:test-topic1	PartitionCount:3	ReplicationFactor:3	Configs:
	Topic: test-topic1	Partition: 0	Leader: 4	Replicas: 4,1,2	Isr: 4,1,2
	Topic: test-topic1	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: test-topic1	Partition: 2	Leader: 2	Replicas: 2,3,4	Isr: 2,3,4
```