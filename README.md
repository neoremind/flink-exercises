# Flink exercises

## Commands
```
docker cp target/flink-examples-jar-with-dependencies.jar hadoop-master:/root
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.socket.SocketWindowWordCount \
            --hostname 192.168.1.104 \
            --port 9000
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar -c com.neoremind.flink.examples.stream.wordcount.WordCount
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.kafka.KafkaSourceWordCount \
            --kafka.bootstrap.servers kafka-broker:9093,kafka-broker:9094 \
            --topic my-flink-test \
            --group.id flink-test-group
```

```
hdfs dfs rm /user/root/word/output
hdfs dfs rmdir /user/root/word/output

./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.windowing.WindowWordCount \
            --input hdfs://hadoop-master:9000/user/root/word/word.txt \
            --output hdfs://hadoop-master:9000/user/root/word/output/word.txt.`date "+%Y-%m-%d-%H%M%S"`

root@hadoop-master:~/flink-1.7.2# hdfs dfs -cat /user/root/word/output/word.txt.2019-03-17-093541
(to,5)
(the,5)
(to,10)
(sleep,5)
(and,5)
(of,5)
(the,10)
(s,5)
(that,5)
(the,10)
(a,5)
(to,10)
(of,10)
(the,10)
(and,10)
(of,10)
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.windowing.SessionWindowing \
            --output hdfs://hadoop-master:9000/user/root/output/session.window.test.`date "+%Y-%m-%d-%H%M%S"`

root@hadoop-master:~/flink-1.7.2# hdfs dfs -cat /user/root/output/session.window.test.2019-03-17-095327
(a,1,1)
(b,1,3)
(c,6,1)
(a,10,1)
(c,11,1)
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.windowing.GroupedProcessingTimeWindowExample
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.windowing.TopSpeedWindowing
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.sideoutput.SideOutputExample
```

```
./bin/flink run /root/flink-examples-jar-with-dependencies.jar \
            -c com.neoremind.flink.examples.stream.join.WindowJoin
```