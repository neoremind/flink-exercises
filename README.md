# Flink exercises

## Run Flink Yarn Session
```
./bin/yarn-session.sh -n 4 -tm 768 -jm 1024 -s 8
Usage:
   Required
     -n,--container <arg>   Number of YARN container to allocate (=Number of Task Managers)
   Optional
     -D <arg>                        Dynamic properties
     -d,--detached                   Start detached
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container with optional unit (default: MB)
     -nm,--name                      Set a custom name for the application on YARN
     -q,--query                      Display available YARN resources (memory, cores)
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container with optional unit (default: MB)
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths for HA mode
```

```
./bin/yarn-session.sh -n 8 -tm 768 -jm 1024 -s 1
```

## Commands

### Copy jar to docker container
```
docker cp target/flink-examples-jar-with-dependencies.jar hadoop-master:/root
```

### 单机main方法

```
StreamingFromCollection
```

### Word count

```
./bin/flink run -c com.neoremind.flink.examples.stream.wordcount.WordCount /root/flink-examples-jar-with-dependencies.jar
```

### Socket window word count
Run the following:
```
./bin/flink run -c com.neoremind.flink.examples.stream.socket.SocketWindowWordCount \
            /root/flink-examples-jar-with-dependencies.jar \
            --hostname 192.168.1.104 \
            --port 9000
```

执行nc -l 9000命令，输入字符串，观察flink的stdout有统计输出。

### Kafka source word count

Kafa灌数据程序在`kafka-example`的test，`com.neoremind.flink.examples.stream.WordCountProducer`，
注意可以区分kafka partition发送，topic如果只有1个，则flink多个并行度没用。
按照proc time 30s一个窗口统计word count。

```
./bin/flink run -c com.neoremind.flink.examples.stream.kafka.KafkaSourceWordCount \
            /root/flink-examples-jar-with-dependencies.jar \
            --kafka.bootstrap.servers kafka-broker:9093,kafka-broker:9094 \
            --topic my-flink-test \
            --group.id flink-test-group
```

假如只有1个分区，那么发送2w条数据，统计结果如下。
```
WordWithCount{word='0_aa', count=4996}
WordWithCount{word='0_ab', count=5005}
WordWithCount{word='0_bb', count=5085}
WordWithCount{word='0_ba', count=4914}
```

### Kafka source event time word count

```
1_hello@,2018-11-25-08:10:15,false,1000
1_abc@,2018-11-25-08:10:25,false,1000
1_world@,2018-11-25-08:10:20,false,1000 分区1乱序的
1_lego@,2018-11-25-08:10:47,false,1000
1_tv@,2018-11-25-08:10:37,false,1000
1_apple@,2018-11-25-08:11:10,false,1000
2_phone@,2018-11-25-08:10:41,false,1000 分区2的watermark到了10:31，此时由于分区0还没有数据，因此不能触发窗口[10:00 - 10:30)
0_car@,2018-11-25-08:11:14,false,120000 分区0有数据触发了[10:00 - 10:30) 打印1_hello@ -> 1
                                                                   1_abc@ -> 1
                                                                   1_world@ -> 1
2_phone@,2018-11-25-08:11:11,false,1000 120s后
1_apple@,2018-11-25-08:11:11,false,1000
0_alibaba@,2018-11-25-08:11:11,false,1000 3个分区都有数据了，触发了[10:30 - 11:00) 打印1_lego@ -> 1
                                                                         1_tv@ -> 1
                                                                         2_phone@ -> 1
```

3个tm的打印日志。
```
[INFO]	2019-05-04 06:29:01,287	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:01.271 CurrentThreadId:19852, text : 1_hello@, Event time : [1543104615000|2018-11-25 00:10:15.000], CurrentMaxTimestamp : [1543104615000|2018-11-25 00:10:15.000], Watermark:[1543104605000|2018-11-25 00:10:05.000]
[INFO]	2019-05-04 06:29:02,041	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:02.039 CurrentThreadId:19852, text : 1_abc@, Event time : [1543104625000|2018-11-25 00:10:25.000], CurrentMaxTimestamp : [1543104625000|2018-11-25 00:10:25.000], Watermark:[1543104615000|2018-11-25 00:10:15.000]
[INFO]	2019-05-04 06:29:03,041	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:03.040 CurrentThreadId:19852, text : 1_world@, Event time : [1543104620000|2018-11-25 00:10:20.000], CurrentMaxTimestamp : [1543104625000|2018-11-25 00:10:25.000], Watermark:[1543104615000|2018-11-25 00:10:15.000]
[INFO]	2019-05-04 06:29:04,048	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:04.048 CurrentThreadId:19852, text : 1_lego@, Event time : [1543104647000|2018-11-25 00:10:47.000], CurrentMaxTimestamp : [1543104647000|2018-11-25 00:10:47.000], Watermark:[1543104637000|2018-11-25 00:10:37.000]
[INFO]	2019-05-04 06:29:05,068	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:05.068 CurrentThreadId:19852, text : 1_tv@, Event time : [1543104637000|2018-11-25 00:10:37.000], CurrentMaxTimestamp : [1543104647000|2018-11-25 00:10:47.000], Watermark:[1543104637000|2018-11-25 00:10:37.000]
[INFO]	2019-05-04 06:29:06,056	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:06.056 CurrentThreadId:19852, text : 1_apple@, Event time : [1543104670000|2018-11-25 00:11:10.000], CurrentMaxTimestamp : [1543104670000|2018-11-25 00:11:10.000], Watermark:[1543104660000|2018-11-25 00:11:00.000]
[INFO]	2019-05-04 06:29:07,085	[Source: Custom Source (3/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:07.084 CurrentThreadId:19848, text : 2_phone@, Event time : [1543104641000|2018-11-25 00:10:41.000], CurrentMaxTimestamp : [1543104641000|2018-11-25 00:10:41.000], Watermark:[1543104631000|2018-11-25 00:10:31.000]
[INFO]	2019-05-04 06:29:08,093	[Source: Custom Source (1/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:29:08.092 CurrentThreadId:19850, text : 0_car@, Event time : [1543104674000|2018-11-25 00:11:14.000], CurrentMaxTimestamp : [1543104674000|2018-11-25 00:11:14.000], Watermark:[1543104664000|2018-11-25 00:11:04.000]
[INFO]	2019-05-04 06:31:08,107	[Source: Custom Source (3/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:31:08.107 CurrentThreadId:19848, text : 2_phone@, Event time : [1543104671000|2018-11-25 00:11:11.000], CurrentMaxTimestamp : [1543104671000|2018-11-25 00:11:11.000], Watermark:[1543104661000|2018-11-25 00:11:01.000]
[INFO]	2019-05-04 06:31:09,086	[Source: Custom Source (2/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:31:09.084 CurrentThreadId:19852, text : 1_apple@, Event time : [1543104671000|2018-11-25 00:11:11.000], CurrentMaxTimestamp : [1543104671000|2018-11-25 00:11:11.000], Watermark:[1543104661000|2018-11-25 00:11:01.000]
[INFO]	2019-05-04 06:31:10,093	[Source: Custom Source (1/3)]	stream.kafka.KafkaSourceEventTimeWordCount$CustomAssignerWithPeriodicWatermarks	(KafkaSourceEventTimeWordCount.java:147)	-2019-05-04 06:31:10.092 CurrentThreadId:19850, text : 0_alibaba@, Event time : [1543104671000|2018-11-25 00:11:11.000], CurrentMaxTimestamp : [1543104674000|2018-11-25 00:11:14.000], Watermark:[1543104664000|2018-11-25 00:11:04.000]
```


```
   [10:00       -      10:30)[10:30       -      11:00)[11:00   -   11:30]
0:                                                        11:11    11:14
1:   10:15  10:25 10:20         10:37        10:47         11:10  11：11
2:                                  10:41                  11:11
```

```
./bin/flink run -c com.neoremind.flink.examples.stream.kafka.KafkaSourceEventTimeWordCount \
            /root/flink-examples-jar-with-dependencies.jar \
            --kafka.bootstrap.servers kafka-broker:9093,kafka-broker:9094 \
            --topic my-flink-word-test \
            --group.id flink-test-group
```

另外还可以改成CustomSortWindowFunction自定义的window function，代码被注释掉了。

### Window word count

Count window示例，输出TODO为什么只有几个单词？

```
cd flink-1.7.2
hdfs dfs -put README.txt /user/root/
hdfs dfs -mkdir /user/root/output
hdfs dfs -rmdir /user/root/output

./bin/flink run -c com.neoremind.flink.examples.stream.windowing.WindowWordCount \
            /root/flink-examples-jar-with-dependencies.jar \
            --input hdfs://hadoop-master:9000/user/root/README.txt \
            --output hdfs://hadoop-master:9000/user/root/output/word.txt.`date "+%Y-%m-%d-%H%M%S"`

root@hadoop-master:~/flink-1.7.2# hdfs dfs -cat /user/root/output/word.txt.2019-05-03-142356
(flink,5)
(the,5)
(and,5)
(software,5)
(of,5)
(apache,5)
(export,5)
```

### Session window

案例如下
```
   1 2 3 4 5 6 7 8 9 10 11 12
a |*|                |*|
b |*   *   *|
c           |*|         |*|
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

### GroupedProcessingTimeWindowExample

4个并行度，自定义一个data source，每个source产出1000个key，1000000个pair，均是（key, 1），按照proc time来计算，
然后自定义一个window function，做reducing操作，
最后抽样mod 100L == 0的key打印出来，会打印出来很多的key
比较烧CPU。

```
./bin/flink run -c com.neoremind.flink.examples.stream.windowing.GroupedProcessingTimeWindowExample \
        /root/flink-examples-jar-with-dependencies.jar
```

### TopSpeedWindowing

source随机生成，4个tuple，（carId，speed，distance，ts），
使用了eventtime，使用了evictor、trigger。trigger的话如果跑过10个meters就出发计算下哪个车跑的远。

```
./bin/flink run -c com.neoremind.flink.examples.stream.windowing.TopSpeedWindowing /root/flink-examples-jar-with-dependencies.jar
```

### Side output example

```
./bin/flink run -c com.neoremind.flink.examples.stream.sideoutput.SideOutputExample \
            /root/flink-examples-jar-with-dependencies.jar
```

打印出非法words：
```
rejected: action
rejected: ophelia
rejected: orisons
rejected: remember
...
(to,15)
(sins,1)
(my,1)
...
```

### Window join

Join两个source，（name，grade） join （name，salary），按照一个时间窗口。
按照下面连个参数来，rate用ThrottledIterator来限流发送，所以不会停止，但是也不会太烧CPU。
```
final long windowSize = params.getLong("windowSize", 5000);
final long rate = params.getLong("rate", 3L);
```

```
./bin/flink run -c com.neoremind.flink.examples.stream.join.WindowJoin /root/flink-examples-jar-with-dependencies.jar
```

### twitter案例

字符串自定义解析Json。twitter为例，没用window，每次都累计计算。

```
./bin/flink run -c com.neoremind.flink.examples.stream.twitter.TwitterExample /root/flink-examples-jar-with-dependencies.jar
```

输出：
```
(apache,1)
(flink,1)
(apache,2)
(flink,2)
(apache,3)
(flink,3)
```

## Watermark案例

https://blog.csdn.net/xu470438000/article/details/83271123
https://blog.csdn.net/xorxos/article/details/80715113
https://blog.csdn.net/a6822342/article/details/78064815
https://yq.aliyun.com/articles/666056
http://www.whitewood.me/2018/06/01/Flink-Watermark-%E6%9C%BA%E5%88%B6%E6%B5%85%E6%9E%90/

```
./bin/flink run -c com.neoremind.flink.examples.stream.watermark.StreamingWindowWatermark \
            /root/flink-examples-jar-with-dependencies.jar \
            --hostname 192.168.1.104 \
            --port 9000
```

Run test `com.neoremind.flink.examples.stream.watermark.SimpleTelnetServer`

网上的案例
```
[00:00:21,00:00:24) [00:00:24,00:00:27) [00:00:27,00:00:30) [00:00:30,00:00:33) [00:00:33,00:00:36)
      22                   26                                 31 as late,32          33,34


[00:00:36,00:00:39) [00:00:39,00:00:42) [00:00:42,00:00:45)
     36,37                39                   43 44 45

```

```
2019-05-04 06:35:50.677 CurrentThreadId:19880, Key : abc, Event time : [1538331082000|2018-09-30 18:11:22.000], CurrentMaxTimestamp : [1538331082000|2018-09-30 18:11:22.000], Watermark:[1538331072000|2018-09-30 18:11:12.000]
2019-05-04 06:36:00.679 CurrentThreadId:19880, Key : abc, Event time : [1538331086000|2018-09-30 18:11:26.000], CurrentMaxTimestamp : [1538331086000|2018-09-30 18:11:26.000], Watermark:[1538331076000|2018-09-30 18:11:16.000]
2019-05-04 06:36:10.680 CurrentThreadId:19880, Key : abc, Event time : [1538331092000|2018-09-30 18:11:32.000], CurrentMaxTimestamp : [1538331092000|2018-09-30 18:11:32.000], Watermark:[1538331082000|2018-09-30 18:11:22.000]
2019-05-04 06:36:20.683 CurrentThreadId:19880, Key : abc, Event time : [1538331093000|2018-09-30 18:11:33.000], CurrentMaxTimestamp : [1538331093000|2018-09-30 18:11:33.000], Watermark:[1538331083000|2018-09-30 18:11:23.000]
2019-05-04 06:36:30.688 CurrentThreadId:19880, Key : abc, Event time : [1538331094000|2018-09-30 18:11:34.000], CurrentMaxTimestamp : [1538331094000|2018-09-30 18:11:34.000], Watermark:[1538331084000|2018-09-30 18:11:24.000]
2019-05-04 06:36:30.877 Key=(abc), list size=1, 1st ts=2018-09-30 18:11:22.000, last ts=2018-09-30 18:11:22.000, window start=2018-09-30 18:11:21.000, window end=2018-09-30 18:11:24.000
2019-05-04 06:36:40.692 CurrentThreadId:19880, Key : abc, Event time : [1538331096000|2018-09-30 18:11:36.000], CurrentMaxTimestamp : [1538331096000|2018-09-30 18:11:36.000], Watermark:[1538331086000|2018-09-30 18:11:26.000]
2019-05-04 06:36:50.702 CurrentThreadId:19880, Key : abc, Event time : [1538331097000|2018-09-30 18:11:37.000], CurrentMaxTimestamp : [1538331097000|2018-09-30 18:11:37.000], Watermark:[1538331087000|2018-09-30 18:11:27.000]
2019-05-04 06:36:50.853 Key=(abc), list size=1, 1st ts=2018-09-30 18:11:26.000, last ts=2018-09-30 18:11:26.000, window start=2018-09-30 18:11:24.000, window end=2018-09-30 18:11:27.000
2019-05-04 06:37:00.703 CurrentThreadId:19880, Key : abc, Event time : [1538331099000|2018-09-30 18:11:39.000], CurrentMaxTimestamp : [1538331099000|2018-09-30 18:11:39.000], Watermark:[1538331089000|2018-09-30 18:11:29.000]
2019-05-04 06:37:10.707 CurrentThreadId:19880, Key : abc, Event time : [1538331091000|2018-09-30 18:11:31.000], CurrentMaxTimestamp : [1538331099000|2018-09-30 18:11:39.000], Watermark:[1538331089000|2018-09-30 18:11:29.000]
2019-05-04 06:37:20.711 CurrentThreadId:19880, Key : abc, Event time : [1538331103000|2018-09-30 18:11:43.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:37:20.870 Key=(abc), list size=2, 1st ts=2018-09-30 18:11:31.000, last ts=2018-09-30 18:11:32.000, window start=2018-09-30 18:11:30.000, window end=2018-09-30 18:11:33.000
2019-05-04 06:37:30.717 CurrentThreadId:19880, Key : abc, Event time : [1538331090000|2018-09-30 18:11:30.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:37:40.721 CurrentThreadId:19880, Key : abc, Event time : [1538331091000|2018-09-30 18:11:31.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:37:50.726 CurrentThreadId:19880, Key : abc, Event time : [1538331092000|2018-09-30 18:11:32.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:38:00.732 CurrentThreadId:19880, Key : abc, Event time : [1538331104000|2018-09-30 18:11:44.000], CurrentMaxTimestamp : [1538331104000|2018-09-30 18:11:44.000], Watermark:[1538331094000|2018-09-30 18:11:34.000]
2019-05-04 06:38:10.736 CurrentThreadId:19880, Key : abc, Event time : [1538331105000|2018-09-30 18:11:45.000], CurrentMaxTimestamp : [1538331105000|2018-09-30 18:11:45.000], Watermark:[1538331095000|2018-09-30 18:11:35.000]
```

另外一个使用allowedLateness的例子：
```
./bin/flink run -c com.neoremind.flink.examples.stream.watermark.StreamingWindowWatermarkAllowedLateness \
            /root/flink-examples-jar-with-dependencies.jar \
            --hostname 192.168.1.104 \
            --port 9000
```

网上的案例
```
[00:00:21,00:00:24) [00:00:24,00:00:27) [00:00:27,00:00:30) [00:00:30,00:00:33) [00:00:33,00:00:36)
      22                   26                                 31 as late,32          33,34


[00:00:36,00:00:39) [00:00:39,00:00:42) [00:00:42,00:00:45)
     36,37                39                   43 44 45

```

```
2019-05-04 06:43:03.757 CurrentThreadId:19897, Key : abc, Event time : [1538331082000|2018-09-30 18:11:22.000], CurrentMaxTimestamp : [1538331082000|2018-09-30 18:11:22.000], Watermark:[1538331072000|2018-09-30 18:11:12.000]
2019-05-04 06:43:13.760 CurrentThreadId:19897, Key : abc, Event time : [1538331086000|2018-09-30 18:11:26.000], CurrentMaxTimestamp : [1538331086000|2018-09-30 18:11:26.000], Watermark:[1538331076000|2018-09-30 18:11:16.000]
2019-05-04 06:43:23.764 CurrentThreadId:19897, Key : abc, Event time : [1538331092000|2018-09-30 18:11:32.000], CurrentMaxTimestamp : [1538331092000|2018-09-30 18:11:32.000], Watermark:[1538331082000|2018-09-30 18:11:22.000]
2019-05-04 06:43:33.768 CurrentThreadId:19897, Key : abc, Event time : [1538331093000|2018-09-30 18:11:33.000], CurrentMaxTimestamp : [1538331093000|2018-09-30 18:11:33.000], Watermark:[1538331083000|2018-09-30 18:11:23.000]
2019-05-04 06:43:43.771 CurrentThreadId:19897, Key : abc, Event time : [1538331094000|2018-09-30 18:11:34.000], CurrentMaxTimestamp : [1538331094000|2018-09-30 18:11:34.000], Watermark:[1538331084000|2018-09-30 18:11:24.000]
2019-05-04 06:43:43.986 Key=(abc), list size=1, 1st ts=2018-09-30 18:11:22.000, last ts=2018-09-30 18:11:22.000, window start=2018-09-30 18:11:21.000, window end=2018-09-30 18:11:24.000
2019-05-04 06:43:53.773 CurrentThreadId:19897, Key : abc, Event time : [1538331096000|2018-09-30 18:11:36.000], CurrentMaxTimestamp : [1538331096000|2018-09-30 18:11:36.000], Watermark:[1538331086000|2018-09-30 18:11:26.000]
2019-05-04 06:44:03.775 CurrentThreadId:19897, Key : abc, Event time : [1538331097000|2018-09-30 18:11:37.000], CurrentMaxTimestamp : [1538331097000|2018-09-30 18:11:37.000], Watermark:[1538331087000|2018-09-30 18:11:27.000]
2019-05-04 06:44:03.978 Key=(abc), list size=1, 1st ts=2018-09-30 18:11:26.000, last ts=2018-09-30 18:11:26.000, window start=2018-09-30 18:11:24.000, window end=2018-09-30 18:11:27.000
2019-05-04 06:44:13.780 CurrentThreadId:19897, Key : abc, Event time : [1538331099000|2018-09-30 18:11:39.000], CurrentMaxTimestamp : [1538331099000|2018-09-30 18:11:39.000], Watermark:[1538331089000|2018-09-30 18:11:29.000]
2019-05-04 06:44:23.784 CurrentThreadId:19897, Key : abc, Event time : [1538331091000|2018-09-30 18:11:31.000], CurrentMaxTimestamp : [1538331099000|2018-09-30 18:11:39.000], Watermark:[1538331089000|2018-09-30 18:11:29.000]
2019-05-04 06:44:33.787 CurrentThreadId:19897, Key : abc, Event time : [1538331103000|2018-09-30 18:11:43.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:44:34.045 Key=(abc), list size=2, 1st ts=2018-09-30 18:11:31.000, last ts=2018-09-30 18:11:32.000, window start=2018-09-30 18:11:30.000, window end=2018-09-30 18:11:33.000
late element 11：30的数据又触发的窗口计算
2019-05-04 06:44:43.794 CurrentThreadId:19897, Key : abc, Event time : [1538331090000|2018-09-30 18:11:30.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:44:43.883 Key=(abc), list size=3, 1st ts=2018-09-30 18:11:30.000, last ts=2018-09-30 18:11:32.000, window start=2018-09-30 18:11:30.000, window end=2018-09-30 18:11:33.000
late element 11：31的数据又触发的窗口计算
2019-05-04 06:44:53.796 CurrentThreadId:19897, Key : abc, Event time : [1538331091000|2018-09-30 18:11:31.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
2019-05-04 06:44:53.822 Key=(abc), list size=4, 1st ts=2018-09-30 18:11:30.000, last ts=2018-09-30 18:11:32.000, window start=2018-09-30 18:11:30.000, window end=2018-09-30 18:11:33.000
2019-05-04 06:45:03.800 CurrentThreadId:19897, Key : abc, Event time : [1538331092000|2018-09-30 18:11:32.000], CurrentMaxTimestamp : [1538331103000|2018-09-30 18:11:43.000], Watermark:[1538331093000|2018-09-30 18:11:33.000]
late element 11：32的数据又触发的窗口计算
2019-05-04 06:45:03.861 Key=(abc), list size=5, 1st ts=2018-09-30 18:11:30.000, last ts=2018-09-30 18:11:32.000, window start=2018-09-30 18:11:30.000, window end=2018-09-30 18:11:33.000
2019-05-04 06:45:13.804 CurrentThreadId:19897, Key : abc, Event time : [1538331104000|2018-09-30 18:11:44.000], CurrentMaxTimestamp : [1538331104000|2018-09-30 18:11:44.000], Watermark:[1538331094000|2018-09-30 18:11:34.000]
2019-05-04 06:45:23.810 CurrentThreadId:19897, Key : abc, Event time : [1538331105000|2018-09-30 18:11:45.000], CurrentMaxTimestamp : [1538331105000|2018-09-30 18:11:45.000], Watermark:[1538331095000|2018-09-30 18:11:35.000]

```