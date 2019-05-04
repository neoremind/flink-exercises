package com.neoremind.flink.examples.stream.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Properties;

public class KafkaSourceWordCount {

  public static void main(String[] args) throws Exception {
    String kafkaBootstrapServers;
    String groupId;
    String topic;
    try {
      ParameterTool parameterTool = ParameterTool.fromArgs(args);
      kafkaBootstrapServers = parameterTool.get("kafka.bootstrap.servers");
      topic = parameterTool.get("topic");
      groupId = parameterTool.get("group.id");
    } catch (Exception e) {
      System.err.println("No kafkaBootstrapServers set.");
      return;
    }

    System.out.println("kafka.bootstrap.servers=" + kafkaBootstrapServers);
    System.out.println("topic=" + topic);
    System.out.println("group.id=" + groupId);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5000);
    env.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/user/root/flink/state/checkpoints/wordcount"));

    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// make sure 500 ms of progress happen between checkpoints
//    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// checkpoints have to complete within one minute, or are discarded
//    env.getCheckpointConfig().setCheckpointTimeout(60000);

// allow only one checkpoint to be in progress at the same time
//    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// enable externalized checkpoints which are retained after job cancellation
//    env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
    properties.setProperty("group.id", groupId);

    DataStream<String> dataSource = env
        .addSource(new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties));

    env.setParallelism(3);

    ((DataStreamSource<String>) dataSource).setParallelism(3);

    DataStream<WordWithCount> windowCounts = dataSource
        .flatMap(new FlatMapFunction<String, WordWithCount>() {
          @Override
          public void flatMap(String value, Collector<WordWithCount> out) {
            for (String word : value.split("\\W+")) {
              out.collect(new WordWithCount(word, 1L));
            }
          }
        })
        .keyBy("word")
//        .timeWindow(Time.seconds(60), Time.seconds(10))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
        .sum("count");

//    BucketingSink<WordWithCount> sink = new BucketingSink<WordWithCount>("/user/root/wordcount/sink/output");
//    sink.setBucketer(new DateTimeBucketer<WordWithCount>("yyyy-MM-dd-HH"));
//    sink.setWriter(new StringWriter<>());
//    sink.setBatchSize(1024 * 40);
//    sink.setBatchRolloverInterval(15 * 60 * 1000);

//    windowCounts.addSink(sink);

    windowCounts.print().setParallelism(1);

    env.execute("Kafka source window count");
  }

  public static class WordWithCount {

    public String word;

    public long count;

    public WordWithCount() {
    }

    public WordWithCount(String word, long count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return "WordWithCount{" +
          "word='" + word + '\'' +
          ", count=" + count +
          '}';
    }
  }


}
