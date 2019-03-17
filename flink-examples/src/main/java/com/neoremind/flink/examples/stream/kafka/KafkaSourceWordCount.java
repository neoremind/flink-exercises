package com.neoremind.flink.examples.stream.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

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

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
    properties.setProperty("group.id", groupId);

    DataStream<String> dataSource = env
        .addSource(new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties));

    DataStream<WordWithCount> windowCounts = dataSource
        .flatMap(new FlatMapFunction<String, WordWithCount>() {
          @Override
          public void flatMap(String value, Collector<WordWithCount> out) {
            for (String word : value.split("\\s")) {
              out.collect(new WordWithCount(word, 1L));
            }
          }
        })
        .keyBy("word")
        .timeWindow(Time.seconds(15), Time.seconds(10))
        .sum("count");

    windowCounts.print().setParallelism(3);

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
