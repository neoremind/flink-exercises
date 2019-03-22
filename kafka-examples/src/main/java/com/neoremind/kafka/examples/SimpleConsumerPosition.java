package com.neoremind.kafka.examples;

import com.google.common.io.Resources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;

/**
 * High level API
 * <p>
 * https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
 * <p>
 * 新提供的New Consumer API，对以前旧版本中几种API实现的功能进行了整合与统一实现。
 *
 * @author xu.zhang
 */
@Slf4j
public class SimpleConsumerPosition implements Constants {

  public static void main(String[] args) throws IOException {
    try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
        // set offset
        seek(kafkaConsumer);
        for (PartitionInfo e : kafkaConsumer.partitionsFor(TOPIC_NAME)) {
          // Get the last committed offset for the given partition
          log.info("{} => {}", e.partition(), kafkaConsumer.position(new TopicPartition(TOPIC_NAME, e.partition())));
        }
      }
    }
  }

  /**
   * 设置的offset是闭区间，包含这个位点。
   */
  private static void seek(KafkaConsumer<String, String> kafkaConsumer) {
    Collection<TopicPartition> partitionCollection = asList(
        new TopicPartition(TOPIC_NAME, 0),
        new TopicPartition(TOPIC_NAME, 1),
        new TopicPartition(TOPIC_NAME, 2)
    );
    kafkaConsumer.assign(partitionCollection);
    kafkaConsumer.seek(((List<TopicPartition>) partitionCollection).get(0), 25);
    kafkaConsumer.seek(((List<TopicPartition>) partitionCollection).get(1), 25);
    kafkaConsumer.seek(((List<TopicPartition>) partitionCollection).get(2), 23);
  }
}
