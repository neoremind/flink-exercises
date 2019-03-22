package com.neoremind.kafka.examples;

import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import com.neoremind.kafka.examples.utils.DateUtils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

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
public class SimpleConsumerSeekToSpecificTimestamp implements Constants {

  public static void main(String[] args) throws IOException {
    try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
        // set offset
        seek(kafkaConsumer);
        while (true) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            log.info("partition = {}, ts = {}, offset = {}, key = {}, value = {}", record.partition(), DateUtils.formatTimestamp(record.timestamp()), record.offset(), record.key(), record.value());
          }
        }
      }
    }
  }

  /**
   * //TODO 老版本报：Exception in thread "main" org.apache.kafka.common.errors.UnsupportedVersionException: Cannot create a v0 ListOffsetRequest because we require features supported only in 1 or later.
   */
  private static void seek(KafkaConsumer<String, String> kafkaConsumer) {
    Collection<TopicPartition> partitionCollection = asList(
        new TopicPartition(TOPIC_NAME, 0),
        new TopicPartition(TOPIC_NAME, 1),
        new TopicPartition(TOPIC_NAME, 2)
    );
    Map<TopicPartition, Long> partition2Timestamp = Maps.newHashMap();
    for (TopicPartition partition : partitionCollection) {
      partition2Timestamp.put(partition, DateTime.now().minusMinutes(60).getMillis());
    }
    kafkaConsumer.assign(partitionCollection);
    Map<TopicPartition, OffsetAndTimestamp> partitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(partition2Timestamp);
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> e : partitionOffsetAndTimestampMap.entrySet()) {
      kafkaConsumer.seek(e.getKey(), e.getValue().offset());
    }
  }
}
