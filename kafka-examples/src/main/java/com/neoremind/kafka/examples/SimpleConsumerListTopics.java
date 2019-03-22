package com.neoremind.kafka.examples;

import com.google.common.io.Resources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
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
public class SimpleConsumerListTopics implements Constants {

  public static void main(String[] args) throws IOException {
    try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
        kafkaConsumer.subscribe(asList(TOPIC_NAME));
        for (Map.Entry<String, List<PartitionInfo>> e : kafkaConsumer.listTopics().entrySet()) {
          log.info(e.getKey() + "=> " + e.getValue());
        }
      }
    }
  }
}
