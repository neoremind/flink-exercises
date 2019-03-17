package com.neoremind.kafka.examples;


import com.google.common.io.Resources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

/**
 * High level API
 *
 * @author xu.zhang
 */
@Slf4j
public class SimpleConsumerNoAutoCommit implements Names {

  public static void main(String[] args) throws IOException {
    try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      properties.put("enable.auto.commit", "false");
      List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
      final int minBatchSize = 5;
      try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            buffer.add(record);
          }
          if (buffer.size() >= minBatchSize) {
            log.info("{}", buffer);
            kafkaConsumer.commitSync();
            buffer.clear();
          }
        }
      }
    }
  }
}
