package com.neoremind.flink.examples.stream;

import com.google.common.io.Resources;

import com.neoremind.kafka.examples.utils.DateUtils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;

/**
 * @author xu.zx
 */
@Slf4j
public class WordConsumer {

  String topicName = "my-flink-word-test";

  @Test
  public void testConsumeWords() throws IOException {
    try (InputStream props = Resources.getResource("word-consumer.properties").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
        kafkaConsumer.subscribe(asList(topicName));
        while (true) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            log.info("partition = {}, ts = {}, offset = {}, key = {}, value = {}", record.partition(), DateUtils.formatTimestamp(record.timestamp()), record.offset(), record.key(), record.value());
          }
        }
      }
    }
  }

}
