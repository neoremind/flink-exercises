package com.neoremind.flink.examples.stream;

import com.google.common.io.Resources;

import com.neoremind.flink.examples.stream.partitioner.PartitionNumberPrefixPartitioner;
import com.neoremind.flink.exercises.common.utils.SleepUtils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;

/**
 * --topic my-flink-test \
 * --group.id flink-test-group
 *
 * 注意topic的分区有几个[Partition(topic = my-flink-test, partition = 0, leader = 1, replicas = [1], isr = [1])]
 *
 * 否则，flink只能接收到1/N的数据。
 *
 * @author xu.zx
 */
@Slf4j
public class WordCountProducer {

  String topicName = "my-flink-test";

  Random random = new Random(0);

  @Test
  public void testSendWords() throws IOException {
    try (InputStream props = Resources.getResource("word-producer.properties").openStream()) {
      Properties properties = new Properties();
      properties.setProperty("partitioner.class", PartitionNumberPrefixPartitioner.class.getName());
      properties.load(props);
      properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      try (final KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
        System.out.println(producer.partitionsFor(topicName));
        for (int i = 0; i < 20000; i++) {
          // String content = random.nextInt(3) + "_" + RandomUtils.getRandomString(random, 2, 10, 12);
          String content = "0" + "_" + RandomUtils.getRandomString(random, 2, 0, 2);
          // System.out.println(content);
          producer.send(new ProducerRecord<>(topicName, content, content));
          // SleepUtils.backoff(random.nextInt(200));
        }
      }
    }
  }
}
