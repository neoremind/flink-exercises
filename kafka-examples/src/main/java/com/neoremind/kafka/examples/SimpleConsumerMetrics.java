package com.neoremind.kafka.examples;

import com.google.common.io.Resources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
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
public class SimpleConsumerMetrics implements Constants {

  public static void main(String[] args) throws IOException {
    try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
        // set offset
        seek(kafkaConsumer);
        int counter = 0;
        while (counter++ < 10) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
          for (ConsumerRecord<String, String> record : records) {
            log.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
          }
        }

        /**
         * MetricName [name=assigned-partitions, group=consumer-coordinator-metrics, description=The number of partitions currently assigned to this consumer, tags={client-id=consumer-1}]->3.0
         * MetricName [name=request-latency-avg, group=consumer-node-metrics, description=, tags={client-id=consumer-1, node-id=node--2}]->0.0
         * MetricName [name=response-rate, group=consumer-node-metrics, description=The average number of responses received per second., tags={client-id=consumer-1, node-id=node-4}]->0.032970656116056714
         * MetricName [name=commit-latency-avg, group=consumer-coordinator-metrics, description=The average time taken for a commit request, tags={client-id=consumer-1}]->0.0
         */
        for (Map.Entry<MetricName, ? extends Metric> metricNameEntry : kafkaConsumer.metrics().entrySet()) {
          System.out.println(metricNameEntry.getKey() + "->" + metricNameEntry.getValue().value());
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
