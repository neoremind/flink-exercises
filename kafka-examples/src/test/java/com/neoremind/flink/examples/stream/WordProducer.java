package com.neoremind.flink.examples.stream;

import com.google.common.io.Resources;

import com.neoremind.flink.examples.stream.partitioner.PartitionNumberPrefixPartitioner;
import com.neoremind.flink.exercises.common.model.Word;
import com.neoremind.flink.exercises.common.utils.SleepUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xu.zx
 */
@Slf4j
public class WordProducer {

  String topicName = "my-flink-word-test";

  DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd-HH:mm:ss");

  @Test
  public void testSendWords() throws IOException {
    try (InputStream props = Resources.getResource("word-producer.properties").openStream()) {
      Properties properties = new Properties();
      properties.setProperty("partitioner.class", PartitionNumberPrefixPartitioner.class.getName());
      properties.load(props);
      try (final KafkaProducer<String, Word> producer = new KafkaProducer<>(properties)) {
        System.out.println(producer.partitionsFor(topicName));
        List<String> lines = FileUtils.readLines(new File("/Users/xu/IdeaProjects/flink-exercises/kafka-examples/src/test/resources/word-producer.txt.properties"));
        lines.stream()
            .map(this::buildWordAndSleepTimeMs)
            .forEach(w -> {
              if (w != null) {
                producer.send(new ProducerRecord<>(topicName, w.word.getText(), w.word));
                SleepUtils.backoff(w.sleepTimeMs);
              }
            });
      }
    }
  }

  @AllArgsConstructor
  @ToString
  class WordAndSleepTimeMs {
    Word word;
    long sleepTimeMs;
  }

  private WordAndSleepTimeMs buildWordAndSleepTimeMs(String line) {
    if (StringUtils.isEmpty(line) || line.startsWith("#")) {
      return null;
    }
    String[] array = line.split(",");
    String text = array[0];
    DateTime createTime = DateTime.parse(array[1], format);
    long createTimeMs = createTime.getMillis();
    boolean invalid = Boolean.valueOf(array[2]);
    long sleepTimeMs = Integer.parseInt(array[3]);
    Word word = new Word(text, invalid, createTimeMs);
    log.info("create {}, {}", word, sleepTimeMs);
    return new WordAndSleepTimeMs(word, sleepTimeMs);
  }
}
