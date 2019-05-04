package com.neoremind.flink.examples.stream.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author xu.zx
 */
public class PartitionNumberPrefixPartitioner implements Partitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    String keyed = (String) key;
    if (keyed.contains("_")) {
      return Integer.parseInt(keyed.split("_")[0]);
    }
    return 0;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

}
