package com.neoremind.kafka.examples;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author xu.zx
 */
public class MyPartitioner implements Partitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return key.hashCode() % 3;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    
  }
}
