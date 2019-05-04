package com.neoremind.kafka.examples.serializer;

import com.neoremind.flink.exercises.common.model.Word;
import com.neoremind.flink.exercises.common.utils.ProtoCodecUtil;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author xu.zx
 */
public class WordSerializer implements Serializer<Word> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String topic, Word data) {
    return ProtoCodecUtil.encode(data);
  }

  @Override
  public void close() {

  }
}
