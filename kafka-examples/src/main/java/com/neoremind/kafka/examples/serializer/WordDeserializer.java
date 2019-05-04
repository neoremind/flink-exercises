package com.neoremind.kafka.examples.serializer;

import com.neoremind.flink.exercises.common.model.Word;
import com.neoremind.flink.exercises.common.utils.ProtoCodecUtil;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author xu.zx
 */
public class WordDeserializer implements Deserializer<Word> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public Word deserialize(String topic, byte[] data) {
    return ProtoCodecUtil.decode(data, Word.class);
  }

  @Override
  public void close() {

  }
}
