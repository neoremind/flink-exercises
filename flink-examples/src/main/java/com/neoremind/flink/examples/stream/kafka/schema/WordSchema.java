package com.neoremind.flink.examples.stream.kafka.schema;

import com.neoremind.flink.exercises.common.model.Word;
import com.neoremind.flink.exercises.common.utils.ProtoCodecUtil;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * @author xu.zx
 */
public class WordSchema implements DeserializationSchema<Word>, SerializationSchema<Word> {

  @Override
  public Word deserialize(byte[] message) throws IOException {
    return ProtoCodecUtil.decode(message, Word.class);
  }

  @Override
  public boolean isEndOfStream(Word nextElement) {
    return false;
  }

  @Override
  public byte[] serialize(Word element) {
    return ProtoCodecUtil.encode(element);
  }

  @Override
  public TypeInformation<Word> getProducedType() {
    return getForClass(Word.class);
  }
}
