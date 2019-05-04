package com.neoremind.flink.exercises.common.utils;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * ProtoCodecUtil
 */
public abstract class ProtoCodecUtil {

  public static <T> byte[] encode(T value) {
    LinkedBuffer buffer = LinkedBuffer.allocate();
    Class<T> clazz = cast(value.getClass());
    Schema<T> schema = RuntimeSchema.getSchema(clazz);

    return ProtobufIOUtil.toByteArray(value, schema, buffer);
  }

  public static <T> T decode(byte[] bytes, Class<T> clazz) {
    Class<T> warpClass = cast(clazz);
    Schema<T> schema = RuntimeSchema.getSchema(warpClass);
    T value = schema.newMessage();
    ProtobufIOUtil.mergeFrom(bytes, value, schema);
    return value;
  }

  public static <O> O cast(Object object) {
    return (O) object;
  }

}
