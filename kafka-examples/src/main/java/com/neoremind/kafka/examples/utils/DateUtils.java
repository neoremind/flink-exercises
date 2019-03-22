package com.neoremind.kafka.examples.utils;

import org.joda.time.DateTime;

/**
 * @author xu.zx
 */
public class DateUtils {

  public static String formatTimestamp(long ts) {
    return new DateTime(ts).toString("yyyy-MM-dd HH:mm:ss:SSS");
  }
}
