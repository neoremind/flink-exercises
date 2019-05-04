package com.neoremind.flink.exercises.common.utils;

/**
 * @author xu.zx
 */
public class SleepUtils {

  public static void backoff(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
