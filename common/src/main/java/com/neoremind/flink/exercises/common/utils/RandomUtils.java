package com.neoremind.flink.exercises.common.utils;

import java.util.Random;

/**
 * @author xu.zx
 */
public class RandomUtils {

  public static String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  public static String getRandomString(Random random, int len) {
    return getRandomString(random, len, 0, 62);
  }

  public static String getRandomString(Random random, int len, int from, int to) {
    StringBuilder str = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      str.append(ALPHABET.charAt(from + random.nextInt(to - from)));
    }
    return str.toString();
  }

}
