package com.neoremind.flink.examples.stream.watermark;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @author zhangxu
 */
public class TelnetServerHandler implements Runnable {

  private Socket socket;

  DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  public TelnetServerHandler(Socket socket) {
    this.socket = socket;
  }

  /**
   * https://tool.lu/timestamp
   *
   * <pre>
   *   [00:00:21,00:00:24) [00:00:24,00:00:27) [00:00:27,00:00:30) [00:00:30,00:00:33) [00:00:33,00:00:36)
   *          22                   26                                 31 as late,32          33,34
   *
   *
   *   [00:00:36,00:00:39) [00:00:39,00:00:42) [00:00:42,00:00:45)
   *         36,37                39                   43
   * </pre>
   */
  @Override
  public void run() {
    String key = "abc";
    long delayMs = 10000;

    BufferedReader in = null;
    PrintWriter out = null;
    try {
      in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
      // 必须带有autoFlush=true！！！
      out = new PrintWriter(new OutputStreamWriter(this.socket.getOutputStream()), true);
      System.out.println("Start to response to " + socket.getInetAddress() + ":" + socket.getPort());
      printWithDelay(out, key, getMillis("2018-10-01 02:11:22"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:26"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:32"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:33"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:34"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:36"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:37"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:39"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:31"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:43"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:30"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:31"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:32"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:44"), delayMs);
      printWithDelay(out, key, getMillis("2018-10-01 02:11:45"), delayMs);
      /*
      boolean isCancel = false;
      while (!isCancel) {
        String body = in.readLine();
        if (body != null) {
          System.out.println(body);
          out.println("Wow! " + body.toUpperCase());
          if (body.equals("bye")) {
            isCancel = true;
          }
        }
      }*/
    } catch (Exception e) {
      if (in != null) {
        try {
          in.close();
        } catch (Exception e2) {
          e2.printStackTrace();
        }
      }

      if (out != null) {
        try {
          out.close();
        } catch (Exception e2) {
          e2.printStackTrace();
        }
      }

      if (this.socket != null) {
        try {
          this.socket.close();
        } catch (Exception e2) {
          e2.printStackTrace();
        }
      }
    }
  }

  private long getMillis(String date) {
    DateTime startTime = DateTime.parse(date, format);
    return startTime.getMillis();
  }

  private void printWithDelay(PrintWriter out, String key, long milliSeconds, long delayMs) {
    System.out.println(key + "," + milliSeconds);
    out.println(key + "," + milliSeconds);
    out.flush();
    try {
      Thread.sleep(delayMs);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
