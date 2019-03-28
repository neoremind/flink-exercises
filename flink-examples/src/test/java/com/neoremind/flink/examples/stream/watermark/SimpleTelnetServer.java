package com.neoremind.flink.examples.stream.watermark;

import org.junit.Test;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * 配合{@link StreamingWindowWatermark}使用做server端。相当于nc -l 9000的服务端。
 *
 * @author zhangxu
 */
public class SimpleTelnetServer {

  @Test
  public void testStartServer() {
    int port = 9000;
    ServerSocket server = null;
    try {
      server = new ServerSocket(port);
      System.out.println("The time server starts in port : " + port);
      while (true) {
        Socket socket = server.accept();
        new Thread(new TelnetServerHandler(socket)).start();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        server.close();
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

}
