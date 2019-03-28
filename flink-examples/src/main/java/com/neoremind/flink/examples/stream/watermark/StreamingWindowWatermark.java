package com.neoremind.flink.examples.stream.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

/**
 * <a href="https://blog.csdn.net/xu470438000/article/details/83271123">link</a>
 * <ul>
 * 1)接收socket数据
 * 2)将每行数据按照逗号分隔，每行数据调用map转换成tuple<String,Long>类型。其中tuple中的第一个元素代表具体的数据，第二行代表数据的eventtime
 * 3)抽取timestamp，生成watermar，允许的最大乱序时间是10s，并打印（key,eventtime,currentMaxTimestamp,watermark）等信息
 * 4)分组聚合，window窗口大小为3秒，输出（key，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
 * </ul>
 */
public class StreamingWindowWatermark {

  public static void main(String[] args) throws Exception {
    // the host and the port to connect to
    final String hostname;
    final int port;
    final int windowSizeInSeconds;
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      hostname = params.has("hostname") ? params.get("hostname") : "localhost";
      port = params.getInt("port");
      windowSizeInSeconds = params.getInt("windowSizeInSeconds", 3);
    } catch (Exception e) {
      System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
          "and port is the address of the text server");
      System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
          "type the input text into the command line");
      return;
    }

    System.out.println("hostname and port is " + hostname + ":" + port);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //设置使用event time，默认是使用process time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    //设置并行度为1,默认并行度是当前机器的cpu数量
    env.setParallelism(1);

    //连接socket获取输入的数据
    DataStream<String> text = env.socketTextStream(hostname, port, "\n");

    //解析输入的数据，输出为key + 时间戳
    DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
      @Override
      public Tuple2<String, Long> map(String value) throws Exception {
        String[] arr = value.split("\\W+");
        return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
      }
    });

    //抽取timestamp和生成watermark
    DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new CustomAssignerWithPeriodicWatermarks());

    DataStream<String> window = waterMarkStream
        .keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeInSeconds)))
        .apply(new CustomSortWindowFunction());

    window.print();

    env.execute("Event time and Watermark Example");
  }


  static class CustomAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    long currentMaxTimestamp = 0L;

    // 最大允许的乱序时间是10s
    final long maxOutOfOrderness = 10000L;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
      // return the watermark as current highest timestamp minus the out-of-orderness bound
      return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
      long timestamp = element.f1;
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
      long id = Thread.currentThread().getId();
      System.out.println(sdf.format(new Date()) + " CurrentThreadId:" + id +
          ", Key : " + element.f0 +
          ", Event time : [" + element.f1 + "|" + sdf.format(element.f1) +
          "], CurrentMaxTimestamp : [" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) +
          "], Watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
      return timestamp;
    }
  }

  static class CustomSortWindowFunction implements WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow> {

    /**
     * 对window内的数据进行排序，保证数据的顺序
     */
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
      String key = tuple.toString();
      List<Long> list = new ArrayList<Long>();
      Iterator<Tuple2<String, Long>> it = input.iterator();
      while (it.hasNext()) {
        Tuple2<String, Long> next = it.next();
        list.add(next.f1);
      }
      Collections.sort(list);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      StringBuilder str = new StringBuilder();
      str.append(sdf.format(new Date()))
          .append(" Key=")
          .append(key)
          .append(", ")
          .append("list size=")
          .append(list.size())
          .append(", ")
          .append("1st ts=")
          .append(sdf.format(list.get(0)))
          .append(", ")
          .append("last ts=")
          .append(sdf.format(list.get(list.size() - 1)))
          .append(", ")
          .append("window start=")
          .append(sdf.format(window.getStart()))
          .append(", ")
          .append("window end=")
          .append(sdf.format(window.getEnd()));
      out.collect(str.toString());
    }
  }

}
