package com.neoremind.flink.examples.stream.kafka;

import com.neoremind.flink.examples.stream.kafka.schema.WordSchema;
import com.neoremind.flink.examples.stream.watermark.StreamingWindowWatermark;
import com.neoremind.flink.examples.stream.wordcount.WordCount;
import com.neoremind.flink.exercises.common.model.Word;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

public class KafkaSourceEventTimeWordCount {

  public static void main(String[] args) throws Exception {
    String kafkaBootstrapServers;
    String groupId;
    String topic;
    try {
      ParameterTool parameterTool = ParameterTool.fromArgs(args);
      kafkaBootstrapServers = parameterTool.get("kafka.bootstrap.servers");
      topic = parameterTool.get("topic");
      groupId = parameterTool.get("group.id");
    } catch (Exception e) {
      System.err.println("No kafkaBootstrapServers set.");
      return;
    }

    System.out.println("kafka.bootstrap.servers=" + kafkaBootstrapServers);
    System.out.println("topic=" + topic);
    System.out.println("group.id=" + groupId);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 0));
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    env.enableCheckpointing(300000);
//    env.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/user/root/flink/state/checkpoints/wordcount"));

    // set mode to exactly-once (this is the default)
//    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// make sure 500 ms of progress happen between checkpoints
//    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// checkpoints have to complete within one minute, or are discarded
//    env.getCheckpointConfig().setCheckpointTimeout(60000);

// allow only one checkpoint to be in progress at the same time
//    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// enable externalized checkpoints which are retained after job cancellation
//    env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaBootstrapServers);
    properties.setProperty("group.id", groupId);

    FlinkKafkaConsumerBase<Word> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new WordSchema(), properties)
        .assignTimestampsAndWatermarks(new CustomAssignerWithPeriodicWatermarks());

    DataStream<Word> dataSource = env.addSource(kafkaConsumer010);

//    env.setParallelism(3);

    ((DataStreamSource<Word>) dataSource).setParallelism(3);

    DataStream<WordWithCount> windowCounts = dataSource
        .map(new MapFunction<Word, WordWithCount>() {
          @Override
          public WordWithCount map(Word value) throws Exception {
            return new WordWithCount(value.getText().trim(), 1L);
          }
        })
        .keyBy("word")
//        .timeWindow(Time.seconds(60), Time.seconds(10))
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
//        .apply(new CustomSortWindowFunction());
        .sum("count");

//    BucketingSink<WordWithCount> sink = new BucketingSink<WordWithCount>("/user/root/wordcount/sink/output");
//    sink.setBucketer(new DateTimeBucketer<WordWithCount>("yyyy-MM-dd-HH"));
//    sink.setWriter(new StringWriter<>());
//    sink.setBatchSize(1024 * 40);
//    sink.setBatchRolloverInterval(15 * 60 * 1000);
//
//    windowCounts.addSink(sink);

    windowCounts.print().setParallelism(1);

    env.execute("Kafka source window count2");
  }

  static class CustomAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Word> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomAssignerWithPeriodicWatermarks.class);

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
    public long extractTimestamp(Word element, long previousElementTimestamp) {
      long timestamp = element.getCreateTimMs();
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
      long id = Thread.currentThread().getId();
      LOG.info(sdf.format(new Date()) + " CurrentThreadId:" + id +
          ", text : " + element.getText() +
          ", Event time : [" + element.getCreateTimMs() + "|" + sdf.format(element.getCreateTimMs()) +
          "], CurrentMaxTimestamp : [" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) +
          "], Watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
      return timestamp;
    }
  }

  static class CustomSortWindowFunction implements WindowFunction<WordWithCount, WordWithCount, Tuple, TimeWindow> {

    /**
     * 对window内的数据进行排序，保证数据的顺序
     */
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<WordWithCount> input, Collector<WordWithCount> out) throws Exception {
      String key = tuple.toString();
      List<String> list = new ArrayList<String>();
      Iterator<WordWithCount> it = input.iterator();
      while (it.hasNext()) {
        WordWithCount next = it.next();
        list.add(next.word);
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
          .append(list.get(0))
          .append(", ")
          .append("last ts=")
          .append(list.get(list.size() - 1))
          .append(", ")
          .append("window start=")
          .append(sdf.format(window.getStart()))
          .append(", ")
          .append("window end=")
          .append(sdf.format(window.getEnd()));
      out.collect(new WordWithCount(str.toString(), 0));
    }
  }

  public static class WordWithCount {

    public String word;

    public long count;

    public WordWithCount() {
    }

    public WordWithCount(String word, long count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return word + " -> " + count;
    }
  }


}
