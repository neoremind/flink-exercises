package com.neoremind.flink.examples.stream.state;


import com.neoremind.flink.exercises.common.utils.RandomUtils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

/**
 * @author xu.zx
 */
public class StateCheckPointExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/user/root/flink/state/checkpoints/state-example"));
    env.enableCheckpointing(30000);
    env.getCheckpointConfig().setCheckpointTimeout(20000);

    env.addSource(new SourceFunction<Tuple2<String, Integer>>() {

      private Logger LOG = LoggerFactory.getLogger(this.getClass());

      private volatile boolean isRunning = true;

      private int count = 0;

      private Random random = new Random(0);

      @Override
      public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
          synchronized (lock) {
            for (int i = 0; i < 10; i++) {
              ctx.collect(Tuple2.of(RandomUtils.getRandomString(random, 2, 26, 28), count));
              Thread.sleep(200);
              count++;
            }
//            if (totalCount > 100) {
//              LOG.error("err_________________ reach 100");
//              throw new Exception("explicit error!");
//            }
            LOG.info("source has emitted " + count + " records");
          }
          // TODO 如果放到synchronized，则网页上checkpoint始终in progress?
          Thread.sleep(2000);
        }
      }

      @Override
      public void cancel() {
        isRunning = false;
      }
    }).keyBy(0)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
        .apply(new RichWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {

          private Logger LOG = LoggerFactory.getLogger(this.getClass());

          private transient ValueState<Integer> state;

          private transient Counter counter;

          private int totalCount = 0;

          @Override
          public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Integer> collector) throws Exception {
            //从state中获取值
            totalCount = state.value();
            for (Tuple2<String, Integer> item : iterable) {
              totalCount++;
              this.counter.inc();
            }
            //更新state值
            state.update(totalCount);
            System.out.println(new Date() + ", windows=" + tuple.toString() + ", totalCount=" + totalCount + ", state totalCount=" + state.value());
            collector.collect(totalCount);
          }

          //获取state
          @Override
          public void open(Configuration parameters) throws Exception {
            System.out.println("Try to initiate state...");
            ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<Integer>(
                    "stateName",
                    TypeInformation.of(new TypeHint<Integer>() {
                    }), 0);
            state = getRuntimeContext().getState(descriptor);

            this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
          }
        })
        .print();
    env.execute();
  }
}
