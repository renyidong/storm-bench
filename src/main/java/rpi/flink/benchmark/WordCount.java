package rpi.flink.benchmark;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import rpi.flink.benchmark.BenchmarkBase;

public class WordCount extends BenchmarkBase {
  
  public WordCount(String[] args) throws Exception {
    super(args);
    // Connect to kafka
    final String kafkaTopic;
    final String globalTopic = (String)globalConfig.get("kafka.topic");
    if (globalTopic == null) {
      kafkaTopic = "word";
    }
    else {
      kafkaTopic = globalTopic;
    }
    
    DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010(kafkaTopic, new SimpleStringSchema(), kafkaProps));

    // Parse data
    DataStream<WordWithCount> windowCounts = messageStream
      .flatMap(new FlatMapFunction<String, WordWithCount>() {
          @Override
          public void flatMap(String value, Collector<WordWithCount> out) {
            for (String word : value.split("\\s")) {
              out.collect(new WordWithCount(word, 1L));
            }
          }
        })
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .reduce(
        new ReduceFunction<WordWithCount>() {
          @Override
          public WordWithCount reduce(WordWithCount a, WordWithCount b) {
            return new WordWithCount(a.word, a.count + b.count);
          }
        }
      );
    windowCounts.print().setParallelism(1);
  }
  
  public static int main(String args[]) throws Exception {
    BenchmarkBase bench = new WordCount(args);
    bench.env.execute();
    return 0;
  }
  
  public static class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {}

    public WordWithCount(String word, long count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return word + " : " + count;
    }
  }
}
