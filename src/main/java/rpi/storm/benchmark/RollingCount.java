package rpi.storm.benchmark;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;

import intel.storm.benchmark.lib.bolt.RollingCountBolt;
import intel.storm.benchmark.lib.bolt.RollingBolt;
import intel.storm.benchmark.lib.spout.FileReadSpout;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.KafkaBenchmark;

import java.util.Map;


public class RollingCount extends KafkaBenchmark {
    private static final Logger log = LoggerFactory.getLogger(RollingCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String COUNT_ID = "rolling_count";

    private int windowLength_;
    private int emitFreq_;

    public RollingCount(Map conf) {
        super(conf);
        windowLength_ = ((Number)conf.get("rollingcount.window_length")).intValue();
        emitFreq_ = ((Number)conf.get("rollingcount.emit_freq")).intValue();
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf_);
        builder.setSpout(SPOUT_ID, kafkaSpout, parallel_);
        builder.setBolt(SPLIT_ID, new WordCount.SplitSentence(), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new RollingCountBolt(windowLength_, emitFreq_), parallel_)
            .fieldsGrouping(SPLIT_ID, new Fields(WordCount.SplitSentence.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("conf", true, "Path to the config file.");
        opts.addOption("topic", true, "Kafka topic to consume.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);
        String configPath = cmd.getOptionValue("conf");
        if (configPath == null) {
            log.error("Null config path");
            System.exit(1);
        }
        Map conf = Utils.findAndReadConfigFile(configPath, true);
        // if specified, overwrite "kafka.topic" in the conf file
        String topic = cmd.getOptionValue("topic");
        if (topic != null)
            conf.put("kafka.topic", topic);

        RollingCount app = new RollingCount(conf);
        app.submitTopology(args[0]);
    }
}

