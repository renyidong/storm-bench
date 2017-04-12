package rpi.storm.benchmark;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;

import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;
import intel.storm.benchmark.lib.bolt.ConstBolt;

import java.io.Serializable;
import java.util.Map;


public class SOL extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(SOL.class);

    public static final String SPOUT_ID = "spout";
    public static final String BOLT_ID = "bolt";

    private int numLevels_;

    public SOL(Map conf) {
        super(conf);
        numLevels_ = ((Number)conf.get("sol.topology_level")).intValue();
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf_);

        // numLevel: total number of layers including one spout and bolts
        builder.setSpout(SPOUT_ID, kafkaSpout, parallel_);
        builder.setBolt(BOLT_ID + 1, new ConstBolt(), parallel_)
            .shuffleGrouping(SPOUT_ID);
        for (int levelNum = 2; levelNum <= numLevels_ - 1; levelNum++) {
            builder.setBolt(BOLT_ID + levelNum, new ConstBolt(), parallel_)
                .shuffleGrouping(BOLT_ID + (levelNum - 1));
        }

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

        SOL app = new SOL(conf);
        app.submitTopology(args[0]);
    }
}
