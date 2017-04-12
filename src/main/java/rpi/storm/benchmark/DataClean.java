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

import intel.storm.benchmark.lib.bolt.FilterBolt;
import intel.storm.benchmark.lib.bolt.PageViewBolt;
import static intel.storm.benchmark.lib.spout.pageview.PageView.Item;
import yahoo.benchmark.common.Utils;
import rpi.storm.benchmark.common.BenchmarkBase;

import java.util.HashMap;
import java.util.Map;

public class DataClean extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(DataClean.class);

    public static final String SPOUT_ID = "spout";
    public static final String VIEW_ID = "view";
    public static final String FILTER_ID = "filter";

    public DataClean(Map conf) {
        super(conf);
    }

    @Override
    public StormTopology getTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf_);
        builder.setSpout(SPOUT_ID, kafkaSpout, parallel_);
        builder.setBolt(VIEW_ID, new PageViewBolt(Item.STATUS, Item.ALL), parallel_)
            .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(FILTER_ID, new FilterBolt<Integer>(200), parallel_)
            .fieldsGrouping(VIEW_ID, new Fields(Item.STATUS.toString()));

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

        DataClean app = new DataClean(conf);
        app.submitTopology(args[0]);
    }
}
