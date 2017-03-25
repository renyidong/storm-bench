package rpi.storm.benchmark.common;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yahoo.benchmark.common.Utils;

import java.util.List;
import java.util.Map;
import java.util.UUID;


abstract public class KafkaBenchmark {
    private static final Logger log = LoggerFactory.getLogger(KafkaBenchmark.class);

    private Config stormConf_;
    protected SpoutConfig spoutConf_;
    protected int parallel_;

    public KafkaBenchmark(Map conf) {
        String zkServerHosts = Utils.joinHosts((List<String>)conf.get("zookeeper.servers"),
                                               Integer.toString((Integer)conf.get("zookeeper.port")));
        String kafkaTopic = (String)conf.get("kafka.topic");
        spoutConf_ = new SpoutConfig(new ZkHosts(zkServerHosts), 
                                     kafkaTopic, "/" + kafkaTopic, 
                                     UUID.randomUUID().toString());
        spoutConf_.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf_.ignoreZkOffsets = true; // Read from the beginning of the topic

        stormConf_ = new Config();
        stormConf_.setNumWorkers(((Number)conf.get("storm.workers")).intValue());
        stormConf_.setNumAckers(((Number)conf.get("storm.ackers")).intValue());
        stormConf_.setMaxSpoutPending(((Number)conf.get("max.spout.pending")).intValue());

        parallel_ = ((Number)conf.get("kafka.partitions")).intValue();
    }

    abstract public StormTopology getTopology();

    public void submitTopology(String name) throws 
        AuthorizationException, AlreadyAliveException, InvalidTopologyException {
        StormSubmitter.submitTopologyWithProgressBar(name, stormConf_, getTopology());
    }
}
