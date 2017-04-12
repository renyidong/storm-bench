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


abstract public class BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkBase.class);

    private Config stormConf_;
    protected SpoutConfig spoutConf_;
    protected int parallel_;

    public BenchmarkBase(Map conf) {
        String zkServerHosts = Utils.joinHosts((List<String>)conf.get("zookeeper.servers"),
                                               Integer.toString((Integer)conf.get("zookeeper.port")));
        String topic = getConfString(conf, "kafka.topic");
        spoutConf_ = new SpoutConfig(new ZkHosts(zkServerHosts), 
                                     topic, "/" + topic, 
                                     UUID.randomUUID().toString());
        spoutConf_.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf_.ignoreZkOffsets = true; // Read from the beginning of the topic

        stormConf_ = new Config();
        stormConf_.setNumWorkers(getConfInt(conf, "storm.workers"));
        stormConf_.setNumAckers(getConfInt(conf, "storm.ackers"));
        int maxSpoutPending = getConfInt(conf,"max.spout.pending");
        if (0 < maxSpoutPending)
            stormConf_.setMaxSpoutPending(maxSpoutPending);

        parallel_ = getConfInt(conf, "kafka.partitions");
    }

    abstract public StormTopology getTopology();

    public void submitTopology(String name) throws 
        AuthorizationException, AlreadyAliveException, InvalidTopologyException {
        StormSubmitter.submitTopologyWithProgressBar(name, stormConf_, getTopology());
    }

    public static int getConfInt(Map conf, String field) {
        Object val = conf.get(field);
        if (val != null) {
            log.info(field + ": " + val);
            return ((Number)val).intValue();
        } 
        else {
            log.info(field + " not found");
            return -1;
        }
    }

    public static String getConfString(Map conf, String field) {
        Object val = conf.get(field);
        if (val != null) {
            log.info(field + ": " + val);
            return (String)val;
        } 
        else {
            log.info(field + " not found");
            return null;
        }
    }
}
