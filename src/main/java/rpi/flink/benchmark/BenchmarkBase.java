package rpi.flink.benchmark;

import java.util.*;
import org.apache.commons.cli.*;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yahoo.benchmark.common.Utils;

abstract public class BenchmarkBase {
  private static final Logger log = LoggerFactory.getLogger(BenchmarkBase.class);

  protected final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  protected Map globalConfig;
  protected Properties kafkaProps = new Properties();
  
  public BenchmarkBase(String args[]) throws Exception {
    // Parse command line options
    Options cmdOpts = new Options();
    Option cmdConfOpt = new Option("c", "conf", true, "Path to the config file.");
    cmdConfOpt.setRequired(true);
    cmdOpts.addOption(cmdConfOpt);
    cmdOpts.addOption("t", "topic", true, "Kafka topic to consume.");
    cmdOpts.addOption("p", "parallel", true, "Parallelism (= number of Kafka partitions)");
    cmdOpts.addOption("h", "help", false, "Print this help.");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(cmdOpts, args );
    if (cmd.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( args[0], cmdOpts);
      System.exit(0);
    }

    // Read config file
    globalConfig = Utils.findAndReadConfigFile(cmd.getOptionValue("conf"), true);
    // add command line options to parameters
    String topic = cmd.getOptionValue("topic");
    if (topic != null) globalConfig.put("kafka.topic", topic);
    String parallel = cmd.getOptionValue("parallel");
    if (parallel != null) globalConfig.put("kafka.partitions", parallel);
    
    
    // make kafka properties
    // Force flink to read from beginning of kafka
    kafkaProps.setProperty("enable.auto.commit", "false");
    kafkaProps.setProperty("auto.offset.reset", "earliest");
    kafkaProps.setProperty("group.id", UUID.randomUUID().toString());
    // Server lists
    String _bootstrapServers  = Utils.joinHosts(
                                  (List<String>)globalConfig.get("kafka.brokers"),
                                  Integer.toString((Integer)globalConfig.get("kafka.port"))
                                );
    kafkaProps.setProperty("bootstrap.servers", _bootstrapServers);
    String _zookeeperConnect  = Utils.joinHosts(
                                  (List<String>)globalConfig.get("zookeeper.servers"),
                                  Integer.toString((Integer)globalConfig.get("zookeeper.port"))
                                );
    kafkaProps.setProperty("zookeeper.connect", _zookeeperConnect);
  }
}
