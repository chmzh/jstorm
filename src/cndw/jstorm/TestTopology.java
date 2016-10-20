package cndw.jstorm;

import java.util.Arrays;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cndw.jstorm.bolt.DvBaseSpout;
import cndw.jstorm.bolt.DvCounterBolt;
import cndw.jstorm.bolt.DvMysqlBolt;
import cndw.jstorm.bolt.DvSplitBolt;

public class TestTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		String zks = "192.168.23.91:2181,192.168.21.7:2181,192.168.23.207:2181";
		String topic = "mytopic";
		String zkRoot = "/jstorm";  // default zookeeper root configuration for
									// storm
		String id = "word";

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.ignoreZkOffsets = false;
		spoutConf.zkServers = Arrays.asList(new String[] { "192.168.23.91", "192.168.21.7", "192.168.23.207" });
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new DvBaseSpout(), 5);
		builder.setBolt("aa", new DvSplitBolt());
		Config conf = new Config();
		conf.setDebug(true);
		String name = TestTopology.class.getSimpleName();
		
		if (args != null && args.length > 0) {
			// Nimbus host name passed from command line
			conf.put(Config.NIMBUS_HOST, args[0]);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			//Thread.sleep(60000);
			// cluster.shutdown();
		}
	}
}
