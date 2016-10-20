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
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import clojure.string__init;
import cndw.jstorm.bolt.DvCounterBolt;
import cndw.jstorm.bolt.DvMysqlBolt;
import cndw.jstorm.bolt.DvSplitBolt;
import cndw.jstorm.util.PropertiesUtil;

public class DvMysqlTopology {


	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		
		String zkHost1 = "10.104.93.205";
		String zkHost2 = "10.104.93.206";
		String zkHost3 = "10.104.93.207";
		
		String zks = zkHost1+":2181,"+zkHost2+":2181,"+zkHost3+":2181";
		String topic = "mytopic";
		String zkRoot = "/jstorm";  // default zookeeper root configuration for
									// storm
		String id = "word";

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		
		
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.ignoreZkOffsets = false;
		spoutConf.zkServers = Arrays.asList(new String[] { zkHost1, zkHost2, zkHost3 });
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); 
		
		
		// Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		
		builder.setBolt("dv-split-bolt", new DvSplitBolt()).shuffleGrouping("kafka-reader");
		builder.setBolt("dv-count-bolt", new DvCounterBolt(),100).fieldsGrouping("dv-split-bolt", new Fields("gameId","logType","sid"));
		
		builder.setBolt("dv-mysql-blot", new DvMysqlBolt(),100).fieldsGrouping("dv-count-bolt", new Fields("gameId","logType","sid"));
		
		
		Config conf = new Config();

		String name = DvMysqlTopology.class.getSimpleName();

//		conf.put(Config.NIMBUS_HOST, "master1");
//		conf.setNumWorkers(3);
//		StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		
		if (args != null && args.length == 1) {
			// Nimbus host name passed from command line
			conf.put(Config.NIMBUS_HOST, args[0]);      
			//PropertiesUtil.loadAllConf(args[1]);      //配置文件路径
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else{
			//PropertiesUtil.loadAllConf(args[0]);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			//Thread.sleep(60000);
			// cluster.shutdown();
		}


	}

}
