package cndw.jstorm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.DistributedRPC.AsyncProcessor.execute;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class DvBaseSpout extends BaseRichSpout {
	private final static Logger LOG = LoggerFactory.getLogger(DvBaseSpout.class);
	private SpoutOutputCollector collector;
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		LOG.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%      nextTuple");
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}
	

}
