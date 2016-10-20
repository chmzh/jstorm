package cndw.jstorm.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cndw.jstorm.DvObject;
import cndw.jstorm.redis.DvCache;
import cndw.jstorm.redis.DvRedisClient;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DvCounterBolt extends BaseRichBolt {

	private final static Logger LOG = LoggerFactory.getLogger(DvCounterBolt.class);
	protected OutputCollector collector;
	private DvRedisClient client;
	
	@Override
	public void execute(Tuple input) {
		
		String gameId = input.getStringByField("gameId");           //游戏ID，用于作为
		String logType = input.getStringByField("logType");         //日志类型
		String sid = input.getStringByField("sid");
		DvObject dvobj = (DvObject)input.getValueByField("dvobj");
		
		
		switch (logType) {
		case "userreginfo":
		case "userlogininfo":
		case "paysucinfo":
			if(!dvobj.isDiff()) break;    //如果统计某段时间总金额，则不需要排重
			//用于排重
			ConcurrentMap<String, Object> map = DvCache.localExists(gameId, dvobj.getRedisKey());
			if(map!=null){      //本地没有缓存，加入本地
//				DvCache.putLocalCache(gameId, dvobj.getRedisKey(), dvobj.getVal());
				LOG.info("===============================local====================="+dvobj.getRedisKey()+"   ,val:"+dvobj.getVal());
				collector.ack(input);
				return;
			}
			map = DvCache.redistExists(gameId, dvobj.getRedisKey());
			if(map!=null){   //Redis没有缓存,加到redis
//				DvCache.putRedisCache(gameId, dvobj.getRedisKey(), dvobj.getVal());
				LOG.info("===============================redis====================="+dvobj.getRedisKey()+"   ,val:"+dvobj.getVal());
				collector.ack(input);
				return;
			}
			break;

		default:
			break;
		}
		
		collector.emit(input, new Values(gameId,logType,sid,dvobj));
		
		collector.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;
		client = DvRedisClient.getInstance();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("gameId","logType","sid","dvobj"));
	}
	
}
