package cndw.jstorm.bolt;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cndw.jstorm.DvDataEntry;
import cndw.jstorm.DvObject;
import cndw.jstorm.db.DvMysqlClient;
import cndw.jstorm.db.MySQLDataSource;
import cndw.jstorm.redis.DvCache;
import cndw.jstorm.redis.DvRedisClient;
import cndw.jstorm.util.MysqlUtil;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 只针对更新操作处理作优化 2016-03-07
 * @author chenmingzhou
 *
 */
public class DvMysqlBolt extends BaseRichBolt {
	private MySQLDataSource ds;
	protected OutputCollector collector;
	private final static ConcurrentMap<String, Boolean> Cache = new ConcurrentHashMap<String, Boolean>();
	private final static Logger LOG = LoggerFactory.getLogger(DvMysqlClient.class);
	private Queue<DvDataEntry> tupleQueue = new ConcurrentLinkedQueue<DvDataEntry>();
	@Override
	public void execute(Tuple tuple) {
    	String gameId = tuple.getStringByField("gameId");
    	String logType = tuple.getStringByField("logType");
    	String sid = tuple.getStringByField("sid");  
    	
    	DvObject dvobj = (DvObject)tuple.getValueByField("dvobj");
    	
    	String cdate = dvobj.getDate();
    	String hh = dvobj.getHh();
    	String mm = dvobj.getMm();
    	double val = dvobj.getVal();
    	
    	DvMysqlClient client = new DvMysqlClient(ds);    	
    	client.initConn();
    	try {
			if(!Cache.containsKey(gameId)){
				client.createDB(gameId);
				Cache.put(gameId, true);
			}
    		
			String tableName = dvobj.getTableName();
			
			
			String sql = "";
			String key = gameId+tableName;
			if(!Cache.containsKey(key)){
				sql = "create table if not exists "+gameId+"."+tableName+"(cdate char(10) not null default '',hh char(2) not null default '',mm char(2) not null default '',sid varchar(10) not null default '',val double not null default 0,INDEX ms(cdate,hh,mm,sid))";
				client.execute(sql);
				Cache.put(key, true);
			}
			
			key = gameId+tableName+cdate+hh+mm+sid;
			boolean bol = false;
			LinkedList<Object> params = new LinkedList<Object>();
			params.add(cdate);
			params.add(hh);
			params.add(mm);
			params.add(sid);
			if(!Cache.containsKey(key)){
				sql = "select 1 from "+gameId+"."+tableName+" where cdate=? and hh=? and mm=? and sid=?"; 

				bol = client.isExists(sql, params);
			}else{
				bol = true;
			}
			if(bol){    //对更新操作进行优化
				sql = "update "+gameId+"."+tableName+" set val=val+"+val+" where cdate='"+cdate+"' and hh='"+hh+"' and mm='"+mm+"' and sid='"+sid+"'";
				client.executeUpdate(sql);
				addCache(gameId, dvobj);
				Cache.put(key, true);
			}else{      //实时插入，考虑到这个操作不是频繁，则不需要批量入库，主要是针对 更新操作作优化
				sql = "insert into "+gameId+"."+tableName+"(cdate,hh,mm,sid,val)values(?,?,?,?,?)";
				params.add(val);
				client.executeUpdate(sql,params);
				
				addCache(gameId, dvobj);
				
				Cache.put(key, true);
			}
			collector.ack(tuple);
			client.closeConn();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	}

	private void addCache(String gameId,DvObject dvobj){
		if(Strings.isNullOrEmpty(dvobj.getRedisKey())){
			return;
		}
		ConcurrentMap<String, Object> map = DvCache.localExists(gameId, dvobj.getRedisKey());
		if(map == null){
			DvCache.putLocalCache(gameId, dvobj.getRedisKey(), dvobj.getVal());
		}
		map = DvCache.redistExists(gameId, dvobj.getRedisKey());
		if(map==null){   //Redis没有缓存,加到redis
			DvCache.putRedisCache(gameId, dvobj.getRedisKey(), dvobj.getVal());
		}
	}
    	
    	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		ds = MySQLDataSource.getInstance();
		collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}
	
	
	@Override
	public void cleanup() {
		super.cleanup();
		ds.shutDown();
	}

}
