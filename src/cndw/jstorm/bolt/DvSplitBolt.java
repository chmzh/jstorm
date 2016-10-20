package cndw.jstorm.bolt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cndw.jstorm.DvObject;
import cndw.jstorm.compute.ICompute;
import cndw.jstorm.compute.PaymentCompute;
import cndw.jstorm.compute.UserLoginCompute;
import cndw.jstorm.compute.UserRegCompute;

public class DvSplitBolt extends BaseRichBolt {
	
	private final static Logger LOG = LoggerFactory.getLogger(DvSplitBolt.class);
	
	protected OutputCollector collector;
	@Override
	public void execute(Tuple input) {
    	String msg = input.getString(0);
    	
    	LOG.info(msg);
    	
    	String[] msgs = StringUtils.split(msg, ",");
    	if(msgs.length<4){
    		collector.ack(input);
    		return;
    	}
    	String[] cols = msgs[2].split("\\|");
    	if(cols == null || cols.length == 0){
    		collector.ack(input);
    		return;
    	}
    	String[] vals = Arrays.copyOfRange(msgs, 3, msgs.length);
    	if(vals == null || vals.length == 0){
    		collector.ack(input);
    		return;
    	}  
    	if(cols.length != vals.length){
    		collector.ack(input);
    		return;
    	} 
    	input.getSourceComponent();       //数据源
    	String gameId = msgs[1];
    	String logType = msgs[0];
    	ICompute compute;
    	java.util.List<DvObject> objs;
		switch (logType) {
		case "userreginfo":      //1、用户注册   指标  注册数(1分钟,5分钟 10分钟 1小时 1天）
			compute = new UserRegCompute();
			objs = compute.execute(logType, cols, vals, 0);
			emit(input, gameId, logType, objs);
			break;
		case "userlogininfo":      //2、用户登陆 指标  注册数(1分钟,5分钟 10分钟 1小时 1天）
			compute = new UserLoginCompute();
			objs = compute.execute(logType, cols, vals, UserLoginCompute.NUMCOUNT);
			emit(input, gameId, logType, objs);
			break;
		case "paysucinfo":    //9、充值成功
			compute = new PaymentCompute();
			objs = compute.execute(logType, cols, vals, 0);
			emit(input, gameId, logType, objs);
			break;
		case "serverselinfo":     //3、服务器选择
			
			break;
		case "rolecreateinfo":     //4、角色创建
			
			break;
		case "rolelogininfo":    //5、角色登陆
			
			break;
		case "rolelvupinfo":    //6、角色等级提升
			
			break;
		case "rolecancelinfo":    //7、角色注销
			
			break;
		case "payrequestinfo":    //8、充值请求
			
			break;
		case "rewardinfo":    //10、跟踪获赠元宝和虚拟货币
			
			break;
		case "consumeinfo":    //11、跟踪游戏消费点
			
			break;
		case "missioninfo":    //12、任务、关卡或副本
			
			break;
		case "playerattrinfo":    //13、玩家属性统计
			
			break;
		case "opinfo":    //14、操作日志
			
			break;
		default:
			collector.ack(input);
			break;
		}
		
		//collector.emit("count-1",input, new Values(gameId,logType,"col", 1));
		//collector.emit("logType",input, new Values(gameId,logType,"col", 1));
		//collector.emit(input, new Values(gameId,logType,"col1", 1));
		//collector.ack(input);

	}

	public void emit(Tuple input,String gameId,String logType,List<DvObject> objs){
		for (DvObject obj:objs) {
			LOG.info(obj.toString());
			collector.emit(input, new Values(gameId,logType,obj.getSid(),obj));
		}
		collector.ack(input);
	}
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("gameId","logType","rowKey","col","ts","master","slave","indicator","val"));
		declarer.declare(new Fields("gameId","logType","sid","dvobj"));
	}
	

}
