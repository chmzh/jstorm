package cndw.jstorm.compute;

import java.util.ArrayList;
import java.util.List;

import cndw.jstorm.DvObject;
import cndw.jstorm.util.TimeUtil;

public class PaymentCompute implements ICompute {
	private int ts;
	private String playerId;
	private double money;
	String sid = "";
	@Override
	public List<DvObject> execute(String logType, String[] cols, String[] vals,
			int type) {
		sid = vals[0];
		money = Double.valueOf(vals[8]);
		ts = Integer.valueOf(vals[15]);
		playerId = vals[12];
		List<DvObject> objs = new ArrayList<DvObject>();
		objs.add(compute5m_users(logType));
		objs.add(compute5m_moneys(logType));
		return objs;
	}
	
	/**
	 * 计算五分钟充值金额  不需要排重
	 * @return
	 */
	private DvObject compute5m_moneys(String logType){
		DvObject obj = new DvObject();
		
		String t = TimeUtil.toTime(ts, "yyyy-MM-dd HH:mm");
		String dt[] = t.split(" ");
		String dd = dt[0];
		String[] a = dt[1].split(":");
		String hh = a[0];
		String mm = a[1];
		
		
		mm = TimeUtil.to5minute(mm);
		obj.setVal(money);
		obj.setDate(dd);
		obj.setHh(hh);
		obj.setMm(mm);
		obj.setDiff(false);
		obj.setTableName(logType+"_5_"+"money");
		obj.setSid(sid);
		return obj;
	}
	
	/**
	 * 计算五分钟充值人数 计算充值人数 单个服，每人只计算一次
	 * @return
	 */
	private DvObject compute5m_users(String logType){
		DvObject obj = new DvObject();
		
		String t = TimeUtil.toTime(ts, "yyyy-MM-dd HH:mm");
		String dt[] = t.split(" ");
		String dd = dt[0];
		String[] a = dt[1].split(":");
		String hh = a[0];
		String mm = a[1];
		
		mm = TimeUtil.to5minute(mm);
		String redisKey = logType+sid+playerId;
		obj.setRedisKey(redisKey);
		obj.setVal(1);
		obj.setDate(dd);
		obj.setHh(hh);
		obj.setMm(mm);
		obj.setTableName(logType+"_5_"+"users");
		obj.setDiff(true);
		obj.setSid(sid);
		return obj;
	}
}
