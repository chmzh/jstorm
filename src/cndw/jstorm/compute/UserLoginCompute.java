package cndw.jstorm.compute;

import java.util.ArrayList;
import java.util.List;

import cndw.jstorm.DvObject;
import cndw.jstorm.util.TimeUtil;

public class UserLoginCompute implements ICompute {
	public final static int NUMCOUNT = 1<<0;       //统计登陆人数
	private int ts;
	private String playerId;
	private String sid = "";
	@Override
	public List<DvObject> execute(String logType,String[] cols, String[] vals, int type) {
		sid = vals[1];
		List<DvObject> objs = new ArrayList<DvObject>();
		ts = Integer.valueOf(vals[7]);       //时间戳 固定不变
		playerId = vals[4];                 //玩家ID
		switch (type) {
		case NUMCOUNT:
			DvObject obj = null;
			obj = compute5m(logType, ts);
			objs.add(obj);
			break;
		default:
			break;
		}

		return objs;
	}
	
	/**
	 * 5分钟登陆人数   需要排重
	 * @return
	 */
	private DvObject compute5m(String logType,int ts){
		int val = 1;
		
		String t = TimeUtil.toTime(ts, "yyyy-MM-dd HH:mm");
		String dt[] = t.split(" ");
		String dd = dt[0];
		String[] a = dt[1].split(":");
		String hh = a[0];
		String mm = a[1];
		
		
		mm = TimeUtil.to5minute(mm);
		DvObject obj = new DvObject();
		String redisKey = logType+sid+playerId;
		obj.setRedisKey(redisKey);
		obj.setVal(val);
		obj.setDate(dd);
		obj.setHh(hh);
		obj.setMm(mm);
		obj.setSid(sid);
		obj.setDiff(true);
		obj.setTableName(logType+"_5_"+"users");
		return obj;
	}
	
}
