package cndw.jstorm.compute;

import java.util.ArrayList;
import java.util.List;

import cndw.jstorm.DvObject;
import cndw.jstorm.util.TimeUtil;
import javafx.geometry.Side;

public class UserRegCompute implements ICompute {
	private String sid = "";
	@Override
	public List<DvObject> execute(String logType, String[] cols, String[] vals,
			int type) {
		sid = vals[4];
		List<DvObject> objs = new ArrayList<DvObject>();
		int ts = Integer.valueOf(vals[10]);       //时间戳 固定不变
		DvObject object = compute5m(logType, ts);
		objs.add(object);
		return objs;
	}

	/**
	 * 5分钟注册人数   不需要排重，因为注册只有一次
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
		obj.setDate(dd);
		obj.setHh(hh);
		obj.setHh(mm);
		String redisKey = "";
		obj.setRedisKey(redisKey);
		obj.setVal(val);
		obj.setTableName(logType+"_"+5+"_user");
		obj.setSid(sid);
		obj.setDiff(false);  //是否排重
		return obj;
	}
}
