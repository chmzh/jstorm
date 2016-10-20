package cndw.jstorm;

public class DvObject {
	private String expireRedisKey;     //过期的redisKey
	private String redisKey;
	private double val;
	private String date;          //天 2016-03-07
	private String hh;           //小时
	private String mm;          //分钟
	private boolean isDiff;  //是否需要排重
	private String sid;
	private String tableName;   //表名
	public double getVal() {
		return val;
	}
	public void setVal(double val) {
		this.val = val;
	}
	
	@Override
	public String toString() {
		//return "date:"+date+",hh:"+hh+",hh:"+mm+",indicator:"+indicator+",val:"+val;
		return "";
	}

	public String getRedisKey() {
		return redisKey;
	}
	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}

	public boolean isDiff() {
		return isDiff;
	}
	public void setDiff(boolean isDiff) {
		this.isDiff = isDiff;
	}
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}
	public String getExpireRedisKey() {
		return expireRedisKey;
	}
	public void setExpireRedisKey(String expireRedisKey) {
		this.expireRedisKey = expireRedisKey;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getHh() {
		return hh;
	}
	public void setHh(String hh) {
		this.hh = hh;
	}
	public String getMm() {
		return mm;
	}
	public void setMm(String mm) {
		this.mm = mm;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
