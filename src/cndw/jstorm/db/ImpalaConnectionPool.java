package com.cndw.statistics;

import java.sql.Connection;
import java.util.Map;

public class ImpalaConnectionPool extends BaseDataSource {
	private static ImpalaConnectionPool instance = new ImpalaConnectionPool();
	private static final String CONNECTION_URL = "jdbc:hive2://119.29.155.236:21050/;auth=noSasl";
	public static ImpalaConnectionPool getInstance(){
		if(instance.DS == null){
			//Map<String, String> conf = PropertiesUtil.getConf("comm");
    		String connectURI = CONNECTION_URL;//CONNECTION_URL.replace("${host}", conf.get("impala"));
    		String username = "";
    		String pswd = "";
    		//String pswd = "";
    		String driverClass = "org.apache.hive.jdbc.HiveDriver";
    		int initialSize = 20;
    		int maxtotal = 30;
    		int maxIdle = 30;
    		int maxWaitMillis = 1000;
    		int minIdle = 10;
    		instance.initDS(connectURI, username, pswd, driverClass, initialSize, maxtotal, maxIdle, maxWaitMillis, minIdle);
    	}
    	return instance;
	}
	
	private ImpalaConnectionPool(){
		
	}
	

	public static void main(String[] args) {
		ImpalaConnectionPool poll = ImpalaConnectionPool.getInstance();
		Connection connection = poll.getConn();
		System.out.println(connection);
	}
}
