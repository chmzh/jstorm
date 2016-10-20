package com.cndw.statistics;

import java.sql.Connection;
import java.util.Map;

public class MySQLConnectionPool extends BaseDataSource {
	private static MySQLConnectionPool instance = new MySQLConnectionPool();
	private static final String CONNECTION_URL = "jdbc:mysql://127.0.0.1:3306/datastatistic?useUnicode=true&amp;characterEncoding=utf-8";
	public static MySQLConnectionPool getInstance(){
		if(instance.DS == null){

    		String connectURI = CONNECTION_URL;
    		String username = "root";
    		String pswd = "cndw@root@#$fdgs";
    		//String pswd = "";
    		String driverClass = "com.mysql.jdbc.Driver";
    		int initialSize = 20;
    		int maxtotal = 30;
    		int maxIdle = 30;
    		int maxWaitMillis = 1000;
    		int minIdle = 10;
    		instance.initDS(connectURI, username, pswd, driverClass, initialSize, maxtotal, maxIdle, maxWaitMillis, minIdle);
    	}
    	return instance;
	}
	
	private MySQLConnectionPool(){
		
	}
	

	public static void main(String[] args) {
		MySQLConnectionPool poll = MySQLConnectionPool.getInstance();
		Connection connection = poll.getConn();
		System.out.println(connection);
	}
}
