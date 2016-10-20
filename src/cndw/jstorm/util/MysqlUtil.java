package cndw.jstorm.util;

import org.apache.storm.jdbc.common.JdbcClient;

public class MysqlUtil {
	
	private static JdbcClient client;
	
	public MysqlUtil(JdbcClient client) {
		MysqlUtil.client = client;
	}
	
	public static void createDB(String dbName){
		client.executeSql("create database if not exists "+dbName);
	}
	
	public static void executeSql(String sql){
		client.executeSql(sql);
	}
}
