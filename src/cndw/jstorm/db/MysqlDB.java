package com.cndw.statistics;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlDB extends BaseDB {

	protected Connection getHiveConnection() throws NullPointerException, ClassNotFoundException, SQLException{		
		Connection connection = MySQLConnectionPool.getInstance().getConn();
		hiveConnection = connection;
		return connection;
	}
	
	public MysqlDB() throws NullPointerException, ClassNotFoundException, SQLException{
		super();
	}
}
