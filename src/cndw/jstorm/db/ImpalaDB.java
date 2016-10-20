package com.cndw.statistics;

import java.sql.Connection;
import java.sql.SQLException;

public class ImpalaDB extends BaseDB {

	protected Connection getHiveConnection() throws NullPointerException, ClassNotFoundException, SQLException{		
		Connection connection = ImpalaConnectionPool.getInstance().getConn();
		hiveConnection = connection;
		return connection;
	}
	
	public ImpalaDB() throws NullPointerException, ClassNotFoundException, SQLException{
		super();
	}
}
