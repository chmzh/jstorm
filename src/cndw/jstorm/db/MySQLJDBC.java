package cndw.jstorm.db;

import java.sql.SQLException;
import java.util.Map;

import com.mysql.jdbc.Driver;


/**
 * MYSQL JDBC
 * @author mingzhou.chen
 * 28458942@qq.com
 * 2016年1月4日 上午11:27:43
 */
public final class MySQLJDBC extends BaseJDBC {
	
	private static MySQLJDBC instance = new MySQLJDBC();
	

	public static MySQLJDBC getInstance() {
		String host = "127.0.0.1";//"192.168.21.34";
		int port = 3306;
		String dbName = "kinghome";
		String userName = "root";
		String password = "";
		instance.connect(host, port, dbName, userName, password);
		return instance;
	}
	
	private MySQLJDBC(){
	}
	
	protected void init(String host,int port,String dbName,String userName,String password){
		driverClass = Driver.class.getName();
		connectStr = "jdbc:mysql://"+host+":"+port+"/"+dbName+"?user="+userName+"&password="+password+"&useUnicode=true&autoReconnect=true&characterEncoding=utf8";
	}
	
	public static void main(String[] args) {
		BaseJDBC client = MySQLJDBC.getInstance();
		try {
			//client.execute("use test");
			//client.select_test("select * from user", null);
			client.createDB("test1");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
