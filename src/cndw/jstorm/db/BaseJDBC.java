package cndw.jstorm.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.Statement;

public class BaseJDBC implements IJDBC {
	protected String driverClass;
	protected String connectStr;
	private Connection conn = null;
	
	protected void init(String host,int port,String dbName,String userName,String password){
		//由子类实现此类（必须） 例如
		//driverClass = "com.mysql.jdbc.Driver";
		//connectStr = "jdbc:mysql://"+host+":"+port+"/"+dbName+"?user="+userName+"&password="+password+"&useUnicode=true&autoReconnect=true&characterEncoding=utf8";
	}
	
	public void loadDriverClass() {
		try {
			Class.forName(driverClass).newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 连接数据库
	 * @param host
	 * @param port
	 * @param db
	 * @param password
	 */
	protected void connect(String host,int port,String dbName,String userName,String password) {
		try {
			init(host, port, dbName, userName, password);
			loadDriverClass();
			conn = DriverManager.getConnection(connectStr);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public <T> List<T> select(String sql, Map<String, Object> params,
			Class<T> clazz) {

		return null;
	}
	public boolean delete(String sql, Map<String, Object> params) {

		return false;
	}
	public boolean update(String sql, Map<String, Object> params) {

		return false;
	}
	
	/**
	 * 创建数据库
	 * @param dbName
	 * @throws SQLException
	 */
	public void createDB(String dbName) throws SQLException{
		execute("create database if not exists "+dbName+" CHARACTER SET utf8 COLLATE utf8_general_ci");
	}
	
	/**
	 * 切换到dbName数据库
	 * @param dbName
	 * @throws SQLException
	 */
	public void useDB(String dbName) throws SQLException{
		execute("use "+dbName);
	}
	/**
	 * 执行sql语句
	 * @param sql
	 * @throws SQLException
	 */
	public void execute(String sql) throws SQLException{
		conn.createStatement().execute(sql);
	}
	
	public void select_test(String sql,LinkedList<Object> params){
		try {
			PreparedStatement stmt = conn.prepareStatement(sql);
			parseParams(stmt,params);
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				int id = rs.getInt("id");
				System.out.println("id="+id);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void parseParams(PreparedStatement stmt,LinkedList<Object> params) throws SQLException{
		if(null==params || params.size()==0)
			return;
		for(int i=0;i<params.size();i++){
			Object obj = params.get(i);
			String typeName = obj.getClass().getSimpleName();
			if(typeName.equals("Integer")){
				stmt.setInt(i+1, (Integer)obj);
			}
		}
	}
	
}
