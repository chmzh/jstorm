package cndw.jstorm.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class DvMysqlClient {
	
	private Connection conn = null;
	private BaseDataSource ds;
	public DvMysqlClient(BaseDataSource ds) {
		this.ds = ds;
		
	}
	
	public void closeConn() throws SQLException{
		conn.close();
		conn = null;
	}
	
	public Connection initConn(){
		if(conn==null){
			conn = ds.getConn();
		}
		return conn;
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
	
	public void executeUpdate(String sql) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.executeUpdate();
	}
	
	public void executeUpdate(String sql,LinkedList<Object> params) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(sql);
		parseParams(stmt,params);
		stmt.executeUpdate();
	}
	
	public boolean isExists(String sql,LinkedList<Object> params) throws SQLException{
		boolean bol = false;
		PreparedStatement stmt = conn.prepareStatement(sql);
		parseParams(stmt,params);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			bol = true;
			break;
		}
		
		return bol;
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
			}else if(typeName.equals("String")){
				stmt.setString(i+1, obj.toString());
			}else if(typeName.equals("Long")){
				stmt.setLong(i+1, (Long)obj);
			}else if(typeName.equals("Double")){
				stmt.setDouble(i+1, (Double)obj);
			}
		}
	}
	
	public static void main(String[] args) {
		MySQLDataSource ds = MySQLDataSource.getInstance();
		DvMysqlClient client = new DvMysqlClient(ds);
		try {
			client.execute("use test");
			client.select_test("select * from user", null);
			client.createDB("test1");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
