package cndw.jstorm.db;

import cndw.jstorm.util.PropertiesUtil;

/**
 * 数据库连接池
 * @author mingzhou.chen
 * 28458942@qq.com
 * 2016年1月4日 下午3:24:12
 */
public final class MySQLDataSource extends BaseDataSource {
	
	private static MySQLDataSource instance = new MySQLDataSource();
	
    public static MySQLDataSource getInstance(){
    	if(DS == null){
    		String host = "127.0.0.1:3306";//PropertiesUtil.getConf("comm").get("mysql");
    		String connectURI = "jdbc:mysql://"+host+"/mysql";
    		String username = "root";
    		String pswd = "cndw@root@#$fdgs";
    		String driverClass = "com.mysql.jdbc.Driver";
    		int initialSize = 50;
    		int maxtotal = 50;
    		int maxIdle = 30;
    		int maxWaitMillis = 1000;
    		int minIdle = 10;
    		initDS(connectURI, username, pswd, driverClass, initialSize, maxtotal, maxIdle, maxWaitMillis, minIdle);
    	}
    	return instance;
    }
    
    private MySQLDataSource(){
    	
    }

    
    
    public static void main(String[] args) {  
    	
    	MySQLDataSource db = MySQLDataSource.getInstance();
    	db.getConn();
//    	db.queryList("select * from user", null, null);
    	
    	
    	/*
        MySQLDataSource db = MySQLDataSource.getInstance();
        Connection conn = null;  
        Statement stmt = null;  
        ResultSet rs = null;  
        try {  
            conn = db.getConn();  
            stmt = conn.createStatement();  
            rs = stmt.executeQuery("select * from user limit 1 ");  
            System.out.println("Results:");  
            int numcols = rs.getMetaData().getColumnCount();  
            while (rs.next()) {  
                for (int i = 1; i <= numcols; i++) {  
                    System.out.print("\t" + rs.getString(i) + "\t");  
                }  
                System.out.println("");  
            }  
            System.out.println(getDataSourceStats());  
        } catch (SQLException e) {  
            e.printStackTrace();  
        } finally {  
            try {  
                if (rs != null)  
                    rs.close();  
                if (stmt != null)  
                    stmt.close();  
                if (conn != null)  
                    conn.close();  
                if (db != null)  
                    shutdownDataSource();  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
        */
    } 
}
