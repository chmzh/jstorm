package com.cndw.statistics;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 数据库连接池
 * @author mingzhou.chen
 * 28458942@qq.com
 * 2016年1月4日 下午4:03:40
 */
public class BaseDataSource {
	/** 数据源，static */  
    protected BasicDataSource DS = null;  
    private final static Logger LOG = LoggerFactory.getLogger(BaseDataSource.class);
    /** 从数据源获得一个连接 */  
    public Connection getConn() {  
        Connection con = null;  
        //LOG.info("=============数据库连接池信息"+DS.toString()+"，，，活动连接数"+DS.getNumActive()+"=======");
        if (DS != null) {  
            try {  
                con = DS.getConnection();  
                
            } catch (Exception e) {  
                e.printStackTrace(System.err);  
            }  
  
//            try {  
//                con.setAutoCommit(false);  
//            } catch (SQLException e) {  
//                e.printStackTrace();  
//            }  
            return con;  
        }else{
        	LOG.info("=============数据库连接池信息"+DS.toString()+"=======con=null");
        }
        return con;  
    }
    
    public void shutDown(){
    	try {
			DS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /** 
     * 指定所有参数连接数据源 
     *  
     * @param connectURI 
     *            数据库 
     * @param username 
     *            用户名 
     * @param pswd 
     *            密码 
     * @param driverClass 
     *            数据库连接驱动名 
     * @param initialSize 
     *            初始连接池连接个数 
     * @param maxtotal 
     *            最大活动连接数 
     * @param maxIdle 
     *            最大连接数 
     * @param maxWaitMillis 
     *            获得连接的最大等待毫秒数 
     * @param minIdle 
     *            最小连接数 
     * @return 
     */  
    protected void initDS(String connectURI, String username, String pswd,  
            String driverClass, int initialSize, int maxtotal, int maxIdle,  
            int maxWaitMillis , int minIdle) {  
        BasicDataSource ds = new BasicDataSource();  
        ds.setDriverClassName(driverClass);  
        ds.setUsername(username);  
        ds.setPassword(pswd);  
        ds.setUrl(connectURI);  
        ds.setInitialSize(initialSize); // 初始的连接数；  
        ds.setMaxActive(maxtotal);  
        ds.setMaxIdle(maxIdle);  
        ds.setMaxWait(maxWaitMillis);  
        ds.setMinIdle(minIdle);  
        
        ds.setValidationQuery("select id from health.test limit 1;");
        //ds.setValidationQueryTimeout(1);
        DS = ds;  
    } 
    
    
    /** 获得数据源连接状态 */  
    protected Map<String, Integer> getDataSourceStats() throws SQLException {  
        BasicDataSource bds = (BasicDataSource) DS;  
        Map<String, Integer> map = new HashMap<String, Integer>(2);  
        map.put("active_number", bds.getNumActive());  
        map.put("idle_number", bds.getNumIdle());  
        return map;  
    }  
  
    /** 关闭数据源 */  
    protected void shutdownDataSource() throws SQLException {  
        BasicDataSource bds = (BasicDataSource) DS;  
        bds.close(); 
    }
    /**
     * 从数据库请求数据
     * @param sql
     * @param params
     * @param clazz
     * @return
     */
//    public <T extends CResultSet> List<T> queryList(String sql,List<Object> params,Class<T> clazz){
//    	//TODO 
//    	try {
//			PreparedStatement pstmt = getConn().prepareStatement(sql);
//			setPreparedStatementParam(pstmt, params);
//			ResultSet rs = pstmt.executeQuery();
//			
//			while(rs.next()){
//				
//				//T obj = Class.forN
//				
//				int id = rs.getInt("id");
//				System.out.println("id="+id);
//			}
//			
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	return null;
//    }
    
    /**
     * 执行SQL语句
     * @param sql
     * @param params
     */
    public void execute(String sql,List<Object> params){
    	
    }
    
    private void setPreparedStatementParam(PreparedStatement pstmt, List<Object> paramList) throws Exception {  
        if(pstmt == null || paramList == null || paramList.isEmpty()) {  
            return;  
        }  
        DateFormat df = DateFormat.getDateTimeInstance();  
        for (int i = 0; i < paramList.size(); i++) {  
        	Object obj = paramList.get(i);
            if(obj instanceof Integer) {  
                int paramValue = ((Integer)obj).intValue();  
                pstmt.setInt(i+1, paramValue);  
            } else if(obj instanceof Float) {  
                float paramValue = ((Float)obj).floatValue();  
                pstmt.setFloat(i+1, paramValue);  
            } else if(obj instanceof Double) {  
                double paramValue = ((Double)obj).doubleValue();  
                pstmt.setDouble(i+1, paramValue);  
            } else if(obj instanceof Date) {  
                pstmt.setString(i+1, df.format((Date)obj));  
            } else if(obj instanceof Long) {  
                long paramValue = ((Long)obj).longValue();  
                pstmt.setLong(i+1, paramValue);  
            } else if(obj instanceof String) {  
                pstmt.setString(i+1, (String)obj);  
            }  
        }  
        return;  
    }  
}
