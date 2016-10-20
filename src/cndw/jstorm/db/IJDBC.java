package cndw.jstorm.db;

import java.util.List;
import java.util.Map;

/**
 * JDBC interface
 * @author mingzhou.chen
 * 28458942@qq.com
 * 2016年1月4日 上午10:49:01
 */
public interface IJDBC {
	/**
	 * 
	 * @param className
	 */
	public void loadDriverClass();
	
	/**
	 * 查询数据
	 * @param sql
	 * @param params
	 * @param clazz
	 * @return
	 */
	public <T extends Object> List<T> select(String sql,Map<String, Object> params,Class<T> clazz);
	/**
	 * 删除数据
	 * @param sql
	 * @param params
	 * @return
	 */
	public boolean delete(String sql,Map<String, Object> params);
	/**
	 * 更新数据
	 * @param sql
	 * @param params
	 * @return
	 */
	public boolean update(String sql,Map<String, Object> params);
}
