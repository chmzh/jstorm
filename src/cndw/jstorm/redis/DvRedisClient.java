package cndw.jstorm.redis;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.RedissonClient;
import org.redisson.core.RMap;

/**
 * Redis 客户端
 * @author chenmingzhou
 *
 */
public class DvRedisClient {
	private final RedissonClient client;
	static class RedisHolder{
		public final static DvRedisClient instance = new DvRedisClient();
		
	}
	
	private DvRedisClient() {
		Config config = new Config();  
		String redisHost = "10.104.93.208:6379";//PropertiesUtil.getConf("comm").get("redis");
		config.useSingleServer().setAddress(redisHost);
		client = Redisson.create(config);
        System.out.println("reids连接成功..."); 
	}
	
	public static DvRedisClient getInstance(){
		return RedisHolder.instance;
	}
	
	/**
	 * 插入数据
	 * @param gameId
	 * @param key
	 * @param value
	 */
	public void put2Map(String gameId,String key,Object value){
		client.getMap(gameId).put(key, value);
	}
	
	/**
	 * 获取数据
	 * @param gameId
	 * @return
	 */
	public RMap<String, Object> getMap(String gameId){
		return client.getMap(gameId);
	}
	
	/**
	 * 移除数据
	 * @param gameId
	 * @param key
	 */
	public void removeValue(String gameId,String key){
		RMap<String, Object> map = client.getMap(gameId);
		map.remove(map.get(key));
	}
}
